"""Signal generation logic for Polymarket Bot.

Contains three strategies:
1. UMA Oracle — monitors near-expiry markets with extreme prices (high certainty signals)
2. News Analysis — AI (Gemini/Ollama) analyzes RSS news vs active markets
3. Metaculus Consensus — compares Metaculus community prediction with Polymarket price

All strategies prioritise markets expiring soonest for maximum capital efficiency.
"""
import asyncio
import feedparser
import os
import re
import json as json_module
import requests
from web3 import Web3
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp

from config import config


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    """Represents a trading signal."""
    strategy: str
    market_id: str
    market_name: str
    current_price: float
    expected_outcome: str
    confidence: float        # 0-100
    expected_profit: float
    liquidity_volume_24h: float
    bid_ask_spread: float
    source_url: str
    polymarket_url: str
    timestamp: datetime
    details: Dict[str, Any]
    end_date: Optional[str] = None   # ISO string — used for soonest-expiry sort
    is_valid: bool = True


# ── Full Polymarket market fetcher (paginated — ALL markets) ──────────────────

async def fetch_all_polymarket_markets() -> List[Dict]:
    """
    Fetch ALL active Polymarket markets using pagination.
    Returns a list of normalised market dicts sorted by days_to_expiry ASC
    (soonest expiring first) with volume as secondary sort.
    """
    PAGE_SIZE = 500
    all_raw: List[Dict] = []
    offset = 0

    print("[API] Starting full Polymarket scan (paginated)...")
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=30)
    ) as session:
        while True:
            url = (
                f"https://gamma-api.polymarket.com/markets"
                f"?active=true&closed=false&limit={PAGE_SIZE}&offset={offset}"
            )
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        print(f"[API ERROR] status {resp.status} at offset {offset}")
                        break
                    data = await resp.json()
            except Exception as e:
                print(f"[API ERROR] fetch page at offset {offset}: {e}")
                break

            page = data if isinstance(data, list) else data.get("data", [])
            if not page:
                break

            all_raw.extend(page)
            print(f"[API]   fetched {len(all_raw)} markets so far...")

            if len(page) < PAGE_SIZE:
                break          # last page
            offset += PAGE_SIZE
            await asyncio.sleep(0.1)   # be polite to the API

    print(f"[API] Raw total from Gamma API: {len(all_raw)}")

    now = datetime.utcnow()
    markets: List[Dict] = []

    for m in all_raw:
        if m.get("closed") is True or m.get("archived") is True:
            continue

        # ── Parse end date ──────────────────────────────────────────────────
        end_date_str = (
            m.get("endDate") or m.get("end_date")
            or m.get("resolution_date") or None
        )
        days_to_expiry: Optional[float] = None
        if end_date_str:
            try:
                ed = str(end_date_str).replace("Z", "").replace("+00:00", "")
                end_dt = datetime.fromisoformat(ed)
                if end_dt < now:
                    continue        # already expired
                days_to_expiry = (end_dt - now).total_seconds() / 86400.0
            except Exception:
                pass

        # ── Parse prices ────────────────────────────────────────────────────
        best_bid = _parse_float(m.get("bestBid"))
        best_ask = _parse_float(m.get("bestAsk"))

        if best_bid <= 0:
            raw_prices = m.get("outcomePrices")
            if isinstance(raw_prices, str):
                try:
                    pl = json_module.loads(raw_prices)
                    if pl:
                        best_bid = float(pl[0])
                        if len(pl) > 1 and best_ask <= 0:
                            best_ask = float(pl[1])
                except Exception:
                    pass
            elif isinstance(raw_prices, list) and raw_prices:
                try:
                    best_bid = float(raw_prices[0])
                    if len(raw_prices) > 1 and best_ask <= 0:
                        best_ask = float(raw_prices[1])
                except Exception:
                    pass

        if best_bid <= 0 and best_ask <= 0:
            continue

        volume = _parse_float(m.get("volume24h") or m.get("volume") or 0)
        spread_pct = 0.0
        if best_ask > best_bid > 0:
            spread_pct = ((best_ask - best_bid) / best_bid) * 100

        market_id = (
            m.get("market_slug") or m.get("slug")
            or m.get("conditionId") or ""
        )
        if not market_id:
            continue

        markets.append({
            "id": market_id,
            "slug": market_id,
            "condition_id": m.get("conditionId", ""),
            "name": m.get("question") or m.get("title") or "",
            "description": m.get("description", ""),
            "category": m.get("category", "General"),
            "volume_24h": volume,
            "best_bid": best_bid if best_bid > 0 else best_ask,
            "best_ask": best_ask if best_ask > 0 else best_bid,
            "spread_pct": spread_pct,
            "end_date": end_date_str,
            "days_to_expiry": days_to_expiry,
            "uma_request_id": m.get("uma_request_id") or m.get("umaRequestId"),
        })

    # Primary sort: soonest expiry first; secondary: volume descending
    markets.sort(
        key=lambda x: (
            x["days_to_expiry"] if x["days_to_expiry"] is not None else 9999,
            -x["volume_24h"]
        )
    )
    print(f"[API] After filtering: {len(markets)} active tradeable markets")
    return markets


def _parse_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ── Liquidity filter ──────────────────────────────────────────────────────────

class LiquidityFilter:
    @staticmethod
    def check(market: Dict) -> Optional[Dict]:
        bid = market.get("best_bid", 0)
        ask = market.get("best_ask", 0)
        volume = market.get("volume_24h", 0)
        spread = market.get("spread_pct", 0)

        if bid <= 0 and ask <= 0:
            return None
        price = bid if bid > 0 else ask

        return {
            "volume_24h": volume,
            "best_bid": price,
            "best_ask": ask if ask > 0 else price,
            "spread_pct": spread,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: UMA Oracle
# Logic: markets expiring within 3 days with price ≥ 0.88 or ≤ 0.12 have
# near-certain outcomes that the UMA oracle is about to confirm.
# Winrate driver: extreme prices near expiry are almost never wrong.
# ═══════════════════════════════════════════════════════════════════════════════

class UMAOracleStrategy:
    """
    Finds markets expiring in ≤ 3 days with YES price ≥ 0.88 or ≤ 0.12.
    These are effectively resolved — UMA will confirm within hours.
    Expected winrate: ~90%+
    """

    EXPIRY_DAYS_MAX = 3          # Only look at near-term markets
    PRICE_HIGH_THRESHOLD = 0.88  # YES ≥ 88% → strong YES signal
    PRICE_LOW_THRESHOLD  = 0.12  # YES ≤ 12% → strong NO signal
    MIN_VOLUME = 500             # Lower bar — near-expiry markets have less daily vol

    def __init__(self):
        self.w3: Optional[Web3] = None
        self._connect_rpc()

    def _connect_rpc(self):
        for url in config.POLYGON_RPC_URLS:
            try:
                w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10}))
                if w3.is_connected():
                    self.w3 = w3
                    print(f"[✅ UMA] Connected to Polygon RPC: {url}")
                    return
            except Exception as e:
                print(f"[⚠️ UMA] RPC {url} failed: {e}")
        print("[⚠️ UMA] No RPC — UMA on-chain feature disabled (signals still work)")

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🔮 UMA] Starting scan...")

        try:
            all_markets = await fetch_all_polymarket_markets()
            if not all_markets:
                print("[🔮 UMA] No markets available")
                return signals

            # Filter: expiring within EXPIRY_DAYS_MAX days
            near_expiry = [
                m for m in all_markets
                if m.get("days_to_expiry") is not None
                and 0 < m["days_to_expiry"] <= self.EXPIRY_DAYS_MAX
            ]
            print(f"[🔮 UMA] Markets expiring in ≤{self.EXPIRY_DAYS_MAX} days: {len(near_expiry)}")

            # Soonest first (already sorted by fetch_all)
            for market in near_expiry:
                liq = LiquidityFilter.check(market)
                if not liq:
                    continue

                volume = liq["volume_24h"]
                price  = liq["best_bid"]
                days   = market["days_to_expiry"]

                if volume < self.MIN_VOLUME:
                    continue
                if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                    continue

                name = market["name"]
                print(f"[🔮 UMA] {name[:55]} | price={price:.2%} | days={days:.1f} | vol=${volume:,.0f}")

                if price >= self.PRICE_HIGH_THRESHOLD:
                    expected_outcome = "YES"
                    # Higher confidence for prices closer to 1
                    raw_conf = 75 + (price - self.PRICE_HIGH_THRESHOLD) / (1 - self.PRICE_HIGH_THRESHOLD) * 20
                    confidence = min(raw_conf, 95)
                    expected_profit = (1.0 - price) * 100

                elif price <= self.PRICE_LOW_THRESHOLD:
                    expected_outcome = "NO"
                    raw_conf = 75 + (self.PRICE_LOW_THRESHOLD - price) / self.PRICE_LOW_THRESHOLD * 20
                    confidence = min(raw_conf, 95)
                    expected_profit = price * 100   # buying NO at (1-price)

                else:
                    continue  # price in uncertain middle zone

                # Extra confidence boost for very soon expiry
                if days <= 1:
                    confidence = min(confidence + 5, 97)

                if expected_profit < config.MIN_PROFIT_PERCENT:
                    continue

                # Safety: skip if confidence is not meaningfully above 75%
                # (low confidence on extreme price = false signal risk)
                if confidence < 78:
                    continue

                sig = Signal(
                    strategy="🔮 UMA Оракул",
                    market_id=market["id"],
                    market_name=name,
                    current_price=price,
                    expected_outcome=expected_outcome,
                    confidence=round(confidence, 1),
                    expected_profit=round(expected_profit, 2),
                    liquidity_volume_24h=volume,
                    bid_ask_spread=liq["spread_pct"],
                    source_url="",
                    polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                    timestamp=datetime.now(),
                    end_date=market.get("end_date"),
                    details={
                        "days_to_expiry": round(days, 2),
                        "price_signal": f"Near-expiry {expected_outcome} @ {price:.2%}",
                        "category": market.get("category", "General"),
                        "on_chain_rpc": self.w3 is not None,
                    }
                )
                signals.append(sig)
                print(f"[🔮 UMA]   ✅ SIGNAL | outcome={expected_outcome} | conf={confidence:.1f}%")

        except Exception as e:
            print(f"[🔮 UMA ERROR] {e}")
            import traceback; traceback.print_exc()

        # Sort: soonest expiry → highest confidence
        signals.sort(key=lambda s: (
            _days_sort_key(s.details.get("days_to_expiry")),
            -s.confidence
        ))
        print(f"[🔮 UMA] Done. {len(signals)} signals\n")
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: News Analysis via Gemini API (or Ollama fallback)
# Logic: scrape RSS headlines, ask AI if they confirm a Polymarket market
# outcome, filter by high confidence only.
# ═══════════════════════════════════════════════════════════════════════════════

class NewsStrategy:
    """
    AI-powered news analysis.
    Primary: Google Gemini Flash (free tier via REST — no SDK needed)
    Fallback: local Ollama model
    """

    RSS_FEEDS = [
        "https://feeds.reuters.com/reuters/businessnews",
        "https://news.google.com/rss/search?q=prediction+market+polymarket",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://news.google.com/rss/search?q=election+results+2025",
        "https://news.google.com/rss/search?q=crypto+bitcoin+2025",
        "https://news.google.com/rss/search?q=sports+championship+winner",
        "https://news.google.com/rss/search?q=geopolitics+war+ceasefire",
    ]

    GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
    OLLAMA_MODELS   = ["llama3.2", "qwen2.5", "mistral", "phi3"]
    OLLAMA_HOST     = os.getenv("OLLAMA_HOST", "http://localhost:11434")

    MIN_NEWS_CONFIDENCE = 72   # Only high-confidence AI calls pass

    def __init__(self):
        self.gemini_key   = os.getenv("GEMINI_API_KEY", "")
        self.ollama_model: Optional[str] = None
        self._init_ollama()

    def _init_ollama(self):
        try:
            resp = requests.get(f"{self.OLLAMA_HOST}/api/tags", timeout=5)
            if resp.status_code != 200:
                return
            models_data = resp.json().get("models", [])
            available   = [m["name"] for m in models_data]
            for pref in self.OLLAMA_MODELS:
                for full in available:
                    if pref in full:
                        self.ollama_model = full
                        print(f"[📰 NEWS] Ollama model: {full}")
                        return
            if available:
                self.ollama_model = available[0]
                print(f"[📰 NEWS] Ollama fallback model: {self.ollama_model}")
        except Exception:
            pass

    @property
    def has_ai(self) -> bool:
        return bool(self.gemini_key) or bool(self.ollama_model)

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[📰 NEWS] Starting news analysis...")

        if not self.has_ai:
            print("[📰 NEWS] No AI backend available (set GEMINI_API_KEY or start Ollama)")
            return signals

        headlines = await self._fetch_headlines()
        print(f"[📰 NEWS] Fetched {len(headlines)} headlines")
        if not headlines:
            return signals

        all_markets = await fetch_all_polymarket_markets()
        if not all_markets:
            return signals

        # Pre-filter for liquidity — check soonest expiry first
        tradeable: List[Dict] = []
        for m in all_markets:
            liq = LiquidityFilter.check(m)
            if not liq:
                continue
            if liq["volume_24h"] < config.MIN_VOLUME_24H:
                continue
            if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                continue
            m["_liq"] = liq
            tradeable.append(m)

        print(f"[📰 NEWS] Tradeable markets: {len(tradeable)} — analysing top 40 (soonest expiry)")

        # Analyse top-40 soonest-expiry tradeable markets
        for i, market in enumerate(tradeable[:40]):
            print(f"[📰 NEWS] [{i+1}/40] {market['name'][:55]}")
            liq   = market["_liq"]
            price = liq["best_bid"]

            analysis = await self._analyze(market, headlines)
            if not analysis:
                continue

            conf = analysis["confidence"]
            if conf < self.MIN_NEWS_CONFIDENCE:
                print(f"[📰 NEWS]   ❌ confidence too low ({conf:.0f}%)")
                continue

            outcome = analysis["expected_outcome"]
            if outcome == "YES":
                exp_profit = (1.0 - price) * 100
            else:
                exp_profit = price * 100

            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue

            sig = Signal(
                strategy="📰 Новостной Анализ",
                market_id=market["id"],
                market_name=market["name"],
                current_price=price,
                expected_outcome=outcome,
                confidence=min(conf, 93),
                expected_profit=round(exp_profit, 2),
                liquidity_volume_24h=liq["volume_24h"],
                bid_ask_spread=liq["spread_pct"],
                source_url=analysis.get("source_url", ""),
                polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                timestamp=datetime.now(),
                end_date=market.get("end_date"),
                details={
                    "relevant_headlines": analysis.get("relevant_headlines", []),
                    "analysis_summary":   analysis.get("summary", ""),
                }
            )
            signals.append(sig)
            print(f"[📰 NEWS]   ✅ SIGNAL | {outcome} | conf={conf:.0f}%")

            if len(signals) >= 8:
                break

        signals.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))
        print(f"[📰 NEWS] Done. {len(signals)} signals\n")
        return signals

    async def _fetch_headlines(self) -> List[Dict]:
        headlines: List[Dict] = []
        for url in self.RSS_FEEDS:
            try:
                feed = await asyncio.to_thread(feedparser.parse, url)
                for entry in feed.entries[:10]:
                    title = entry.get("title", "").strip()
                    if title:
                        headlines.append({
                            "title":     title,
                            "summary":   entry.get("summary", "")[:300],
                            "link":      entry.get("link", ""),
                            "source":    feed.feed.get("title", "Unknown"),
                        })
            except Exception as e:
                print(f"[📰 NEWS] Feed error {url}: {e}")
        return headlines

    async def _analyze(self, market: Dict, headlines: List[Dict]) -> Optional[Dict]:
        """Call AI (Gemini first, then Ollama) to analyse headline relevance."""
        price = market.get("_liq", {}).get("best_bid", 0.5)
        question = market["name"]

        hl_text = "\n".join(
            f"{i+1}. [{h['source']}] {h['title']}"
            for i, h in enumerate(headlines[:25])
        )

        prompt = f"""You are a professional prediction market analyst.

MARKET QUESTION: {question}
CURRENT MARKET PRICE (probability YES): {price:.2%}

RECENT NEWS HEADLINES:
{hl_text}

Tasks:
1. Is any headline directly and clearly relevant to this question? (YES/NO)
2. If YES: what is the most likely resolution? (YES or NO)
3. Your confidence in this prediction (50-100)?
4. Which headline numbers? (comma-separated)
5. One-sentence explanation?

Respond EXACTLY in this format (no extra text):
RELEVANT: YES or NO
OUTCOME: YES or NO
CONFIDENCE: number
HEADLINES: numbers
SUMMARY: explanation"""

        # Try Gemini first
        if self.gemini_key:
            result = await self._call_gemini(prompt)
            if result:
                return self._parse_analysis(result, headlines)

        # Fallback: Ollama
        if self.ollama_model:
            result = await self._call_ollama(prompt)
            if result:
                return self._parse_analysis(result, headlines)

        return None

    async def _call_gemini(self, prompt: str) -> Optional[str]:
        try:
            url  = f"{self.GEMINI_ENDPOINT}?key={self.gemini_key}"
            body = {"contents": [{"parts": [{"text": prompt}]}]}
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            ) as session:
                async with session.post(url, json=body) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"[📰 NEWS] Gemini error {resp.status}: {text[:200]}")
                        return None
                    data = await resp.json()
                    return (
                        data.get("candidates", [{}])[0]
                           .get("content", {})
                           .get("parts", [{}])[0]
                           .get("text", "")
                    )
        except Exception as e:
            print(f"[📰 NEWS] Gemini call error: {e}")
            return None

    async def _call_ollama(self, prompt: str) -> Optional[str]:
        try:
            payload = {
                "model":  self.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 200},
            }
            resp = await asyncio.to_thread(
                requests.post,
                f"{self.OLLAMA_HOST}/api/generate",
                json=payload,
                timeout=90,
            )
            if resp.status_code != 200:
                return None
            await asyncio.sleep(1.5)
            return resp.json().get("response", "")
        except Exception as e:
            print(f"[📰 NEWS] Ollama error: {e}")
            return None

    @staticmethod
    def _parse_analysis(text: str, headlines: List[Dict]) -> Optional[Dict]:
        lines = {}
        for line in text.split("\n"):
            if ":" in line:
                k, _, v = line.partition(":")
                lines[k.strip().upper()] = v.strip()

        if lines.get("RELEVANT", "NO").upper() != "YES":
            return None

        outcome = lines.get("OUTCOME", "YES").upper()
        if outcome not in ("YES", "NO"):
            outcome = "YES"

        try:
            confidence = float(re.findall(r"[\d.]+", lines.get("CONFIDENCE", "0"))[0])
            confidence = max(0, min(100, confidence))
        except Exception:
            confidence = 0

        # Gather relevant headline links
        hl_nums: List[int] = []
        try:
            hl_nums = [
                int(x.strip()) - 1
                for x in lines.get("HEADLINES", "").split(",")
                if x.strip().isdigit()
            ]
        except Exception:
            pass

        source_url    = ""
        relevant_titles: List[str] = []
        for idx in hl_nums:
            if 0 <= idx < len(headlines):
                relevant_titles.append(headlines[idx]["title"])
                if not source_url:
                    source_url = headlines[idx]["link"]

        return {
            "is_relevant":       True,
            "expected_outcome":  outcome,
            "confidence":        confidence,
            "relevant_headlines": relevant_titles,
            "summary":           lines.get("SUMMARY", ""),
            "source_url":        source_url,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: Metaculus Real API + Polymarket Price Divergence
# Logic: query Metaculus v4 for questions with community predictions,
# find Polymarket markets that match, compare probabilities.
# Winrate driver: Metaculus superforecasters beat market 60%+ of the time.
# ═══════════════════════════════════════════════════════════════════════════════

class MetaculusStrategy:
    """
    Fetches real Metaculus questions via the v4 API.
    Matches them to Polymarket markets by keyword similarity.
    Large probability divergence (>12%) = trading opportunity.
    Fallback: if Metaculus API unavailable, uses Gemini/Ollama expert estimate.
    """

    METACULUS_API_BASE = "https://www.metaculus.com/api2"
    MIN_DIVERGENCE     = 0.10    # 10% gap between Metaculus & Polymarket
    MIN_FORECASTERS    = 3       # Minimum forecasters for a valid signal

    # Gemini / Ollama same as NewsStrategy
    OLLAMA_MODELS = ["llama3.2", "qwen2.5", "mistral", "phi3"]
    OLLAMA_HOST   = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

    def __init__(self):
        self.gemini_key   = os.getenv("GEMINI_API_KEY", "")
        self.metaculus_key = config.METACULUS_API_KEY
        self.ollama_model: Optional[str] = None
        self._init_ollama()

    def _init_ollama(self):
        try:
            resp = requests.get(f"{self.OLLAMA_HOST}/api/tags", timeout=5)
            if resp.status_code != 200:
                return
            models_data = resp.json().get("models", [])
            available   = [m["name"] for m in models_data]
            for pref in self.OLLAMA_MODELS:
                for full in available:
                    if pref in full:
                        self.ollama_model = full
                        print(f"[🎯 META] Ollama model: {full}")
                        return
            if available:
                self.ollama_model = available[0]
        except Exception:
            pass

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🎯 META] Starting Metaculus consensus analysis...")

        try:
            all_markets = await fetch_all_polymarket_markets()
            if not all_markets:
                return signals

            # Pre-filter tradeable markets (soonest expiry first)
            tradeable: List[Dict] = []
            for m in all_markets:
                liq = LiquidityFilter.check(m)
                if not liq:
                    continue
                if liq["volume_24h"] < config.MIN_VOLUME_24H:
                    continue
                if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                    continue
                # Skip markets priced near 0 or 1 — already efficient
                # Also skip extreme prices where risk/reward is hard to judge
                price = liq["best_bid"]
                if price < 0.10 or price > 0.90:
                    continue
                m["_liq"] = liq
                tradeable.append(m)

            print(f"[🎯 META] Tradeable (price 8-92%): {len(tradeable)}")

            # Try real Metaculus API first
            metaculus_qs = await self._fetch_metaculus_questions()
            print(f"[🎯 META] Metaculus questions fetched: {len(metaculus_qs)}")

            if metaculus_qs:
                signals = await self._match_with_metaculus(tradeable, metaculus_qs)

            # AI fallback: если Metaculus не дал достаточно сигналов — используем AI эксперта
            if len(signals) < 3 and (self.gemini_key or self.ollama_model):
                print(f"[🎯 META] Only {len(signals)} signals from Metaculus, trying AI expert fallback...")
                ai_signals = await self._ai_expert_signals(tradeable, signals)
                signals.extend(ai_signals)
                print(f"[🎯 META] AI expert added {len(ai_signals)} signals")

        except Exception as e:
            print(f"[🎯 META ERROR] {e}")
            import traceback; traceback.print_exc()

        # Sort soonest expiry first
        signals.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))
        print(f"[🎯 META] Done. {len(signals)} signals\n")
        return signals

    async def _fetch_metaculus_questions(self) -> List[Dict]:
        """Fetch open binary questions from Metaculus with community predictions.
        Tries both v4 (new) and v2 (legacy) API formats."""
        questions: List[Dict] = []
        headers = {"Accept": "application/json"}
        if self.metaculus_key:
            headers["Authorization"] = f"Token {self.metaculus_key}"

        # Try Metaculus v4 API (posts/questions endpoint)
        v4_params = {
            "statuses": "open",
            "forecast_type": "binary",
            "limit": 200,
            "order_by": "-activity",
            "has_community_prediction": "true",
        }
        # Also try v2 legacy params
        v2_params = {
            "status": "open",
            "type": "forecast",
            "limit": 200,
            "order_by": "-activity",
        }

        for api_url, params in [
            ("https://www.metaculus.com/api/posts/", v4_params),
            (f"{self.METACULUS_API_BASE}/questions/", v2_params),
        ]:
            if questions:
                break
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=25),
                    headers=headers,
                ) as session:
                    async with session.get(api_url, params=params) as resp:
                        if resp.status not in (200, 201):
                            print(f"[🎯 META] {api_url} → status {resp.status}")
                            continue
                        data = await resp.json()
                        results = data.get("results", data if isinstance(data, list) else [])
                        print(f"[🎯 META] API returned {len(results)} results from {api_url}")

                        for q in results:
                            # Handle v4 structure: question nested inside post
                            question_data = q.get("question", q)
                            title = question_data.get("title") or q.get("title", "")
                            if not title:
                                continue

                            q_type = question_data.get("type") or q.get("type", "")
                            if q_type not in ("binary", "forecast", ""):
                                continue

                            n_forecasts = (
                                question_data.get("nr_forecasters") or
                                question_data.get("number_of_forecasters") or
                                q.get("nr_forecasters") or
                                q.get("number_of_forecasters") or 0
                            )

                            # Try every known field for community probability
                            pred = None
                            # v4 format
                            agg = question_data.get("aggregations", {})
                            if agg:
                                recency = agg.get("recency_weighted", {}) or {}
                                latest = recency.get("history", [{}])
                                if latest:
                                    pred = latest[-1].get("means", [None])[0] if latest[-1].get("means") else None
                                if pred is None:
                                    pred = recency.get("latest", {}).get("means", [None])[0] if recency.get("latest") else None
                            # v2 format
                            if pred is None:
                                cp = question_data.get("community_prediction") or q.get("community_prediction") or {}
                                if isinstance(cp, dict):
                                    pred = (cp.get("full", {}) or {}).get("q2") or cp.get("q2")
                                elif isinstance(cp, (int, float)):
                                    pred = cp
                            # metaculus_prediction fallback
                            if pred is None:
                                mp = question_data.get("metaculus_prediction") or q.get("metaculus_prediction") or {}
                                if isinstance(mp, dict):
                                    pred = (mp.get("full", {}) or {}).get("q2") or mp.get("q2")
                            # direct probability field
                            if pred is None:
                                pred = question_data.get("probability") or q.get("probability")

                            if pred is None:
                                continue
                            try:
                                pred_float = float(pred)
                                if pred_float > 1:
                                    pred_float /= 100.0
                                pred_float = max(0.0, min(1.0, pred_float))
                            except (TypeError, ValueError):
                                continue

                            qid = question_data.get("id") or q.get("id")
                            questions.append({
                                "id":          qid,
                                "title":       title,
                                "probability": pred_float,
                                "forecasters": int(n_forecasts),
                                "url":         f"https://www.metaculus.com/questions/{qid}/",
                            })

            except Exception as e:
                print(f"[🎯 META] API error ({api_url}): {e}")

        print(f"[🎯 META] Parsed {len(questions)} questions with predictions")
        return questions

    async def _match_with_metaculus(
        self, markets: List[Dict], questions: List[Dict]
    ) -> List[Signal]:
        """Match Metaculus questions to Polymarket markets by keyword overlap."""
        signals: List[Signal] = []

        for q in questions:
            q_words = set(_tokenise(q["title"]))
            meta_prob = q["probability"]

            best_market: Optional[Dict] = None
            best_overlap = 0

            for m in markets:
                m_words = set(_tokenise(m["name"]))
                overlap = len(q_words & m_words) / max(len(q_words | m_words), 1)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_market = m

            if best_market is None or best_overlap < 0.15:
                continue   # No good match

            liq  = best_market["_liq"]
            poly_prob = liq["best_bid"]
            divergence = abs(poly_prob - meta_prob)

            print(f"[🎯 META] Match ({best_overlap:.0%}): {best_market['name'][:45]}")
            print(f"[🎯 META]   Meta={meta_prob:.2%}, Poly={poly_prob:.2%}, div={divergence:.2%}")

            if divergence < self.MIN_DIVERGENCE:
                continue

            outcome = "YES" if meta_prob > poly_prob else "NO"
            confidence = 60 + divergence * 200
            confidence = min(confidence, 90)

            exp_profit = divergence * 100
            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue

            sig = Signal(
                strategy="🎯 Metaculus Консенсус",
                market_id=best_market["id"],
                market_name=best_market["name"],
                current_price=poly_prob,
                expected_outcome=outcome,
                confidence=round(confidence, 1),
                expected_profit=round(exp_profit, 2),
                liquidity_volume_24h=liq["volume_24h"],
                bid_ask_spread=liq["spread_pct"],
                source_url=q["url"],
                polymarket_url=f"https://polymarket.com/event/{best_market['slug']}",
                timestamp=datetime.now(),
                end_date=best_market.get("end_date"),
                details={
                    "metaculus_probability": meta_prob,
                    "polymarket_probability": poly_prob,
                    "divergence": divergence,
                    "forecasters": q["forecasters"],
                    "match_overlap": best_overlap,
                }
            )
            signals.append(sig)
            print(f"[🎯 META]   ✅ SIGNAL | {outcome} | conf={confidence:.1f}%")

            if len(signals) >= 6:
                break

        return signals

    async def _ai_expert_signals(
        self, markets: List[Dict], already_found: List[Signal]
    ) -> List[Signal]:
        """Use AI as an independent expert when Metaculus match count is low."""
        signals: List[Signal] = []
        found_ids = {s.market_id for s in already_found}

        for market in markets[:25]:
            if market["id"] in found_ids:
                continue

            liq = market["_liq"]
            poly_prob = liq["best_bid"]

            expert_prob = await self._ai_estimate_probability(market)
            if expert_prob is None:
                continue

            divergence = abs(poly_prob - expert_prob)
            print(f"[🎯 META/AI] {market['name'][:45]}")
            print(f"[🎯 META/AI]   AI={expert_prob:.2%}, Poly={poly_prob:.2%}, div={divergence:.2%}")

            if divergence < 0.15:
                continue

            outcome = "YES" if expert_prob > poly_prob else "NO"
            confidence = 55 + divergence * 180
            confidence = min(confidence, 87)

            exp_profit = divergence * 100
            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue

            sig = Signal(
                strategy="🎯 Metaculus Консенсус",
                market_id=market["id"],
                market_name=market["name"],
                current_price=poly_prob,
                expected_outcome=outcome,
                confidence=round(confidence, 1),
                expected_profit=round(exp_profit, 2),
                liquidity_volume_24h=liq["volume_24h"],
                bid_ask_spread=liq["spread_pct"],
                source_url="",
                polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                timestamp=datetime.now(),
                end_date=market.get("end_date"),
                details={
                    "metaculus_probability": expert_prob,
                    "polymarket_probability": poly_prob,
                    "divergence": divergence,
                    "forecasters": 0,
                    "source": "AI Expert Estimate",
                }
            )
            signals.append(sig)
            found_ids.add(market["id"])
            print(f"[🎯 META/AI]   ✅ SIGNAL | {outcome} | conf={confidence:.1f}%")

            if len(signals) >= 4:
                break

        return signals

    async def _ai_estimate_probability(self, market: Dict) -> Optional[float]:
        prompt = f"""You are a superforecaster. Estimate the probability of this event resolving YES.

QUESTION: {market['name']}
CURRENT POLYMARKET PRICE: {market.get('_liq', {}).get('best_bid', 0.5):.1%}

Reply with ONLY a number between 0 and 100. No other text."""

        # Try Gemini
        if self.gemini_key:
            try:
                url  = f"{self.GEMINI_ENDPOINT}?key={self.gemini_key}"
                body = {"contents": [{"parts": [{"text": prompt}]}]}
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as session:
                    async with session.post(url, json=body) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            text = (
                                data.get("candidates", [{}])[0]
                                   .get("content", {})
                                   .get("parts", [{}])[0]
                                   .get("text", "")
                            )
                            nums = re.findall(r"[\d.]+", text)
                            if nums:
                                p = float(nums[0])
                                return max(0.0, min(1.0, p / 100.0 if p > 1 else p))
            except Exception as e:
                print(f"[🎯 META/AI] Gemini error: {e}")

        # Fallback: Ollama
        if self.ollama_model:
            try:
                payload = {
                    "model":  self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 20},
                }
                resp = await asyncio.to_thread(
                    requests.post,
                    f"{self.OLLAMA_HOST}/api/generate",
                    json=payload,
                    timeout=60,
                )
                if resp.status_code == 200:
                    text = resp.json().get("response", "")
                    nums = re.findall(r"[\d.]+", text)
                    if nums:
                        p = float(nums[0])
                        return max(0.0, min(1.0, p / 100.0 if p > 1 else p))
                await asyncio.sleep(1.5)
            except Exception as e:
                print(f"[🎯 META/AI] Ollama error: {e}")

        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Main Signal Generator
# ═══════════════════════════════════════════════════════════════════════════════

class SignalGenerator:
    """Combines all three strategies and returns deduplicated, sorted signals."""

    def __init__(self):
        self.uma_strategy       = UMAOracleStrategy()
        self.news_strategy      = NewsStrategy()
        self.metaculus_strategy = MetaculusStrategy()

    async def generate_all_signals(self) -> List[Signal]:
        print("\n" + "=" * 60)
        print("[🚀 SIGNAL GENERATOR] Full Polymarket scan + all strategies")
        print(f"[🚀] Filters: MinVol=${config.MIN_VOLUME_24H:,.0f} | "
              f"MaxSpread={config.MAX_SPREAD_PERCENT:.0f}% | "
              f"MinProfit={config.MIN_PROFIT_PERCENT:.1f}%")
        print("=" * 60)

        uma_res, news_res, meta_res = await asyncio.gather(
            self.uma_strategy.get_signals(),
            self.news_strategy.get_signals(),
            self.metaculus_strategy.get_signals(),
            return_exceptions=True,
        )

        all_signals: List[Signal] = []
        for result, name in [
            (uma_res,  "UMA Oracle"),
            (news_res, "News Analysis"),
            (meta_res, "Metaculus"),
        ]:
            if isinstance(result, Exception):
                print(f"[🚀] {name} FAILED: {result}")
            elif isinstance(result, list):
                all_signals.extend(result)
                print(f"[🚀] {name}: {len(result)} signals")

        # Deduplicate by market_id + strategy
        seen:    set          = set()
        unique: List[Signal] = []
        for sig in all_signals:
            key = f"{sig.market_id}:{sig.strategy}"
            if key not in seen:
                seen.add(key)
                unique.append(sig)

        # Global sort: soonest expiry → confidence
        unique.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))

        print(f"\n[🚀] TOTAL UNIQUE SIGNALS: {len(unique)}")
        for i, s in enumerate(unique[:5], 1):
            print(f"  {i}. {s.strategy} | {s.market_name[:40]} | "
                  f"conf={s.confidence:.1f}% | profit={s.expected_profit:.1f}%")
        print("=" * 60 + "\n")
        return unique


# ── Helpers ───────────────────────────────────────────────────────────────────

def _days_sort_key(days) -> float:
    if days is None:
        return 9999.0
    try:
        return float(days)
    except Exception:
        return 9999.0


def _days_sort_key_str(end_date_str: Optional[str]) -> float:
    if not end_date_str:
        return 9999.0
    try:
        ed = str(end_date_str).replace("Z", "").replace("+00:00", "")
        dt = datetime.fromisoformat(ed)
        return max((dt - datetime.utcnow()).total_seconds() / 86400.0, 0.0)
    except Exception:
        return 9999.0


def _tokenise(text: str) -> List[str]:
    """Lowercase word tokens for keyword matching."""
    stop = {
        "the", "a", "an", "of", "in", "on", "at", "to", "by", "is", "will",
        "be", "for", "or", "and", "with", "that", "this", "it", "from",
        "are", "has", "have", "was", "were", "not", "no", "do", "does",
    }
    return [
        w for w in re.findall(r"[a-z0-9]+", text.lower())
        if w not in stop and len(w) > 2
    ]