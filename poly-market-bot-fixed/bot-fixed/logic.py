"""Signal generation logic for Polymarket Bot — MEMORY OPTIMIZED.

Key changes vs original:
- Single shared market cache (5 min TTL) — avoids 3x fetching
- Max 500 markets fetched (not paginated to thousands)
- News: 15 markets scanned instead of 40
- Meta AI fallback: 10 markets instead of 25
- web3 removed entirely (saves ~50MB RAM)
- Metaculus: 100 questions instead of 200
"""
import asyncio
import feedparser
import os
import re
import json as json_module
import requests
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
import aiohttp

from config import config


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    strategy: str
    market_id: str
    market_name: str
    current_price: float
    expected_outcome: str
    confidence: float
    expected_profit: float
    liquidity_volume_24h: float
    bid_ask_spread: float
    source_url: str
    polymarket_url: str
    timestamp: datetime
    details: Dict[str, Any]
    end_date: Optional[str] = None
    is_valid: bool = True


# ── Shared market cache ───────────────────────────────────────────────────────

_market_cache: Optional[List[Dict]] = None
_market_cache_time: Optional[datetime] = None
_CACHE_TTL_SECONDS = 300  # 5 minutes


async def fetch_all_polymarket_markets() -> List[Dict]:
    """Fetch markets with 5-min cache. Max 500 markets to control RAM."""
    global _market_cache, _market_cache_time

    if (
        _market_cache is not None
        and _market_cache_time is not None
        and (datetime.utcnow() - _market_cache_time).total_seconds() < _CACHE_TTL_SECONDS
    ):
        print(f"[API] Using cached markets ({len(_market_cache)} total)")
        return _market_cache

    print("[API] Fetching Polymarket markets (max 500)...")
    all_raw: List[Dict] = []

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=500&offset=0"
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[API ERROR] status {resp.status}")
                    return _market_cache or []
                data = await resp.json()
                page = data if isinstance(data, list) else data.get("data", [])
                all_raw.extend(page)
        except Exception as e:
            print(f"[API ERROR] {e}")
            return _market_cache or []

    now = datetime.utcnow()
    markets: List[Dict] = []

    for m in all_raw:
        if m.get("closed") is True or m.get("archived") is True:
            continue

        end_date_str = m.get("endDate") or m.get("end_date") or m.get("resolution_date") or None
        days_to_expiry: Optional[float] = None
        if end_date_str:
            try:
                ed = str(end_date_str).replace("Z", "").replace("+00:00", "")
                end_dt = datetime.fromisoformat(ed)
                if end_dt < now:
                    continue
                days_to_expiry = (end_dt - now).total_seconds() / 86400.0
            except Exception:
                pass

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

        market_id = m.get("market_slug") or m.get("slug") or m.get("conditionId") or ""
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

    markets.sort(key=lambda x: (
        x["days_to_expiry"] if x["days_to_expiry"] is not None else 9999,
        -x["volume_24h"]
    ))

    _market_cache = markets
    _market_cache_time = datetime.utcnow()
    print(f"[API] Cached {len(markets)} active markets")
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
# STRATEGY 1: UMA Oracle (no web3 — saves ~50MB RAM)
# ═══════════════════════════════════════════════════════════════════════════════

class UMAOracleStrategy:
    EXPIRY_DAYS_MAX = 3
    PRICE_HIGH_THRESHOLD = 0.88
    PRICE_LOW_THRESHOLD  = 0.12
    MIN_VOLUME = 500

    def __init__(self):
        print("[✅ UMA] Price-oracle mode (web3 disabled to save RAM)")

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🔮 UMA] Starting scan...")

        try:
            all_markets = await fetch_all_polymarket_markets()
            if not all_markets:
                return signals

            near_expiry = [
                m for m in all_markets
                if m.get("days_to_expiry") is not None
                and 0 < m["days_to_expiry"] <= self.EXPIRY_DAYS_MAX
            ]
            print(f"[🔮 UMA] Near-expiry markets: {len(near_expiry)}")

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

                if price >= self.PRICE_HIGH_THRESHOLD:
                    expected_outcome = "YES"
                    raw_conf = 75 + (price - self.PRICE_HIGH_THRESHOLD) / (1 - self.PRICE_HIGH_THRESHOLD) * 20
                    confidence = min(raw_conf, 95)
                    expected_profit = (1.0 - price) * 100
                elif price <= self.PRICE_LOW_THRESHOLD:
                    expected_outcome = "NO"
                    raw_conf = 75 + (self.PRICE_LOW_THRESHOLD - price) / self.PRICE_LOW_THRESHOLD * 20
                    confidence = min(raw_conf, 95)
                    expected_profit = price * 100
                else:
                    continue

                if days <= 1:
                    confidence = min(confidence + 5, 97)
                if expected_profit < config.MIN_PROFIT_PERCENT:
                    continue
                if confidence < 78:
                    continue

                signals.append(Signal(
                    strategy="🔮 UMA Оракул",
                    market_id=market["id"],
                    market_name=market["name"],
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
                        "on_chain_rpc": False,
                    }
                ))

        except Exception as e:
            print(f"[🔮 UMA ERROR] {e}")

        signals.sort(key=lambda s: (
            _days_sort_key(s.details.get("days_to_expiry")), -s.confidence
        ))
        print(f"[🔮 UMA] Done. {len(signals)} signals\n")
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: News Analysis (max 15 markets instead of 40)
# ═══════════════════════════════════════════════════════════════════════════════

class NewsStrategy:
    RSS_FEEDS = [
        "https://feeds.reuters.com/reuters/businessnews",
        "https://news.google.com/rss/search?q=prediction+market+polymarket",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://news.google.com/rss/search?q=election+results+2025",
        "https://news.google.com/rss/search?q=crypto+bitcoin+2025",
    ]
    GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
    OLLAMA_MODELS   = ["llama3.2", "qwen2.5", "mistral", "phi3"]
    OLLAMA_HOST     = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    MIN_NEWS_CONFIDENCE = 72
    MAX_MARKETS_TO_SCAN = 15  # was 40

    def __init__(self):
        self.gemini_key   = os.getenv("GEMINI_API_KEY", "")
        self.ollama_model: Optional[str] = None
        if os.getenv("OLLAMA_HOST"):
            self._init_ollama()

    def _init_ollama(self):
        try:
            resp = requests.get(f"{self.OLLAMA_HOST}/api/tags", timeout=5)
            if resp.status_code != 200:
                return
            models_data = resp.json().get("models", [])
            available = [m["name"] for m in models_data]
            for pref in self.OLLAMA_MODELS:
                for full in available:
                    if pref in full:
                        self.ollama_model = full
                        return
        except Exception:
            pass

    @property
    def has_ai(self) -> bool:
        return bool(self.gemini_key) or bool(self.ollama_model)

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[📰 NEWS] Starting news analysis...")

        if not self.has_ai:
            print("[📰 NEWS] No AI backend (set GEMINI_API_KEY)")
            return signals

        headlines = await self._fetch_headlines()
        if not headlines:
            return signals

        all_markets = await fetch_all_polymarket_markets()
        if not all_markets:
            return signals

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

        print(f"[📰 NEWS] Scanning top {self.MAX_MARKETS_TO_SCAN} markets")

        for i, market in enumerate(tradeable[:self.MAX_MARKETS_TO_SCAN]):
            liq   = market["_liq"]
            price = liq["best_bid"]
            analysis = await self._analyze(market, headlines)
            if not analysis:
                continue
            conf = analysis["confidence"]
            if conf < self.MIN_NEWS_CONFIDENCE:
                continue
            outcome = analysis["expected_outcome"]
            exp_profit = (1.0 - price) * 100 if outcome == "YES" else price * 100
            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue

            signals.append(Signal(
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
            ))
            if len(signals) >= 5:
                break

        signals.sort(key=lambda s: (_days_sort_key_str(s.end_date), -s.confidence))
        print(f"[📰 NEWS] Done. {len(signals)} signals\n")
        return signals

    async def _fetch_headlines(self) -> List[Dict]:
        headlines: List[Dict] = []
        for url in self.RSS_FEEDS:
            try:
                feed = await asyncio.to_thread(feedparser.parse, url)
                for entry in feed.entries[:8]:
                    title = entry.get("title", "").strip()
                    if title:
                        headlines.append({
                            "title":   title,
                            "summary": entry.get("summary", "")[:200],
                            "link":    entry.get("link", ""),
                            "source":  feed.feed.get("title", "Unknown"),
                        })
            except Exception as e:
                print(f"[📰 NEWS] Feed error: {e}")
        return headlines

    async def _analyze(self, market: Dict, headlines: List[Dict]) -> Optional[Dict]:
        price    = market.get("_liq", {}).get("best_bid", 0.5)
        question = market["name"]
        hl_text  = "\n".join(
            f"{i+1}. [{h['source']}] {h['title']}"
            for i, h in enumerate(headlines[:20])
        )
        prompt = f"""Prediction market analyst.

MARKET: {question}
PRICE: {price:.2%}

HEADLINES:
{hl_text}

Reply EXACTLY:
RELEVANT: YES or NO
OUTCOME: YES or NO
CONFIDENCE: number
HEADLINES: numbers
SUMMARY: one sentence"""

        if self.gemini_key:
            result = await self._call_gemini(prompt)
            if result:
                return self._parse_analysis(result, headlines)
        if self.ollama_model:
            result = await self._call_ollama(prompt)
            if result:
                return self._parse_analysis(result, headlines)
        return None

    async def _call_gemini(self, prompt: str) -> Optional[str]:
        try:
            url  = f"{self.GEMINI_ENDPOINT}?key={self.gemini_key}"
            body = {"contents": [{"parts": [{"text": prompt}]}]}
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
                async with session.post(url, json=body) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    return (
                        data.get("candidates", [{}])[0]
                           .get("content", {})
                           .get("parts", [{}])[0]
                           .get("text", "")
                    )
        except Exception:
            return None

    async def _call_ollama(self, prompt: str) -> Optional[str]:
        try:
            payload = {
                "model":  self.ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.2, "num_predict": 150},
            }
            resp = await asyncio.to_thread(
                requests.post, f"{self.OLLAMA_HOST}/api/generate", json=payload, timeout=60
            )
            if resp.status_code != 200:
                return None
            return resp.json().get("response", "")
        except Exception:
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
        hl_nums: List[int] = []
        try:
            hl_nums = [
                int(x.strip()) - 1
                for x in lines.get("HEADLINES", "").split(",")
                if x.strip().isdigit()
            ]
        except Exception:
            pass
        source_url = ""
        relevant_titles: List[str] = []
        for idx in hl_nums:
            if 0 <= idx < len(headlines):
                relevant_titles.append(headlines[idx]["title"])
                if not source_url:
                    source_url = headlines[idx]["link"]
        return {
            "is_relevant":        True,
            "expected_outcome":   outcome,
            "confidence":         confidence,
            "relevant_headlines": relevant_titles,
            "summary":            lines.get("SUMMARY", ""),
            "source_url":         source_url,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: Metaculus (100 questions, 10 AI markets)
# ═══════════════════════════════════════════════════════════════════════════════

class MetaculusStrategy:
    METACULUS_API_BASE = "https://www.metaculus.com/api2"
    MIN_DIVERGENCE     = 0.10
    GEMINI_ENDPOINT    = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"
    OLLAMA_MODELS      = ["llama3.2", "qwen2.5", "mistral", "phi3"]
    OLLAMA_HOST        = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    MAX_AI_MARKETS     = 10  # was 25

    def __init__(self):
        self.gemini_key    = os.getenv("GEMINI_API_KEY", "")
        self.metaculus_key = config.METACULUS_API_KEY
        self.ollama_model: Optional[str] = None
        if os.getenv("OLLAMA_HOST"):
            self._init_ollama()

    def _init_ollama(self):
        try:
            resp = requests.get(f"{self.OLLAMA_HOST}/api/tags", timeout=5)
            if resp.status_code != 200:
                return
            models_data = resp.json().get("models", [])
            available = [m["name"] for m in models_data]
            for pref in self.OLLAMA_MODELS:
                for full in available:
                    if pref in full:
                        self.ollama_model = full
                        return
        except Exception:
            pass

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🎯 META] Starting Metaculus analysis...")

        try:
            all_markets = await fetch_all_polymarket_markets()
            if not all_markets:
                return signals

            tradeable: List[Dict] = []
            for m in all_markets:
                liq = LiquidityFilter.check(m)
                if not liq:
                    continue
                if liq["volume_24h"] < config.MIN_VOLUME_24H:
                    continue
                if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                    continue
                price = liq["best_bid"]
                if price < 0.10 or price > 0.90:
                    continue
                m["_liq"] = liq
                tradeable.append(m)

            metaculus_qs = await self._fetch_metaculus_questions()
            if metaculus_qs:
                signals = await self._match_with_metaculus(tradeable, metaculus_qs)

            if len(signals) < 3 and (self.gemini_key or self.ollama_model):
                ai_signals = await self._ai_expert_signals(tradeable, signals)
                signals.extend(ai_signals)

        except Exception as e:
            print(f"[🎯 META ERROR] {e}")

        signals.sort(key=lambda s: (_days_sort_key_str(s.end_date), -s.confidence))
        print(f"[🎯 META] Done. {len(signals)} signals\n")
        return signals

    async def _fetch_metaculus_questions(self) -> List[Dict]:
        questions: List[Dict] = []
        headers = {"Accept": "application/json"}
        if self.metaculus_key:
            headers["Authorization"] = f"Token {self.metaculus_key}"

        for api_url, params in [
            ("https://www.metaculus.com/api/posts/", {
                "statuses": "open", "forecast_type": "binary",
                "limit": 100, "order_by": "-activity", "has_community_prediction": "true",
            }),
            (f"{self.METACULUS_API_BASE}/questions/", {
                "status": "open", "type": "forecast", "limit": 100, "order_by": "-activity",
            }),
        ]:
            if questions:
                break
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=20), headers=headers
                ) as session:
                    async with session.get(api_url, params=params) as resp:
                        if resp.status not in (200, 201):
                            continue
                        data = await resp.json()
                        results = data.get("results", data if isinstance(data, list) else [])
                        for q in results:
                            question_data = q.get("question", q)
                            title = question_data.get("title") or q.get("title", "")
                            if not title:
                                continue
                            n_forecasts = (
                                question_data.get("nr_forecasters") or
                                question_data.get("number_of_forecasters") or 0
                            )
                            pred = None
                            agg = question_data.get("aggregations", {})
                            if agg:
                                recency = agg.get("recency_weighted", {}) or {}
                                latest = recency.get("history", [{}])
                                if latest:
                                    pred = latest[-1].get("means", [None])[0] if latest[-1].get("means") else None
                            if pred is None:
                                cp = question_data.get("community_prediction") or q.get("community_prediction") or {}
                                if isinstance(cp, dict):
                                    pred = (cp.get("full", {}) or {}).get("q2") or cp.get("q2")
                                elif isinstance(cp, (int, float)):
                                    pred = cp
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
                                "id": qid, "title": title,
                                "probability": pred_float,
                                "forecasters": int(n_forecasts),
                                "url": f"https://www.metaculus.com/questions/{qid}/",
                            })
            except Exception as e:
                print(f"[🎯 META] API error: {e}")
        return questions

    async def _match_with_metaculus(self, markets: List[Dict], questions: List[Dict]) -> List[Signal]:
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
                continue
            liq = best_market["_liq"]
            poly_prob = liq["best_bid"]
            divergence = abs(poly_prob - meta_prob)
            if divergence < self.MIN_DIVERGENCE:
                continue
            outcome = "YES" if meta_prob > poly_prob else "NO"
            confidence = min(60 + divergence * 200, 90)
            exp_profit = divergence * 100
            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue
            signals.append(Signal(
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
            ))
            if len(signals) >= 6:
                break
        return signals

    async def _ai_expert_signals(self, markets: List[Dict], already_found: List[Signal]) -> List[Signal]:
        signals: List[Signal] = []
        found_ids = {s.market_id for s in already_found}
        for market in markets[:self.MAX_AI_MARKETS]:
            if market["id"] in found_ids:
                continue
            liq = market["_liq"]
            poly_prob = liq["best_bid"]
            expert_prob = await self._ai_estimate_probability(market)
            if expert_prob is None:
                continue
            divergence = abs(poly_prob - expert_prob)
            if divergence < 0.15:
                continue
            outcome = "YES" if expert_prob > poly_prob else "NO"
            confidence = min(55 + divergence * 180, 87)
            exp_profit = divergence * 100
            if exp_profit < config.MIN_PROFIT_PERCENT:
                continue
            signals.append(Signal(
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
            ))
            found_ids.add(market["id"])
            if len(signals) >= 3:
                break
        return signals

    async def _ai_estimate_probability(self, market: Dict) -> Optional[float]:
        prompt = f"""Superforecaster. Probability of YES?

QUESTION: {market['name']}
POLYMARKET: {market.get('_liq', {}).get('best_bid', 0.5):.1%}

Reply ONLY with a number 0-100."""
        if self.gemini_key:
            try:
                url  = f"{self.GEMINI_ENDPOINT}?key={self.gemini_key}"
                body = {"contents": [{"parts": [{"text": prompt}]}]}
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
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
            except Exception:
                pass
        if self.ollama_model:
            try:
                payload = {
                    "model":  self.ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.1, "num_predict": 10},
                }
                resp = await asyncio.to_thread(
                    requests.post, f"{self.OLLAMA_HOST}/api/generate", json=payload, timeout=30
                )
                if resp.status_code == 200:
                    text = resp.json().get("response", "")
                    nums = re.findall(r"[\d.]+", text)
                    if nums:
                        p = float(nums[0])
                        return max(0.0, min(1.0, p / 100.0 if p > 1 else p))
            except Exception:
                pass
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Main Signal Generator
# ═══════════════════════════════════════════════════════════════════════════════

class SignalGenerator:
    def __init__(self):
        self.uma_strategy       = UMAOracleStrategy()
        self.news_strategy      = NewsStrategy()
        self.metaculus_strategy = MetaculusStrategy()

    async def generate_all_signals(self) -> List[Signal]:
        print("\n" + "=" * 60)
        print("[🚀] Generating signals (shared market cache)...")
        print("=" * 60)

        uma_res, news_res, meta_res = await asyncio.gather(
            self.uma_strategy.get_signals(),
            self.news_strategy.get_signals(),
            self.metaculus_strategy.get_signals(),
            return_exceptions=True,
        )

        all_signals: List[Signal] = []
        for result, name in [
            (uma_res, "UMA Oracle"),
            (news_res, "News Analysis"),
            (meta_res, "Metaculus"),
        ]:
            if isinstance(result, Exception):
                print(f"[🚀] {name} FAILED: {result}")
            elif isinstance(result, list):
                all_signals.extend(result)
                print(f"[🚀] {name}: {len(result)} signals")

        seen: set = set()
        unique: List[Signal] = []
        for sig in all_signals:
            key = f"{sig.market_id}:{sig.strategy}"
            if key not in seen:
                seen.add(key)
                unique.append(sig)

        unique.sort(key=lambda s: (_days_sort_key_str(s.end_date), -s.confidence))
        print(f"\n[🚀] TOTAL UNIQUE SIGNALS: {len(unique)}")
        print("=" * 60 + "\n")
        return unique


# ── Helpers ───────────────────────────────────────────────────────────────────

def _days_sort_key(days) -> float:
    try:
        return float(days) if days is not None else 9999.0
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
    stop = {
        "the", "a", "an", "of", "in", "on", "at", "to", "by", "is", "will",
        "be", "for", "or", "and", "with", "that", "this", "it", "from",
        "are", "has", "have", "was", "were", "not", "no", "do", "does",
    }
    return [
        w for w in re.findall(r"[a-z0-9]+", text.lower())
        if w not in stop and len(w) > 2
    ]
