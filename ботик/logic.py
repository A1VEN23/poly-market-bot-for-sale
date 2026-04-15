"""Signal generation logic for Polymarket Bot.

Memory-optimised for Render free tier (512 MB RAM).

Key changes vs original:
- Markets fetched ONCE per cycle and cached (shared across all 3 strategies)
- Streaming page-by-page filter: raw pages are discarded immediately — never
  accumulate all raw pages in memory simultaneously
- Hard cap: keep only top MAX_MARKETS_IN_MEMORY markets after each page
- web3 / Polygon RPC removed (UMA strategy works purely on prices)
- Each strategy receives a pre-filtered slice — no second full scan

Three strategies:
1. UMA Oracle        — near-expiry markets with extreme prices (≥88% / ≤12%)
2. News Analysis     — AI (Gemini Flash free tier) cross-checks RSS vs markets
3. Metaculus Consensus — Metaculus community vs Polymarket price divergence
"""
from __future__ import annotations

import asyncio
import feedparser
import os
import re
import json as json_module
import time
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
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
# Markets are fetched once per signal-generation cycle and reused by all
# strategies, saving ~2× memory and ~2× network I/O.

@dataclass
class _MarketCache:
    markets: List[Dict] = field(default_factory=list)
    fetched_at: float = 0.0          # unix timestamp
    ttl: float = 120.0               # seconds — refresh no more than once per 2 min

    def is_fresh(self) -> bool:
        return bool(self.markets) and (time.monotonic() - self.fetched_at) < self.ttl

    def store(self, markets: List[Dict]):
        self.markets = markets
        self.fetched_at = time.monotonic()

    def clear(self):
        self.markets = []
        self.fetched_at = 0.0


_cache = _MarketCache()

# Maximum number of market dicts kept in memory at once.
# Each dict is ~400 bytes → 1 000 markets ≈ 0.4 MB.  Very safe for 512 MB.
MAX_MARKETS_IN_MEMORY = 1_000
# How many pages to fetch (PAGE_SIZE=100 → 1 000 markets max)
PAGE_SIZE = 100


def _parse_float(val) -> float:
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


# ── Memory-efficient market fetcher ──────────────────────────────────────────

async def fetch_polymarket_markets(force: bool = False) -> List[Dict]:
    """
    Fetch active Polymarket markets.

    • Returns the cached list if it is still fresh (< 2 min old).
    • Otherwise fetches page-by-page, filters on the fly, and keeps only
      the top MAX_MARKETS_IN_MEMORY markets — raw page data is discarded
      immediately so peak RAM is O(page_size), not O(all_markets).
    • Markets are sorted: soonest expiry first, then volume descending.
    """
    if not force and _cache.is_fresh():
        return _cache.markets

    now = datetime.utcnow()
    kept: List[Dict] = []
    offset = 0
    total_raw = 0

    print("[API] Fetching Polymarket markets (memory-efficient mode)...")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=4)

    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            while len(kept) < MAX_MARKETS_IN_MEMORY:
                url = (
                    f"https://gamma-api.polymarket.com/markets"
                    f"?active=true&closed=false&limit={PAGE_SIZE}&offset={offset}"
                )
                try:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            print(f"[API ERROR] status={resp.status} offset={offset}")
                            break
                        data = await resp.json()
                except Exception as e:
                    print(f"[API ERROR] page offset={offset}: {e}")
                    break

                page: List[Dict] = data if isinstance(data, list) else data.get("data", [])
                if not page:
                    break

                total_raw += len(page)

                # ── Filter & normalise this page in place ──────────────────
                for m in page:
                    if m.get("closed") or m.get("archived"):
                        continue

                    # Parse end date
                    end_date_str = (
                        m.get("endDate") or m.get("end_date")
                        or m.get("resolution_date")
                    )
                    days_to_expiry: Optional[float] = None
                    if end_date_str:
                        try:
                            ed = str(end_date_str).replace("Z", "").replace("+00:00", "")
                            end_dt = datetime.fromisoformat(ed)
                            if end_dt < now:
                                continue   # already expired
                            days_to_expiry = (end_dt - now).total_seconds() / 86400.0
                        except Exception:
                            pass

                    # Parse prices
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

                    price = best_bid if best_bid > 0 else best_ask
                    ask   = best_ask if best_ask > 0 else price

                    volume = _parse_float(
                        m.get("volume24h") or m.get("volume") or 0
                    )
                    spread_pct = (
                        ((ask - price) / price * 100) if ask > price > 0 else 0.0
                    )

                    market_id = (
                        m.get("market_slug") or m.get("slug")
                        or m.get("conditionId") or ""
                    )
                    if not market_id:
                        continue

                    # ── Store only what strategies actually need ────────────
                    # (~10 fields vs ~40+ raw) — saves ~75% per-market memory
                    kept.append({
                        "id":            market_id,
                        "slug":          market_id,
                        "name":          (m.get("question") or m.get("title") or "")[:200],
                        "description":   (m.get("description") or "")[:300],
                        "category":      m.get("category", "General"),
                        "volume_24h":    volume,
                        "best_bid":      price,
                        "best_ask":      ask,
                        "spread_pct":    spread_pct,
                        "end_date":      end_date_str,
                        "days_to_expiry": days_to_expiry,
                    })

                # Discard raw page immediately
                del page, data

                print(f"[API]   raw={total_raw} | kept={len(kept)}")

                if len(kept) >= MAX_MARKETS_IN_MEMORY:
                    break
                if len(page if False else []) < PAGE_SIZE:  # checked via total_raw delta
                    pass
                # Stop if last page was smaller than PAGE_SIZE
                if total_raw % PAGE_SIZE != 0 and total_raw > 0:
                    break

                offset += PAGE_SIZE
                await asyncio.sleep(0.05)
    except Exception as e:
        print(f"[API] Unexpected error: {e}")

    # Sort: soonest expiry first, then volume desc
    kept.sort(key=lambda x: (
        x["days_to_expiry"] if x["days_to_expiry"] is not None else 9999.0,
        -x["volume_24h"],
    ))

    print(f"[API] Done. {len(kept)} markets in cache (raw fetched: {total_raw})")
    _cache.store(kept)
    return kept


# ── Liquidity filter ──────────────────────────────────────────────────────────

class LiquidityFilter:
    @staticmethod
    def check(market: Dict) -> Optional[Dict]:
        bid = market.get("best_bid", 0.0)
        ask = market.get("best_ask", 0.0)
        if bid <= 0 and ask <= 0:
            return None
        price = bid if bid > 0 else ask
        return {
            "volume_24h": market.get("volume_24h", 0.0),
            "best_bid":   price,
            "best_ask":   ask if ask > 0 else price,
            "spread_pct": market.get("spread_pct", 0.0),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 1: UMA Oracle
# Near-expiry markets with extreme prices → almost-certain outcome signals.
# Expected winrate: ~90 %+
# ═══════════════════════════════════════════════════════════════════════════════

class UMAOracleStrategy:
    EXPIRY_DAYS_MAX    = 3
    PRICE_HIGH         = 0.88
    PRICE_LOW          = 0.12
    MIN_VOLUME         = 500

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🔮 UMA] Starting scan...")

        try:
            all_markets = await fetch_polymarket_markets()
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

                if price >= self.PRICE_HIGH:
                    outcome       = "YES"
                    raw_conf      = 75 + (price - self.PRICE_HIGH) / (1 - self.PRICE_HIGH) * 20
                    confidence    = min(raw_conf, 95)
                    exp_profit    = (1.0 - price) * 100
                elif price <= self.PRICE_LOW:
                    outcome       = "NO"
                    raw_conf      = 75 + (self.PRICE_LOW - price) / self.PRICE_LOW * 20
                    confidence    = min(raw_conf, 95)
                    exp_profit    = price * 100
                else:
                    continue

                if days <= 1:
                    confidence = min(confidence + 5, 97)
                if confidence < 78 or exp_profit < config.MIN_PROFIT_PERCENT:
                    continue

                signals.append(Signal(
                    strategy="🔮 UMA Оракул",
                    market_id=market["id"],
                    market_name=market["name"],
                    current_price=price,
                    expected_outcome=outcome,
                    confidence=round(confidence, 1),
                    expected_profit=round(exp_profit, 2),
                    liquidity_volume_24h=volume,
                    bid_ask_spread=liq["spread_pct"],
                    source_url="",
                    polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                    timestamp=datetime.now(),
                    end_date=market.get("end_date"),
                    details={
                        "days_to_expiry": round(days, 2),
                        "price_signal":   f"Near-expiry {outcome} @ {price:.2%}",
                        "category":       market.get("category", "General"),
                    }
                ))
                print(f"[🔮 UMA] ✅ {market['name'][:50]} | {outcome} | conf={confidence:.1f}%")

        except Exception as e:
            print(f"[🔮 UMA ERROR] {e}")

        signals.sort(key=lambda s: (
            _days_key(s.details.get("days_to_expiry")),
            -s.confidence,
        ))
        print(f"[🔮 UMA] Done. {len(signals)} signals\n")
        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 2: News Analysis via Gemini Flash (free tier)
# ═══════════════════════════════════════════════════════════════════════════════

class NewsStrategy:
    RSS_FEEDS = [
        "https://feeds.reuters.com/reuters/businessnews",
        "https://news.google.com/rss/search?q=prediction+market+polymarket",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://news.google.com/rss/search?q=election+results+2025",
        "https://news.google.com/rss/search?q=crypto+bitcoin+2025",
        "https://news.google.com/rss/search?q=sports+championship+winner",
        "https://news.google.com/rss/search?q=geopolitics+war+ceasefire",
    ]
    GEMINI_ENDPOINT  = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        "gemini-1.5-flash:generateContent"
    )
    MIN_CONFIDENCE   = 72
    # How many tradeable markets to analyse — keep low to save memory & API quota
    ANALYSE_TOP_N    = 30

    def __init__(self):
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")

    @property
    def has_ai(self) -> bool:
        return bool(self.gemini_key)

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[📰 NEWS] Starting news analysis...")

        if not self.has_ai:
            print("[📰 NEWS] No GEMINI_API_KEY — skipping news strategy")
            return signals

        headlines = await self._fetch_headlines()
        print(f"[📰 NEWS] Headlines fetched: {len(headlines)}")
        if not headlines:
            return signals

        all_markets = await fetch_polymarket_markets()

        # Build tradeable slice (generator → no extra list copy)
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
            if len(tradeable) >= self.ANALYSE_TOP_N:
                break

        print(f"[📰 NEWS] Analysing top {len(tradeable)} markets...")

        for i, market in enumerate(tradeable):
            print(f"[📰 NEWS] [{i+1}/{len(tradeable)}] {market['name'][:55]}")
            liq   = market["_liq"]
            price = liq["best_bid"]

            analysis = await self._analyze(market, headlines)
            if not analysis:
                continue

            conf = analysis["confidence"]
            if conf < self.MIN_CONFIDENCE:
                continue

            outcome    = analysis["expected_outcome"]
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
            print(f"[📰 NEWS] ✅ {outcome} | conf={conf:.0f}%")

            if len(signals) >= 8:
                break

            # Small delay to avoid hammering Gemini free-tier rate limits
            await asyncio.sleep(0.5)

        signals.sort(key=lambda s: (_days_key_str(s.end_date), -s.confidence))
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
                print(f"[📰 NEWS] Feed error {url}: {e}")
        return headlines

    async def _analyze(self, market: Dict, headlines: List[Dict]) -> Optional[Dict]:
        price    = market.get("_liq", {}).get("best_bid", 0.5)
        question = market["name"]

        hl_text = "\n".join(
            f"{i+1}. [{h['source']}] {h['title']}"
            for i, h in enumerate(headlines[:20])
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

        result = await self._call_gemini(prompt)
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
                        print(f"[📰 NEWS] Gemini {resp.status}: {text[:150]}")
                        return None
                    data = await resp.json()
                    return (
                        data.get("candidates", [{}])[0]
                            .get("content", {})
                            .get("parts", [{}])[0]
                            .get("text", "")
                    )
        except Exception as e:
            print(f"[📰 NEWS] Gemini error: {e}")
            return None

    def _parse_analysis(self, text: str, headlines: List[Dict]) -> Optional[Dict]:
        try:
            lines = {
                k.strip(): v.strip()
                for k, _, v in (
                    line.partition(":")
                    for line in text.strip().splitlines()
                    if ":" in line
                )
            }
            if lines.get("RELEVANT", "NO").upper() != "YES":
                return None

            outcome    = lines.get("OUTCOME", "").upper()
            if outcome not in ("YES", "NO"):
                return None

            conf_str   = re.findall(r"[\d.]+", lines.get("CONFIDENCE", "0"))
            confidence = float(conf_str[0]) if conf_str else 0.0

            hl_nums    = [
                int(n) - 1
                for n in re.findall(r"\d+", lines.get("HEADLINES", ""))
            ]
            relevant_hl = [
                headlines[i]["title"]
                for i in hl_nums
                if 0 <= i < len(headlines)
            ]
            source_url = headlines[hl_nums[0]]["link"] if hl_nums and hl_nums[0] < len(headlines) else ""

            return {
                "expected_outcome":   outcome,
                "confidence":         confidence,
                "relevant_headlines": relevant_hl,
                "source_url":         source_url,
                "summary":            lines.get("SUMMARY", ""),
            }
        except Exception as e:
            print(f"[📰 NEWS] Parse error: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 3: Metaculus Consensus
# Compare Metaculus community probability vs Polymarket price.
# ═══════════════════════════════════════════════════════════════════════════════

class MetaculusStrategy:
    METACULUS_API   = "https://www.metaculus.com/api2/questions/"
    MIN_FORECASTERS = 10
    MIN_DIVERGENCE  = 0.15
    ANALYSE_TOP_N   = 60     # markets to check against Metaculus
    AI_FALLBACK_N   = 20     # markets for AI-expert fallback

    GEMINI_ENDPOINT = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        "gemini-1.5-flash:generateContent"
    )

    def __init__(self):
        self.metaculus_key = config.METACULUS_API_KEY
        self.gemini_key    = os.getenv("GEMINI_API_KEY", "")

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🎯 META] Starting Metaculus scan...")

        all_markets = await fetch_polymarket_markets()
        tradeable   = self._filter_tradeable(all_markets, self.ANALYSE_TOP_N)
        print(f"[🎯 META] Tradeable slice: {len(tradeable)}")

        meta_signals = await self._match_metaculus(tradeable)
        signals.extend(meta_signals)
        print(f"[🎯 META] Metaculus matches: {len(meta_signals)}")

        # AI fallback when Metaculus coverage is thin
        if len(signals) < 3 and self.gemini_key:
            ai_signals = await self._ai_expert_signals(tradeable, signals)
            signals.extend(ai_signals)
            print(f"[🎯 META] AI fallback added: {len(ai_signals)}")

        signals.sort(key=lambda s: (_days_key_str(s.end_date), -s.confidence))
        print(f"[🎯 META] Done. {len(signals)} signals\n")
        return signals

    def _filter_tradeable(self, all_markets: List[Dict], limit: int) -> List[Dict]:
        result: List[Dict] = []
        for m in all_markets:
            liq = LiquidityFilter.check(m)
            if not liq:
                continue
            if liq["volume_24h"] < config.MIN_VOLUME_24H:
                continue
            if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                continue
            m["_liq"] = liq
            result.append(m)
            if len(result) >= limit:
                break
        return result

    async def _match_metaculus(self, tradeable: List[Dict]) -> List[Signal]:
        signals: List[Signal] = []
        meta_questions = await self._fetch_metaculus_questions(limit=50)
        if not meta_questions:
            return signals

        for market in tradeable:
            name   = market["name"].lower()
            tokens = _tokenise(name)

            best_q:     Optional[Dict] = None
            best_score: int            = 0

            for q in meta_questions:
                q_tokens = _tokenise(q.get("title", "").lower())
                score    = len(set(tokens) & set(q_tokens))
                if score > best_score and score >= 3:
                    best_score = score
                    best_q     = q

            if not best_q:
                continue

            cp = best_q.get("community_prediction")
            if cp is None:
                continue
            meta_prob = float(cp)
            liq       = market["_liq"]
            poly_prob = liq["best_bid"]
            div       = abs(poly_prob - meta_prob)
            forecasters = best_q.get("number_of_forecasters", 0)

            print(f"[🎯 META] {market['name'][:45]}")
            print(f"[🎯 META]   meta={meta_prob:.2%} poly={poly_prob:.2%} "
                  f"div={div:.2%} forecasters={forecasters}")

            if div < self.MIN_DIVERGENCE or forecasters < self.MIN_FORECASTERS:
                continue

            outcome    = "YES" if meta_prob > poly_prob else "NO"
            confidence = min(55 + div * 200, 92)
            exp_profit = div * 100
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
                source_url=best_q.get("url", ""),
                polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                timestamp=datetime.now(),
                end_date=market.get("end_date"),
                details={
                    "metaculus_probability": meta_prob,
                    "polymarket_probability": poly_prob,
                    "divergence":            div,
                    "forecasters":           forecasters,
                    "metaculus_title":       best_q.get("title", ""),
                }
            ))
            print(f"[🎯 META] ✅ SIGNAL | {outcome} | conf={confidence:.1f}%")

            if len(signals) >= 6:
                break

        return signals

    async def _fetch_metaculus_questions(self, limit: int = 50) -> List[Dict]:
        headers = {}
        if self.metaculus_key:
            headers["Authorization"] = f"Token {self.metaculus_key}"

        params = {
            "status":   "open",
            "type":     "forecast",
            "order_by": "-activity",
            "limit":    limit,
        }
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            ) as session:
                async with session.get(
                    self.METACULUS_API,
                    params=params,
                    headers=headers,
                ) as resp:
                    if resp.status != 200:
                        print(f"[🎯 META] Metaculus API {resp.status}")
                        return []
                    data = await resp.json()
                    return data.get("results", [])
        except Exception as e:
            print(f"[🎯 META] Metaculus fetch error: {e}")
            return []

    async def _ai_expert_signals(
        self, tradeable: List[Dict], already: List[Signal]
    ) -> List[Signal]:
        if not self.gemini_key:
            return []

        signals:  List[Signal] = []
        found_ids = {s.market_id for s in already}

        for market in tradeable[: self.AI_FALLBACK_N]:
            if market["id"] in found_ids:
                continue

            liq       = market["_liq"]
            poly_prob = liq["best_bid"]

            expert_prob = await self._ai_estimate(market)
            if expert_prob is None:
                continue

            div = abs(poly_prob - expert_prob)
            print(f"[🎯 META/AI] {market['name'][:45]} "
                  f"ai={expert_prob:.2%} poly={poly_prob:.2%} div={div:.2%}")

            if div < 0.15:
                continue

            outcome    = "YES" if expert_prob > poly_prob else "NO"
            confidence = min(55 + div * 180, 87)
            exp_profit = div * 100
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
                    "divergence":            div,
                    "forecasters":           0,
                    "source":                "AI Expert Estimate",
                }
            ))
            found_ids.add(market["id"])
            print(f"[🎯 META/AI] ✅ SIGNAL | {outcome} | conf={confidence:.1f}%")

            if len(signals) >= 4:
                break

            await asyncio.sleep(0.5)

        return signals

    async def _ai_estimate(self, market: Dict) -> Optional[float]:
        prompt = (
            f"You are a superforecaster. Estimate the probability of this event resolving YES.\n\n"
            f"QUESTION: {market['name']}\n"
            f"CURRENT POLYMARKET PRICE: {market.get('_liq', {}).get('best_bid', 0.5):.1%}\n\n"
            f"Reply with ONLY a number between 0 and 100. No other text."
        )
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
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Main Signal Generator
# ═══════════════════════════════════════════════════════════════════════════════

class SignalGenerator:
    """Combines all three strategies; deduplicates and sorts results."""

    def __init__(self):
        self.uma_strategy       = UMAOracleStrategy()
        self.news_strategy      = NewsStrategy()
        self.metaculus_strategy = MetaculusStrategy()

    async def generate_all_signals(self) -> List[Signal]:
        print("\n" + "=" * 60)
        print("[🚀 SIGNAL GENERATOR] Starting full scan")
        print(f"[🚀] Filters: MinVol=${config.MIN_VOLUME_24H:,.0f} | "
              f"MaxSpread={config.MAX_SPREAD_PERCENT:.0f}% | "
              f"MinProfit={config.MIN_PROFIT_PERCENT:.1f}%")
        print("=" * 60)

        # Warm up cache ONCE before parallel strategies run
        await fetch_polymarket_markets()

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

        # Deduplicate
        seen:   set          = set()
        unique: List[Signal] = []
        for sig in all_signals:
            key = f"{sig.market_id}:{sig.strategy}"
            if key not in seen:
                seen.add(key)
                unique.append(sig)

        unique.sort(key=lambda s: (_days_key_str(s.end_date), -s.confidence))

        print(f"\n[🚀] TOTAL UNIQUE SIGNALS: {len(unique)}")
        for i, s in enumerate(unique[:5], 1):
            print(f"  {i}. {s.strategy} | {s.market_name[:40]} | "
                  f"conf={s.confidence:.1f}% | profit={s.expected_profit:.1f}%")
        print("=" * 60 + "\n")

        # Release cache after cycle to free memory
        _cache.clear()
        return unique


# ── Helpers ───────────────────────────────────────────────────────────────────

def _days_key(days) -> float:
    if days is None:
        return 9999.0
    try:
        return float(days)
    except Exception:
        return 9999.0


def _days_key_str(end_date_str: Optional[str]) -> float:
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
