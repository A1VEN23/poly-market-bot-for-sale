"""Signal generation logic for Polymarket Bot.

Contains six strategies:
1. UMA Oracle — monitors near-expiry markets with extreme prices (high certainty signals)
2. News Analysis — AI (Grok/Gemini) analyzes RSS news vs active markets
3. Metaculus Consensus — compares Metaculus community prediction with Polymarket price
4. Order Book — detects large smart-money orders on Polymarket CLOB API
5. Twitter/X Sentiment — Grok live search detects sentiment shifts before market reacts
6. Official Data — monitors scheduled economic/political events via public APIs

All strategies prioritise markets expiring soonest for maximum capital efficiency.
AI backend: Grok (xAI) primary, Gemini fallback.
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


# ── Market cache — shared across all strategies to avoid duplicate fetches ────
# TTL: 5 minutes. All 6 strategies read the same list per scan cycle.
_market_cache: Optional[List[Dict]] = None
_market_cache_ts: Optional[datetime] = None
_CACHE_TTL_SECONDS = 300   # 5 minutes
MAX_MARKETS = 4000         # Hard cap — keeps RAM well under 512 MB on Render Free


async def fetch_all_polymarket_markets_cached() -> List[Dict]:
    """Cached wrapper around fetch_all_polymarket_markets().
    Returns a shared list for up to 5 minutes, then re-fetches.
    This prevents 6 strategies from each fetching 5000+ markets in parallel."""
    global _market_cache, _market_cache_ts
    now = datetime.utcnow()
    if (
        _market_cache is not None
        and _market_cache_ts is not None
        and (now - _market_cache_ts).total_seconds() < _CACHE_TTL_SECONDS
    ):
        print(f"[CACHE] Using cached markets ({len(_market_cache)} markets, "
              f"age={(now - _market_cache_ts).total_seconds():.0f}s)")
        return _market_cache
    fresh = await fetch_all_polymarket_markets()
    _market_cache = fresh
    _market_cache_ts = now
    return fresh


def clear_market_cache():
    """Call after a full scan cycle to free memory immediately."""
    global _market_cache, _market_cache_ts
    _market_cache = None
    _market_cache_ts = None
    print("[CACHE] Market cache cleared")


# ══════════════════════════════════════════════════════════════════════════════
# AI CLIENT — Cloudflare Workers AI (primary, truly free, no hard limits)
#              + Groq llama-3.3-70b (fallback)
#              + xAI Grok (optional, paid, live X search)
#
# Cloudflare Workers AI free tier:
#   - 10 000 neurons/day (≈ hundreds of requests, enough for our bot)
#   - No RPM limit that causes 429
#   - Model: @cf/meta/llama-3.1-8b-instruct (fast, good quality)
#   - Requires: CF_ACCOUNT_ID + CF_API_TOKEN in .env
#   - Get free: https://dash.cloudflare.com → Workers AI → API tokens
#
# Priority: Cloudflare → Groq → xAI
# ══════════════════════════════════════════════════════════════════════════════

import time as _time

# Cloudflare Workers AI
CF_AI_MODEL     = "@cf/meta/llama-3.3-70b-instruct-fp8-fast"   # best free model
CF_AI_MODEL_FAST = "@cf/meta/llama-3.1-8b-instruct"            # faster fallback

# Groq (fallback)
GROQ_API_URL    = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL      = "llama-3.3-70b-versatile"

# xAI (optional paid)
XAI_API_URL     = "https://api.x.ai/v1/chat/completions"
XAI_MODEL       = "grok-3-mini"

# ── Groq rate limiter: 30 req/min ────────────────────────────────────────────
_groq_last_call: float = 0.0
_GROQ_MIN_INTERVAL: float = 3.0
_groq_lock = asyncio.Lock()

async def _groq_rate_limit():
    async with _groq_lock:
        global _groq_last_call
        now = _time.monotonic()
        wait = _GROQ_MIN_INTERVAL - (now - _groq_last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        _groq_last_call = _time.monotonic()


async def call_cloudflare(
    prompt: str,
    system: str = "You are a precise forecasting assistant.",
    temperature: float = 0.1,
) -> Optional[str]:
    """
    Call Cloudflare Workers AI — truly free, 10k neurons/day, no RPM limit.
    Setup (2 minutes):
      1. Go to https://dash.cloudflare.com → Workers & Pages → Workers AI
      2. Copy your Account ID from the right sidebar
      3. Create API Token: My Profile → API Tokens → Create Token
         → Use template "Cloudflare Workers AI" → Create Token
      4. Add to .env:
           CF_ACCOUNT_ID=your_account_id_here
           CF_API_TOKEN=your_api_token_here
    """
    cf_account = os.getenv("CF_ACCOUNT_ID", "").strip()
    cf_token   = os.getenv("CF_API_TOKEN", "").strip()
    if not cf_account or not cf_token:
        return None

    headers = {
        "Authorization": f"Bearer {cf_token}",
        "Content-Type": "application/json",
    }

    messages = [
        {"role": "system", "content": system},
        {"role": "user",   "content": prompt},
    ]

    for model in [CF_AI_MODEL, CF_AI_MODEL_FAST]:
        url = f"https://api.cloudflare.com/client/v4/accounts/{cf_account}/ai/run/{model}"
        body = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": 2048,
        }
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=45)
                ) as session:
                    async with session.post(url, json=body, headers=headers) as resp:
                        if resp.status == 429:
                            wait = 5 * (attempt + 1)
                            print(f"[CF] Rate limit — waiting {wait}s")
                            await asyncio.sleep(wait)
                            continue
                        if resp.status == 404:
                            print(f"[CF] Model {model} not found, trying next")
                            break  # try next model
                        if resp.status != 200:
                            text = await resp.text()
                            print(f"[CF] Error {resp.status}: {text[:200]}")
                            break
                        data = await resp.json()
                        # CF response: {"result": {"response": "..."}, "success": true}
                        if data.get("success"):
                            result = data.get("result", {})
                            # Handle both response formats
                            text = (result.get("response") or
                                    result.get("generated_text") or
                                    "")
                            if text:
                                # Always return a plain string — never raw list/dict
                                if isinstance(text, str):
                                    return text
                                return json_module.dumps(text)
                        else:
                            errors = data.get("errors", [])
                            print(f"[CF] API error: {errors}")
                        break
            except asyncio.TimeoutError:
                print(f"[CF] Timeout (attempt {attempt+1})")
                await asyncio.sleep(2)
            except Exception as e:
                print(f"[CF] Exception: {e}")
                return None
    return None


async def call_groq(
    prompt: str,
    system: str = "You are a precise forecasting assistant.",
    temperature: float = 0.1,
) -> Optional[str]:
    """Groq llama-3.3-70b — fallback when Cloudflare unavailable."""
    groq_key = os.getenv("GROQ_API_KEY", "").strip()
    xai_key  = os.getenv("XAI_API_KEY", "").strip()

    if groq_key:
        await _groq_rate_limit()
        result = await _call_openai_compat(
            GROQ_API_URL, groq_key, GROQ_MODEL, prompt, system, temperature
        )
        if result:
            return result

    if xai_key:
        result = await _call_openai_compat(
            XAI_API_URL, xai_key, XAI_MODEL, prompt, system, temperature
        )
        if result:
            return result

    return None


async def call_ai(
    prompt: str,
    system: str = "You are a precise forecasting assistant.",
    temperature: float = 0.1,
) -> Optional[str]:
    """
    Universal AI caller — priority: Cloudflare → Groq → xAI.
    Cloudflare Workers AI is the primary backend: free, no hard RPM limits.
    """
    # 1. Cloudflare Workers AI (primary — free, no RPM issues)
    result = await call_cloudflare(prompt, system, temperature)
    if result:
        return result

    # 2. Groq fallback
    print("[AI] Cloudflare unavailable, trying Groq fallback...")
    result = await call_groq(prompt, system, temperature)
    if result:
        return result

    print("[AI] All AI backends failed")
    return None


async def _call_openai_compat(
    url: str, key: str, model: str, prompt: str, system: str, temperature: float
) -> Optional[str]:
    """Generic OpenAI-compatible chat completions call with retry on 429."""
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    body = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
    }
    for attempt in range(4):
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.post(url, json=body, headers=headers) as resp:
                    if resp.status == 429:
                        wait = 10 * (2 ** attempt)  # 10s, 20s, 40s, 80s
                        print(f"[AI] Rate limit 429 from {url} — waiting {wait}s (attempt {attempt+1}/4)")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"[AI] Error {resp.status} from {url}: {text[:200]}")
                        return None
                    data = await resp.json()
                    return data["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"[AI] Exception calling {url}: {e}")
            return None
    return None


def _normalize_ai_response(result) -> Optional[str]:
    """
    Cloudflare Workers AI sometimes returns already-parsed Python objects
    (list or dict) instead of a JSON string. This normalizes both cases
    into a plain string so all downstream parsers work correctly.
    """
    if result is None:
        return None
    if isinstance(result, str):
        return result
    # Already a Python object (list/dict) — serialize back to JSON string
    try:
        return json_module.dumps(result)
    except Exception:
        return str(result)


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
            if len(all_raw) >= MAX_MARKETS:
                print(f"[API] Reached MAX_MARKETS cap ({MAX_MARKETS}) — stopping pagination")
                break
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

        # Parse CLOB token IDs (YES token = index 0, needed for OrderBook)
        clob_token_ids = []
        raw_clob = m.get("clobTokenIds") or m.get("clob_token_ids") or m.get("tokens")
        if isinstance(raw_clob, str):
            try:
                clob_token_ids = json_module.loads(raw_clob)
            except Exception:
                pass
        elif isinstance(raw_clob, list):
            clob_token_ids = raw_clob

        markets.append({
            "id": market_id,
            "slug": market_id,
            "condition_id": m.get("conditionId", ""),
            "clob_token_ids": clob_token_ids,
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

def _calc_roi(price: float, outcome: str) -> float:
    """
    True ROI% on capital invested.
    YES: buy YES token at `price`, win $1 → ROI = (1-price)/price * 100
    NO:  buy NO  token at (1-price), win $1 → ROI = price/(1-price) * 100
    Clamp price away from 0/1 to avoid div-by-zero.
    Cap at 500% — anything higher is a near-zero-price market (very illiquid/risky).
    """
    price = max(0.01, min(0.99, price))   # tighter clamp: min 1¢, max 99¢
    if outcome == "YES":
        roi = (1.0 - price) / price * 100
    else:
        roi = price / (1.0 - price) * 100
    return round(min(roi, 500.0), 1)      # hard cap at 500%



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

    EXPIRY_DAYS_MAX = 3.0        # ≤3 days — быстрые сигналы только
    PRICE_HIGH_THRESHOLD = 0.70  # YES ≥ 70% → сигнал YES (больше рынков)
    PRICE_LOW_THRESHOLD  = 0.30  # YES ≤ 30% → сигнал NO (больше рынков)
    MIN_VOLUME = 200             # Lower volume floor for more coverage

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
                print(f"[UMA] RPC {url} unavailable: {e}")
        # RPC is optional — all UMA price-based signals work without it
        print("[✅ UMA] Running in price-oracle mode (no RPC required — all signals active)")

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🔮 UMA] Starting scan...")

        try:
            all_markets = await fetch_all_polymarket_markets_cached()
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
                    # Confidence scales with price: 70%→62conf, 82%→78conf, 90%→88conf, 99%→95conf
                    if price >= 0.90:
                        raw_conf = 85 + (price - 0.90) / 0.10 * 10
                    elif price >= 0.82:
                        raw_conf = 78 + (price - 0.82) / 0.08 * 7
                    elif price >= 0.75:
                        raw_conf = 70 + (price - 0.75) / 0.07 * 8
                    else:
                        raw_conf = 62 + (price - self.PRICE_HIGH_THRESHOLD) / (0.75 - self.PRICE_HIGH_THRESHOLD) * 8
                    confidence = min(raw_conf, 95)
                    expected_profit = _calc_roi(price, "YES")

                elif price <= self.PRICE_LOW_THRESHOLD:
                    expected_outcome = "NO"
                    # Mirror: 30%→62conf, 18%→78conf, 10%→88conf
                    if price <= 0.10:
                        raw_conf = 85 + (0.10 - price) / 0.10 * 10
                    elif price <= 0.18:
                        raw_conf = 78 + (0.18 - price) / 0.08 * 7
                    elif price <= 0.25:
                        raw_conf = 70 + (0.25 - price) / 0.07 * 8
                    else:
                        raw_conf = 62 + (self.PRICE_LOW_THRESHOLD - price) / (self.PRICE_LOW_THRESHOLD - 0.25) * 8
                    confidence = min(raw_conf, 95)
                    expected_profit = _calc_roi(price, "NO")

                else:
                    continue  # price in uncertain middle zone

                # Extra confidence boost for very soon expiry
                if days <= 1:
                    confidence = min(confidence + 5, 97)

                # ROI floor: 0.1% — UMA near-expiry markets are near-certain wins
                # even with tiny ROI (e.g. 99.6% YES = 0.4% profit but ~0% risk)
                uma_min_roi = 0.1
                if expected_profit < uma_min_roi:
                    continue

                # Skip very low confidence — below 60% is noise
                if confidence < 60:
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
    AI-powered news analysis via batch prompt (1 AI call per scan cycle).
    Primary AI: Gemini 2.0 Flash (free, 1500/day, no rate-limit issues).
    Fallback: Groq llama-3.3-70b.
    """

    RSS_FEEDS = [
        "https://feeds.reuters.com/reuters/businessnews",
        "https://feeds.reuters.com/reuters/topNews",
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://news.google.com/rss/search?q=prediction+market+polymarket",
        "https://news.google.com/rss/search?q=election+results+2026",
        "https://news.google.com/rss/search?q=crypto+bitcoin+ethereum+2026",
        "https://news.google.com/rss/search?q=sports+championship+winner+2026",
        "https://news.google.com/rss/search?q=geopolitics+war+ceasefire+peace",
        "https://news.google.com/rss/search?q=fed+interest+rate+inflation",
        "https://news.google.com/rss/search?q=nba+nfl+mlb+winner+champion",
        "https://news.google.com/rss/search?q=supreme+court+ruling+decision",
        "https://news.google.com/rss?hl=en-US&gl=US&ceid=US:en",
    ]

    MIN_NEWS_CONFIDENCE = 62

    @property
    def has_ai(self) -> bool:
        return bool(os.getenv("GEMINI_API_KEY")) or bool(os.getenv("GROQ_API_KEY")) or bool(os.getenv("XAI_API_KEY"))

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[📰 NEWS] Starting news analysis...")

        if not self.has_ai:
            print("[📰 NEWS] No AI backend — set GEMINI_API_KEY (free at aistudio.google.com)")
            return signals

        headlines = await self._fetch_headlines()
        print(f"[📰 NEWS] Fetched {len(headlines)} headlines")
        if not headlines:
            return signals

        all_markets = await fetch_all_polymarket_markets_cached()
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
            price = liq["best_bid"]
            if price > 0.95 or price < 0.05:
                continue
            # Приоритет — рынки истекающие в течение 3 дней
            days = m.get("days_to_expiry")
            if days is not None and days > 3:
                continue
            m["_liq"] = liq
            tradeable.append(m)

        # Sort soonest expiry first so batch gets the most urgent markets
        tradeable.sort(key=lambda x: (
            x.get("days_to_expiry") if x.get("days_to_expiry") is not None else 9999,
            -x.get("volume_24h", 0)
        ))

        print(f"[📰 NEWS] Tradeable markets (≤3d): {len(tradeable)} — batch analysis (top 40)")

        # BATCH mode: single AI call for all markets → avoids Groq 429 floods
        batch_results = await self._analyze_batch(tradeable[:40], headlines)
        print(f"[📰 NEWS] Batch returned {len(batch_results)} relevant markets")

        for market, analysis in batch_results:
            liq   = market["_liq"]
            price = liq["best_bid"]
            conf  = analysis["confidence"]

            if conf < self.MIN_NEWS_CONFIDENCE:
                print(f"[📰 NEWS]   ❌ conf too low ({conf:.0f}%): {market['name'][:40]}")
                continue

            outcome    = analysis["expected_outcome"]
            exp_profit = _calc_roi(price, outcome)

            if exp_profit < 0.5:
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
            print(f"[📰 NEWS]   ✅ SIGNAL | {market['name'][:40]} | {outcome} | conf={conf:.0f}%")

            if len(signals) >= 12:
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

    async def _analyze_batch(self, markets: List[Dict], headlines: List[Dict]) -> List[tuple]:
        """
        Single AI call: analyse all markets against headlines at once.
        Returns list of (market, analysis_dict) for relevant markets only.
        """
        hl_text = "\n".join(
            f"{i+1}. [{h['source']}] {h['title']}"
            for i, h in enumerate(headlines[:30])
        )
        market_lines = []
        for i, m in enumerate(markets):
            price = m.get("_liq", {}).get("best_bid", 0.5)
            market_lines.append(f"{i+1}. [{price:.0%} YES] {m['name']}")
        markets_text = "\n".join(market_lines)

        prompt = f"""You are a professional prediction market analyst.

NEWS HEADLINES (numbered):
{hl_text}

PREDICTION MARKETS (numbered, current YES price shown):
{markets_text}

For each market, check if any headline is RELEVANT and implies a likely outcome.
Only include markets where you find relevant news.

Respond ONLY with a JSON array. Each item:
{{"market": <market_number>, "outcome": "YES" or "NO", "confidence": <50-95>, "headlines": [<headline numbers>], "summary": "<one sentence>"}}

Example: [{{"market": 3, "outcome": "NO", "confidence": 78, "headlines": [2, 7], "summary": "Fed held rates steady per latest decision."}}]

If no markets have relevant news, respond with: []
Respond with JSON only, no other text."""

        result_text = await call_ai(
            prompt,
            system="You are a precise prediction market analyst. Respond only with valid JSON array.",
            temperature=0.0
        )

        if not result_text:
            print("[📰 NEWS] No AI response for batch analysis")
            return []

        # Normalize: CF may return list/dict directly instead of JSON string
        result_text = _normalize_ai_response(result_text)

        try:
            clean = result_text.strip().strip("```json").strip("```").strip()
            match = re.search(r'\[.*\]', clean, re.DOTALL)
            if match:
                items = json_module.loads(match.group())
            else:
                items = json_module.loads(clean)
        except Exception as e:
            print(f"[📰 NEWS] Batch parse error: {e} | text={result_text[:200]}")
            return []

        results = []
        for item in items:
            try:
                idx = int(item["market"]) - 1
                if not (0 <= idx < len(markets)):
                    continue
                market = markets[idx]
                conf = float(item.get("confidence", 0))
                outcome = str(item.get("outcome", "YES")).upper()
                if outcome not in ("YES", "NO"):
                    outcome = "YES"
                hl_nums = [int(n) - 1 for n in item.get("headlines", []) if str(n).isdigit()]
                source_url = ""
                rel_titles = []
                for hn in hl_nums:
                    if 0 <= hn < len(headlines):
                        rel_titles.append(headlines[hn]["title"])
                        if not source_url:
                            source_url = headlines[hn].get("link", "")
                analysis = {
                    "expected_outcome": outcome,
                    "confidence": conf,
                    "relevant_headlines": rel_titles,
                    "summary": item.get("summary", ""),
                    "source_url": source_url,
                }
                results.append((market, analysis))
            except Exception as e:
                print(f"[📰 NEWS] Batch item parse error: {e}")
        return results




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
    MIN_DIVERGENCE     = 0.08    # 8% gap minimum (was 12% — too strict)
    MIN_FORECASTERS    = 0       # Accept questions even without forecaster count
    MIN_OVERLAP        = 0.18    # Keyword match threshold (was 0.25 — too strict)
    META_EXTREME_LOW   = 0.04
    META_EXTREME_HIGH  = 0.96

    def __init__(self):
        self.metaculus_key = config.METACULUS_API_KEY

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🎯 META] Starting Metaculus consensus analysis...")

        try:
            all_markets = await fetch_all_polymarket_markets_cached()
            if not all_markets:
                return signals

            # Pre-filter tradeable markets (soonest expiry first, ≤14 days only)
            tradeable: List[Dict] = []
            for m in all_markets:
                liq = LiquidityFilter.check(m)
                if not liq:
                    continue
                if liq["volume_24h"] < config.MIN_VOLUME_24H:
                    continue
                if liq["spread_pct"] > config.MAX_SPREAD_PERCENT:
                    continue
                # Только быстрые рынки — сигналы должны быть actioable за 1-3 дня
                days = m.get("days_to_expiry")
                if days is None or days > 3:
                    continue
                # Skip markets priced near 0 or 1 — already efficient
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

            # AI expert fallback — use when Metaculus API returns no parseable data
            # Gemini/Grok acts as independent expert forecaster
            if len(signals) < 3:
                ai_signals = await self._ai_expert_signals(tradeable, signals)
                signals.extend(ai_signals)

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
        """
        Fetch prediction questions from Manifold Markets API.
        Manifold is fully open (no API key), returns real community probabilities,
        and has thousands of binary markets similar to Polymarket.
        Falls back to Metaculus v2 with METACULUS_API_KEY if set.
        """
        questions: List[Dict] = []

        # ── Primary: Manifold Markets (open API, real probabilities) ──────────
        try:
            manifold_questions = await self._fetch_manifold_questions()
            if manifold_questions:
                questions.extend(manifold_questions)
                print(f"[🎯 META] Manifold Markets: {len(manifold_questions)} questions with probabilities")
        except Exception as e:
            print(f"[🎯 META] Manifold error: {e}")

        # ── Secondary: Metaculus v2 with API key ──────────────────────────────
        if len(questions) < 20 and self.metaculus_key:
            try:
                meta_questions = await self._fetch_metaculus_with_key()
                if meta_questions:
                    questions.extend(meta_questions)
                    print(f"[🎯 META] Metaculus (keyed): {len(meta_questions)} questions")
            except Exception as e:
                print(f"[🎯 META] Metaculus keyed error: {e}")

        print(f"[🎯 META] Total parsed: {len(questions)} questions with predictions")
        return questions

    async def _fetch_manifold_questions(self) -> List[Dict]:
        """
        Fetch binary markets from Manifold Markets API.
        Endpoint: GET https://api.manifold.markets/v0/markets
        Returns markets with probability field (0-1) — no auth needed.
        """
        questions: List[Dict] = []
        base_url = "https://api.manifold.markets/v0/markets"

        # Fetch multiple pages — different sorts for diversity
        fetch_params_list = [
            {"limit": 200, "sort": "score"},
            {"limit": 200, "sort": "liquidity"},
        ]

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20),
            headers={"Accept": "application/json"},
        ) as session:
            for params in fetch_params_list:
                if len(questions) >= 300:
                    break
                try:
                    async with session.get(base_url, params=params) as resp:
                        if resp.status != 200:
                            print(f"[🎯 META] Manifold HTTP {resp.status} params={params}")
                            continue
                        data = await resp.json()

                    for m in data:
                        # Only binary markets with real probability
                        if m.get("outcomeType") != "BINARY":
                            continue
                        if m.get("isResolved", False):
                            continue

                        prob = m.get("probability")
                        if prob is None:
                            continue
                        try:
                            pf = float(prob)
                            if not (0.01 <= pf <= 0.99):
                                continue
                        except (TypeError, ValueError):
                            continue

                        title = m.get("question", "") or m.get("title", "")
                        if not title or len(title) < 10:
                            continue

                        qid = m.get("id", "")
                        url = m.get("url", f"https://manifold.markets/{qid}")
                        traders = m.get("uniqueBettorCount", 0) or 0

                        questions.append({
                            "id":          qid,
                            "title":       title,
                            "probability": pf,
                            "forecasters": int(traders),
                            "url":         url,
                        })

                except Exception as e:
                    print(f"[🎯 META] Manifold page error: {e}")

        return questions

    async def _fetch_metaculus_with_key(self) -> List[Dict]:
        """Fetch from Metaculus v2 using API key — returns real community predictions."""
        questions: List[Dict] = []
        headers = {
            "Accept": "application/json",
            "Authorization": f"Token {self.metaculus_key}",
        }
        url = f"{self.METACULUS_API_BASE}/questions/"
        params = {
            "status": "open",
            "type": "forecast",
            "limit": 100,
            "order_by": "-activity",
        }
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=20),
                headers=headers,
            ) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return questions
                    data = await resp.json()

            for q in data.get("results", []):
                title = q.get("title", "")
                if not title:
                    continue
                cp = q.get("community_prediction") or {}
                prob = None
                if isinstance(cp, dict):
                    full = cp.get("full") or {}
                    prob = full.get("q2") or cp.get("q2") or cp.get("mean")
                elif isinstance(cp, (int, float)):
                    prob = cp
                if prob is None:
                    continue
                try:
                    pf = max(0.01, min(0.99, float(prob)))
                except Exception:
                    continue
                qid = q.get("id", "")
                questions.append({
                    "id":          qid,
                    "title":       title,
                    "probability": pf,
                    "forecasters": q.get("number_of_forecasters", 0) or 0,
                    "url":         f"https://www.metaculus.com/questions/{qid}/",
                })
        except Exception as e:
            print(f"[🎯 META] Metaculus key fetch error: {e}")
        return questions

    async def _match_with_metaculus(
        self, markets: List[Dict], questions: List[Dict]
    ) -> List[Signal]:
        """Match Metaculus questions to Polymarket markets by keyword overlap."""
        signals: List[Signal] = []

        for q in questions:
            meta_prob   = q["probability"]
            forecasters = q["forecasters"]

            # ── Guard 1: require enough forecasters ─────────────────────────
            if self.MIN_FORECASTERS > 0 and forecasters < self.MIN_FORECASTERS:
                print(f"[🎯 META] SKIP (only {forecasters} forecasters): {q['title'][:50]}")
                continue

            q_words = set(_tokenise(q["title"]))

            best_market: Optional[Dict] = None
            best_overlap = 0.0

            for m in markets:
                m_words = set(_tokenise(m["name"]))
                overlap = len(q_words & m_words) / max(len(q_words | m_words), 1)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_market = m

            # ── Guard 2: strict overlap threshold ───────────────────────────
            if best_market is None or best_overlap < self.MIN_OVERLAP:
                if best_market:
                    print(f"[🎯 META] SKIP (overlap {best_overlap:.0%} < {self.MIN_OVERLAP:.0%}): "
                          f"'{q['title'][:40]}' ~ '{best_market['name'][:40]}'")
                continue

            liq       = best_market["_liq"]
            poly_prob = liq["best_bid"]
            divergence = abs(poly_prob - meta_prob)

            print(f"[🎯 META] Match ({best_overlap:.0%}, {forecasters}fc): {best_market['name'][:45]}")
            print(f"[🎯 META]   Meta={meta_prob:.2%}, Poly={poly_prob:.2%}, div={divergence:.2%}")

            # ── Guard 3: extreme Meta values with big divergence = likely mismatch ──
            # e.g. Meta 2% vs Poly 44% almost never means same question
            if meta_prob < self.META_EXTREME_LOW and poly_prob > 0.20:
                print(f"[🎯 META] SKIP (extreme Meta {meta_prob:.0%} vs Poly {poly_prob:.0%} — likely different question)")
                continue
            if meta_prob > self.META_EXTREME_HIGH and poly_prob < 0.80:
                print(f"[🎯 META] SKIP (extreme Meta {meta_prob:.0%} vs Poly {poly_prob:.0%} — likely different question)")
                continue

            if divergence < self.MIN_DIVERGENCE:
                continue

            outcome = "YES" if meta_prob > poly_prob else "NO"

            # ── Confidence: anchored to overlap quality + forecaster count ──
            # Base: 55 (minimum viable). Bonuses:
            #   +15 for overlap ≥ 0.50 (strong keyword match)
            #   +10 for forecasters ≥ 50
            #   +10 for forecasters ≥ 200
            #   divergence bonus: up to +10 (capped — divergence alone ≠ quality)
            base_conf = 55.0
            if best_overlap >= 0.50:
                base_conf += 15.0
            if forecasters >= 50:
                base_conf += 10.0
            if forecasters >= 200:
                base_conf += 10.0
            div_bonus = min(divergence * 40, 10.0)   # max +10 from divergence
            confidence = min(base_conf + div_bonus, 82.0)  # hard cap 82%
            
            # Skip low-confidence Metaculus signals — not worth the risk
            if confidence < 65.0:
                continue

            exp_profit = divergence * 100
            if exp_profit < 0.5:  # logic-level min ROI floor
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
                    "forecasters": forecasters,
                    "match_overlap": best_overlap,
                }
            )
            signals.append(sig)
            print(f"[🎯 META]   ✅ SIGNAL | {outcome} | conf={confidence:.1f}% | overlap={best_overlap:.0%} | fc={forecasters}")

            if len(signals) >= 6:
                break

        return signals

    async def _ai_expert_signals(
        self, markets: List[Dict], already_found: List[Signal]
    ) -> List[Signal]:
        """
        Use AI as an independent expert when Manifold/Metaculus match count is low.
        BATCH mode: send up to 15 markets in ONE prompt → 1 AI call instead of 15.
        Prioritises near-expiry markets (≤7 days).
        """
        signals: List[Signal] = []
        found_ids = {s.market_id for s in already_found}

        # Sort candidates: soonest expiry first, then volume
        candidates = [m for m in markets if m["id"] not in found_ids]
        candidates.sort(key=lambda m: (
            m.get("days_to_expiry") if m.get("days_to_expiry") is not None else 9999,
            -m.get("volume_24h", 0)
        ))

        if not candidates:
            return signals

        batch_result = await self._ai_estimate_batch(candidates[:15])
        if not batch_result:
            return signals

        for market, expert_prob in batch_result:
            liq = market["_liq"]
            poly_prob = liq["best_bid"]
            divergence = abs(poly_prob - expert_prob)

            print(f"[🎯 META/AI] {market['name'][:45]}")
            print(f"[🎯 META/AI]   AI={expert_prob:.2%}, Poly={poly_prob:.2%}, div={divergence:.2%}")

            if divergence < 0.05:   # lowered from 0.07 — catch more signals
                continue

            outcome = "YES" if expert_prob > poly_prob else "NO"
            confidence = min(58.0 + divergence * 200, 84.0)
            exp_profit = _calc_roi(poly_prob, outcome)

            if exp_profit < 0.5:
                continue
            if confidence < 60.0:
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
                    "days_to_expiry": market.get("days_to_expiry"),
                }
            )
            signals.append(sig)
            found_ids.add(market["id"])
            print(f"[🎯 META/AI]   ✅ SIGNAL | {outcome} | conf={confidence:.1f}%")

            if len(signals) >= 5:
                break

        return signals

    async def _ai_estimate_batch(
        self, markets: List[Dict]
    ) -> Optional[List[tuple]]:
        """
        Batch-estimate probabilities for multiple markets in ONE AI call.
        Returns list of (market, probability) tuples.
        """
        lines = []
        for i, m in enumerate(markets):
            poly = m.get("_liq", {}).get("best_bid", 0.5)
            lines.append(f"{i+1}. [{poly:.0%}] {m['name']}")

        market_list = "\n".join(lines)
        prompt = f"""You are a superforecaster. For each prediction market below, estimate the true probability of YES resolution based on your knowledge.

Markets (current Polymarket price shown in brackets):
{market_list}

Reply with ONLY a JSON array of numbers (0-100), one per market, in order. Example: [72, 18, 45, 91, 33]
No other text, no explanation."""

        result_text = await call_ai(
            prompt,
            system="You are a precise probability estimator. Reply only with a JSON array of numbers.",
            temperature=0.0
        )

        if not result_text:
            print("[🎯 META/AI] No AI response for batch estimation")
            return None

        # Normalize: CF may return list directly instead of JSON string
        result_text = _normalize_ai_response(result_text)

        # Parse JSON array from response
        try:
            # If CF returned a list directly, it's now a JSON string like "[52,42,...]"
            clean = result_text.strip().strip("```json").strip("```").strip()

            # Try direct parse first
            try:
                probs_raw = json_module.loads(clean)
            except Exception:
                match = re.search(r'\[[\d\s.,]+\]', clean)
                if match:
                    probs_raw = json_module.loads(match.group())
                else:
                    raise ValueError("No array found")

            # Handle both [52, 42, ...] and [{"prob": 52}, ...] shapes
            if probs_raw and isinstance(probs_raw[0], dict):
                probs_raw = [list(p.values())[0] for p in probs_raw]

            result = []
            for i, market in enumerate(markets):
                if i >= len(probs_raw):
                    break
                p = float(probs_raw[i])
                prob = max(0.01, min(0.99, p / 100.0 if p > 1.0 else p))
                result.append((market, prob))
            return result if result else None

        except Exception as e:
            print(f"[🎯 META/AI] Batch parse error: {e} | text={result_text[:100]}")
            # Fallback: extract all numbers from response string
            nums = re.findall(r'\b(\d{1,3}(?:\.\d+)?)\b', result_text)
            if len(nums) >= len(markets) // 2:
                result = []
                for i, market in enumerate(markets):
                    if i >= len(nums):
                        break
                    p = float(nums[i])
                    prob = max(0.01, min(0.99, p / 100.0 if p > 1.0 else p))
                    result.append((market, prob))
                return result if result else None
            return None




# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 4: Order Book — Smart Money Detection
# Logic: Polymarket CLOB API exposes order books. Large asymmetric orders
# from wallets with high historical accuracy = smart money signal.
# Winrate driver: informed traders place large orders before resolution.
# ═══════════════════════════════════════════════════════════════════════════════

class OrderBookStrategy:
    """
    Fetches Polymarket CLOB order books via public API.
    Uses YES-token_id from market data to query /book endpoint.
    Falls back to Gamma API market detail if clob_token_ids not in cache.
    Detects large one-sided depth imbalance = smart money signal.
    """

    CLOB_API       = "https://clob.polymarket.com"
    GAMMA_API      = "https://gamma-api.polymarket.com"
    MIN_ORDER_SIZE = 100    # $100+ orders count as significant
    MIN_IMBALANCE  = 0.55   # 55%+ lean = signal (was 60% — too strict)
    EXPIRY_DAYS_MAX = 3     # ≤3 дней — только быстрые сигналы
    MIN_VOLUME     = 300    # Lower volume floor for more coverage

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[📊 ORDERBOOK] Starting order book scan...")

        try:
            all_markets = await fetch_all_polymarket_markets_cached()
            # Use near-expiry + higher volume markets — most likely to have smart money
            candidates = [
                m for m in all_markets
                if m.get("days_to_expiry") is not None
                and 0 < m["days_to_expiry"] <= self.EXPIRY_DAYS_MAX
            ]
            candidates.sort(key=lambda x: -x.get("volume_24h", 0))
            print(f"[📊 ORDERBOOK] Candidates: {len(candidates)} (≤{self.EXPIRY_DAYS_MAX}d expiry)")

            checked = 0
            for market in candidates[:150]:
                liq = LiquidityFilter.check(market)
                if not liq or liq["volume_24h"] < self.MIN_VOLUME:
                    continue

                token_id = await self._get_yes_token_id(market)
                if not token_id:
                    continue

                book = await self._fetch_order_book(token_id)
                if not book:
                    continue

                checked += 1
                signal = self._analyze_book(market, liq, book)
                if signal:
                    signals.append(signal)
                    print(f"[📊 ORDERBOOK] ✅ Signal #{len(signals)}: {market['name'][:45]}")
                    if len(signals) >= 5:
                        break

                await asyncio.sleep(0.1)

            print(f"[📊 ORDERBOOK] Checked {checked} order books")

        except Exception as e:
            print(f"[📊 ORDERBOOK ERROR] {e}")
            import traceback; traceback.print_exc()

        signals.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))
        print(f"[📊 ORDERBOOK] Done. {len(signals)} signals\n")
        return signals

    async def _get_yes_token_id(self, market: Dict) -> Optional[str]:
        """
        Get the YES-outcome CLOB token ID for a market.
        Priority: clob_token_ids[0] from cache → Gamma detail API → conditionId fallback.
        """
        # 1. Already parsed in market cache
        clob_ids = market.get("clob_token_ids", [])
        if clob_ids:
            first = clob_ids[0]
            # Can be a dict {"token_id": "...", "outcome": "Yes"} or a plain string
            if isinstance(first, dict):
                return first.get("token_id") or first.get("id")
            if isinstance(first, str) and len(first) > 10:
                return first

        # 2. Fetch from Gamma detail endpoint
        slug = market.get("slug") or market.get("id", "")
        if slug:
            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as session:
                    async with session.get(
                        f"{self.GAMMA_API}/markets/{slug}"
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            raw = data.get("clobTokenIds") or data.get("tokens") or []
                            if isinstance(raw, str):
                                raw = json_module.loads(raw)
                            if raw:
                                item = raw[0]
                                if isinstance(item, dict):
                                    return item.get("token_id") or item.get("id")
                                if isinstance(item, str) and len(item) > 10:
                                    return item
            except Exception:
                pass

        # 3. Last resort — use conditionId (sometimes accepted by CLOB)
        cid = market.get("condition_id", "")
        if cid and len(cid) > 10:
            return cid

        return None

    async def _fetch_order_book(self, token_id: str) -> Optional[Dict]:
        """Fetch order book for a YES token from CLOB API."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            ) as session:
                async with session.get(
                    f"{self.CLOB_API}/book",
                    params={"token_id": token_id},
                    headers={"Accept": "application/json"},
                ) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json()
                    # CLOB returns {"bids": [...], "asks": [...]} or nested
                    if isinstance(data, dict) and ("bids" in data or "asks" in data):
                        return data
                    return None
        except Exception:
            return None

    def _analyze_book(self, market: Dict, liq: Dict, book: Dict) -> Optional[Signal]:
        """Detect large imbalance in order book depth."""
        try:
            bids = book.get("bids", [])
            asks = book.get("asks", [])

            # Sum sizes of large orders only
            bid_depth = sum(
                float(o.get("size", 0)) for o in bids
                if float(o.get("size", 0)) >= self.MIN_ORDER_SIZE
            )
            ask_depth = sum(
                float(o.get("size", 0)) for o in asks
                if float(o.get("size", 0)) >= self.MIN_ORDER_SIZE
            )

            total = bid_depth + ask_depth
            if total < self.MIN_ORDER_SIZE * 2:
                return None  # not enough large orders

            bid_ratio = bid_depth / total if total > 0 else 0.5

            if bid_ratio >= self.MIN_IMBALANCE:
                outcome   = "YES"
                imbalance = bid_ratio
            elif (1 - bid_ratio) >= self.MIN_IMBALANCE:
                outcome   = "NO"
                imbalance = 1 - bid_ratio
            else:
                return None  # balanced book — no signal

            price    = liq["best_bid"]

            # Skip markets where YES price < 3% or > 97% — these are
            # near-resolved and produce unreliable/extreme ROI values
            if price < 0.03 or price > 0.97:
                return None

            # Confidence: 60 base + imbalance bonus (max +20)
            confidence = min(60.0 + (imbalance - self.MIN_IMBALANCE) * 200, 80.0)
            exp_profit = _calc_roi(price, outcome)

            if exp_profit < 0.5:  # logic-level min ROI floor
                return None

            print(f"[📊 ORDERBOOK] ✅ {market['name'][:45]} | {outcome} | "
                  f"imbalance={imbalance:.0%} | conf={confidence:.1f}%")

            return Signal(
                strategy="📊 Smart Money",
                market_id=market["id"],
                market_name=market["name"],
                current_price=price,
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
                    "bid_depth":    round(bid_depth, 0),
                    "ask_depth":    round(ask_depth, 0),
                    "bid_ratio":    round(bid_ratio, 3),
                    "imbalance":    round(imbalance, 3),
                }
            )
        except Exception as e:
            print(f"[📊 ORDERBOOK] analysis error: {e}")
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 5: Twitter/X Sentiment via Grok Live Search
# Logic: Grok has real-time X search. Ask it to find sentiment around a market
# question. Sharp sentiment shift before market reacts = signal.
# Winrate driver: social media reacts faster than prediction markets on breaking news.
# ═══════════════════════════════════════════════════════════════════════════════

class GrokSentimentStrategy:
    """
    Uses Grok's live X/Twitter search to detect sentiment shifts.
    Only runs if XAI_API_KEY is set.
    Checks top markets by volume where price is in uncertain zone (25-75%).
    """

    GROK_LIVE_URL = "https://api.x.ai/v1/chat/completions"
    MIN_VOLUME    = 500     # Lower threshold — include more markets
    MAX_MARKETS   = 40      # Больше рынков в батче для большего числа сигналов

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        xai_key    = os.getenv("XAI_API_KEY", "")
        groq_key   = os.getenv("GROQ_API_KEY", "")
        cf_account = os.getenv("CF_ACCOUNT_ID", "")

        if not xai_key and not groq_key and not cf_account:
            print("[🐦 SENTIMENT] Skipping — no AI key set")
            return signals

        mode = "xai_live" if xai_key else "ai_knowledge"
        print(f"\n[🐦 SENTIMENT] Starting sentiment scan (mode={mode})...")

        try:
            all_markets = await fetch_all_polymarket_markets_cached()

            # Цель: рынки истекающие в ≤3 дня — только быстрые сигналы
            targets = [
                m for m in all_markets
                if m.get("volume_24h", 0) >= self.MIN_VOLUME
                and m.get("days_to_expiry") is not None
                and 0 < m["days_to_expiry"] <= 3
            ]
            targets.sort(key=lambda x: (x.get("days_to_expiry", 999), -x.get("volume_24h", 0)))
            print(f"[🐦 SENTIMENT] Targets (≤3d, vol≥{self.MIN_VOLUME}): {len(targets)}")

            # ONE batch AI call for all targets — no per-market rate limit issues
            signals = await self._batch_sentiment_signals(targets[:self.MAX_MARKETS])

        except Exception as e:
            print(f"[🐦 SENTIMENT ERROR] {e}")
            import traceback; traceback.print_exc()

        signals.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))
        print(f"[🐦 GROK-X] Done. {len(signals)} signals\n")
        return signals



    async def _groq_sentiment_signal(
        self, market: Dict, liq: Dict
    ) -> Optional[Signal]:
        # Deprecated — use batch method instead
        return None

    async def _grok_sentiment_signal(
        self, market: Dict, liq: Dict, key: str
    ) -> Optional[Signal]:
        # Deprecated — use batch method instead
        return None

    async def _batch_sentiment_signals(
        self, targets: List[Dict]
    ) -> List[Signal]:
        """
        Single AI call to analyze all target markets for sentiment signals.
        Returns list of Signal objects.
        Works with Cloudflare, Groq, or xAI — whichever is available.
        """
        signals: List[Signal] = []
        if not targets:
            return signals

        lines = []
        for i, m in enumerate(targets):
            liq = LiquidityFilter.check(m)
            if not liq:
                continue
            price = liq["best_bid"]
            days = m.get("days_to_expiry", 0)
            lines.append(f"{i+1}. [{price:.0%} YES, {days:.1f}d] {m['name']}")
            m["_liq"] = liq

        if not lines:
            return signals

        markets_text = "\n".join(lines)
        prompt = f"""You are a prediction market analyst with knowledge of current events, news, and public sentiment.

For each market below, determine if the outcome is LIKELY based on recent news and public knowledge.
Only include markets where you have CLEAR knowledge pointing to an outcome.

Markets (current YES price and days to expiry shown):
{markets_text}

For markets where you know the likely outcome, respond with JSON array:
[{{"market": <number>, "outcome": "YES" or "NO", "confidence": <60-90>, "reason": "<one sentence>"}}]

Rules:
- Only include markets where evidence is CLEAR (confidence 60+)
- Do NOT include markets where outcome is genuinely uncertain
- confidence 60-70: some evidence; 70-80: clear lean; 80-90: strong evidence

Respond with JSON array only. If no markets qualify, respond with []"""

        result_text = await call_ai(
            prompt,
            system="You are a precise prediction market analyst. Respond only with valid JSON array.",
            temperature=0.0
        )

        if not result_text:
            print("[🐦 SENTIMENT] No AI response")
            return signals

        result_text = _normalize_ai_response(result_text)

        try:
            clean = result_text.strip().strip("```json").strip("```").strip()
            match = re.search(r'\[.*\]', clean, re.DOTALL)
            items = json_module.loads(match.group() if match else clean)
        except Exception as e:
            print(f"[🐦 SENTIMENT] Parse error: {e} | text={result_text[:150]}")
            return signals

        for item in items:
            try:
                idx = int(item["market"]) - 1
                if not (0 <= idx < len(targets)):
                    continue
                market = targets[idx]
                liq = market.get("_liq") or LiquidityFilter.check(market)
                if not liq:
                    continue
                price = liq["best_bid"]
                outcome = str(item.get("outcome", "YES")).upper()
                if outcome not in ("YES", "NO"):
                    continue
                conf = float(item.get("confidence", 0))
                if conf < 60:
                    continue
                exp_profit = _calc_roi(price, outcome)
                if exp_profit < 0.5:
                    continue
                reason = item.get("reason", "")
                print(f"[🐦 SENTIMENT] ✅ {market['name'][:45]} | {outcome} | conf={conf:.0f}%")
                signals.append(Signal(
                    strategy="🐦 X Sentiment",
                    market_id=market["id"],
                    market_name=market["name"],
                    current_price=price,
                    expected_outcome=outcome,
                    confidence=round(min(conf, 88), 1),
                    expected_profit=round(exp_profit, 2),
                    liquidity_volume_24h=liq["volume_24h"],
                    bid_ask_spread=liq["spread_pct"],
                    source_url="",
                    polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                    timestamp=datetime.now(),
                    end_date=market.get("end_date"),
                    details={
                        "sentiment": "AI_BATCH",
                        "evidence": reason,
                        "source": "AI Knowledge Batch",
                        "days_to_expiry": market.get("days_to_expiry"),
                    }
                ))
            except Exception as e:
                print(f"[🐦 SENTIMENT] Item error: {e}")

        return signals

    def _parse_sentiment(self, text, market: Dict, liq: Dict) -> Optional[Signal]:
        # Legacy method — kept for compatibility but not used in batch mode
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# STRATEGY 6: Official Data — Scheduled Events
# Logic: Monitor public APIs for scheduled government/economic releases.
# When a release directly answers a Polymarket question = near-certain signal.
# Sources: US BLS (jobs), Fed calendar, WHO, AP Elections.
# Winrate driver: official data directly resolves markets — no interpretation needed.
# ═══════════════════════════════════════════════════════════════════════════════

class OfficialDataStrategy:
    """
    Checks public official data endpoints for events that directly resolve markets.
    Free, no API key needed.
    """

    # Public economic calendar — BLS, Fed, etc.
    FRED_RELEASES_URL = "https://api.stlouisfed.org/fred/releases/dates"
    # AP Elections (free public results feed)
    AP_ELECTIONS_URL  = "https://api.ap.org/v2/elections"

    # Keyword → likely market category mapping
    ECONOMIC_KEYWORDS = {
        "unemployment": ["unemployment", "jobs", "nonfarm", "payroll"],
        "inflation":    ["inflation", "cpi", "consumer price", "pce"],
        "fed_rate":     ["federal reserve", "fed rate", "fomc", "interest rate"],
        "gdp":          ["gdp", "gross domestic", "economic growth"],
    }

    async def get_signals(self) -> List[Signal]:
        signals: List[Signal] = []
        print("\n[🏛️ OFFICIAL] Starting official data scan...")

        try:
            all_markets = await fetch_all_polymarket_markets_cached()
            tradeable = []
            for m in all_markets:
                liq = LiquidityFilter.check(m)
                if not liq or liq["volume_24h"] < 5000:
                    continue
                m["_liq"] = liq
                tradeable.append(m)

            # Fetch recent major data releases from FRED (free, no key needed for dates)
            releases = await self._fetch_recent_releases()
            if releases:
                new_sigs = self._match_releases_to_markets(releases, tradeable)
                signals.extend(new_sigs)

        except Exception as e:
            print(f"[🏛️ OFFICIAL ERROR] {e}")

        signals.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),
            -s.confidence
        ))
        print(f"[🏛️ OFFICIAL] Done. {len(signals)} signals\n")
        return signals

    async def _fetch_recent_releases(self) -> List[Dict]:
        """
        Fetch upcoming economic/data release dates.
        Primary: FRED API (needs key) → Fallback: hard-coded recurring schedule.
        """
        releases = []

        # Try FRED if API key is set
        fred_key = os.getenv("FRED_API_KEY", "")
        if fred_key:
            try:
                params = {
                    "api_key":       fred_key,
                    "realtime_start": datetime.utcnow().strftime("%Y-%m-%d"),
                    "realtime_end":   (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d"),
                    "file_type":      "json",
                }
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as session:
                    async with session.get(self.FRED_RELEASES_URL, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            releases = data.get("release_dates", [])
                            print(f"[🏛️ OFFICIAL] FRED: {len(releases)} upcoming releases")
            except Exception as e:
                print(f"[🏛️ OFFICIAL] FRED error: {e}")

        # Always supplement with known recurring schedule
        # These major releases happen on predictable dates every month
        now = datetime.utcnow()
        month = now.month
        year  = now.year

        scheduled = [
            {"release_name": "nonfarm payroll employment",        "date": f"{year}-{month:02d}-04"},
            {"release_name": "unemployment rate jobs report",     "date": f"{year}-{month:02d}-04"},
            {"release_name": "consumer price index inflation cpi","date": f"{year}-{month:02d}-10"},
            {"release_name": "federal reserve fomc interest rate","date": f"{year}-{month:02d}-19"},
            {"release_name": "gdp gross domestic product growth", "date": f"{year}-{month:02d}-25"},
            {"release_name": "pce inflation personal consumption","date": f"{year}-{month:02d}-28"},
        ]
        releases.extend(scheduled)
        print(f"[🏛️ OFFICIAL] Total releases (FRED + scheduled): {len(releases)}")
        return releases

    def _match_releases_to_markets(
        self, releases: List[Dict], markets: List[Dict]
    ) -> List[Signal]:
        """Match upcoming data releases to Polymarket questions by keywords."""
        signals = []
        for release in releases[:20]:
            release_name = release.get("release_name", "").lower()
            release_date = release.get("date", "")

            matched_category = None
            for cat, keywords in self.ECONOMIC_KEYWORDS.items():
                if any(kw in release_name for kw in keywords):
                    matched_category = cat
                    break
            if not matched_category:
                continue

            # Find markets that mention this category
            for market in markets:
                name_lower = market["name"].lower()
                keywords   = self.ECONOMIC_KEYWORDS[matched_category]
                if not any(kw in name_lower for kw in keywords):
                    continue

                days = market.get("days_to_expiry")
                if days is None or days > 3:
                    continue

                liq   = market["_liq"]
                price = liq["best_bid"]

                # Official data signal: release answers the question directly
                # Confidence based on how directly the keywords match
                keyword_hits = sum(1 for kw in keywords if kw in name_lower)
                confidence   = min(65.0 + keyword_hits * 8, 80.0)

                # We don't know the actual value yet — signal is "watch this market"
                # Direction: if price is extreme, official data will likely confirm
                if price >= 0.75:
                    outcome = "YES"
                    exp_profit = _calc_roi(price, "YES")
                elif price <= 0.25:
                    outcome = "NO"
                    exp_profit = _calc_roi(price, "NO")
                else:
                    continue  # can't determine direction without actual data value

                if exp_profit < 0.5:  # logic-level min ROI floor
                    continue

                print(f"[🏛️ OFFICIAL] ✅ {market['name'][:45]} | {outcome} | "
                      f"release={release_name[:30]} | conf={confidence:.0f}%")

                signals.append(Signal(
                    strategy="🏛️ Официальные данные",
                    market_id=market["id"],
                    market_name=market["name"],
                    current_price=price,
                    expected_outcome=outcome,
                    confidence=round(confidence, 1),
                    expected_profit=round(exp_profit, 2),
                    liquidity_volume_24h=liq["volume_24h"],
                    bid_ask_spread=liq["spread_pct"],
                    source_url="https://fred.stlouisfed.org/releases",
                    polymarket_url=f"https://polymarket.com/event/{market['slug']}",
                    timestamp=datetime.now(),
                    end_date=market.get("end_date"),
                    details={
                        "release_name": release_name,
                        "release_date": release_date,
                        "category":     matched_category,
                    }
                ))

                if len(signals) >= 3:
                    return signals

        return signals


# ═══════════════════════════════════════════════════════════════════════════════
# Main Signal Generator
# ═══════════════════════════════════════════════════════════════════════════════

class SignalGenerator:
    """Combines all six strategies and returns deduplicated, sorted signals."""

    def __init__(self):
        self.uma_strategy       = UMAOracleStrategy()
        self.news_strategy      = NewsStrategy()
        self.metaculus_strategy = MetaculusStrategy()
        self.orderbook_strategy = OrderBookStrategy()
        self.grok_sentiment     = GrokSentimentStrategy()
        self.official_strategy  = OfficialDataStrategy()

    async def generate_all_signals(self) -> List[Signal]:
        print("\n" + "=" * 60)
        print("[🚀 SIGNAL GENERATOR] Full scan — 6 strategies")
        print(f"[🚀] Filters: MinVol=${config.MIN_VOLUME_24H:,.0f} | "
              f"MaxSpread={config.MAX_SPREAD_PERCENT:.0f}% | "
              f"MinProfit={config.MIN_PROFIT_PERCENT:.1f}%")
        print("=" * 60)

        try:
            result = await asyncio.wait_for(
                self._generate_all_signals_impl(),
                timeout=600  # 10 minute hard cap
            )
            return result
        except asyncio.TimeoutError:
            print("[🚀] SCAN TIMEOUT (10 min) — returning partial results")
            clear_market_cache()
            return []
        except Exception as e:
            print(f"[🚀] SCAN ERROR: {e}")
            clear_market_cache()
            return []

    async def _generate_all_signals_impl(self) -> List[Signal]:
        print("\n" + "=" * 60)
        print("[🚀 SIGNAL GENERATOR] Full scan — 6 strategies")
        print(f"[🚀] Filters: MinVol=${config.MIN_VOLUME_24H:,.0f} | "
              f"MaxSpread={config.MAX_SPREAD_PERCENT:.0f}% | "
              f"MinProfit={config.MIN_PROFIT_PERCENT:.1f}%")
        print("=" * 60)

        # Pre-warm the cache so all strategies share one fetch
        print("[🚀] Pre-warming market cache...")
        await fetch_all_polymarket_markets_cached()

        # Wave 1: fast, non-AI strategies run in parallel (no Groq calls)
        print("[🚀] Wave 1: fast strategies (UMA, OrderBook, Official)...")
        wave1 = await asyncio.gather(
            self.uma_strategy.get_signals(),
            self.orderbook_strategy.get_signals(),
            self.official_strategy.get_signals(),
            return_exceptions=True,
        )

        # Wave 2: AI strategies run SEQUENTIALLY to share Groq rate limit
        # Each has its own timeout to prevent one slow strategy from blocking others
        print("[🚀] Wave 2: AI strategies (Metaculus, News, X Sentiment) — sequential...")

        async def _safe_run(coro, name: str, timeout: int):
            try:
                return await asyncio.wait_for(coro, timeout=timeout)
            except asyncio.TimeoutError:
                print(f"[🚀] {name} TIMEOUT ({timeout}s) — skipped")
                return []
            except Exception as e:
                print(f"[🚀] {name} ERROR: {e}")
                return []

        meta_result  = await _safe_run(self.metaculus_strategy.get_signals(), "Metaculus", 240)
        news_result  = await _safe_run(self.news_strategy.get_signals(), "News", 180)
        xsent_result = await _safe_run(self.grok_sentiment.get_signals(), "X Sentiment", 120)

        wave2 = [meta_result, news_result, xsent_result]

        results = list(wave1) + wave2
        names = ["UMA Oracle", "Order Book", "Official Data", "Metaculus", "News", "X Sentiment"]

        all_signals: List[Signal] = []
        for result, name in zip(results, names):
            if isinstance(result, Exception):
                print(f"[🚀] {name} FAILED: {result}")
            elif isinstance(result, list):
                all_signals.extend(result)
                print(f"[🚀] {name}: {len(result)} signals")

        # Free cached market list immediately — largest memory block
        clear_market_cache()

        # Deduplicate by market_id — keep highest confidence signal per market
        seen_markets: dict = {}
        for sig in all_signals:
            mid = sig.market_id
            if mid not in seen_markets or sig.confidence > seen_markets[mid].confidence:
                seen_markets[mid] = sig
        unique = list(seen_markets.values())

        # Global profit filter — apply MIN_PROFIT_PERCENT for all strategies
        # Exception: UMA Oracle keeps tiny-profit signals (near-zero risk near expiry)
        min_profit = config.MIN_PROFIT_PERCENT
        filtered = []
        for sig in unique:
            if "UMA" in sig.strategy:
                # UMA: lower floor (0.3%) — near-expiry certainty compensates low ROI
                if sig.expected_profit >= 0.3:
                    filtered.append(sig)
            else:
                if sig.expected_profit >= min_profit:
                    filtered.append(sig)
        unique = filtered

        # Global sort: soonest expiry FIRST → then confidence descending
        # This ensures users see the most time-sensitive signals at the top
        unique.sort(key=lambda s: (
            _days_sort_key_str(s.end_date),   # primary: days to expiry ASC
            -s.confidence                      # secondary: confidence DESC
        ))

        print(f"\n[🚀] TOTAL UNIQUE SIGNALS: {len(unique)}")
        for i, s in enumerate(unique[:7], 1):
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
