"""Configuration for Polymarket Signal Bot."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    """Bot configuration."""
    BOT_TOKEN: str = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN", "")
    METACULUS_API_KEY: str = os.getenv("METACULUS_API_KEY", "")
    POLYGON_RPC_URLS: list = None  # Set in __post_init__
    ADMIN_IDS: list = None          # Set in __post_init__
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "polymarket_bot.db")

    # ── Liquidity & signal filters ────────────────────────────────────────────
    MIN_VOLUME_24H: float = float(os.getenv("MIN_VOLUME_24H", "1000"))
    MAX_SPREAD_PERCENT: float = float(os.getenv("MAX_SPREAD_PERCENT", "30.0"))
    MIN_PROFIT_PERCENT: float = float(os.getenv("MIN_PROFIT_PERCENT", "1.5"))

    # ── Superuser — always has VIP ────────────────────────────────────────────
    SUPERUSER_ID: int = 1192740493

    # ── VIP pricing ───────────────────────────────────────────────────────────
    VIP_PRICE_30_DAYS: float = 10.0
    VIP_PRICE_90_DAYS: float = 25.0
    VIP_PRICE_180_DAYS: float = 50.0
    VIP_PRICE_365_DAYS: float = 100.0

    # ── Cloudflare Workers AI (ОСНОВНОЙ AI — бесплатно, без RPM лимитов) ────────
    # 10 000 neurons/day бесплатно, нет лимита запросов в минуту
    # Получить: https://dash.cloudflare.com → Workers AI → API Tokens
    CF_ACCOUNT_ID: str = os.getenv("CF_ACCOUNT_ID", "")
    CF_API_TOKEN: str = os.getenv("CF_API_TOKEN", "")

    # ── Groq API (РЕЗЕРВ: console.groq.com) ──────────────────────────────────
    # Модель: llama-3.3-70b-versatile | Лимит: 14400 запросов/день
    GROQ_API_KEY: str = os.getenv("GROQ_API_KEY", "")

    # ── xAI Grok API (резерв) ────────────────────────────────────────────────────
    XAI_API_KEY: str = os.getenv("XAI_API_KEY", "")

    # ── Gemini (оставлено для совместимости, больше не primary) ─────────────────
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")

    # ── Crypto Bot (t.me/CryptoBot) ───────────────────────────────────────────
    # Получи токен: написать @CryptoBot → /pay → создать приложение
    CRYPTO_BOT_TOKEN: str = os.getenv("CRYPTO_BOT_TOKEN", "")
    # Валюта для оплаты: USDT, TON, BTC
    CRYPTO_BOT_CURRENCY: str = os.getenv("CRYPTO_BOT_CURRENCY", "USDT")

    def __post_init__(self):
        # Parse ADMIN_IDS
        if self.ADMIN_IDS is None:
            admin_ids_str = os.getenv("ADMIN_IDS", "")
            ids = []
            if admin_ids_str:
                ids = [int(x.strip()) for x in admin_ids_str.split(",") if x.strip().isdigit()]
            if self.SUPERUSER_ID not in ids:
                ids.append(self.SUPERUSER_ID)
            object.__setattr__(self, "ADMIN_IDS", ids)

        # Set RPC URLs
        if self.POLYGON_RPC_URLS is None:
            rpc_from_env = os.getenv("POLYGON_RPC_URL", "")
            if rpc_from_env:
                rpc_urls = [rpc_from_env]
            else:
                rpc_urls = [
                    "https://1rpc.io/matic",
                    "https://polygon-rpc.com",
                    "https://rpc-mainnet.maticvigil.com",
                ]
            object.__setattr__(self, "POLYGON_RPC_URLS", rpc_urls)


config = Config()
