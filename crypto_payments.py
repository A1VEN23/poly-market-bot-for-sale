"""
CryptoBot payment integration for Polymarket Signal Bot.

Uses the official Crypto Pay API (t.me/CryptoBot).
Docs: https://help.crypt.bot/crypto-pay-api

HOW TO GET TOKEN:
1. Open @CryptoBot in Telegram
2. Send /pay
3. Create new app → get API token
4. Add to .env: CRYPTO_BOT_TOKEN=your_token_here
"""

import aiohttp
import asyncio
import json
import hmac
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any

from config import config

CRYPTO_BOT_API = "https://pay.crypt.bot/api"

# Поддерживаемые валюты через CryptoBot
SUPPORTED_CURRENCIES = ["USDT_BSC", "USDT_TON", "USDT_ARB", "TON", "ETH", "LTC", "SOL"]

CURRENCY_LABELS = {
    "USDT_BSC": "💵 USDT (BNB)",
    "USDT_TON": "💵 USDT (TON)",
    "USDT_ARB": "💵 USDT (ARB)",
    "TON":      "💎 TON",
    "ETH":      "🔷 ETH",
    "LTC":      "⚪ LTC",
    "SOL":      "🟣 SOL",
}

# CryptoBot asset codes (API принимает эти значения)
CURRENCY_ASSETS = {
    "USDT_BSC": "USDT",
    "USDT_TON": "USDT",
    "USDT_ARB": "USDT",
    "TON":      "TON",
    "ETH":      "ETH",
    "LTC":      "LTC",
    "SOL":      "SOL",
}

# VIP тарифы: callback_data → (дни, цена, метка)
VIP_PLANS: Dict[str, tuple] = {
    "vip_30":  (30,  config.VIP_PRICE_30_DAYS,  "1 месяц"),
    "vip_90":  (90,  config.VIP_PRICE_90_DAYS,  "3 месяца"),
    "vip_180": (180, config.VIP_PRICE_180_DAYS, "6 месяцев"),
    "vip_365": (365, config.VIP_PRICE_365_DAYS, "1 год"),
}


async def create_invoice(
    telegram_id: int,
    plan_key: str,
    currency: str = None,
) -> Optional[Dict[str, Any]]:
    """
    Создаёт инвойс в CryptoBot.
    Возвращает dict с полями: invoice_id, pay_url, amount, days
    или None при ошибке.
    """
    if not config.CRYPTO_BOT_TOKEN:
        return None

    days, price, label = VIP_PLANS[plan_key]
    # Используем валюту из аргумента или по умолчанию USDT_TON
    currency = currency or "USDT_TON"
    if currency not in SUPPORTED_CURRENCIES:
        currency = "USDT_TON"
    asset = CURRENCY_ASSETS.get(currency, "USDT")

    payload = {
        "currency_type": "crypto",
        "asset": asset,
        "amount": str(price),
        "description": f"Polymarket Signals — VIP {label} ({days} дней)",
        "payload": json.dumps({
            "telegram_id": telegram_id,
            "plan_key": plan_key,
            "days": days,
        }),
        "paid_btn_name": "openBot",
        "paid_btn_url": f"https://t.me/{_get_bot_username()}",
        "allow_comments": False,
        "allow_anonymous": False,
        "expires_in": 3600,  # инвойс действителен 1 час
    }

    headers = {
        "Crypto-Pay-API-Token": config.CRYPTO_BOT_TOKEN,
        "Content-Type": "application/json",
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{CRYPTO_BOT_API}/createInvoice",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()

        if data.get("ok"):
            invoice = data["result"]
            return {
                "invoice_id": invoice["invoice_id"],
                "pay_url": invoice["pay_url"],
                "amount": price,
                "currency": currency,
                "days": days,
                "label": label,
            }
        else:
            print(f"[CRYPTOBOT] createInvoice error: {data}")
            return None

    except Exception as e:
        print(f"[CRYPTOBOT] Exception in create_invoice: {e}")
        return None


async def get_invoice_status(invoice_id: int) -> Optional[str]:
    """
    Проверяет статус инвойса.
    Возвращает: 'active' | 'paid' | 'expired' | None при ошибке.
    """
    if not config.CRYPTO_BOT_TOKEN:
        return None

    headers = {"Crypto-Pay-API-Token": config.CRYPTO_BOT_TOKEN}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{CRYPTO_BOT_API}/getInvoices",
                params={"invoice_ids": str(invoice_id)},
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                data = await resp.json()

        if data.get("ok"):
            items = data["result"].get("items", [])
            if items:
                return items[0].get("status")
        return None

    except Exception as e:
        print(f"[CRYPTOBOT] Exception in get_invoice_status: {e}")
        return None


def verify_webhook(body: bytes, secret_token: str, received_hash: str) -> bool:
    """
    Верифицирует подпись вебхука от CryptoBot.
    secret_token = SHA256 от CRYPTO_BOT_TOKEN
    """
    secret = hashlib.sha256(secret_token.encode()).digest()
    computed = hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(computed, received_hash)


def parse_webhook_payload(body: dict) -> Optional[Dict[str, Any]]:
    """
    Парсит payload вебхука от CryptoBot при оплате.
    Возвращает dict с telegram_id, plan_key, days или None.
    """
    try:
        if body.get("update_type") != "invoice_paid":
            return None

        invoice = body["payload"]
        raw_payload = invoice.get("payload", "{}")
        data = json.loads(raw_payload)

        return {
            "invoice_id": invoice["invoice_id"],
            "telegram_id": int(data["telegram_id"]),
            "plan_key": data["plan_key"],
            "days": int(data["days"]),
            "amount": invoice.get("amount"),
            "currency": invoice.get("asset"),
        }
    except Exception as e:
        print(f"[CRYPTOBOT] parse_webhook_payload error: {e}")
        return None


# ── In-memory pending invoices: {invoice_id: {telegram_id, days, created_at}} ─
_pending_invoices: Dict[int, Dict] = {}


def register_pending_invoice(invoice_id: int, telegram_id: int, days: int, plan_key: str):
    """Запоминает ожидающий инвойс для polling-проверки."""
    _pending_invoices[invoice_id] = {
        "telegram_id": telegram_id,
        "days": days,
        "plan_key": plan_key,
        "created_at": datetime.now(),
    }


def get_pending_invoice(invoice_id: int) -> Optional[Dict]:
    return _pending_invoices.get(invoice_id)


def remove_pending_invoice(invoice_id: int):
    _pending_invoices.pop(invoice_id, None)


def get_all_pending_invoices() -> Dict[int, Dict]:
    return dict(_pending_invoices)


def _get_bot_username() -> str:
    """Возвращает username бота для кнопки после оплаты."""
    import os
    return os.getenv("BOT_USERNAME", "ваш_бот")