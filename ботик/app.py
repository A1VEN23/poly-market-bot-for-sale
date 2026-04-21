"""
Polymarket Signal Bot — Web Service entry point (Render / HF Spaces).

FastAPI app listens on PORT from environment.
Telegram bot runs via asyncio.create_task inside the same event loop as FastAPI.
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException


# ── Token validation ─────────────────────────────────────────────────────────
_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN", "")
if not _token:
    print("=" * 60)
    print("ERROR: TELEGRAM_TOKEN (or BOT_TOKEN) is not set!")
    print("  Set it as an environment variable before starting.")
    print("=" * 60)


# ── Lifespan: start/stop bot alongside FastAPI ───────────────────────────────
_bot_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(application: FastAPI):
    global _bot_task

    if not _token:
        print("[BOT] Skipping bot start — no TELEGRAM_TOKEN provided.")
        yield
        return

    from telegram_bot import dp, bot, on_startup, on_shutdown, signal_check_loop

    print("=" * 50)
    print("Polymarket Signal Bot Starting...")
    print("=" * 50)

    await on_startup(bot)

    async def _bot_main():
        signal_task = asyncio.create_task(signal_check_loop())
        try:
            print("Starting Telegram polling...")
            await dp.start_polling(bot)
        finally:
            signal_task.cancel()
            try:
                await signal_task
            except asyncio.CancelledError:
                pass
            await on_shutdown(bot)
            await bot.session.close()

    _bot_task = asyncio.create_task(_bot_main())
    print("[MAIN] Telegram bot task started")

    yield  # FastAPI is running

    # Shutdown
    if _bot_task and not _bot_task.done():
        _bot_task.cancel()
        try:
            await _bot_task
        except asyncio.CancelledError:
            pass
    print("[MAIN] Bot stopped")


# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(title="Polik Bott", lifespan=lifespan)


@app.get("/")
async def health(request: Request):
    return {"status": "ok"}


@app.head("/")
async def health_head(request: Request):
    return Response(status_code=200)


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "bot": "running" if _bot_task and not _bot_task.done() else "stopped",
    }


# ── CryptoBot Webhook ─────────────────────────────────────────────────────────
@app.post("/cryptobot/webhook")
async def cryptobot_webhook(request: Request):
    """
    Receives payment notifications from CryptoBot.
    CryptoBot sends POST when invoice is paid.
    Header: crypto-pay-api-signature (HMAC-SHA256 signed with token)
    """
    from crypto_payments import verify_webhook, parse_webhook_payload, remove_pending_invoice, VIP_PLANS
    from config import config

    if not config.CRYPTO_BOT_TOKEN:
        raise HTTPException(status_code=503, detail="CryptoBot not configured")

    body_bytes = await request.body()
    received_hash = request.headers.get("crypto-pay-api-signature", "")

    if not received_hash:
        raise HTTPException(status_code=400, detail="Missing signature header")

    if not verify_webhook(body_bytes, config.CRYPTO_BOT_TOKEN, received_hash):
        print("[WEBHOOK] Invalid signature — rejected")
        raise HTTPException(status_code=403, detail="Invalid signature")

    try:
        body = json.loads(body_bytes)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    data = parse_webhook_payload(body)
    if not data:
        return {"ok": True}

    telegram_id = data["telegram_id"]
    days = data["days"]
    plan_key = data["plan_key"]
    invoice_id = data["invoice_id"]

    print(f"[WEBHOOK] Payment confirmed: user={telegram_id}, days={days}, invoice={invoice_id}")

    from database import db

    success = await db.add_vip(telegram_id, config.SUPERUSER_ID, days)
    remove_pending_invoice(invoice_id)

    if success:
        days_label = VIP_PLANS.get(plan_key, (days, 0, f"{days} дней"))[2]

        try:
            from telegram_bot import bot
            from aiogram.utils.markdown import hbold

            await bot.send_message(
                telegram_id,
                f"🎉 {hbold('Оплата получена! Доступ открыт!')}\n\n"
                f"✅ Тариф: {days_label} ({days} дней)\n\n"
                f"Нажмите /start чтобы войти в бота.",
            )

            # Referral bonus
            referrer_id = await db.get_referrer(telegram_id)
            if referrer_id and referrer_id != config.SUPERUSER_ID:
                bonus_granted = await db.pay_referral_bonus(referrer_id, telegram_id)
                if bonus_granted:
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🎁 {hbold('+7 дней к подписке!')}\n\n"
                            f"Ваш реферал оформил VIP — бонус начислен автоматически.\n\n"
                            f"Приводите ещё! /start → 👥 Реферальная программа",
                        )
                    except Exception:
                        pass

            # Notify superuser
            try:
                await bot.send_message(
                    config.SUPERUSER_ID,
                    f"💰 {hbold('Новая оплата (webhook)!')}\n\n"
                    f"Пользователь: {telegram_id}\n"
                    f"Тариф: {days_label}\n"
                    f"Invoice: {invoice_id}",
                )
            except Exception:
                pass

        except Exception as e:
            print(f"[WEBHOOK] Notification error: {e}")

    return {"ok": True}