"""
Polymarket Signal Bot — Web Service entry point (Render).

Telegram бот работает через WEBHOOK — Telegram сам шлёт запросы на наш сервер.
Это значит сервер всегда активен (есть входящий трафик) и не засыпает.

Эндпоинты:
  POST /telegram          ← Telegram шлёт сюда обновления
  GET  /health            ← healthcheck для Render
  GET  /ping              ← лёгкий пинг
  POST /cryptobot/webhook ← оплаты от CryptoBot
"""
from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException
from aiogram.types import Update


# ── Конфиг ───────────────────────────────────────────────────────────────────
_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN", "")

# URL вашего сервиса на Render, например: https://polik-bott.onrender.com
# Обязательно задайте эту переменную в Environment на Render!
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")

WEBHOOK_PATH = "/telegram"
WEBHOOK_URL  = f"{RENDER_EXTERNAL_URL}{WEBHOOK_PATH}" if RENDER_EXTERNAL_URL else ""

if not _token:
    print("=" * 60)
    print("ERROR: TELEGRAM_TOKEN (или BOT_TOKEN) не задан!")
    print("=" * 60)

if not RENDER_EXTERNAL_URL:
    print("=" * 60)
    print("WARNING: RENDER_EXTERNAL_URL не задан — вебхук не будет установлен!")
    print("Добавьте переменную в Environment на Render.")
    print("=" * 60)


# ── Lifespan ──────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(application: FastAPI):
    if not _token:
        print("[BOT] Нет токена — бот не запущен.")
        yield
        return

    from telegram_bot import dp, bot, on_startup, on_shutdown, signal_check_loop

    print("=" * 50)
    print("Polymarket Signal Bot Starting (WEBHOOK mode)...")
    print("=" * 50)

    await on_startup(bot)

    # Устанавливаем вебхук в Telegram
    if WEBHOOK_URL:
        await bot.set_webhook(
            url=WEBHOOK_URL,
            drop_pending_updates=True,   # игнорируем накопившиеся обновления
            allowed_updates=dp.resolve_used_update_types(),
        )
        print(f"[WEBHOOK] Установлен: {WEBHOOK_URL}")
    else:
        print("[WEBHOOK] RENDER_EXTERNAL_URL не задан, вебхук не установлен!")

    # Фоновый loop для проверки сигналов и платежей
    signal_task = asyncio.create_task(signal_check_loop())
    print("[MAIN] Фоновый loop запущен")

    yield  # FastAPI работает, принимает запросы

    # ── Shutdown ──────────────────────────────────────────────────────────────
    signal_task.cancel()
    try:
        await signal_task
    except asyncio.CancelledError:
        pass

    # Удаляем вебхук при остановке (необязательно, но аккуратно)
    try:
        await bot.delete_webhook()
        print("[WEBHOOK] Удалён")
    except Exception:
        pass

    await on_shutdown(bot)
    try:
        await bot.session.close()
    except Exception:
        pass
    print("[MAIN] Бот остановлен")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Polik Bott", lifespan=lifespan)


# ── Главный эндпоинт: сюда Telegram шлёт все обновления ──────────────────────
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Принимает Update от Telegram и передаёт в диспетчер aiogram."""
    from telegram_bot import dp, bot
    body = await request.body()
    try:
        update = Update.model_validate_json(body)
    except Exception as e:
        print(f"[WEBHOOK] Ошибка парсинга Update: {e}")
        return Response(status_code=200)  # всегда 200, иначе Telegram будет повторять
    await dp.feed_update(bot=bot, update=update)
    return Response(status_code=200)


# ── Health / ping ─────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "mode": "webhook"}


@app.head("/")
async def root_head():
    return Response(status_code=200)


@app.get("/ping")
async def ping():
    return Response(content="pong", media_type="text/plain")


@app.get("/health")
async def health_check():
    webhook_set = bool(WEBHOOK_URL)
    return {
        "status": "ok",
        "mode": "webhook",
        "webhook_url": WEBHOOK_URL if webhook_set else "NOT SET",
    }


# ── CryptoBot Webhook ─────────────────────────────────────────────────────────
@app.post("/cryptobot/webhook")
async def cryptobot_webhook(request: Request):
    """
    Получает уведомления об оплате от CryptoBot.
    """
    from crypto_payments import verify_webhook, parse_webhook_payload, remove_pending_invoice, VIP_PLANS
    from config import config

    if not config.CRYPTO_BOT_TOKEN:
        raise HTTPException(status_code=503, detail="CryptoBot не настроен")

    body_bytes = await request.body()
    received_hash = request.headers.get("crypto-pay-api-signature", "")

    if not received_hash:
        raise HTTPException(status_code=400, detail="Нет подписи")

    if not verify_webhook(body_bytes, config.CRYPTO_BOT_TOKEN, received_hash):
        print("[WEBHOOK] Неверная подпись — отклонено")
        raise HTTPException(status_code=403, detail="Неверная подпись")

    try:
        body = json.loads(body_bytes)
    except Exception:
        raise HTTPException(status_code=400, detail="Неверный JSON")

    data = parse_webhook_payload(body)
    if not data:
        return {"ok": True}

    telegram_id = data["telegram_id"]
    days        = data["days"]
    plan_key    = data["plan_key"]
    invoice_id  = data["invoice_id"]

    print(f"[WEBHOOK] Оплата: user={telegram_id}, days={days}, invoice={invoice_id}")

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
            print(f"[WEBHOOK] Ошибка уведомления: {e}")

    return {"ok": True}
