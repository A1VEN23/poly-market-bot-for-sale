"""
Polymarket Signal Bot — Web Service entry point (Render / HF Spaces).

FastAPI app listens on PORT from environment.
Telegram bot runs via asyncio.create_task inside the same event loop as FastAPI.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response


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
    return {"status": "ok", "bot": "running" if _bot_task and not _bot_task.done() else "stopped"}
