"""
Polymarket Signal Bot — Web Service entry point (Render).

FastAPI listens on PORT.
Telegram bot runs as an asyncio task inside the same event loop.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response

_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN", "")
if not _token:
    print("=" * 60)
    print("WARNING: TELEGRAM_TOKEN is not set — bot will not start.")
    print("=" * 60)

_bot_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(application: FastAPI):
    global _bot_task

    if not _token:
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

    yield  # FastAPI serves requests here

    if _bot_task and not _bot_task.done():
        _bot_task.cancel()
        try:
            await _bot_task
        except asyncio.CancelledError:
            pass
    print("[MAIN] Bot stopped")


app = FastAPI(title="Polymarket Signal Bot", lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "ok"}


@app.head("/")
async def root_head():
    return Response(status_code=200)


@app.get("/health")
async def health():
    bot_ok = _bot_task is not None and not _bot_task.done()
    return {
        "status": "ok",
        "bot":    "running" if bot_ok else "stopped",
    }


@app.head("/health")
async def health_head():
    return Response(status_code=200)
