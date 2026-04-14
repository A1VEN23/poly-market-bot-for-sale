"""
Polymarket Signal Bot — Hugging Face Spaces entry point.

FastAPI app on port 7860 (required by HF Spaces).
Telegram bot runs in a separate thread with its own asyncio event loop.
"""
import asyncio
import os
import threading

from fastapi import FastAPI

# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="Polik Bott")


@app.get("/")
def health():
    return {"status": "running", "bot": "polik-bott"}


# ── Telegram bot in background thread ─────────────────────────────────────────

def _run_bot():
    """Run the aiogram bot polling loop in a dedicated event loop."""
    from telegram_bot import dp, bot, on_startup, on_shutdown, signal_check_loop

    async def _main():
        print("=" * 50)
        print("Polymarket Signal Bot Starting...")
        print("=" * 50)

        await on_startup(bot)

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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_main())
    except Exception as e:
        print(f"[BOT ERROR] {e}")
    finally:
        loop.close()


# Start bot thread at import time (uvicorn imports this module once)
_bot_thread = threading.Thread(target=_run_bot, daemon=True, name="telegram-bot")
_bot_thread.start()
print("[MAIN] Telegram bot thread started")
