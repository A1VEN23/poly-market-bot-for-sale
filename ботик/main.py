"""
Polymarket Signal Bot - Main Entry Point

A production-ready Telegram bot providing trading signals for Polymarket
using three strategies: UMA Oracle monitoring, AI news analysis, and Metaculus consensus.

Usage:
    python main.py

Environment:
    Requires .env file with BOT_TOKEN
"""
import asyncio
import signal
import sys

from telegram_bot import dp, bot, on_startup, on_shutdown, signal_check_loop


async def main():
    """Main entry point."""
    print("=" * 50)
    print("Polymarket Signal Bot Starting...")
    print("=" * 50)

    # Initialize database
    await on_startup(bot)

    # Create background tasks
    signal_task = asyncio.create_task(signal_check_loop())

    try:
        # Start polling
        print("Starting Telegram polling...")
        print("Tip: If you see 'TelegramConflictError', stop other bot instances:")
        print("  Windows: taskkill /F /IM python.exe")
        print("  Linux:   pkill -f 'python main.py'")
        await dp.start_polling(bot)
    finally:
        # Cancel background tasks
        signal_task.cancel()
        try:
            await signal_task
        except asyncio.CancelledError:
            pass

        await on_shutdown(bot)
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
