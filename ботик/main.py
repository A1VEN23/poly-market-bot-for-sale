"""
Polymarket Signal Bot - Main Entry Point
"""
import asyncio
import sys

from telegram_bot import dp, bot, on_startup, on_shutdown, signal_check_loop


async def main():
    print("=" * 50)
    print("Polymarket Signal Bot Starting...")
    print("=" * 50)

    await on_startup(bot)
    signal_task = asyncio.create_task(signal_check_loop())

    try:
        print("Starting Telegram polling...")
        print("Tip: If you see 'TelegramConflictError', stop other bot instances:")
        print("  Windows: taskkill /F /IM python.exe")
        print("  Linux:   pkill -f 'python main.py'")
        await dp.start_polling(bot, drop_pending_updates=True)
    except Exception as e:
        print(f"[POLLING ERROR] {e}")
        import traceback
        traceback.print_exc()
    finally:
        signal_task.cancel()
        try:
            await signal_task
        except asyncio.CancelledError:
            pass
        await on_shutdown(bot)
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
        sys.exit(0)
    except Exception as e:
        import traceback
        print(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)