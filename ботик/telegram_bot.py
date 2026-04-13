"""Telegram Bot implementation for Polymarket Signal Bot using aiogram 3.x."""
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command, CommandStart
from aiogram.utils.markdown import hbold, hitalic
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import config
from database import db, User
from logic import SignalGenerator, Signal


# ── Bot / dispatcher ──────────────────────────────────────────────────────────

bot = Bot(
    token=config.BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
signal_generator = SignalGenerator()

# ── In-memory signal queue (per user) ─────────────────────────────────────────
_signal_queues: Dict[int, List[Signal]] = {}

# ── Per-user settings (in-memory) ─────────────────────────────────────────────
# Structure: { telegram_id: { "min_profit": float, "min_confidence": int, "strategies": set } }
_user_settings: Dict[int, Dict] = {}
_awaiting_input: Dict[int, str] = {}

DEFAULT_SETTINGS = {
    "min_profit": 3.0,
    "min_confidence": 80,
    "strategies": {"uma", "news", "meta"},
    "max_signals": 3,  # how many signals to send per manual request
}

STRATEGY_LABELS = {
    "uma":  "🔮 UMA Оракул",
    "news": "📰 Новости",
    "meta": "🎯 Metaculus",
}


def get_user_settings(telegram_id: int) -> Dict:
    if telegram_id not in _user_settings:
        _user_settings[telegram_id] = {
            "min_profit": DEFAULT_SETTINGS["min_profit"],
            "min_confidence": DEFAULT_SETTINGS["min_confidence"],
            "strategies": set(DEFAULT_SETTINGS["strategies"]),
            "max_signals": DEFAULT_SETTINGS["max_signals"],
        }
    return _user_settings[telegram_id]


# ── Keyboards ─────────────────────────────────────────────────────────────────

def get_main_menu_keyboard(signals_enabled: bool = True, is_vip: bool = False, is_superuser: bool = False):
    if is_superuser:
        vip_btn = InlineKeyboardButton(text="👑 ПРОФИЛЬ (Developer)", callback_data="profile")
    elif is_vip:
        vip_btn = InlineKeyboardButton(text="💎 VIP АКТИВЕН — Профиль", callback_data="profile")
    else:
        vip_btn = InlineKeyboardButton(text="💎 Купить VIP", callback_data="vip_info")

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Получить сигнал", callback_data="get_signal")],
        [
            vip_btn,
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text="📚 ГАЙДЫ",    callback_data="guides"),
            InlineKeyboardButton(text="🆘 ПОДДЕРЖКА", callback_data="support"),
        ],
    ])


def get_signal_keyboard(market_name: str, source_url: str, has_more: bool = False) -> InlineKeyboardMarkup:
    rows = []

    if source_url and source_url.startswith("http"):
        rows.append([InlineKeyboardButton(text="🔗 Источник / Обоснование", url=source_url)])

    rows.append([InlineKeyboardButton(text="🔄 Другой сигнал", callback_data="next_signal")])
    rows.append([InlineKeyboardButton(text="« Главное меню", callback_data="back_to_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_vip_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 месяц — $10",   callback_data="vip_30")],
        [InlineKeyboardButton(text="3 месяца — $25",  callback_data="vip_90")],
        [InlineKeyboardButton(text="6 месяцев — $50", callback_data="vip_180")],
        [InlineKeyboardButton(text="1 год — $100",    callback_data="vip_365")],
        [InlineKeyboardButton(text="💳 Связаться для оплаты", callback_data="pay_contact")],
        [InlineKeyboardButton(text="« Назад", callback_data="back_to_menu")],
    ])


def get_profile_keyboard(is_superuser: bool = False):
    rows = []
    if not is_superuser:
        rows.append([InlineKeyboardButton(text="💎 Продлить VIP", callback_data="vip_info")])
    rows.append([InlineKeyboardButton(text="« Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_guides_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Как начать?",            callback_data="guide_start")],
        [InlineKeyboardButton(text="📊 Стратегии сигналов",     callback_data="guide_strategies")],
        [InlineKeyboardButton(text="💰 Управление рисками",     callback_data="guide_risk")],
        [InlineKeyboardButton(text="« Назад",                   callback_data="back_to_menu")],
    ])


def get_settings_keyboard(telegram_id: int, is_vip: bool) -> InlineKeyboardMarkup:
    s = get_user_settings(telegram_id)
    strats = s["strategies"]

    uma_check  = "✅" if "uma"  in strats else "⬜️"
    news_check = "✅" if "news" in strats else "⬜️"
    meta_check = "✅" if "meta" in strats else "⬜️"

    max_sig = s.get("max_signals", 3)
    rows = [
        # -- Profit section --
        [InlineKeyboardButton(
            text=f"📈 Мин. профит: {s['min_profit']:.0f}% — нажмите чтобы изменить",
            callback_data="input_profit"
        )],
        # -- Confidence section --
        [InlineKeyboardButton(
            text=f"🎯 Мин. уверенность: {s['min_confidence']}% — нажмите чтобы изменить",
            callback_data="input_conf"
        )],
    ]

    if not is_vip:
        rows.append([InlineKeyboardButton(
            text="🔒 Уверенность ниже 80% — только VIP",
            callback_data="vip_info"
        )])

    # -- Strategies section --
    rows.append([InlineKeyboardButton(
        text="📡 Стратегии  ↓ вкл/выкл:",
        callback_data="noop"
    )])
    rows.append([
        InlineKeyboardButton(text=f"{uma_check} UMA Оракул", callback_data="toggle_strat_uma"),
        InlineKeyboardButton(text=f"{news_check} Новости",   callback_data="toggle_strat_news"),
        InlineKeyboardButton(text=f"{meta_check} Metaculus", callback_data="toggle_strat_meta"),
    ])
    rows.append([InlineKeyboardButton(text="« Назад в меню", callback_data="back_to_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Command handlers ──────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user      = await db.get_or_create_user(message.from_user.id, message.from_user.username)
    is_vip    = await db.is_vip(message.from_user.id)
    is_super  = await db.is_superuser(message.from_user.id)
    days_left = await db.get_vip_days_remaining(message.from_user.id)

    if is_super:
        sub = f"👑 {hbold('Статус: Developer / Lifetime VIP')}"
    elif is_vip:
        sub = f"💎 {hbold(f'Статус: VIP активен (осталось {days_left} дн.)')}"
    else:
        sub = (
            f"⚠️ {hbold('Статус: Free')}\n"
            f"🔒 Free получают сигналы с confidence 80%+"
        )

    text = (
        f"{hbold('🚀 Добро пожаловать в Polymarket Signals Bot!')}\n\n"
        f"{sub}\n\n"
        f"Торговые сигналы на основе трёх стратегий:\n"
        f"🔮 {hbold('UMA Оракул')} — рынки близко к истечению\n"
        f"📰 {hbold('Новостной анализ')} — AI анализ новостей\n"
        f"🎯 {hbold('Metaculus Консенсус')} — прогнозы экспертов vs рынок\n\n"
        f"<i>Нажмите кнопку ниже для поиска сигнала 👇</i>"
    )
    await message.answer(
        text,
        reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_super)
    )


@dp.message(Command("addvip"))
async def cmd_addvip(message: Message):
    uid = message.from_user.id
    if uid not in config.ADMIN_IDS and uid != config.SUPERUSER_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    args = message.text.split()[1:]
    if len(args) != 2:
        await message.answer("❌ Использование: /addvip [user_id] [days]")
        return

    try:
        target_id = int(args[0])
        days      = int(args[1])
    except ValueError:
        await message.answer("❌ user_id и days должны быть числами.")
        return

    success = await db.add_vip(target_id, uid, days)
    if success:
        await message.answer(f"✅ VIP добавлен! Пользователь: {target_id}, дней: {days}")
        try:
            await bot.send_message(
                target_id,
                f"🎉 {hbold('Вам выдан VIP доступ!')}\n\nСрок: {days} дней\n"
                f"Используйте /start для обновления меню."
            )
        except Exception:
            pass
    else:
        await message.answer("❌ Не удалось. Пользователь должен написать /start боту.")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    uid = message.from_user.id
    if uid not in config.ADMIN_IDS and uid != config.SUPERUSER_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return

    stats     = await db.get_signal_stats(days=30)
    users     = await db.get_active_users()
    vip_users = await db.get_vip_users()

    text = (
        f"{hbold('📊 Статистика бота за 30 дней')}\n\n"
        f"👥 Активных пользователей: {len(users)}\n"
        f"💎 VIP пользователей: {len(vip_users)}\n\n"
        f"{hbold('Сигналы по стратегиям:')}\n"
    )
    for strategy, data in (stats or {}).items():
        text += f"• {strategy}: {data['total']} всего, {data['verified']} проверено\n"
    if not stats:
        text += "Сигналов ещё нет\n"

    await message.answer(text)


# ── Callback handlers ─────────────────────────────────────────────────────────

@dp.callback_query(F.data == "noop")
async def on_noop(callback: CallbackQuery):
    await callback.answer()


@dp.callback_query(F.data == "get_signal")
async def on_get_signal(callback: CallbackQuery):
    _awaiting_input.pop(callback.from_user.id, None)
    await callback.answer("🔍 Сканирую...")
    await check_signals_for_user(callback.from_user.id)


@dp.callback_query(F.data == "next_signal")
async def on_next_signal(callback: CallbackQuery):
    uid   = callback.from_user.id
    queue = _signal_queues.get(uid, [])

    if not queue:
        # Queue exhausted — run a fresh scan
        await callback.answer("🔄 Ищу новые сигналы...")
        await check_signals_for_user(uid)
        return

    await callback.answer("📤 Отправляю следующий сигнал...")
    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)
    signal = queue.pop(0)
    _signal_queues[uid] = queue
    await send_signal_to_user(uid, signal, is_vip or is_sup, has_more=bool(queue))


# ── Settings handlers ─────────────────────────────────────────────────────────

@dp.callback_query(F.data == "settings")
async def on_settings(callback: CallbackQuery):
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    s = get_user_settings(callback.from_user.id)
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in s["strategies"]) or "нет"
    min_profit_str = f"{s['min_profit']:.0f}%"
    min_conf_str = f"{s['min_confidence']}%"
    text = (
        f"{hbold('⚙️ Настройки сигналов')}\n\n"
        f"📈 Мин. профит: {hbold(min_profit_str)}\n"
        f"🎯 Мин. уверенность: {hbold(min_conf_str)}\n"
        f"📡 Стратегии: {strats_text}\n\n"
        f"{hitalic('Нажмите кнопку чтобы изменить:')}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=get_settings_keyboard(callback.from_user.id, is_vip or is_sup)
    )


async def _refresh_settings_message(callback: CallbackQuery, is_vip: bool):
    s = get_user_settings(callback.from_user.id)
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in s["strategies"]) or "нет"
    mp = f"{s['min_profit']:.0f}%"
    mc = f"{s['min_confidence']}%"
    await callback.message.edit_text(
        f"{hbold('⚙️ Настройки сигналов')}\n\n"
        f"📈 Мин. профит: {hbold(mp)}\n"
        f"🎯 Мин. уверенность: {hbold(mc)}\n"
        f"📡 Стратегии: {strats_text}\n\n"
        f"{hitalic('Нажмите кнопку чтобы изменить:')}",
        reply_markup=get_settings_keyboard(callback.from_user.id, is_vip)
    )


@dp.callback_query(F.data == "input_profit")
async def on_input_profit(callback: CallbackQuery):
    _awaiting_input[callback.from_user.id] = "profit"
    await callback.answer()
    await callback.message.answer(
        "📈 Введите минимальный профит в % (например: 5)\n"
        "Допустимый диапазон: 1–50"
    )


@dp.callback_query(F.data == "input_conf")
async def on_input_conf(callback: CallbackQuery):
    _awaiting_input[callback.from_user.id] = "conf"
    await callback.answer()
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    min_val = 65 if (is_vip or is_sup) else 80
    await callback.message.answer(
        f"🎯 Введите минимальную уверенность в % (например: 85)\n"
        f"Допустимый диапазон: {min_val}–99"
    )


@dp.callback_query(F.data == "input_max_signals")
async def on_input_max_signals(callback: CallbackQuery):
    _awaiting_input[callback.from_user.id] = "max_signals"
    await callback.answer()
    await callback.message.answer(
        "📦 Сколько сигналов присылать за один запрос?\n"
        "Введите число от 1 до 10 (например: 3)"
    )


@dp.message(F.text)
async def on_settings_text_input(message: Message):
    uid = message.from_user.id
    if uid not in _awaiting_input:
        return  # Not waiting for input, ignore

    input_type = _awaiting_input.pop(uid)
    text = message.text.strip().replace("%", "").replace(",", ".")

    try:
        val = float(text)
    except ValueError:
        # Put state back so user can retry
        _awaiting_input[uid] = input_type
        await message.answer("❌ Введите только число, например: 5 или 85")
        return

    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)
    s = get_user_settings(uid)

    if input_type == "profit":
        if val < 1 or val > 50:
            _awaiting_input[uid] = "profit"
            await message.answer("❌ Профит должен быть от 1 до 50%. Попробуйте снова:")
            return
        s["min_profit"] = val
        await message.answer(f"✅ Мин. профит установлен: {val:.0f}%")

    elif input_type == "conf":
        min_val = 65 if (is_vip or is_sup) else 80
        val = int(val)
        if val < min_val or val > 99:
            _awaiting_input[uid] = "conf"
            lock = " (нужен VIP для <80%)" if not (is_vip or is_sup) else ""
            await message.answer(f"❌ Уверенность должна быть от {min_val} до 99%{lock}. Попробуйте снова:")
            return
        s["min_confidence"] = val
        await message.answer(f"✅ Мин. уверенность установлена: {val}%")

    elif input_type == "max_signals":
        val = int(val)
        if val < 1 or val > 10:
            _awaiting_input[uid] = "max_signals"
            await message.answer("❌ Введите число от 1 до 10. Попробуйте снова:")
            return
        s["max_signals"] = val
        await message.answer(f"✅ Бот будет присылать {val} сигнал(а/ов) за раз")

    # Show updated settings
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in s["strategies"]) or "нет"
    settings_text = (
        f"⚙️ <b>Настройки сигналов</b>\n\n"
        f"📈 Мин. профит: <b>{s['min_profit']:.0f}%</b>\n"
        f"🎯 Мин. уверенность: <b>{s['min_confidence']}%</b>\n"
        f"📡 Стратегии: {strats_text}\n\n"
        f"<i>Нажмите кнопку чтобы изменить:</i>"
    )
    await message.answer(
        settings_text,
        reply_markup=get_settings_keyboard(uid, is_vip or is_sup)
    )


@dp.callback_query(F.data.startswith("toggle_strat_"))
async def on_toggle_strat(callback: CallbackQuery):
    strat = callback.data.split("_")[-1]  # uma / news / meta
    s = get_user_settings(callback.from_user.id)
    strats = s["strategies"]
    if strat in strats:
        if len(strats) <= 1:
            await callback.answer("⚠️ Нужна хотя бы одна стратегия!", show_alert=True)
            return
        strats.discard(strat)
        await callback.answer(f"❌ {STRATEGY_LABELS.get(strat, strat)} выключена")
    else:
        strats.add(strat)
        await callback.answer(f"✅ {STRATEGY_LABELS.get(strat, strat)} включена")

    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    await _refresh_settings_message(callback, is_vip or is_sup)


# ── Other callback handlers ───────────────────────────────────────────────────

@dp.callback_query(F.data == "profile")
async def on_profile(callback: CallbackQuery):
    profile = await db.get_user_profile(callback.from_user.id)
    if not profile:
        await callback.answer("Ошибка загрузки профиля")
        return

    is_sup = profile["is_superuser"]
    if is_sup:
        status_line = f"👑 {hbold('Статус: Developer / Lifetime')}"
        days_line   = "∞ бессрочно"
    elif profile["days_remaining"] > 0:
        status_line = f"💎 {hbold('Статус: VIP Active')}"
        days_line   = f"📅 Осталось: {profile['days_remaining']} дн."
    else:
        status_line = f"⚠️ {hbold('Статус: Free')}"
        days_line   = "🔒 Нет активной подписки"

    text = (
        f"👤 {hbold('Ваш профиль')}\n\n"
        f"🆔 ID: {profile['telegram_id']}\n"
        f"📝 Username: @{profile['username'] or 'N/A'}\n\n"
        f"{status_line}\n{days_line}\n\n"
        f"📅 Регистрация: {str(profile['created_at'])[:10]}"
    )
    await callback.message.edit_text(text, reply_markup=get_profile_keyboard(is_sup))


@dp.callback_query(F.data == "stats_view")
async def on_stats_view(callback: CallbackQuery):
    await callback.answer()
    is_sup = await db.is_superuser(callback.from_user.id)
    is_vip = await db.is_vip(callback.from_user.id)

    if is_sup or is_vip:
        stats     = await db.get_signal_stats(days=30)
        users     = await db.get_active_users()
        vip_users = await db.get_vip_users()
        text = (
            f"{hbold('📊 Статистика бота')}\n\n"
            f"👥 Активных: {len(users)}\n💎 VIP: {len(vip_users)}\n\n"
        )
        for strategy, data in (stats or {}).items():
            text += f"• {strategy}: {data['total']} сигналов\n"
        if not stats:
            text += "Сигналов пока нет\n"
    else:
        text = f"{hbold('📊 Статистика')}\n\nДля просмотра оформите VIP."

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад", callback_data="profile")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query(F.data == "vip_info")
async def on_vip_info(callback: CallbackQuery):
    is_vip    = await db.is_vip(callback.from_user.id)
    days_left = await db.get_vip_days_remaining(callback.from_user.id)

    if is_vip:
        text = (
            f"{hbold('💎 Ваш VIP статус активен!')}\n\n"
            f"📅 Осталось: {days_left if days_left > 0 else '∞'} дн.\n\n"
            f"✅ Все сигналы без ограничений\n"
            f"✅ Confidence от 65%+\n✅ Все 3 стратегии\n"
        )
    else:
        text = (
            f"{hbold('💎 VIP Доступ')}\n\n"
            f"С VIP вы получаете:\n"
            f"✅ Все сигналы (confidence 65%+)\n"
            f"✅ Все 3 стратегии\n"
            f"✅ Настройки уверенности от 65%\n"
            f"✅ Приоритетная поддержка\n\n"
            f"Без VIP: только сигналы 80%+\n\n"
            f"{hitalic('Выберите тариф и свяжитесь для оплаты')}"
        )
    await callback.message.edit_text(text, reply_markup=get_vip_keyboard())


@dp.callback_query(F.data.in_({"vip_30", "vip_90", "vip_180", "vip_365"}))
async def on_vip_select(callback: CallbackQuery):
    periods = {
        "vip_30": (30, "$10"), "vip_90": (90, "$25"),
        "vip_180": (180, "$50"), "vip_365": (365, "$100"),
    }
    days, price = periods[callback.data]
    await callback.answer(f"Выбрано: {days} дней — {price}")
    text = (
        f"{hbold(f'💎 VIP на {days} дней — {price}')}\n\n"
        f"Для оплаты свяжитесь с администратором:\n"
        f"👉 @Homyak_investorr\n\n"
        f"Ваш Telegram ID: {hbold(str(callback.from_user.id))}\n"
        f"Тариф: {days} дней\n\n"
        f"{hitalic('После оплаты доступ активируется вручную')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад к тарифам", callback_data="vip_info")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "pay_contact")
async def on_pay_contact(callback: CallbackQuery):
    await callback.answer()
    text = (
        f"{hbold('💳 Оплата VIP')}\n\n"
        f"Свяжитесь с администратором:\n"
        f"👉 @Homyak_investorr\n\n"
        f"Ваш ID: {hbold(str(callback.from_user.id))}\n\n"
        f"Принимаем: USDT, BTC, банковский перевод"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад", callback_data="vip_info")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "guides")
async def on_guides(callback: CallbackQuery):
    await callback.message.edit_text(
        f"{hbold('📚 Обучающие материалы')}\n\nВыберите раздел:",
        reply_markup=get_guides_keyboard()
    )


@dp.callback_query(F.data == "guide_start")
async def on_guide_start(callback: CallbackQuery):
    text = (
        f"{hbold('📖 Как начать торговать на Polymarket?')}\n\n"
        f"1️⃣ {hbold('Регистрация')}\n"
        f"• Перейдите на polymarket.com\n"
        f"• Подключите кошелёк (MetaMask / Coinbase Wallet)\n"
        f"• Пополните баланс USDC на сети Polygon\n\n"
        f"2️⃣ {hbold('Понимание рынков')}\n"
        f"• YES токены = ставка на «Да» (цена = вероятность)\n"
        f"• NO токены = ставка на «Нет»\n"
        f"• Рынок разрешается в 1.0 или 0.0\n\n"
        f"3️⃣ {hbold('Использование сигналов бота')}\n"
        f"• Нажмите «Получить сигнал»\n"
        f"• Скопируйте название рынка кнопкой 📋\n"
        f"• Найдите рынок на polymarket.com и войдите в позицию\n"
        f"• Не вкладывайте более 5% депозита в одну позицию\n\n"
        f"🌐 {hbold('Если вы в России — нужен VPN')}\n"
        f"• Polymarket заблокирован в РФ — без VPN сайт не откроется\n"
        f"• Рекомендуем бесплатный {hbold('Urban VPN')} — urbanvpn.com\n"
        f"• Доступен на Android, iOS и ПК — полностью бесплатный\n"
        f"• Выбирайте японский сервер — стабильно и быстро\n\n"
        f"{hitalic('⚠️ Торговля связана с риском.')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад к гайдам", callback_data="guides")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "guide_strategies")
async def on_guide_strategies(callback: CallbackQuery):
    text = (
        f"{hbold('📊 Стратегии генерации сигналов')}\n\n"
        f"🔮 {hbold('UMA Оракул')}\n"
        f"Находит рынки, истекающие в ближайшие 3 дня, с ценой ≥88% или ≤12%. "
        f"Такие рынки фактически уже разрешены — UMA оракул лишь подтверждает. "
        f"Ожидаемый winrate: ~90%\n\n"
        f"📰 {hbold('Новостной Анализ')}\n"
        f"AI (Gemini / Ollama) анализирует RSS-новости и сопоставляет заголовки "
        f"с активными рынками. Порог: confidence ≥72%.\n\n"
        f"🎯 {hbold('Metaculus Консенсус')}\n"
        f"Сравнивает прогнозы суперфорекастеров Metaculus с ценой Polymarket. "
        f"Расхождение >12% — арбитражная возможность.\n\n"
        f"{hitalic('Минимальный confidence: VIP = 65%, Free = 80%')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад к гайдам", callback_data="guides")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "guide_risk")
async def on_guide_risk(callback: CallbackQuery):
    text = (
        f"{hbold('💰 Управление рисками')}\n\n"
        f"⚠️ {hbold('Золотые правила')}\n"
        f"• Максимум 5% от депозита на одну позицию\n"
        f"• Диверсифицируйте по разным рынкам\n"
        f"• Стоп-лосс мысленный: -25%\n\n"
        f"📊 {hbold('Размер позиции по confidence')}\n"
        f"• 65-75% → 1-2% от депозита\n"
        f"• 75-85% → 2-3% от депозита\n"
        f"• 85%+   → до 5% от депозита\n\n"
        f"🔄 {hbold('Выход')}\n"
        f"• Берите прибыль при +25-30%\n"
        f"• Следите за датой истечения рынка\n\n"
        f"{hitalic('Даже при 65% winrate правильный мани-менеджмент даёт прибыль!')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад к гайдам", callback_data="guides")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "support")
async def on_support(callback: CallbackQuery):
    text = (
        f"{hbold('🆘 Поддержка')}\n\n"
        f"По вопросам VIP доступа и сотрудничества:\n"
        f"📩 @Homyak_investorr\n\n"
        f"Ваш ID: {hbold(str(callback.from_user.id))}\n\n"
        f"{hitalic('Отвечаем в течение 24 часов')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="« Назад", callback_data="back_to_menu")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "back_to_menu")
async def on_back_to_menu(callback: CallbackQuery):
    _awaiting_input.pop(callback.from_user.id, None)  # cancel any pending input
    user   = await db.get_or_create_user(callback.from_user.id, callback.from_user.username)
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    await callback.message.edit_text(
        f"{hbold('📡 Polymarket Signals')}\n\n"
        f"Нажмите кнопку чтобы получить торговый сигнал 👇",
        reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_sup)
    )


# ── Signal formatting ─────────────────────────────────────────────────────────

async def format_signal_message(signal: Signal, total: int = 1) -> str:
    # Days to expiry label
    days_label = ""
    if signal.end_date:
        try:
            ed = str(signal.end_date).replace("Z", "").replace("+00:00", "")
            dt = datetime.fromisoformat(ed)
            days = (dt - datetime.utcnow()).total_seconds() / 86400
            if days < 0:
                days_label = "⏳ Истекает: скоро"
            elif days < 1/24:
                mins = max(1, int(days * 24 * 60))
                days_label = f"⏳ Истекает через: {mins} мин."
            elif days < 1:
                hours = max(1, int(days * 24))
                days_label = f"⏳ Истекает через: {hours} ч."
            else:
                days_label = f"⏳ Истекает через: {days:.1f} дн."
        except Exception:
            pass

    conf_int = int(signal.confidence)

    # ── Действие (купить YES или NO) ─────────────────────────────────────────
    if signal.expected_outcome == "YES":
        action = "Купить YES"
    else:
        action = "Купить NO"

    msg = (
        f"{signal.strategy}\n\n"
        f"<code>{signal.market_name}</code>\n\n"
        f"🎯 Позиция      {hbold(action)}\n"
        f"💵 Вход         {signal.current_price:.0%}\n"
        f"📈 Профит       +{signal.expected_profit:.1f}%\n"
        f"🔥 Уверенность  {conf_int}%\n"
    )

    if days_label:
        msg += f"⏳ Срок         {days_label.replace('⏳ ', '').replace('Истекает через: ', '')}\n"

    # ── Короткое обоснование ──────────────────────────────────────────────────
    reasoning = ""

    if signal.strategy == "🔮 UMA Оракул":
        days_left = signal.details.get("days_to_expiry", "?")
        try:
            days_float = float(days_left)
            days_str = f"{days_float:.1f} дн." if days_float >= 1 else f"{int(days_float*24)} ч."
        except Exception:
            days_str = str(days_left)
        if signal.expected_outcome == "YES":
            reasoning = f"Исход «Да» предрешён — оракул UMA подтвердит через {days_str}."
        else:
            reasoning = f"Исход «Нет» предрешён — оракул UMA подтвердит через {days_str}."

    elif signal.strategy == "📰 Новостной Анализ":
        summary = signal.details.get("analysis_summary", "")
        if summary:
            reasoning = summary[:200]

    elif signal.strategy == "🎯 Metaculus Консенсус":
        meta_prob = signal.details.get("metaculus_probability", 0) * 100
        poly_prob = signal.details.get("polymarket_probability", signal.current_price) * 100
        div       = signal.details.get("divergence", 0) * 100
        forecasters = signal.details.get("forecasters", 0)
        fc_str = f" ({forecasters} прогнозистов)" if forecasters > 0 else ""
        if signal.expected_outcome == "YES":
            reasoning = f"Metaculus{fc_str}: {meta_prob:.0f}% vs рынок {poly_prob:.0f}% — расхождение {div:.0f}%."
        else:
            reasoning = f"Metaculus{fc_str}: {meta_prob:.0f}% vs рынок {poly_prob:.0f}% — расхождение {div:.0f}%."

    if reasoning:
        msg += f"\n{hitalic(reasoning)}"

    return msg
# ── Core signal delivery ──────────────────────────────────────────────────────

async def check_signals_for_user(telegram_id: int):
    """Fetch signals, apply user settings, send the best one, queue the rest."""
    try:
        is_vip  = await db.is_vip(telegram_id)
        is_sup  = await db.is_superuser(telegram_id)
        is_priv = is_vip or is_sup

        # Load user settings
        s          = get_user_settings(telegram_id)
        min_conf   = max(s["min_confidence"], 65 if is_priv else 80)
        min_profit = s["min_profit"]
        strat_map  = {
            "uma":  "🔮 UMA Оракул",
            "news": "📰 Новостной Анализ",
            "meta": "🎯 Metaculus Консенсус",
        }
        allowed_strats = {strat_map[k] for k in s["strategies"]}

        await bot.send_message(
            telegram_id,
            f"🔍 {hbold('Сканирую рынки...')} {'(VIP)' if is_priv else '(Free — 80%+)'}\n"
            f"{hitalic(f'Фильтры: уверенность ≥{min_conf}% · профит ≥{min_profit:.0f}%')}"
        )

        signals = await signal_generator.generate_all_signals()

        if not signals:
            await bot.send_message(telegram_id, "ℹ️ Рынки не показывают возможностей прямо сейчас.")
            return

        user = await db.get_or_create_user(telegram_id=telegram_id)
        valid: List[Signal] = []

        for sig in signals:
            if not sig.is_valid:
                continue
            if sig.strategy not in allowed_strats:
                continue
            if sig.confidence < min_conf:
                continue
            if sig.expected_profit < min_profit:
                continue
            already = await db.was_signal_sent(user.id, sig.market_id, sig.strategy, hours=6)
            if already:
                continue
            valid.append(sig)

        if not valid:
            total_valid = len([s for s in signals if s.is_valid])
            hint = " Попробуйте снизить пороги в ⚙️ Настройках." if total_valid > 0 else " Рынок спокойный."
            back_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
                [InlineKeyboardButton(text="« Главное меню", callback_data="back_to_menu")],
            ])
            await bot.send_message(
                telegram_id,
                f"ℹ️ Новых сигналов по вашим фильтрам нет.{hint}",
                reply_markup=back_kb
            )
            return

        # Apply user's max_signals cap
        max_sig = s.get("max_signals", 3)
        valid = valid[:max_sig]

        _signal_queues[telegram_id] = list(valid[1:])
        await send_signal_to_user(telegram_id, valid[0], is_priv, has_more=len(valid) > 1)

    except Exception as e:
        print(f"[ERR check_signals_for_user({telegram_id})] {e}")
        import traceback; traceback.print_exc()
        try:
            await bot.send_message(telegram_id, "⚠️ Ошибка при сканировании. Попробуйте через минуту.")
        except Exception:
            pass


async def send_signal_to_user(
    telegram_id: int, signal: Signal, is_vip: bool = False, has_more: bool = False
):
    """Send a single signal with action buttons."""
    try:
        msg_text = await format_signal_message(
            signal,
            total=len(_signal_queues.get(telegram_id, [])) + 1
        )
        if is_vip:
            msg_text = f"💎 {hbold('VIP SIGNAL')}\n{msg_text}"

        kb = get_signal_keyboard(signal.market_name, signal.source_url, has_more=has_more)

        await bot.send_message(
            chat_id=telegram_id,
            text=msg_text,
            reply_markup=kb,
            disable_web_page_preview=True
        )

        user = await db.get_or_create_user(telegram_id=telegram_id)
        await db.record_signal_sent(user.id, signal.market_id, signal.strategy)

    except Exception as e:
        print(f"[ERR send_signal_to_user({telegram_id})] {e}")


async def broadcast_signal(signal: Signal):
    """Broadcast a signal to all eligible active users."""
    users = await db.get_active_users()
    sent  = 0

    for user in users:
        try:
            is_user_vip = (
                user.telegram_id == config.SUPERUSER_ID or
                (user.vip_until and user.vip_until > datetime.now())
            )
            s          = get_user_settings(user.telegram_id)
            min_conf   = max(s["min_confidence"], 65 if is_user_vip else 80)
            min_profit = s["min_profit"]
            strat_map  = {
                "uma":  "🔮 UMA Оракул",
                "news": "📰 Новостной Анализ",
                "meta": "🎯 Metaculus Консенсус",
            }
            allowed_strats = {strat_map[k] for k in s["strategies"]}

            if signal.strategy not in allowed_strats:
                continue
            if signal.confidence < min_conf:
                continue
            if signal.expected_profit < min_profit:
                continue
            if await db.was_signal_sent(user.id, signal.market_id, signal.strategy, hours=6):
                continue

            msg_text = await format_signal_message(signal)
            if is_user_vip:
                msg_text = f"💎 {hbold('VIP SIGNAL')}\n{msg_text}"

            kb = get_signal_keyboard(signal.market_name, signal.source_url, has_more=False)

            await bot.send_message(
                chat_id=user.telegram_id,
                text=msg_text,
                reply_markup=kb,
                disable_web_page_preview=True
            )
            await db.record_signal_sent(user.id, signal.market_id, signal.strategy)
            sent += 1
            await asyncio.sleep(0.05)

        except Exception as e:
            print(f"[BROADCAST] Failed to send to {user.telegram_id}: {e}")

    return sent


# ── Background loop ───────────────────────────────────────────────────────────

async def signal_check_loop():
    """Background loop — kept alive but does NOT auto-broadcast.
    Signals are only sent when user manually presses 'Получить сигнал'."""
    print("[LOOP] Background loop started (manual-only mode — no auto-broadcast)")
    while True:
        await asyncio.sleep(3600)  # just keep the task alive


# ── Startup / Shutdown ────────────────────────────────────────────────────────

async def on_startup(bot: Bot):
    await db.init()
    print("Bot started and database initialized!")


async def on_shutdown(bot: Bot):
    print("Bot shutting down...")