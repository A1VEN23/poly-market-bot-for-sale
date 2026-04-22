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
from crypto_payments import (
    create_invoice,
    get_invoice_status,
    register_pending_invoice,
    remove_pending_invoice,
    get_all_pending_invoices,
    VIP_PLANS,
    SUPPORTED_CURRENCIES,
    CURRENCY_LABELS,
)


# ── Bot / dispatcher ──────────────────────────────────────────────────────────

if not config.BOT_TOKEN:
    print("=" * 60)
    print("ERROR: TELEGRAM_TOKEN (or BOT_TOKEN) is not set!")
    print("  Set it as an environment variable before starting.")
    print("=" * 60)

bot = Bot(
    token=config.BOT_TOKEN or "INVALID_TOKEN_PLACEHOLDER",
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
signal_generator = SignalGenerator()

# ── In-memory signal queue (per user) ─────────────────────────────────────────
_signal_queues: Dict[int, List[Signal]] = {}

# ── Per-user settings (in-memory cache, persisted to DB) ──────────────────────
_user_settings: Dict[int, Dict] = {}
_awaiting_input: Dict[int, str] = {}
_scanning_users: set = set()  # users currently scanning — can cancel

DEFAULT_SETTINGS = {
    "min_profit": 3.0,
    "min_confidence": 80,
    "strategies": {"uma", "news", "meta", "orderbook", "xsent", "official"},
    "max_signals": 3,
    "topics": {"crypto", "politics", "sports", "economics", "science", "general"},
    "multi_mode": False,
}

STRATEGY_LABELS = {
    "uma":      "🔮 UMA Оракул",
    "news":     "📰 Новости",
    "meta":     "🎯 Metaculus",
    "orderbook":"📊 Smart Money",
    "xsent":    "🐦 X Sentiment",
    "official": "🏛️ Офиц. данные",
}

# Maps strategy key → exact strategy name used in Signal.strategy
STRATEGY_SIGNAL_NAMES = {
    "uma":       "🔮 UMA Оракул",
    "news":      "📰 Новостной Анализ",
    "meta":      "🎯 Metaculus Консенсус",
    "orderbook": "📊 Smart Money",
    "xsent":     "🐦 X Sentiment",
    "official":  "🏛️ Официальные данные",
}

# ── Topic (category) filter ───────────────────────────────────────────────────
# Maps our topic keys → substrings that appear in Polymarket market category
TOPIC_LABELS = {
    "crypto":    "₿ Крипто",
    "politics":  "🗳 Политика",
    "sports":    "⚽ Спорт",
    "economics": "📊 Экономика",
    "science":   "🔬 Наука/Тех",
    "general":   "🌐 Общее",
}

# Keywords used to classify a market's category string into our topic keys
TOPIC_KEYWORDS: Dict[str, list] = {
    "crypto":    ["crypto", "bitcoin", "btc", "eth", "ethereum", "token", "defi",
                  "blockchain", "nft", "coin", "solana", "sol", "bnb", "xrp",
                  "крипт", "биткоин"],
    "politics":  ["politic", "election", "president", "senate", "congress", "vote",
                  "government", "prime minister", "democrat", "republican",
                  "выбор", "президент", "политик", "война", "war", "ceasefire"],
    "sports":    ["sport", "football", "soccer", "nba", "nfl", "mlb", "nhl",
                  "tennis", "golf", "olympic", "championship", "league", "cup",
                  "спорт", "футбол", "баскетбол"],
    "economics": ["econom", "market", "stock", "gdp", "inflation", "fed", "rate",
                  "trade", "tariff", "recession", "finance", "банк", "экономик"],
    "science":   ["science", "tech", "ai", "artificial", "climate", "space",
                  "health", "drug", "fda", "nasa", "наука", "технол", "климат"],
}


def get_user_settings(telegram_id: int) -> Dict:
    """Return in-memory settings (must be pre-loaded via ensure_user_settings)."""
    if telegram_id not in _user_settings:
        _user_settings[telegram_id] = {
            "min_profit": DEFAULT_SETTINGS["min_profit"],
            "min_confidence": DEFAULT_SETTINGS["min_confidence"],
            "strategies": set(DEFAULT_SETTINGS["strategies"]),
            "max_signals": DEFAULT_SETTINGS["max_signals"],
            "topics": set(DEFAULT_SETTINGS["topics"]),
        }
    s = _user_settings[telegram_id]
    if "topics" not in s:
        s["topics"] = set(DEFAULT_SETTINGS["topics"])
    if "multi_mode" not in s:
        s["multi_mode"] = False
    # Back-compat: add new strategy keys for existing sessions
    if "orderbook" not in s["strategies"] and "uma" in s["strategies"]:
        s["strategies"].update({"orderbook", "xsent", "official"})
    return s


async def ensure_user_settings(telegram_id: int) -> Dict:
    """Load settings from DB if not in cache, then return them."""
    if telegram_id not in _user_settings:
        saved = await db.load_user_settings(telegram_id)
        if saved:
            _user_settings[telegram_id] = saved
    return get_user_settings(telegram_id)


async def persist_user_settings(telegram_id: int):
    """Save current in-memory settings to DB."""
    s = _user_settings.get(telegram_id)
    if s:
        await db.save_user_settings(telegram_id, s)


def classify_market_topic(category: str, name: str = "") -> str:
    """Return the topic key that best matches a market's category string."""
    text = (category + " " + name).lower()
    for topic_key, keywords in TOPIC_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return topic_key
    return "general"


# ── Keyboards ─────────────────────────────────────────────────────────────────

def get_landing_keyboard() -> InlineKeyboardMarkup:
    """Keyboard for the onboarding screen (non-VIP users)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Купить подписку", callback_data="vip_info")],
        [InlineKeyboardButton(text="📖 Как оплатить? (пошагово)", callback_data="how_to_pay")],
        [InlineKeyboardButton(text="❓ Что такое Polymarket Signals?", callback_data="about_signals")],
    ])


def get_about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Купить подписку", callback_data="vip_info")],
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_landing")],
    ])


def get_main_menu_keyboard(signals_enabled: bool = True, is_vip: bool = False, is_superuser: bool = False, scanning: bool = False):
    if is_superuser:
        vip_btn = InlineKeyboardButton(text="👑 ПРОФИЛЬ (Developer)", callback_data="profile")
    elif is_vip:
        vip_btn = InlineKeyboardButton(text="💎 VIP АКТИВЕН — Профиль", callback_data="profile")
    else:
        vip_btn = InlineKeyboardButton(text="💎 Купить подписку", callback_data="vip_info")

    if scanning:
        scan_btn = InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")
    else:
        scan_btn = InlineKeyboardButton(text="🏆 Искать золото", callback_data="get_signal")

    rows = [
        [scan_btn],
        [
            vip_btn,
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text="📚 ГАЙДЫ",    callback_data="guides"),
            InlineKeyboardButton(text="🆘 ПОДДЕРЖКА", callback_data="support"),
        ],
    ]
    if not is_vip and not is_superuser:
        rows.insert(1, [InlineKeyboardButton(text="📖 Как оплатить подписку?", callback_data="how_to_pay")])
    else:
        rows.append([InlineKeyboardButton(text="👥 Реферальная программа", callback_data="referral")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_signal_keyboard(market_name: str, source_url: str, has_more: bool = False, scanning: bool = False) -> InlineKeyboardMarkup:
    rows = []

    if source_url and source_url.startswith("http"):
        rows.append([InlineKeyboardButton(text="🔗 Источник / Обоснование", url=source_url)])

    if has_more:
        rows.append([InlineKeyboardButton(text="⚡ Другой сигнал", callback_data="next_signal")])

    if scanning:
        rows.append([InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")])
    else:
        rows.append([InlineKeyboardButton(text="🔄 Новый скан", callback_data="get_signal"),
                     InlineKeyboardButton(text="« Меню", callback_data="back_to_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_vip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 месяц — $10",   callback_data="vip_30")],
        [InlineKeyboardButton(text="3 месяца — $25",  callback_data="vip_90")],
        [InlineKeyboardButton(text="6 месяцев — $50", callback_data="vip_180")],
        [InlineKeyboardButton(text="1 год — $100",    callback_data="vip_365")],
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_landing")],
    ])


def get_profile_keyboard(is_superuser: bool = False):
    rows = []
    if not is_superuser:
        rows.append([InlineKeyboardButton(text="💎 Продлить подписку", callback_data="vip_info")])
    else:
        rows.append([InlineKeyboardButton(text="👑 Управление пользователями", callback_data="su_manage")])
    rows.append([InlineKeyboardButton(text="← Назад", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_su_manage_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Выдать VIP",      callback_data="su_add_vip")],
        [InlineKeyboardButton(text="➖ Убрать VIP",      callback_data="su_remove_vip")],
        [InlineKeyboardButton(text="🛡 Выдать Admin",    callback_data="su_add_admin")],
        [InlineKeyboardButton(text="🚫 Убрать Admin",    callback_data="su_remove_admin")],
        [InlineKeyboardButton(text="📋 Список VIP",      callback_data="su_list_vip")],
        [InlineKeyboardButton(text="← Назад в профиль", callback_data="profile")],
    ])


def get_guides_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Как начать?",            callback_data="guide_start")],
        [InlineKeyboardButton(text="📊 Стратегии сигналов",     callback_data="guide_strategies")],
        [InlineKeyboardButton(text="💰 Управление рисками",     callback_data="guide_risk")],
        [InlineKeyboardButton(text="← Назад",                   callback_data="back_to_menu")],
    ])



# ── Scan mode presets ──────────────────────────────────────────────────────────

SCAN_MODE_PRESETS = {
    "easy": {
        "label":          "🟢 Easy — Осторожный",
        "min_profit":     0.5,   # 0.5% ROI min — UMA near-certain signals
        "max_profit":     5.0,   # cap at 5% — low-risk signals only
        "min_confidence": 88,
        "max_signals":    3,
        "strategies":     {"uma", "official", "orderbook"},
        "topics":         {"crypto", "politics", "economics", "science", "sports", "general"},
        "description": (
            "🟢 <b>Easy — Осторожный режим</b>\n\n"
            "Только самые надёжные сигналы с высокой уверенностью модели.\n\n"
            "📊 <b>Параметры:</b>\n"
            "• ROI: 0.5%–5%\n"
            "• Мин. уверенность: 88%\n"
            "• Макс. сигналов за раз: 3\n"
            "• Стратегии: UMA Оракул + Smart Money + Офиц. данные\n\n"
            "📈 <b>Что ожидать:</b>\n"
            "• 3–8 сигналов в день\n"
            "• ROI: 0.5–5% за сделку (рынки с ценой 80–99%)\n"
            "• Уверенность модели: ≥88%\n"
            "• Риск: минимальный\n\n"
            "<i>Идеально для новичков. Только сигналы с высокой уверенностью модели.</i>\n"
            "<i>⚠️ Прошлые результаты не гарантируют будущих. Торгуйте осознанно.</i>"
        ),
    },
    "mid": {
        "label":          "🟡 Mid — Сбалансированный",
        "min_profit":     5.0,   # 5–20% ROI
        "max_profit":     20.0,
        "min_confidence": 70,
        "max_signals":    5,
        "strategies":     {"uma", "news", "meta", "orderbook", "official"},
        "topics":         {"crypto", "politics", "economics", "science", "sports", "general"},
        "description": (
            "🟡 <b>Mid — Сбалансированный режим</b>\n\n"
            "Баланс ROI, уверенности и количества сигналов.\n\n"
            "📊 <b>Параметры:</b>\n"
            "• ROI: 5%–20%\n"
            "• Мин. уверенность: 70%\n"
            "• Макс. сигналов за раз: 5\n"
            "• Стратегии: UMA + Smart Money + Новости + Metaculus + Офиц. данные\n\n"
            "📈 <b>Что ожидать:</b>\n"
            "• 4–12 сигналов в день\n"
            "• ROI: 5–20% за сделку (цены 15–80%)\n"
            "• Уверенность модели: ≥70%\n"
            "• Риск: умеренный\n\n"
            "<i>Лучший выбор для большинства трейдеров.</i>\n"
            "<i>⚠️ Прошлые результаты не гарантируют будущих. Торгуйте осознанно.</i>"
        ),
    },
    "hard": {
        "label":          "🔴 Hard — Агрессивный",
        "min_profit":     20.0,  # 20%+ ROI — ищем рынки с ценой 10–40%
        "max_profit":     None,  # no cap
        "min_confidence": 60,
        "max_signals":    10,
        "strategies":     {"uma", "news", "meta", "orderbook", "xsent", "official"},
        "topics":         {"crypto", "politics", "economics", "science", "sports", "general"},
        "description": (
            "🔴 <b>Hard — Агрессивный режим</b>\n\n"
            "Высокий ROI, высокий риск. Рынки с ценой 10–40%.\n\n"
            "📊 <b>Параметры:</b>\n"
            "• ROI: 20%+\n"
            "• Мин. уверенность: 60%\n"
            "• Макс. сигналов за раз: 10\n"
            "• Стратегии: все 6 источников\n\n"
            "📈 <b>Что ожидать:</b>\n"
            "• 10–30+ сигналов в день\n"
            "• ROI: 20–300%+ за сделку (зависит от рынка)\n"
            "• Уверенность модели: ≥60%\n"
            "• Риск: высокий — много сделок, нужен контроль объёма\n\n"
            "<i>Только для опытных трейдеров! Не ставить всё на один сигнал.</i>\n"
            "<i>⚠️ Прошлые результаты не гарантируют будущих. Торгуйте осознанно.</i>"
        ),
    },
}


def get_settings_select_keyboard() -> InlineKeyboardMarkup:
    """Top-level settings menu: Simple / Advanced tabs."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎮 Простой режим",    callback_data="settings_simple"),
            InlineKeyboardButton(text="🔬 Продвинутый",      callback_data="settings_advanced"),
        ],
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_menu")],
    ])


def get_scan_mode_keyboard(current_mode: str = "") -> InlineKeyboardMarkup:
    """Simple mode: pick Easy / Mid / Hard preset."""
    rows = []
    for key, preset in SCAN_MODE_PRESETS.items():
        active = "  ←" if key == current_mode else ""
        rows.append([InlineKeyboardButton(
            text=f"{preset['label']}{active}",
            callback_data=f"preset_info_{key}"
        )])
    rows.append([InlineKeyboardButton(text="← Назад к настройкам", callback_data="settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_preset_confirm_keyboard(mode_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Применить этот режим", callback_data=f"apply_preset_{mode_key}")],
        [InlineKeyboardButton(text="← Выбрать другой режим",  callback_data="settings_simple")],
    ])


def get_settings_keyboard(telegram_id: int, is_vip: bool) -> InlineKeyboardMarkup:
    """Advanced settings keyboard with all manual controls."""
    s = get_user_settings(telegram_id)
    strats = s["strategies"]
    topics = s["topics"]

    def chk(key): return "✅" if key in strats else "⬜️"

    rows = [
        # ── Min profit ───────────────────────────────────────────────────────
        [InlineKeyboardButton(
            text=f"📈 Мин. ROI (профит): {s['min_profit']:.0f}% — изменить",
            callback_data="input_profit"
        )],
        [
            InlineKeyboardButton(text="3%",  callback_data="quick_profit_3"),
            InlineKeyboardButton(text="5%",  callback_data="quick_profit_5"),
            InlineKeyboardButton(text="10%", callback_data="quick_profit_10"),
            InlineKeyboardButton(text="20%", callback_data="quick_profit_20"),
        ],
        # ── Min confidence ───────────────────────────────────────────────────
        [InlineKeyboardButton(
            text=f"🎯 Мин. уверенность: {s['min_confidence']}% — изменить",
            callback_data="input_conf"
        )],
    ]

    if not is_vip:
        rows.append([InlineKeyboardButton(
            text="🔒 Уверенность ниже 80% — только VIP",
            callback_data="vip_info"
        )])

    # ── Max signals ──────────────────────────────────────────────────────────
    rows.append([InlineKeyboardButton(
        text=f"📦 Сигналов за раз: {s['max_signals']} — изменить",
        callback_data="input_max_signals"
    )])

    # ── Strategies (all 6, 2 per row) ───────────────────────────────────────
    rows.append([InlineKeyboardButton(text="📡 Стратегии ↓ вкл/выкл:", callback_data="noop")])
    strat_keys = ["uma", "news", "meta", "orderbook", "xsent", "official"]
    for i in range(0, len(strat_keys), 2):
        row_btns = []
        for key in strat_keys[i:i+2]:
            row_btns.append(InlineKeyboardButton(
                text=f"{chk(key)} {STRATEGY_LABELS[key]}",
                callback_data=f"toggle_strat_{key}"
            ))
        rows.append(row_btns)

    # ── Topic filters (2 per row) ─────────────────────────────────────────────
    rows.append([InlineKeyboardButton(text="🗂 Темы сигналов ↓ вкл/выкл:", callback_data="noop")])
    topic_keys = list(TOPIC_LABELS.keys())
    for i in range(0, len(topic_keys), 2):
        row_btns = []
        for key in topic_keys[i:i+2]:
            check = "✅" if key in topics else "⬜️"
            row_btns.append(InlineKeyboardButton(
                text=f"{check} {TOPIC_LABELS[key]}",
                callback_data=f"toggle_topic_{key}"
            ))
        rows.append(row_btns)

    rows.append([InlineKeyboardButton(text="← Назад к настройкам", callback_data="settings")])

    # ── Multi mode toggle ─────────────────────────────────────────────────────
    multi_on = s.get("multi_mode", False)
    multi_icon = "✅" if multi_on else "⬜️"
    multi_label = "ВКЛ — все сигналы по кнопке" if multi_on else "ВЫКЛ — лучший сигнал сразу"
    rows.insert(-1, [InlineKeyboardButton(
        text=f"{multi_icon} 🔢 Мульти-режим: {multi_label}",
        callback_data="toggle_multi_mode"
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── Text helpers ──────────────────────────────────────────────────────────────

LANDING_TEXT = (
    f"{hbold('📡 Polymarket Signals')}\n\n"
    f"Торговые сигналы на предсказательных рынках.\n"
    f"Доступ — только по подписке."
)

ABOUT_TEXT = (
    f"{hbold('🔮 Polymarket Signals')}\n\n"
    f"Мы мониторим тысячи предсказательных рынков в режиме реального времени "
    f"и находим позиции с реальным математическим перевесом — "
    f"до того, как их заметит рынок.\n\n"

    f"{hbold('6 независимых источников сигналов:')}\n\n"

    f"🔮 {hbold('UMA Оракул')}\n"
    f"Рынки, истекающие в ≤36 ч. с ценой ≥93% или ≤7% — исход предрешён, "
    f"оракул лишь подтверждает. Уверенность модели: 85–97%.\n\n"

    f"📰 {hbold('Новостной ИИ')}\n"
    f"Grok сканирует RSS-новости и сопоставляет заголовки с активными рынками. "
    f"Если событие уже произошло, но цена не изменилась — это сигнал.\n\n"

    f"🎯 {hbold('Metaculus Консенсус')}\n"
    f"Суперфорекастеры Metaculus дают прогнозы независимо от рынка. "
    f"Расхождение более 20% — арбитражная возможность.\n\n"

    f"📊 {hbold('Smart Money (Order Book)')}\n"
    f"Polymarket CLOB API показывает реальные ордера. Крупные заявки "
    f"(≥$500) с перекосом 70%+ в одну сторону = инсайдерские деньги ставят до публики.\n\n"

    f"🐦 {hbold('X Sentiment (AI-анализ)')}\n"
    f"AI-модель анализирует публичные данные и новости по теме рынка. "
    f"Резкий сдвиг тональности до того, как рынок отреагировал — сигнал на опережение.\n\n"

    f"🏛️ {hbold('Официальные данные')}\n"
    f"Мониторим расписание релизов BLS, ФРС, FRED. Когда дата публикации "
    f"данных совпадает с истекающим рынком — статистика разрешит вопрос напрямую.\n\n"

    f"{hitalic('Каждый сигнал: позиция · точка входа · ожидаемый профит · уверенность.')}\n\n"
    f"{hitalic('Не советы. Статистический перевес.')}"
)

MAIN_MENU_TEXT = (
    f"{hbold('📡 Polymarket Signals')}\n\n"
    f"Нажмите 🏆 чтобы запустить бесконечный поиск золота 👇\n"
    f"{hitalic('Сканирование будет повторяться каждые 30 минут автоматически.')}"
)

NO_ACCESS_TEXT = (
    f"🔒 {hbold('Требуется подписка')}\n\n"
    f"Эта функция доступна только подписчикам.\n"
    f"Оформите доступ — это займёт меньше минуты."
)


# ── Command handlers ──────────────────────────────────────────────────────────

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    await db.get_or_create_user(user_id, message.from_user.username)
    await ensure_user_settings(user_id)  # preload settings from DB

    # ── Parse referral parameter (/start ref_12345) ───────────────────────────
    args = message.text.split(maxsplit=1)
    if len(args) > 1:
        param = args[1].strip()
        if param.startswith("ref_"):
            try:
                referrer_id = int(param[4:])
                # Don't let users refer themselves; only register once
                if referrer_id != user_id and not await db.has_referral_record(user_id):
                    # Referrer must exist and be a real user (not self-invite)
                    await db.register_referral(referrer_id, user_id)
            except (ValueError, Exception):
                pass

    is_vip   = await db.is_vip(user_id)
    is_super = await db.is_superuser(user_id)

    if is_super or is_vip:
        # ── Already has access: show main menu ────────────────────────────────
        user      = await db.get_or_create_user(user_id)
        days_left = await db.get_vip_days_remaining(user_id)

        if is_super:
            sub = f"👑 {hbold('Статус: Developer / Lifetime')}"
        else:
            sub = f"💎 {hbold(f'Статус: VIP активен — {days_left} дн.')}"

        await message.answer(
            f"{hbold('🚀 Polymarket Signals')}\n\n{sub}\n\n"
            f"{MAIN_MENU_TEXT}",
            reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_super)
        )
    else:
        # ── No access: show landing ───────────────────────────────────────────
        await message.answer(
            f"{hbold('👋 Добро пожаловать!')}\n\n"
            f"Это бот торговых сигналов для Polymarket — "
            f"платформы предсказательных рынков.\n\n"
            f"Доступ к сигналам открывается после оформления подписки.",
            reply_markup=get_landing_keyboard()
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


# ── Landing / onboarding ──────────────────────────────────────────────────────

@dp.callback_query(F.data == "back_to_landing")
async def on_back_to_landing(callback: CallbackQuery):
    _awaiting_input.pop(callback.from_user.id, None)
    await callback.message.edit_text(
        f"{hbold('👋 Polymarket Signals')}\n\n"
        f"Это бот торговых сигналов для Polymarket — "
        f"платформы предсказательных рынков.\n\n"
        f"Доступ к сигналам открывается после оформления подписки.",
        reply_markup=get_landing_keyboard()
    )


@dp.callback_query(F.data == "about_signals")
async def on_about_signals(callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text(ABOUT_TEXT, reply_markup=get_about_keyboard())


HOW_TO_PAY_TEXT = (
    f"{hbold('📖 Как оплатить подписку — пошагово')}\n\n"

    f"{hbold('Шаг 1. Скачайте @CryptoBot')}\n"
    f"Найдите в Telegram: @CryptoBot\n"
    f"Это официальный бот для крипто-платежей внутри Telegram.\n\n"

    f"{hbold('Шаг 2. Пополните баланс')}\n"
    f"В @CryptoBot нажмите {hbold('Пополнить')}.\n"
    f"Доступные способы:\n"
    f"• 💳 Карта (Visa/MC через P2P биржи)\n"
    f"• 🏦 СБП / перевод на карту RU\n"
    f"• 📲 Перевод с Binance, OKX, Bybit\n"
    f"• 💵 Наличные через P2P (Binance P2P)\n\n"

    f"{hbold('Как пополнить через P2P (самый простой способ для СНГ):')}\n"
    f"1. Скопируйте адрес кошелька из @CryptoBot\n"
    f"2. Зайдите на Binance P2P → «Купить» → выберите нужную монету\n"
    f"3. Оплатите картой продавцу\n"
    f"4. Монеты придут на Binance → переведите в @CryptoBot\n\n"

    f"{hbold('Шаг 3. Выберите тариф и оплатите')}\n"
    f"Вернитесь в этот бот → нажмите {hbold('💎 Купить подписку')} → выберите срок.\n"
    f"Вы получите ссылку для оплаты — нажмите её, откроется @CryptoBot.\n"
    f"Подтвердите платёж — доступ активируется {hbold('автоматически за секунды')}.\n\n"

    f"{hbold('Доступные валюты для оплаты:')}\n"
    f"USDT (сети BNB, TON, ARB) · TON · ETH · LTC · SOL\n\n"

    f"{hitalic('❓ Остались вопросы — пишите @Poly_whale_adminn')}"
)


@dp.callback_query(F.data == "how_to_pay")
async def on_how_to_pay(callback: CallbackQuery):
    await callback.answer()
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    back = "back_to_menu" if (is_vip or is_sup) else "back_to_landing"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💎 Выбрать тариф", callback_data="vip_info")],
        [InlineKeyboardButton(text="← Назад", callback_data=back)],
    ])
    await callback.message.edit_text(HOW_TO_PAY_TEXT, reply_markup=kb)


# ── Main menu ──────────────────────────────────────────────────────────────────

@dp.callback_query(F.data == "get_signal")
async def on_get_signal(callback: CallbackQuery):
    is_vip  = await db.is_vip(callback.from_user.id)
    is_sup  = await db.is_superuser(callback.from_user.id)
    if not (is_vip or is_sup):
        await callback.answer()
        await callback.message.edit_text(
            NO_ACCESS_TEXT,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Оформить подписку", callback_data="vip_info")],
                [InlineKeyboardButton(text="❓ Что такое Polymarket Signals?", callback_data="about_signals")],
            ])
        )
        return

    uid = callback.from_user.id
    _awaiting_input.pop(uid, None)

    # If already scanning — ignore duplicate tap
    if uid in _scanning_users:
        await callback.answer("⏳ Сканирование уже запущено", show_alert=False)
        return

    _scanning_users.add(uid)

    await callback.message.edit_text(
        f"🏆 {hbold('Поиск золота запущен!')}\n\n"
        f"{hitalic('Сканирую тысячи рынков по 6 стратегиям...')}\n"
        f"{hitalic('Следующее сканирование — автоматически через 30 минут.')}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")]
        ])
    )
    await callback.answer("🏆 Ищу золото...")

    # Launch infinite scan loop in background
    asyncio.create_task(_scan_loop_for_user(uid))


@dp.callback_query(F.data == "stop_scan")
async def on_stop_scan(callback: CallbackQuery):
    uid = callback.from_user.id
    _scanning_users.discard(uid)
    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)
    user   = await db.get_or_create_user(uid, callback.from_user.username)
    await callback.answer("⛔ Сканирование остановлено", show_alert=False)
    await callback.message.edit_text(
        f"⛔ {hbold('Сканирование остановлено')}\n\n{MAIN_MENU_TEXT}",
        reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_sup, scanning=False)
    )


@dp.callback_query(F.data == "next_signal")
async def on_next_signal(callback: CallbackQuery):
    uid    = callback.from_user.id
    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)

    if not (is_vip or is_sup):
        await callback.answer("🔒 Требуется подписка", show_alert=True)
        return

    queue = _signal_queues.get(uid, [])
    if not queue:
        await callback.answer("✅ Это был последний сигнал из текущего скана", show_alert=False)
        user = await db.get_or_create_user(uid, callback.from_user.username)
        await callback.message.edit_reply_markup(
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Новый скан", callback_data="get_signal")],
                [InlineKeyboardButton(text="« Главное меню", callback_data="back_to_menu")],
            ])
        )
        return

    await callback.answer("📤 Следующий сигнал...")
    signal = queue.pop(0)
    _signal_queues[uid] = queue

    s = get_user_settings(uid)
    multi_mode = s.get("multi_mode", False)

    if multi_mode:
        # Считаем номер текущего сигнала по размеру очереди
        max_sig = s.get("max_signals", 3)
        remaining = len(queue)
        signal_num = max_sig - remaining  # номер этого сигнала
        await send_signal_to_user(
            uid, signal, is_vip or is_sup,
            has_more=bool(queue),
            multi_mode=True,
            signal_num=signal_num,
            total_signals=max_sig,
        )
    else:
        await send_signal_to_user(uid, signal, is_vip or is_sup, has_more=bool(queue))


# ── Settings handlers ──────────────────────────────────────────────────────────

@dp.callback_query(F.data == "settings")
async def on_settings(callback: CallbackQuery):
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)

    if not (is_vip or is_sup):
        await callback.answer()
        await callback.message.edit_text(
            NO_ACCESS_TEXT,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Оформить подписку", callback_data="vip_info")],
                [InlineKeyboardButton(text="← Назад", callback_data="back_to_landing")],
            ])
        )
        return

    uid = callback.from_user.id
    s = await ensure_user_settings(uid)
    mode = s.get("scan_mode", "—")
    mode_label = SCAN_MODE_PRESETS[mode]["label"] if mode in SCAN_MODE_PRESETS else "ручной"
    mp = f"{s['min_profit']:.0f}%"
    mc = f"{s['min_confidence']}%"
    text = (
        f"{hbold('⚙️ Настройки сигналов')}\n\n"
        f"🎮 Режим сканирования: {hbold(mode_label)}\n"
        f"📈 Мин. ROI (профит): {hbold(mp)}\n"
        f"🎯 Мин. уверенность: {hbold(mc)}\n"
        f"📡 Стратегий активно: {hbold(str(len(s['strategies'])) + '/6')}\n\n"
        f"Выберите режим настройки:"
    )
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=get_settings_select_keyboard())


@dp.callback_query(F.data == "settings_simple")
async def on_settings_simple(callback: CallbackQuery):
    uid = callback.from_user.id
    s = get_user_settings(uid)
    current = s.get("scan_mode", "")
    await callback.answer()
    await callback.message.edit_text(
        f"{hbold('🎮 Простой режим — выбор пресета')}\n\n"
        f"Выберите режим сканирования под свой стиль торговли:\n\n"
        f"🟢 <b>Easy</b> — мало сигналов, высокий WR, малый профит\n"
        f"🟡 <b>Mid</b> — баланс сигналов, профита и риска\n"
        f"🔴 <b>Hard</b> — много сигналов, большой профит, высокий риск",
        reply_markup=get_scan_mode_keyboard(current)
    )


@dp.callback_query(F.data.startswith("preset_info_"))
async def on_preset_info(callback: CallbackQuery):
    mode_key = callback.data[len("preset_info_"):]
    if mode_key not in SCAN_MODE_PRESETS:
        await callback.answer("❌ Неизвестный режим", show_alert=True)
        return
    await callback.answer()
    preset = SCAN_MODE_PRESETS[mode_key]
    await callback.message.edit_text(
        preset["description"],
        reply_markup=get_preset_confirm_keyboard(mode_key)
    )


@dp.callback_query(F.data.startswith("apply_preset_"))
async def on_apply_preset(callback: CallbackQuery):
    mode_key = callback.data[len("apply_preset_"):]
    if mode_key not in SCAN_MODE_PRESETS:
        await callback.answer("❌ Неизвестный режим", show_alert=True)
        return

    uid = callback.from_user.id
    preset = SCAN_MODE_PRESETS[mode_key]
    s = get_user_settings(uid)
    s["scan_mode"]      = mode_key
    s["min_profit"]     = preset["min_profit"]
    s["max_profit"]     = preset.get("max_profit")  # None means no cap
    s["min_confidence"] = preset["min_confidence"]
    s["max_signals"]    = preset["max_signals"]
    s["strategies"]     = set(preset["strategies"])
    s["topics"]         = set(preset["topics"])
    await persist_user_settings(uid)

    max_p = preset.get("max_profit")
    roi_label = (
        f"{preset['min_profit']:.0f}%–{max_p:.0f}%"
        if max_p is not None
        else f"{preset['min_profit']:.0f}%+"
    )

    await callback.answer(f"✅ Режим «{preset['label']}» применён!", show_alert=True)
    await callback.message.edit_text(
        f"✅ {hbold('Режим применён!')}\n\n"
        f"{preset['label']}\n\n"
        f"📈 ROI диапазон: {hbold(roi_label)}\n"
        f"🎯 Мин. уверенность: {hbold(str(preset['min_confidence']) + '%')}\n"
        f"📦 Сигналов за раз: {hbold(str(preset['max_signals']))}\n"
        f"📡 Стратегий: {hbold(str(len(preset['strategies'])) + '/6')}\n\n"
        f"{hitalic('Нажмите 🏆 Искать золото чтобы запустить сканирование.')}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🎮 Сменить режим",    callback_data="settings_simple")],
            [InlineKeyboardButton(text="🔬 Тонкая настройка", callback_data="settings_advanced")],
            [InlineKeyboardButton(text="« Главное меню",      callback_data="back_to_menu")],
        ])
    )


@dp.callback_query(F.data == "settings_advanced")
async def on_settings_advanced(callback: CallbackQuery):
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    uid = callback.from_user.id
    s = get_user_settings(uid)
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in sorted(s["strategies"]) if k in STRATEGY_LABELS) or "нет"
    topics_text = " · ".join(TOPIC_LABELS[k] for k in s["topics"]) or "нет"
    mp = f"{s['min_profit']:.0f}%"
    mc = f"{s['min_confidence']}%"
    multi_label = "✅ ВКЛ" if s.get("multi_mode") else "⬜️ ВЫКЛ"
    text = (
        f"{hbold('🔬 Продвинутые настройки')}\n\n"
        f"📈 Мин. ROI (профит): {hbold(mp)}\n"
        f"🎯 Мин. уверенность: {hbold(mc)}\n"
        f"📦 Сигналов за раз: {hbold(str(s['max_signals']))}\n"
        f"🔢 Мульти-режим: {hbold(multi_label)}\n"
        f"📡 Стратегии ({len(s['strategies'])}/6): {strats_text}\n"
        f"🗂 Темы: {topics_text}\n\n"
        f"{hitalic('Нажмите кнопку чтобы изменить:')}"
    )
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_settings_keyboard(uid, is_vip or is_sup)
    )


@dp.callback_query(F.data == "toggle_multi_mode")
async def on_toggle_multi_mode(callback: CallbackQuery):
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    uid = callback.from_user.id
    s = get_user_settings(uid)
    s["multi_mode"] = not s.get("multi_mode", False)
    await persist_user_settings(uid)
    status = "✅ включён" if s["multi_mode"] else "⬜️ выключен"
    await callback.answer(f"🔢 Мульти-режим {status}", show_alert=False)
    await _refresh_settings_message(callback, is_vip or is_sup)


async def _refresh_settings_message(callback: CallbackQuery, is_vip: bool):
    uid = callback.from_user.id
    s = get_user_settings(uid)
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in sorted(s["strategies"]) if k in STRATEGY_LABELS) or "нет"
    topics_text = " · ".join(TOPIC_LABELS[k] for k in s["topics"]) or "нет"
    mp = f"{s['min_profit']:.0f}%"
    mc = f"{s['min_confidence']}%"
    multi_label = "✅ ВКЛ" if s.get("multi_mode") else "⬜️ ВЫКЛ"
    await callback.message.edit_text(
        f"{hbold('🔬 Продвинутые настройки')}\n\n"
        f"📈 Мин. ROI (профит): {hbold(mp)}\n"
        f"🎯 Мин. уверенность: {hbold(mc)}\n"
        f"📦 Сигналов за раз: {hbold(str(s['max_signals']))}\n"
        f"🔢 Мульти-режим: {hbold(multi_label)}\n"
        f"📡 Стратегии ({len(s['strategies'])}/6): {strats_text}\n"
        f"🗂 Темы: {topics_text}\n\n"
        f"{hitalic('Нажмите кнопку чтобы изменить:')}",
        reply_markup=get_settings_keyboard(uid, is_vip)
    )


@dp.callback_query(F.data == "input_profit")
async def on_input_profit(callback: CallbackQuery):
    _awaiting_input[callback.from_user.id] = "profit"
    await callback.answer()
    await callback.message.answer(
        "📈 Введите минимальный ROI в % (например: 5)\n"
        "Допустимый диапазон: 0.5–500"
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
        return

    input_type = _awaiting_input.pop(uid)

    # ── Superuser management inputs ───────────────────────────────────────────
    if input_type.startswith("su_") and uid == config.SUPERUSER_ID:
        await _handle_su_text_input(message, input_type)
        return
    text = message.text.strip().replace("%", "").replace(",", ".")

    try:
        val = float(text)
    except ValueError:
        _awaiting_input[uid] = input_type
        await message.answer("❌ Введите только число, например: 5 или 85")
        return

    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)
    s = get_user_settings(uid)

    if input_type == "profit":
        if val < 0.5 or val > 500:
            _awaiting_input[uid] = "profit"
            await message.answer("❌ Мин. ROI должен быть от 0.5 до 500%. Попробуйте снова:")
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

    await persist_user_settings(uid)
    # Show updated settings
    strats_text = " · ".join(STRATEGY_LABELS[k] for k in s["strategies"]) or "нет"
    topics_text = " · ".join(TOPIC_LABELS[k] for k in s["topics"]) or "нет"
    mp2 = f"{s['min_profit']:.0f}%"
    mc2 = f"{s['min_confidence']}%"
    await message.answer(
        f"{hbold('🔬 Продвинутые настройки обновлены')}\n\n"
        f"📈 Мин. ROI (профит): {hbold(mp2)}\n"
        f"🎯 Мин. уверенность: {hbold(mc2)}\n"
        f"📦 Сигналов за раз: {hbold(str(s['max_signals']))}\n"
        f"📡 Стратегии: {strats_text}\n"
        f"🗂 Темы: {topics_text}",
        reply_markup=get_settings_keyboard(uid, is_vip or is_sup)
    )


@dp.callback_query(F.data.startswith("toggle_strat_"))
async def on_toggle_strat(callback: CallbackQuery):
    strat = callback.data.split("_")[-1]
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

    await persist_user_settings(callback.from_user.id)
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    await _refresh_settings_message(callback, is_vip or is_sup)


@dp.callback_query(F.data.startswith("toggle_topic_"))
async def on_toggle_topic(callback: CallbackQuery):
    topic = callback.data[len("toggle_topic_"):]
    s = get_user_settings(callback.from_user.id)
    topics = s["topics"]
    if topic in topics:
        if len(topics) <= 1:
            await callback.answer("⚠️ Нужна хотя бы одна тема!", show_alert=True)
            return
        topics.discard(topic)
        await callback.answer(f"❌ {TOPIC_LABELS.get(topic, topic)} выключена")
    else:
        topics.add(topic)
        await callback.answer(f"✅ {TOPIC_LABELS.get(topic, topic)} включена")

    await persist_user_settings(callback.from_user.id)
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    await _refresh_settings_message(callback, is_vip or is_sup)


@dp.callback_query(F.data.startswith("quick_profit_"))
async def on_quick_profit(callback: CallbackQuery):
    try:
        val = float(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("❌ Ошибка", show_alert=True)
        return

    s = get_user_settings(callback.from_user.id)
    s["min_profit"] = val
    await persist_user_settings(callback.from_user.id)
    await callback.answer(f"✅ Мин. ROI (профит): {val:.0f}%")

    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    await _refresh_settings_message(callback, is_vip or is_sup)


# ── Other callback handlers ────────────────────────────────────────────────────

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
        [InlineKeyboardButton(text="← Назад", callback_data="profile")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query(F.data == "vip_info")
async def on_vip_info(callback: CallbackQuery):
    is_vip    = await db.is_vip(callback.from_user.id)
    is_sup    = await db.is_superuser(callback.from_user.id)
    days_left = await db.get_vip_days_remaining(callback.from_user.id)

    if is_sup:
        await callback.answer("👑 У вас lifetime доступ", show_alert=True)
        return

    if is_vip:
        text = (
            f"{hbold('💎 Ваша подписка активна')}\n\n"
            f"📅 Осталось: {days_left} дн.\n\n"
            f"✅ Все сигналы без ограничений\n"
            f"✅ Confidence от 65%+\n"
            f"✅ Все 6 стратегий\n\n"
            f"{hitalic('Можно продлить досрочно — дни суммируются')}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1 месяц — $10",   callback_data="vip_30")],
            [InlineKeyboardButton(text="3 месяца — $25",  callback_data="vip_90")],
            [InlineKeyboardButton(text="6 месяцев — $50", callback_data="vip_180")],
            [InlineKeyboardButton(text="1 год — $100",    callback_data="vip_365")],
            [InlineKeyboardButton(text="← Назад", callback_data="back_to_menu")],
        ])
    else:
        text = (
            f"{hbold('💎 Подписка Polymarket Signals')}\n\n"
            f"✅ Сигналы в реальном времени\n"
            f"✅ Шесть независимых стратегий\n"
            f"✅ Confidence от 65%+ (без ограничений)\n"
            f"✅ Настройка фильтров по стратегиям и темам\n"
            f"✅ Гайды по управлению рисками\n\n"
            f"{hitalic('Выберите срок подписки:')}"
        )
        kb = get_vip_keyboard()

    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.in_({"vip_30", "vip_90", "vip_180", "vip_365"}))
async def on_vip_select(callback: CallbackQuery):
    plan_key = callback.data
    days, price, label = VIP_PLANS[plan_key]
    await callback.answer()

    if not config.CRYPTO_BOT_TOKEN:
        text = (
            f"{hbold(f'💎 Подписка {label} — ${price:.0f}')}\n\n"
            f"Для оплаты напишите администратору:\n"
            f"👉 @Poly_whale_adminn\n\n"
            f"Ваш Telegram ID: <code>{callback.from_user.id}</code>\n"
            f"Тариф: {label} ({days} дней)\n\n"
            f"{hitalic('После подтверждения оплаты доступ активируется вручную')}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад к тарифам", callback_data="vip_info")]
        ])
        await callback.message.edit_text(text, reply_markup=kb)
        return

    # Show currency selection
    currency_rows = []
    row = []
    for i, cur in enumerate(SUPPORTED_CURRENCIES):
        row.append(InlineKeyboardButton(
            text=CURRENCY_LABELS.get(cur, cur),
            callback_data=f"pay_{plan_key}_{cur}"
        ))
        if len(row) == 2:
            currency_rows.append(row)
            row = []
    if row:
        currency_rows.append(row)
    currency_rows.append([InlineKeyboardButton(text="← Назад к тарифам", callback_data="vip_info")])

    text = (
        f"💎 {hbold(f'Подписка {label} — ${price:.0f}')}\n\n"
        f"Выберите валюту для оплаты через @CryptoBot:\n\n"
        f"{hitalic('Все валюты конвертируются автоматически.')}"
    )
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=currency_rows))


@dp.callback_query(F.data.startswith("pay_vip_"))
async def on_currency_selected(callback: CallbackQuery):
    # callback_data: pay_vip_30_USDT  or  pay_vip_365_BTC
    parts = callback.data.split("_")
    # parts: ['pay', 'vip', '30', 'USDT']
    plan_key = f"vip_{parts[2]}"
    currency = parts[3]

    days, price, label = VIP_PLANS[plan_key]
    await callback.answer(f"⏳ Создаю счёт в {currency}...")

    invoice = await create_invoice(callback.from_user.id, plan_key, currency)

    if not invoice:
        text = (
            f"{hbold('⚠️ Автооплата временно недоступна')}\n\n"
            f"Оплатите вручную через администратора:\n"
            f"👉 @Poly_whale_adminn\n\n"
            f"Ваш ID: <code>{callback.from_user.id}</code>\n"
            f"Тариф: {label} ({days} дней) — ${price:.0f}"
        )
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="vip_info")]
        ])
        await callback.message.edit_text(text, reply_markup=kb)
        return

    register_pending_invoice(invoice["invoice_id"], callback.from_user.id, days, plan_key)

    inv_id = invoice["invoice_id"]
    cur = invoice["currency"]
    text = (
        f"💎 {hbold(f'Подписка {label} — ${price:.0f}')}\n\n"
        f"💳 Валюта оплаты: {hbold(cur)}\n"
        f"⏰ Счёт действителен: 1 час\n\n"
        f"1️⃣ Нажмите кнопку {hbold('«Оплатить»')} ниже\n"
        f"2️⃣ Оплатите в @CryptoBot\n"
        f"3️⃣ Доступ активируется {hbold('автоматически')} ✅\n\n"
        f"{hitalic(f'Invoice ID: {inv_id}')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"💳 Оплатить в @CryptoBot", url=invoice["pay_url"])],
        [InlineKeyboardButton(text="✅ Я оплатил — проверить", callback_data=f"check_payment_{inv_id}")],
        [InlineKeyboardButton(text="← Назад к тарифам", callback_data="vip_info")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data.startswith("check_payment_"))
async def on_check_payment(callback: CallbackQuery):
    try:
        invoice_id = int(callback.data.split("_")[-1])
    except ValueError:
        await callback.answer("❌ Неверный номер счёта", show_alert=True)
        return

    await callback.answer("🔍 Проверяю оплату...")

    status = await get_invoice_status(invoice_id)

    if status == "paid":
        from crypto_payments import get_pending_invoice
        pending = get_pending_invoice(invoice_id)
        if pending:
            days = pending["days"]
            success = await db.add_vip(callback.from_user.id, config.SUPERUSER_ID, days)
            remove_pending_invoice(invoice_id)
            if success:
                days_label = VIP_PLANS.get(pending["plan_key"], (days, 0, f"{days} дней"))[2]
                # ── Referral bonus ────────────────────────────────────────────
                referrer_id = await db.get_referrer(callback.from_user.id)
                if referrer_id and referrer_id != config.SUPERUSER_ID:
                    bonus_granted = await db.pay_referral_bonus(referrer_id, callback.from_user.id)
                    if bonus_granted:
                        try:
                            await bot.send_message(
                                referrer_id,
                                f"🎁 {hbold('+7 дней к подписке!')}\n\n"
                                f"Ваш реферал оформил VIP — бонус начислен автоматически.\n\n"
                                f"Приводите ещё! /start → 👥 Реферальная программа"
                            )
                        except Exception:
                            pass
                await callback.message.edit_text(
                    f"🎉 {hbold('Оплата подтверждена! Доступ открыт!')}\n\n"
                    f"✅ Тариф: {days_label}\n"
                    f"✅ Дней добавлено: {days}\n\n"
                    f"Добро пожаловать в Polymarket Signals 👇",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🚀 Открыть меню", callback_data="back_to_menu")]
                    ])
                )
                return

        is_vip = await db.is_vip(callback.from_user.id)
        if is_vip:
            await callback.message.edit_text(
                f"✅ {hbold('Подписка активна!')}\n\nВсё в порядке.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🚀 Открыть меню", callback_data="back_to_menu")]
                ])
            )
        else:
            await callback.answer("✅ Оплата найдена, активируем...", show_alert=True)

    elif status == "active":
        await callback.answer(
            "⏳ Оплата ещё не поступила. Попробуйте через минуту.",
            show_alert=True
        )

    elif status == "expired":
        await callback.message.edit_text(
            f"⏰ {hbold('Счёт истёк.')}\n\nПожалуйста, создайте новый.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 Новый счёт", callback_data="vip_info")]
            ])
        )

    else:
        await callback.answer("❌ Не удалось проверить статус. Попробуйте позже.", show_alert=True)


@dp.callback_query(F.data == "guides")
async def on_guides(callback: CallbackQuery):
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)

    if not (is_vip or is_sup):
        await callback.answer()
        await callback.message.edit_text(
            NO_ACCESS_TEXT,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Оформить подписку", callback_data="vip_info")],
                [InlineKeyboardButton(text="← Назад", callback_data="back_to_landing")],
            ])
        )
        return

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
        [InlineKeyboardButton(text="← Назад к гайдам", callback_data="guides")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "guide_strategies")
async def on_guide_strategies(callback: CallbackQuery):
    text = (
        f"{hbold('📊 Стратегии генерации сигналов')}\n\n"
        f"🔮 {hbold('UMA Оракул')}\n"
        f"Рынки, истекающие в ≤36 ч. с ценой ≥93% или ≤7%. "
        f"Исход практически предрешён — UMA оракул подтвердит. "
        f"Уверенность модели: 85–97%.\n\n"

        f"📰 {hbold('Новостной ИИ')}\n"
        f"Grok/Gemini анализирует RSS-новости и находит рынки, "
        f"которые ещё не отреагировали на уже произошедшее событие. "
        f"Порог: confidence ≥85%.\n\n"

        f"🎯 {hbold('Metaculus Консенсус')}\n"
        f"Сравнивает прогнозы суперфорекастеров Metaculus с ценой Polymarket. "
        f"Расхождение >20% с участием 30+ прогнозистов — сигнал.\n\n"

        f"📊 {hbold('Smart Money (Order Book)')}\n"
        f"Анализирует книгу заявок CLOB API. Крупные ордера ≥$500 "
        f"с перекосом 70%+ означают, что крупный игрок ставит до публики.\n\n"

        f"🐦 {hbold('X Sentiment')}\n"
        f"AI-модель анализирует публичные данные по теме рынка. "
        f"Сигнал — когда соцсети уже реагируют, а рынок ещё нет.\n\n"

        f"🏛️ {hbold('Официальные данные')}\n"
        f"Расписание релизов BLS, ФРС (FRED). Когда дата публикации "
        f"данных совпадает с истекающим рынком — сигнал на ожидание.\n\n"

        f"{hitalic('Минимальный confidence: VIP = 65%, стандарт = 80%')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад к гайдам", callback_data="guides")]
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
        f"{hitalic('Правильный мани-менеджмент — основа долгосрочной торговли.')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад к гайдам", callback_data="guides")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "support")
async def on_support(callback: CallbackQuery):
    text = (
        f"{hbold('🆘 Поддержка')}\n\n"
        f"По вопросам подписки и сотрудничества:\n"
        f"📩 @Poly_whale_adminn\n\n"
        f"Ваш ID: {hbold(str(callback.from_user.id))}\n\n"
        f"{hitalic('Отвечаем в течение 24 часов')}"
    )
    is_vip = await db.is_vip(callback.from_user.id)
    is_sup = await db.is_superuser(callback.from_user.id)
    if is_vip or is_sup:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="back_to_menu")]
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="← Назад", callback_data="back_to_landing")]
        ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "referral")
async def on_referral(callback: CallbackQuery):
    uid    = callback.from_user.id
    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)

    if not (is_vip or is_sup):
        await callback.answer("🔒 Только для VIP", show_alert=True)
        return

    await callback.answer()

    bot_username = (await bot.get_me()).username
    ref_link = f"https://t.me/{bot_username}?start=ref_{uid}"
    stats = await db.get_referral_stats(uid)

    slots_left = stats["slots_left_this_month"]
    paid_month = stats["paid_this_month"]

    if slots_left > 0:
        month_line = f"📅 В этом месяце: {paid_month}/4 рефералов ({slots_left} слот(а) осталось)"
    else:
        month_line = "📅 В этом месяце: лимит 4/4 исчерпан — бонусы обновятся 1-го числа"

    text = (
        f"👥 {hbold('Реферальная программа')}\n\n"
        f"Приглашайте друзей — получайте бонусные дни к подписке!\n\n"
        f"🔗 {hbold('Ваша ссылка:')}\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 {hbold('Ваша статистика:')}\n"
        f"👤 Приглашено всего: {stats['total']}\n"
        f"✅ Купили VIP: {stats['paid']}\n"
        f"🎁 Бонус заработан: {stats['bonus_days_total']} дн.\n"
        f"{month_line}\n\n"
        f"📋 {hbold('Условия:')}\n"
        f"• За каждого нового VIP-покупателя — +7 дней\n"
        f"• Максимум 4 реферала в месяц (+30 дн.)\n"
        f"• Бонус только за первую покупку реферала\n\n"
        f"{hitalic('Скопируйте ссылку и отправьте другу 👆')}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад", callback_data="back_to_menu")],
    ])
    await callback.message.edit_text(text, reply_markup=kb)


@dp.callback_query(F.data == "back_to_menu")
async def on_back_to_menu(callback: CallbackQuery):
    uid = callback.from_user.id
    _awaiting_input.pop(uid, None)
    user   = await db.get_or_create_user(uid, callback.from_user.username)
    is_vip = await db.is_vip(uid)
    is_sup = await db.is_superuser(uid)
    scanning = uid in _scanning_users

    if not (is_vip or is_sup):
        # User somehow ended up here without VIP — redirect to landing
        await callback.message.edit_text(
            f"{hbold('👋 Polymarket Signals')}\n\n"
            f"Это бот торговых сигналов для Polymarket — "
            f"платформы предсказательных рынков.\n\n"
            f"Доступ к сигналам открывается после оформления подписки.",
            reply_markup=get_landing_keyboard()
        )
        return

    await callback.message.edit_text(
        MAIN_MENU_TEXT,
        reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_sup, scanning=scanning)
    )


# ── Signal formatting ─────────────────────────────────────────────────────────

async def format_signal_message(signal: Signal, total: int = 1) -> str:
    # ── Time remaining ────────────────────────────────────────────────────────
    time_label = ""
    urgency_prefix = ""
    if signal.end_date:
        try:
            ed = str(signal.end_date).replace("Z", "").replace("+00:00", "")
            dt = datetime.fromisoformat(ed)
            secs = (dt - datetime.utcnow()).total_seconds()
            if secs < 0:
                time_label = "скоро"
                urgency_prefix = "⚡ "
            elif secs < 3600:
                mins = max(1, int(secs / 60))
                time_label = f"{mins} мин."
                urgency_prefix = "⚡ "
            elif secs < 86400:
                hours = max(1, int(secs / 3600))
                time_label = f"{hours} ч."
                urgency_prefix = "🔴 " if secs < 3 * 3600 else ""
            else:
                days = secs / 86400
                time_label = f"{days:.1f} дн."
                urgency_prefix = "🔴 " if days < 3 else ""
        except Exception:
            pass

    conf_int = int(signal.confidence)
    price = signal.current_price

    if signal.expected_outcome == "YES":
        action = "Купить YES"
        entry_price = price
    else:
        action = "Купить NO"
        entry_price = 1.0 - price   # NO token costs (1 - YES_price)

    roi = signal.expected_profit

    msg = (
        f"{signal.strategy}\n\n"
        f"<code>{signal.market_name}</code>\n\n"
        f"🎯 Позиция      {hbold(action)}\n"
        f"💵 Цена входа   {entry_price:.1%}\n"
        f"📈 Профит       +{roi:.1f}% ROI\n"
        f"🔥 Уверенность  {conf_int}%\n"
    )

    if time_label:
        msg += f"⏳ Срок         {urgency_prefix}{time_label}\n"

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
        reasoning = f"Metaculus{fc_str}: {meta_prob:.0f}% vs рынок {poly_prob:.0f}% — расхождение {div:.0f}%."

    elif signal.strategy == "📊 Smart Money":
        bid_d = signal.details.get("bid_depth", 0)
        ask_d = signal.details.get("ask_depth", 0)
        imb   = signal.details.get("imbalance", 0) * 100
        reasoning = f"Крупные ордера: bid ${bid_d:,.0f} / ask ${ask_d:,.0f} — перекос {imb:.0f}% в сторону {signal.expected_outcome}."

    elif signal.strategy == "🐦 X Sentiment":
        evidence = signal.details.get("evidence", "")
        if evidence:
            reasoning = evidence[:200]

    elif signal.strategy == "🏛️ Официальные данные":
        rel = signal.details.get("release_name", "")
        rel_date = signal.details.get("release_date", "")
        if rel:
            reasoning = f"Выход данных «{rel}»{(' ' + rel_date) if rel_date else ''} напрямую отвечает на вопрос рынка."

    if reasoning:
        msg += f"\n{hitalic(reasoning)}"

    return msg


# ── Core signal delivery ──────────────────────────────────────────────────────

async def _send_main_menu(telegram_id: int):
    """Send main menu as a new message."""
    user   = await db.get_or_create_user(telegram_id=telegram_id)
    is_vip = await db.is_vip(telegram_id)
    is_sup = await db.is_superuser(telegram_id)
    await bot.send_message(
        telegram_id,
        MAIN_MENU_TEXT,
        reply_markup=get_main_menu_keyboard(user.signals_enabled, is_vip, is_sup)
    )


async def _scan_loop_for_user(telegram_id: int):
    """Infinite scan loop — runs every 30 minutes until user stops it."""
    SCAN_INTERVAL = 30 * 60  # 30 minutes
    scan_count = 0

    while telegram_id in _scanning_users:
        scan_count += 1
        print(f"[SCAN LOOP] User {telegram_id} — scan #{scan_count}")

        try:
            await check_signals_for_user(telegram_id)
        except Exception as e:
            print(f"[SCAN LOOP] Error for {telegram_id}: {e}")

        # Wait 30 minutes, checking for cancellation every 5 seconds
        elapsed = 0
        while elapsed < SCAN_INTERVAL and telegram_id in _scanning_users:
            await asyncio.sleep(5)
            elapsed += 5

        # No reminder sent — bot just silently starts the next scan cycle

    print(f"[SCAN LOOP] User {telegram_id} — loop ended after {scan_count} scans")


async def check_signals_for_user(telegram_id: int):
    """Fetch signals, apply user settings, send the best one, queue the rest."""
    try:
        is_vip  = await db.is_vip(telegram_id)
        is_sup  = await db.is_superuser(telegram_id)
        is_priv = is_vip or is_sup

        s          = await ensure_user_settings(telegram_id)
        min_conf   = max(s["min_confidence"], 65 if is_priv else 80)
        min_profit = s["min_profit"]
        scan_mode_key = s.get("scan_mode", "")
        max_profit = SCAN_MODE_PRESETS.get(scan_mode_key, {}).get("max_profit", None)
        allowed_strats = {STRATEGY_SIGNAL_NAMES[k] for k in s["strategies"] if k in STRATEGY_SIGNAL_NAMES}

        signals = await signal_generator.generate_all_signals()

        # Check if user cancelled while scanning
        if telegram_id not in _scanning_users:
            return

        # Don't discard from _scanning_users here — the loop manages that

        if not signals:
            await bot.send_message(
                telegram_id,
                "ℹ️ Рынки не показывают возможностей прямо сейчас.\n"
                "Следующее автосканирование через 30 минут 🔄",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")]
                ])
            )
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
            # UMA Oracle signals expiring ≤5 days get a reduced ROI floor,
            # but must still meet a mode-appropriate minimum.
            # easy/default: 0.3% floor  |  mid: 3% floor  |  hard: 10% floor
            scan_mode = s.get("scan_mode", "")
            is_uma_near = (
                sig.strategy == "🔮 UMA Оракул"
                and sig.details.get("days_to_expiry", 999) <= 5
            )
            if is_uma_near:
                uma_mode_floor = {"easy": 0.3, "mid": 3.0, "hard": 10.0}.get(scan_mode, 0.3)
                if sig.expected_profit < uma_mode_floor:
                    continue
            else:
                if sig.expected_profit < min_profit:
                    continue
            # ── Max profit cap (Easy/Mid mode upper bound) ────────────────────
            if max_profit is not None and sig.expected_profit > max_profit:
                continue
            # ── Topic filter ──────────────────────────────────────────────────
            sig_category = sig.details.get("category", "")
            sig_topic = classify_market_topic(sig_category, sig.market_name)
            if sig_topic not in s["topics"]:
                continue
            already = await db.was_signal_sent(user.id, sig.market_id, sig.strategy, hours=6)
            if already:
                continue
            valid.append(sig)

        if not valid:
            total_valid = len([s for s in signals if s.is_valid])
            hint = " Попробуйте снизить пороги в ⚙️ Настройках." if total_valid > 0 else " Рынок спокойный."
            await bot.send_message(
                telegram_id,
                f"ℹ️ Новых сигналов по вашим фильтрам нет.{hint}\n"
                f"Следующее автосканирование через 30 минут 🔄",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings")],
                    [InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")],
                ])
            )
            return

        max_sig = s.get("max_signals", 3)
        multi_mode = s.get("multi_mode", False)

        # В мульти-режиме берём ВСЕ подходящие сигналы (до max_signals),
        # кладём их в очередь и шлём первый + кнопку "Следующий сигнал"
        # В обычном режиме — отправляем сразу все пачкой (старое поведение)

        # Sort by soonest expiry first, then by highest confidence
        from logic import _days_sort_key_str
        valid.sort(key=lambda sig: (
            _days_sort_key_str(sig.end_date),
            -sig.confidence
        ))
        valid = valid[:max_sig]

        if multi_mode:
            # Мульти-режим: первый сигнал + очередь остальных
            _signal_queues[telegram_id] = list(valid[1:])
            total_found = len(valid)
            await send_signal_to_user(
                telegram_id, valid[0], is_priv,
                has_more=len(valid) > 1,
                multi_mode=True,
                signal_num=1,
                total_signals=total_found,
            )
        else:
            # Обычный режим: первый сигнал + кнопка "Другой сигнал" если есть ещё
            _signal_queues[telegram_id] = list(valid[1:])
            await send_signal_to_user(telegram_id, valid[0], is_priv, has_more=len(valid) > 1)

    except Exception as e:
        print(f"[ERR check_signals_for_user({telegram_id})] {e}")
        import traceback; traceback.print_exc()
        # Don't discard from _scanning_users on error — let loop handle retry
        try:
            await bot.send_message(
                telegram_id,
                "⚠️ Ошибка при сканировании. Повтор через 30 минут.\n"
                "Или нажмите ⛔ чтобы остановить.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⛔ Остановить сканирование", callback_data="stop_scan")]
                ])
            )
        except Exception:
            pass


async def send_signal_to_user(
    telegram_id: int, signal: Signal, is_vip: bool = False, has_more: bool = False,
    multi_mode: bool = False, signal_num: int = 1, total_signals: int = 1,
):
    try:
        msg_text = await format_signal_message(
            signal,
            total=len(_signal_queues.get(telegram_id, [])) + 1
        )

        if multi_mode and total_signals > 1:
            # Шапка с прогрессом: "Сигнал 1 из 5"
            progress = f"🔢 {hbold(f'Сигнал {signal_num} из {total_signals}')}\n"
            msg_text = f"{progress}{msg_text}"
        elif is_vip:
            msg_text = f"💎 {hbold('VIP SIGNAL')}\n{msg_text}"

        if is_vip and (multi_mode and total_signals > 1):
            msg_text = f"💎 {hbold('VIP')} · {msg_text}"

        kb = get_signal_keyboard(
            signal.market_name,
            signal.source_url,
            has_more=has_more,
            scanning=(telegram_id in _scanning_users)
        )

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
            # Only VIP users receive broadcast signals
            if not is_user_vip:
                continue

            s          = await ensure_user_settings(user.telegram_id)
            min_conf   = max(s["min_confidence"], 65)
            min_profit = s["min_profit"]
            allowed_strats = {STRATEGY_SIGNAL_NAMES[k] for k in s["strategies"] if k in STRATEGY_SIGNAL_NAMES}

            if signal.strategy not in allowed_strats:
                continue
            if signal.confidence < min_conf:
                continue
            scan_mode = s.get("scan_mode", "")
            is_uma_near = (
                signal.strategy == "🔮 UMA Оракул"
                and signal.details.get("days_to_expiry", 999) <= 5
            )
            if is_uma_near:
                uma_mode_floor = {"easy": 0.3, "mid": 3.0, "hard": 10.0}.get(scan_mode, 0.3)
                if signal.expected_profit < uma_mode_floor:
                    continue
            else:
                if signal.expected_profit < min_profit:
                    continue
            # ── Topic filter ──────────────────────────────────────────────────
            sig_category = signal.details.get("category", "")
            sig_topic = classify_market_topic(sig_category, signal.market_name)
            if sig_topic not in s["topics"]:
                continue
            if await db.was_signal_sent(user.id, signal.market_id, signal.strategy, hours=6):
                continue

            msg_text = await format_signal_message(signal)
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
    """Background loop — checks pending payments and VIP expiry every 30 seconds."""
    print("[LOOP] Background loop started (payment polling + VIP expiry)")
    while True:
        await _check_pending_payments()
        await _check_vip_expiry()
        await asyncio.sleep(30)


async def _check_vip_expiry():
    """Notify users whose VIP just expired and downgrade them to regular user."""
    try:
        expired_ids = await db.get_recently_expired_vip_users()
        for telegram_id in expired_ids:
            if telegram_id == config.SUPERUSER_ID:
                continue
            print(f"[VIP EXPIRY] Subscription expired for user {telegram_id}")
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💎 Продлить подписку", callback_data="vip_info")],
            ])
            try:
                await bot.send_message(
                    telegram_id,
                    f"⏳ {hbold('Ваша VIP-подписка истекла')}\n\n"
                    f"Доступ к эксклюзивным сигналам закрыт 🔒\n\n"
                    f"Продлите подписку, чтобы снова получать точные сигналы "
                    f"с Polymarket и быть на шаг впереди рынка 📊",
                    reply_markup=keyboard,
                )
            except Exception as e:
                print(f"[VIP EXPIRY] Failed to notify {telegram_id}: {e}")
    except Exception as e:
        print(f"[VIP EXPIRY] Error in expiry check: {e}")


async def _check_pending_payments():
    """Auto-activates VIP for paid invoices."""
    pending = get_all_pending_invoices()
    if not pending:
        return

    for invoice_id, data in list(pending.items()):
        try:
            age = (datetime.now() - data["created_at"]).total_seconds()
            if age > 7200:
                remove_pending_invoice(invoice_id)
                continue

            status = await get_invoice_status(invoice_id)

            if status == "paid":
                telegram_id = data["telegram_id"]
                days = data["days"]
                plan_key = data["plan_key"]

                success = await db.add_vip(telegram_id, config.SUPERUSER_ID, days)
                remove_pending_invoice(invoice_id)

                if success:
                    days_label = VIP_PLANS.get(plan_key, (days, 0, f"{days} дней"))[2]
                    print(f"[PAYMENT] VIP activated: user={telegram_id}, days={days}")
                    try:
                        await bot.send_message(
                            telegram_id,
                            f"🎉 {hbold('Оплата получена! Доступ открыт!')}\n\n"
                            f"✅ Тариф: {days_label} ({days} дней)\n\n"
                            f"Нажмите /start чтобы войти в бота.",
                        )
                    except Exception as e:
                        print(f"[PAYMENT] Notification failed for {telegram_id}: {e}")

                    # ── Referral bonus ────────────────────────────────────────
                    referrer_id = await db.get_referrer(telegram_id)
                    if referrer_id and referrer_id != config.SUPERUSER_ID:
                        bonus_granted = await db.pay_referral_bonus(referrer_id, telegram_id)
                        if bonus_granted:
                            print(f"[REFERRAL] Bonus: referrer={referrer_id}, referred={telegram_id}")
                            try:
                                await bot.send_message(
                                    referrer_id,
                                    f"🎁 {hbold('+7 дней к подписке!')}\n\n"
                                    f"Ваш реферал оформил VIP — бонус начислен автоматически.\n\n"
                                    f"Приводите ещё! /start → 👥 Реферальная программа"
                                )
                            except Exception:
                                pass

                    if telegram_id != config.SUPERUSER_ID:
                        try:
                            await bot.send_message(
                                config.SUPERUSER_ID,
                                f"💰 {hbold('Новая оплата!')}\n\n"
                                f"👤 User ID: <code>{telegram_id}</code>\n"
                                f"📦 Тариф: {days_label} ({days} дней)\n"
                                f"🧾 Invoice: {invoice_id}"
                            )
                        except Exception:
                            pass

            elif status == "expired":
                remove_pending_invoice(invoice_id)

        except Exception as e:
            print(f"[PAYMENT POLL] Error for invoice {invoice_id}: {e}")


# ── Superuser management panel ────────────────────────────────────────────────

@dp.callback_query(F.data == "su_manage")
async def on_su_manage(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await callback.answer()
    await callback.message.edit_text(
        f"👑 {hbold('Панель управления')}\n\nВыберите действие:",
        reply_markup=get_su_manage_keyboard()
    )


@dp.callback_query(F.data == "su_add_vip")
async def on_su_add_vip(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌", show_alert=True)
        return
    _awaiting_input[callback.from_user.id] = "su_add_vip"
    await callback.answer()
    await callback.message.answer(
        "➕ Выдать VIP\n\n"
        "Введите: <code>@username days</code>\n"
        "Например: <code>@username 30</code>"
    )


@dp.callback_query(F.data == "su_remove_vip")
async def on_su_remove_vip(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌", show_alert=True)
        return
    _awaiting_input[callback.from_user.id] = "su_remove_vip"
    await callback.answer()
    await callback.message.answer(
        "➖ Убрать VIP\n\n"
        "Введите username пользователя:\n"
        "Например: <code>@username</code>"
    )


@dp.callback_query(F.data == "su_add_admin")
async def on_su_add_admin(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌", show_alert=True)
        return
    _awaiting_input[callback.from_user.id] = "su_add_admin"
    await callback.answer()
    await callback.message.answer(
        "🛡 Выдать права Admin\n\n"
        "Введите username:\n"
        "Например: <code>@username</code>"
    )


@dp.callback_query(F.data == "su_remove_admin")
async def on_su_remove_admin(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌", show_alert=True)
        return
    _awaiting_input[callback.from_user.id] = "su_remove_admin"
    await callback.answer()
    await callback.message.answer(
        "🚫 Убрать Admin\n\n"
        "Введите username:\n"
        "Например: <code>@username</code>"
    )


@dp.callback_query(F.data == "su_list_vip")
async def on_su_list_vip(callback: CallbackQuery):
    if callback.from_user.id != config.SUPERUSER_ID:
        await callback.answer("❌", show_alert=True)
        return
    await callback.answer()
    vip_users = await db.get_vip_users()
    if not vip_users:
        text = f"📋 {hbold('VIP пользователи')}\n\nНет активных VIP."
    else:
        lines = [f"📋 {hbold(f'VIP пользователей: {len(vip_users)}')}\n"]
        for u in vip_users:
            days_left = (u.vip_until - datetime.now()).days if u.vip_until else 0
            uname = f"@{u.username}" if u.username else str(u.telegram_id)
            lines.append(f"• {uname} (<code>{u.telegram_id}</code>) — {days_left} дн.")
        text = "\n".join(lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Назад", callback_data="su_manage")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)


async def _handle_su_text_input(message: Message, input_type: str):
    """Process superuser text inputs for user management."""
    uid = message.from_user.id
    text = message.text.strip()

    async def resolve_username(raw: str) -> Optional[int]:
        """Resolve @username or plain username to telegram_id."""
        tid = await db.get_telegram_id_by_username(raw)
        return tid

    if input_type == "su_add_vip":
        parts = text.split()
        if len(parts) != 2:
            _awaiting_input[uid] = input_type
            await message.answer("❌ Введите: <code>@username days</code>\nНапример: <code>@username 30</code>")
            return
        username_raw, days_raw = parts
        try:
            days = int(days_raw)
        except ValueError:
            _awaiting_input[uid] = input_type
            await message.answer("❌ Количество дней должно быть числом. Попробуйте снова.")
            return

        target_id = await resolve_username(username_raw)
        if not target_id:
            await message.answer(f"❌ Пользователь <code>{username_raw}</code> не найден.\nОн должен написать /start боту.")
            return

        success = await db.add_vip(target_id, uid, days)
        if success:
            await message.answer(f"✅ VIP выдан: {username_raw} на {days} дн.")
            try:
                await bot.send_message(
                    target_id,
                    f"🎉 {hbold('Вам выдан VIP доступ!')}\n\n"
                    f"Срок: {days} дней\n"
                    f"Используйте /start для обновления меню."
                )
            except Exception:
                pass
        else:
            await message.answer(f"❌ Не удалось выдать VIP пользователю {username_raw}.")

    elif input_type == "su_remove_vip":
        target_id = await resolve_username(text)
        if not target_id:
            await message.answer(f"❌ Пользователь <code>{text}</code> не найден.\nОн должен написать /start боту.")
            return
        success = await db.remove_vip(target_id)
        if success:
            await message.answer(f"✅ VIP убран у пользователя {text}")
        else:
            await message.answer(f"❌ Не удалось. Пользователь {text} не найден.")

    elif input_type == "su_add_admin":
        target_id = await resolve_username(text)
        if not target_id:
            await message.answer(f"❌ Пользователь <code>{text}</code> не найден.\nОн должен написать /start боту.")
            return
        success = await db.add_admin(target_id)
        if target_id not in config.ADMIN_IDS:
            config.ADMIN_IDS.append(target_id)
        if success:
            await message.answer(f"✅ Admin выдан: {text} (<code>{target_id}</code>)")
        else:
            await message.answer(f"❌ Ошибка при добавлении.")

    elif input_type == "su_remove_admin":
        target_id = await resolve_username(text)
        if not target_id:
            await message.answer(f"❌ Пользователь <code>{text}</code> не найден.")
            return
        if target_id == config.SUPERUSER_ID:
            await message.answer("❌ Нельзя убрать права Superuser.")
            return
        success = await db.remove_admin(target_id)
        if target_id in config.ADMIN_IDS:
            config.ADMIN_IDS.remove(target_id)
        if success:
            await message.answer(f"✅ Admin убран у {text}")
        else:
            await message.answer(f"❌ Ошибка.")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="← Управление", callback_data="su_manage")]
    ])
    await message.answer("Вернуться в панель:", reply_markup=kb)


# ── Startup / Shutdown ────────────────────────────────────────────────────────

async def on_startup(bot: Bot):
    await db.init()
    # Reload persisted admins from DB into config.ADMIN_IDS
    try:
        db_admins = await db.get_admin_ids_from_db()
        for aid in db_admins:
            if aid not in config.ADMIN_IDS:
                config.ADMIN_IDS.append(aid)
        print(f"[STARTUP] Loaded {len(db_admins)} admin(s) from DB")
    except Exception as e:
        print(f"[STARTUP] Could not load admins from DB: {e}")
    print("Bot started and database initialized!")


async def on_shutdown(bot: Bot):
    print("Bot shutting down...")
