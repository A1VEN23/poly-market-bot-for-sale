"""
Database operations for Polymarket Signal Bot.
Supports SQLite (local/dev) and PostgreSQL (production via DATABASE_URL env var).
"""
import os
import aiosqlite
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from config import config

# Backend detection
_DATABASE_URL = os.getenv("DATABASE_URL", "")
_USE_POSTGRES = bool(_DATABASE_URL)

if _USE_POSTGRES:
    try:
        import asyncpg
    except ImportError:
        print("[DB] asyncpg not installed — falling back to SQLite")
        _USE_POSTGRES = False


@dataclass
class User:
    id: int
    telegram_id: int
    username: Optional[str]
    signals_enabled: bool
    vip_until: Optional[datetime]
    created_at: datetime
    last_active: datetime


@dataclass
class SentSignal:
    id: int
    user_id: int
    market_id: str
    strategy: str
    sent_at: datetime
    opened: bool
    profit_loss: Optional[float]


@dataclass
class SignalHistory:
    id: int
    market_id: str
    market_name: str
    strategy: str
    price_at_signal: float
    expected_outcome: str
    confidence: float
    sent_at: datetime
    outcome_verified: bool
    actual_outcome: Optional[str]


class Database:
    """
    Async database manager.
    Uses PostgreSQL when DATABASE_URL env var is set (Render production),
    falls back to SQLite for local development.
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or os.getenv("DATABASE_PATH", "polymarket_bot.db")
        self.database_url = _DATABASE_URL
        self._pg_pool = None  # asyncpg connection pool

    async def _get_pg_pool(self):
        """Get or create asyncpg connection pool."""
        if self._pg_pool is None:
            self._pg_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=1,
                max_size=5,
                command_timeout=30,
            )
        return self._pg_pool

    async def init(self):
        """Initialize database tables. Supports both SQLite and PostgreSQL."""
        if _USE_POSTGRES:
            await self._init_postgres()
        else:
            await self._init_sqlite()

    async def _init_sqlite(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE NOT NULL,
                    username TEXT,
                    signals_enabled BOOLEAN DEFAULT 1,
                    vip_until TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sent_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    market_id TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    opened BOOLEAN DEFAULT 0,
                    profit_loss REAL,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS signal_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    price_at_signal REAL NOT NULL,
                    expected_outcome TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    outcome_verified BOOLEAN DEFAULT 0,
                    actual_outcome TEXT
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS vip_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    admin_id INTEGER NOT NULL,
                    days_added INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    referrer_telegram_id INTEGER NOT NULL,
                    referred_telegram_id INTEGER NOT NULL UNIQUE,
                    bonus_paid BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    telegram_id INTEGER PRIMARY KEY,
                    min_profit REAL DEFAULT 3.0,
                    min_confidence REAL DEFAULT 0.6,
                    max_signals INTEGER DEFAULT 5,
                    strategies TEXT DEFAULT 'uma,news,meta,orderbook,xsent,official',
                    topics TEXT DEFAULT 'politics,sports,economics,science,other',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    telegram_id INTEGER PRIMARY KEY
                )
            """)
            await db.commit()

    async def _init_postgres(self):
        pool = await self._get_pg_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    telegram_id BIGINT UNIQUE NOT NULL,
                    username TEXT,
                    signals_enabled BOOLEAN DEFAULT TRUE,
                    vip_until TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    last_active TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sent_signals (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    market_id TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    sent_at TIMESTAMP DEFAULT NOW(),
                    opened BOOLEAN DEFAULT FALSE,
                    profit_loss REAL
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_history (
                    id SERIAL PRIMARY KEY,
                    market_id TEXT NOT NULL,
                    market_name TEXT NOT NULL,
                    strategy TEXT NOT NULL,
                    price_at_signal REAL NOT NULL,
                    expected_outcome TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    sent_at TIMESTAMP DEFAULT NOW(),
                    outcome_verified BOOLEAN DEFAULT FALSE,
                    actual_outcome TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS vip_transactions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    admin_id BIGINT NOT NULL,
                    days_added INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id SERIAL PRIMARY KEY,
                    referrer_telegram_id BIGINT NOT NULL,
                    referred_telegram_id BIGINT NOT NULL UNIQUE,
                    bonus_paid BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_settings (
                    telegram_id BIGINT PRIMARY KEY,
                    min_profit REAL DEFAULT 3.0,
                    min_confidence REAL DEFAULT 0.6,
                    max_signals INTEGER DEFAULT 5,
                    strategies TEXT DEFAULT 'uma,news,meta,orderbook,xsent,official',
                    topics TEXT DEFAULT 'politics,sports,economics,science,other',
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    telegram_id BIGINT PRIMARY KEY
                )
            """)
        print("[DB] PostgreSQL tables initialized")

    # ── Universal query helpers ────────────────────────────────────────────────

    async def _execute(self, sql: str, params: tuple = (), fetch: str = None):
        """
        Run a SQL query on SQLite or PostgreSQL.
        fetch: None (execute only), 'one' (fetchone), 'all' (fetchall), 'lastid'
        PostgreSQL uses $1,$2 placeholders; SQLite uses ?. We auto-convert.
        """
        if _USE_POSTGRES:
            # Convert ? placeholders to $1, $2, ...
            pg_sql = sql
            idx = 0
            while '?' in pg_sql:
                idx += 1
                pg_sql = pg_sql.replace('?', f'${idx}', 1)
            # Convert AUTOINCREMENT → (not needed in PG, SERIAL handles it)
            pool = await self._get_pg_pool()
            async with pool.acquire() as conn:
                if fetch == 'one':
                    row = await conn.fetchrow(pg_sql, *params)
                    return dict(row) if row else None
                elif fetch == 'all':
                    rows = await conn.fetch(pg_sql, *params)
                    return [dict(r) for r in rows]
                elif fetch == 'lastid':
                    row = await conn.fetchrow(pg_sql + ' RETURNING id', *params)
                    return row['id'] if row else None
                else:
                    await conn.execute(pg_sql, *params)
                    return None
        else:
            async with aiosqlite.connect(self.db_path) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(sql, params)
                if fetch == 'one':
                    row = await cursor.fetchone()
                    return dict(row) if row else None
                elif fetch == 'all':
                    rows = await cursor.fetchall()
                    return [dict(r) for r in rows]
                elif fetch == 'lastid':
                    await db.commit()
                    return cursor.lastrowid
                else:
                    await db.commit()
                    return None


    async def save_user_settings(self, telegram_id: int, settings: dict):
        """Persist user settings to DB."""
        strategies = ",".join(sorted(settings.get("strategies", [])))
        topics = ",".join(sorted(settings.get("topics", [])))
        if _USE_POSTGRES:
            await self._execute("""
                INSERT INTO user_settings (telegram_id, min_profit, min_confidence, max_signals, strategies, topics, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, NOW())
                ON CONFLICT(telegram_id) DO UPDATE SET
                    min_profit=EXCLUDED.min_profit,
                    min_confidence=EXCLUDED.min_confidence,
                    max_signals=EXCLUDED.max_signals,
                    strategies=EXCLUDED.strategies,
                    topics=EXCLUDED.topics,
                    updated_at=NOW()
            """, (telegram_id, settings.get("min_profit", 3.0), settings.get("min_confidence", 0.6),
                  settings.get("max_signals", 5), strategies, topics))
        else:
            await self._execute("""
                INSERT INTO user_settings (telegram_id, min_profit, min_confidence, max_signals, strategies, topics, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    min_profit=excluded.min_profit,
                    min_confidence=excluded.min_confidence,
                    max_signals=excluded.max_signals,
                    strategies=excluded.strategies,
                    topics=excluded.topics,
                    updated_at=CURRENT_TIMESTAMP
            """, (telegram_id, settings.get("min_profit", 3.0), settings.get("min_confidence", 0.6),
                  settings.get("max_signals", 5), strategies, topics))

    async def load_user_settings(self, telegram_id: int) -> Optional[dict]:
        """Load user settings from DB."""
        row = await self._execute(
            "SELECT min_profit, min_confidence, max_signals, strategies, topics FROM user_settings WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row:
            return None
        return {
            "min_profit": row["min_profit"],
            "min_confidence": row["min_confidence"],
            "max_signals": row["max_signals"],
            "strategies": set(row["strategies"].split(",")) if row["strategies"] else set(),
            "topics": set(row["topics"].split(",")) if row["topics"] else set(),
        }

    async def get_or_create_user(self, telegram_id: int, username: Optional[str] = None) -> User:
        """Get existing user or create new one."""
        row = await self._execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if row:
            await self._execute(
                "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE telegram_id = ?",
                (telegram_id,)
            )
            return User(
                id=row["id"],
                telegram_id=row["telegram_id"],
                username=row["username"],
                signals_enabled=bool(row["signals_enabled"]),
                vip_until=row["vip_until"] if isinstance(row["vip_until"], datetime)
                          else (datetime.fromisoformat(row["vip_until"]) if row["vip_until"] else None),
                created_at=row["created_at"] if isinstance(row["created_at"], datetime)
                           else datetime.fromisoformat(row["created_at"]),
                last_active=datetime.now()
            )
        # Create new user
        new_id = await self._execute(
            "INSERT INTO users (telegram_id, username, signals_enabled) VALUES (?, ?, ?)",
            (telegram_id, username, True), fetch='lastid'
        )
        return User(
            id=new_id,
            telegram_id=telegram_id,
            username=username,
            signals_enabled=True,
            vip_until=None,
            created_at=datetime.now(),
            last_active=datetime.now()
        )

    async def toggle_signals(self, telegram_id: int) -> bool:
        """Toggle signal notifications for user. Returns new state."""
        row = await self._execute(
            "SELECT signals_enabled FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row:
            return False
        new_state = not bool(row["signals_enabled"])
        await self._execute(
            "UPDATE users SET signals_enabled = ? WHERE telegram_id = ?",
            (new_state, telegram_id)
        )
        return new_state

    async def add_vip(self, telegram_id: int, admin_id: int, days: int) -> bool:
        """Add VIP days to user."""
        row = await self._execute(
            "SELECT id, vip_until FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row:
            return False
        user_id = row["id"]
        current_vip = row["vip_until"]
        current_vip_dt = (current_vip if isinstance(current_vip, datetime)
                          else datetime.fromisoformat(current_vip)) if current_vip else datetime.now()
        if current_vip_dt < datetime.now():
            current_vip_dt = datetime.now()
        new_vip_until = current_vip_dt + timedelta(days=days)
        await self._execute(
            "UPDATE users SET vip_until = ? WHERE id = ?",
            (new_vip_until.isoformat(), user_id)
        )
        await self._execute(
            "INSERT INTO vip_transactions (user_id, admin_id, days_added) VALUES (?, ?, ?)",
            (user_id, admin_id, days)
        )
        return True

    async def is_vip(self, telegram_id: int) -> bool:
        """Check if user has active VIP."""
        if telegram_id == config.SUPERUSER_ID:
            return True
        row = await self._execute(
            "SELECT vip_until FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row or not row["vip_until"]:
            return False
        vip_until = (row["vip_until"] if isinstance(row["vip_until"], datetime)
                     else datetime.fromisoformat(row["vip_until"]))
        return vip_until > datetime.now()

    async def is_superuser(self, telegram_id: int) -> bool:
        return telegram_id == config.SUPERUSER_ID

    async def get_vip_days_remaining(self, telegram_id: int) -> int:
        if telegram_id == config.SUPERUSER_ID:
            return -1
        row = await self._execute(
            "SELECT vip_until FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row or not row["vip_until"]:
            return 0
        vip_until = (row["vip_until"] if isinstance(row["vip_until"], datetime)
                     else datetime.fromisoformat(row["vip_until"]))
        if vip_until <= datetime.now():
            return 0
        return (vip_until - datetime.now()).days

    async def get_user_profile(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        row = await self._execute(
            "SELECT * FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row:
            return None
        is_super = telegram_id == config.SUPERUSER_ID
        days_remaining = -1 if is_super else await self.get_vip_days_remaining(telegram_id)
        vip_status = "Lifetime / Developer" if is_super else ("Active" if days_remaining > 0 else "Inactive")
        return {
            "id": row["id"],
            "telegram_id": row["telegram_id"],
            "username": row["username"],
            "signals_enabled": bool(row["signals_enabled"]),
            "vip_status": vip_status,
            "days_remaining": days_remaining,
            "is_superuser": is_super,
            "created_at": row["created_at"],
            "last_active": row["last_active"],
        }

    async def get_active_users(self) -> List[User]:
        rows = await self._execute(
            "SELECT * FROM users WHERE signals_enabled = 1",
            fetch='all'
        )
        result = []
        for row in (rows or []):
            result.append(User(
                id=row["id"],
                telegram_id=row["telegram_id"],
                username=row["username"],
                signals_enabled=bool(row["signals_enabled"]),
                vip_until=(row["vip_until"] if isinstance(row["vip_until"], datetime)
                           else (datetime.fromisoformat(row["vip_until"]) if row["vip_until"] else None)),
                created_at=(row["created_at"] if isinstance(row["created_at"], datetime)
                            else datetime.fromisoformat(row["created_at"])),
                last_active=(row["last_active"] if isinstance(row["last_active"], datetime)
                             else datetime.fromisoformat(row["last_active"])),
            ))
        return result

    async def get_vip_users(self) -> List[User]:
        users = await self.get_active_users()
        return [u for u in users if u.vip_until and u.vip_until > datetime.now()]

    async def get_telegram_id_by_username(self, username: str) -> Optional[int]:
        clean = username.lstrip("@").lower()
        row = await self._execute(
            "SELECT telegram_id FROM users WHERE LOWER(username) = ?",
            (clean,), fetch='one'
        )
        return row["telegram_id"] if row else None

    async def get_recently_expired_vip_users(self) -> List[int]:
        now = datetime.now()
        window_start = (now - timedelta(minutes=10)).isoformat()
        window_end = now.isoformat()
        if _USE_POSTGRES:
            rows = await self._execute(
                """SELECT telegram_id FROM users
                   WHERE vip_until IS NOT NULL
                     AND vip_until > ?::timestamp
                     AND vip_until <= ?::timestamp""",
                (window_start, window_end), fetch='all'
            )
        else:
            rows = await self._execute(
                """SELECT telegram_id FROM users
                   WHERE vip_until IS NOT NULL
                     AND vip_until > ?
                     AND vip_until <= ?""",
                (window_start, window_end), fetch='all'
            )
        return [r["telegram_id"] for r in (rows or [])]

    async def remove_vip(self, telegram_id: int) -> bool:
        row = await self._execute(
            "SELECT id FROM users WHERE telegram_id = ?",
            (telegram_id,), fetch='one'
        )
        if not row:
            return False
        await self._execute(
            "UPDATE users SET vip_until = NULL WHERE telegram_id = ?",
            (telegram_id,)
        )
        return True

    async def get_admin_ids_from_db(self) -> List[int]:
        try:
            rows = await self._execute("SELECT telegram_id FROM admins", fetch='all')
            return [r["telegram_id"] for r in (rows or [])]
        except Exception:
            return []

    async def add_admin(self, telegram_id: int) -> bool:
        try:
            if _USE_POSTGRES:
                await self._execute(
                    "INSERT INTO admins (telegram_id) VALUES (?) ON CONFLICT DO NOTHING",
                    (telegram_id,)
                )
            else:
                await self._execute(
                    "INSERT OR IGNORE INTO admins (telegram_id) VALUES (?)",
                    (telegram_id,)
                )
            return True
        except Exception:
            return False

    async def remove_admin(self, telegram_id: int) -> bool:
        try:
            await self._execute("DELETE FROM admins WHERE telegram_id = ?", (telegram_id,))
            return True
        except Exception:
            return False

    async def record_signal_sent(self, user_id: int, market_id: str, strategy: str):
        await self._execute(
            "INSERT INTO sent_signals (user_id, market_id, strategy) VALUES (?, ?, ?)",
            (user_id, market_id, strategy)
        )

    async def was_signal_sent(self, user_id: int, market_id: str, strategy: str, hours: int = 24) -> bool:
        since = (datetime.now() - timedelta(hours=hours)).isoformat()
        row = await self._execute(
            """SELECT 1 FROM sent_signals
               WHERE user_id = ? AND market_id = ? AND strategy = ?
               AND sent_at > ?""",
            (user_id, market_id, strategy, since), fetch='one'
        )
        return row is not None

    async def save_signal_history(self, market_id: str, market_name: str,
                                   strategy: str, price_at_signal: float,
                                   expected_outcome: str, confidence: float) -> int:
        new_id = await self._execute(
            """INSERT INTO signal_history
               (market_id, market_name, strategy, price_at_signal, expected_outcome, confidence)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (market_id, market_name, strategy, price_at_signal, expected_outcome, confidence),
            fetch='lastid'
        )
        return new_id

    async def update_signal_outcome(self, signal_id: int, actual_outcome: str):
        await self._execute(
            "UPDATE signal_history SET outcome_verified = 1, actual_outcome = ? WHERE id = ?",
            (actual_outcome, signal_id)
        )

    async def get_signal_stats(self, days: int = 30) -> Dict[str, Any]:
        since = (datetime.now() - timedelta(days=days)).isoformat()
        rows = await self._execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN outcome_verified = 1 THEN 1 ELSE 0 END) as verified,
                      strategy
               FROM signal_history
               WHERE sent_at > ?
               GROUP BY strategy""",
            (since,), fetch='all'
        )
        stats = {}
        for row in (rows or []):
            stats[row["strategy"]] = {"total": row["total"], "verified": row["verified"]}
        return stats

    # ── Referral methods ──────────────────────────────────────────────────────

    async def register_referral(self, referrer_telegram_id: int, referred_telegram_id: int) -> bool:
        try:
            if _USE_POSTGRES:
                await self._execute(
                    """INSERT INTO referrals (referrer_telegram_id, referred_telegram_id)
                       VALUES (?, ?) ON CONFLICT DO NOTHING""",
                    (referrer_telegram_id, referred_telegram_id)
                )
            else:
                await self._execute(
                    """INSERT OR IGNORE INTO referrals (referrer_telegram_id, referred_telegram_id)
                       VALUES (?, ?)""",
                    (referrer_telegram_id, referred_telegram_id)
                )
            row = await self._execute(
                "SELECT referrer_telegram_id FROM referrals WHERE referred_telegram_id = ?",
                (referred_telegram_id,), fetch='one'
            )
            return row is not None and row["referrer_telegram_id"] == referrer_telegram_id
        except Exception as e:
            print(f"[DB] register_referral error: {e}")
            return False

    async def get_referrer(self, referred_telegram_id: int) -> Optional[int]:
        row = await self._execute(
            "SELECT referrer_telegram_id FROM referrals WHERE referred_telegram_id = ?",
            (referred_telegram_id,), fetch='one'
        )
        return row["referrer_telegram_id"] if row else None

    async def get_referral_stats(self, referrer_telegram_id: int) -> Dict[str, Any]:
        row = await self._execute(
            "SELECT COUNT(*) as cnt FROM referrals WHERE referrer_telegram_id = ?",
            (referrer_telegram_id,), fetch='one'
        )
        total = row["cnt"] if row else 0

        row = await self._execute(
            "SELECT COUNT(*) as cnt FROM referrals WHERE referrer_telegram_id = ? AND bonus_paid = 1",
            (referrer_telegram_id,), fetch='one'
        )
        paid = row["cnt"] if row else 0

        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        row = await self._execute(
            """SELECT COUNT(*) as cnt FROM referrals
               WHERE referrer_telegram_id = ? AND bonus_paid = 1 AND created_at >= ?""",
            (referrer_telegram_id, month_start.isoformat()), fetch='one'
        )
        paid_this_month = row["cnt"] if row else 0

        BONUS_DAYS_PER_REF = 7
        MAX_REFS_PER_MONTH = 4

        return {
            "total": total,
            "paid": paid,
            "bonus_days_total": paid * BONUS_DAYS_PER_REF,
            "bonus_days_this_month": min(paid_this_month, MAX_REFS_PER_MONTH) * BONUS_DAYS_PER_REF,
            "paid_this_month": paid_this_month,
            "slots_left_this_month": max(0, MAX_REFS_PER_MONTH - paid_this_month),
        }

    async def pay_referral_bonus(self, referrer_telegram_id: int, referred_telegram_id: int) -> bool:
        BONUS_DAYS = 7
        MAX_REFS_PER_MONTH = 4

        row = await self._execute(
            "SELECT bonus_paid FROM referrals WHERE referred_telegram_id = ?",
            (referred_telegram_id,), fetch='one'
        )
        if not row or row["bonus_paid"]:
            return False

        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        row = await self._execute(
            """SELECT COUNT(*) as cnt FROM referrals
               WHERE referrer_telegram_id = ? AND bonus_paid = 1 AND created_at >= ?""",
            (referrer_telegram_id, month_start.isoformat()), fetch='one'
        )
        paid_this_month = row["cnt"] if row else 0

        await self._execute(
            "UPDATE referrals SET bonus_paid = 1 WHERE referred_telegram_id = ?",
            (referred_telegram_id,)
        )

        if paid_this_month >= MAX_REFS_PER_MONTH:
            return False  # Cap reached — marked paid but no bonus days

        await self.add_vip(referrer_telegram_id, 0, BONUS_DAYS)
        return True

    async def has_referral_record(self, referred_telegram_id: int) -> bool:
        row = await self._execute(
            "SELECT 1 FROM referrals WHERE referred_telegram_id = ?",
            (referred_telegram_id,), fetch='one'
        )
        return row is not None


db = Database()