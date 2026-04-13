"""SQLite database operations for Polymarket Signal Bot."""
import aiosqlite
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from config import config


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
    """Async SQLite database manager."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or config.DATABASE_PATH

    async def init(self):
        """Initialize database tables."""
        async with aiosqlite.connect(self.db_path) as db:
            # Users table
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

            # Sent signals table
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

            # Signal history table
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

            # VIP transactions table
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

            await db.commit()

    async def get_or_create_user(self, telegram_id: int, username: Optional[str] = None) -> User:
        """Get existing user or create new one."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            cursor = await db.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

            if row:
                await db.execute(
                    "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE telegram_id = ?",
                    (telegram_id,)
                )
                await db.commit()
                return User(
                    id=row["id"],
                    telegram_id=row["telegram_id"],
                    username=row["username"],
                    signals_enabled=bool(row["signals_enabled"]),
                    vip_until=datetime.fromisoformat(row["vip_until"]) if row["vip_until"] else None,
                    created_at=datetime.fromisoformat(row["created_at"]),
                    last_active=datetime.now()
                )

            # Create new user
            cursor = await db.execute(
                """INSERT INTO users (telegram_id, username, signals_enabled)
                   VALUES (?, ?, ?)""",
                (telegram_id, username, True)
            )
            await db.commit()

            return User(
                id=cursor.lastrowid,
                telegram_id=telegram_id,
                username=username,
                signals_enabled=True,
                vip_until=None,
                created_at=datetime.now(),
                last_active=datetime.now()
            )

    async def toggle_signals(self, telegram_id: int) -> bool:
        """Toggle signal notifications for user. Returns new state."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT signals_enabled FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

            if not row:
                return False

            new_state = not bool(row[0])
            await db.execute(
                "UPDATE users SET signals_enabled = ? WHERE telegram_id = ?",
                (new_state, telegram_id)
            )
            await db.commit()
            return new_state

    async def add_vip(self, telegram_id: int, admin_id: int, days: int) -> bool:
        """Add VIP days to user."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT id, vip_until FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

            if not row:
                return False

            user_id, current_vip = row
            current_vip_dt = datetime.fromisoformat(current_vip) if current_vip else datetime.now()

            if current_vip_dt < datetime.now():
                current_vip_dt = datetime.now()

            new_vip_until = current_vip_dt + timedelta(days=days)

            await db.execute(
                "UPDATE users SET vip_until = ? WHERE id = ?",
                (new_vip_until.isoformat(), user_id)
            )

            await db.execute(
                """INSERT INTO vip_transactions (user_id, admin_id, days_added)
                   VALUES (?, ?, ?)""",
                (user_id, admin_id, days)
            )

            await db.commit()
            return True

    async def is_vip(self, telegram_id: int) -> bool:
        """Check if user has active VIP (including superuser)."""
        # Superuser always has VIP
        if telegram_id == config.SUPERUSER_ID:
            return True

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT vip_until FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

            if not row or not row[0]:
                return False

            vip_until = datetime.fromisoformat(row[0])
            return vip_until > datetime.now()

    async def is_superuser(self, telegram_id: int) -> bool:
        """Check if user is the superuser (developer)."""
        return telegram_id == config.SUPERUSER_ID

    async def get_vip_days_remaining(self, telegram_id: int) -> int:
        """Get number of days remaining for VIP subscription."""
        if telegram_id == config.SUPERUSER_ID:
            return -1  # Lifetime

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT vip_until FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

            if not row or not row[0]:
                return 0

            vip_until = datetime.fromisoformat(row[0])
            if vip_until <= datetime.now():
                return 0

            remaining = vip_until - datetime.now()
            return remaining.days

    async def get_user_profile(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Get user profile information."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()

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
                "last_active": row["last_active"]
            }

    async def get_active_users(self) -> List[User]:
        """Get all users with signals enabled."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM users WHERE signals_enabled = 1"
            )
            rows = await cursor.fetchall()

            return [
                User(
                    id=row["id"],
                    telegram_id=row["telegram_id"],
                    username=row["username"],
                    signals_enabled=bool(row["signals_enabled"]),
                    vip_until=datetime.fromisoformat(row["vip_until"]) if row["vip_until"] else None,
                    created_at=datetime.fromisoformat(row["created_at"]),
                    last_active=datetime.fromisoformat(row["last_active"])
                )
                for row in rows
            ]

    async def get_vip_users(self) -> List[User]:
        """Get all users with active VIP."""
        users = await self.get_active_users()
        return [u for u in users if u.vip_until and u.vip_until > datetime.now()]

    async def record_signal_sent(self, user_id: int, market_id: str, strategy: str):
        """Record that a signal was sent to a user."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO sent_signals (user_id, market_id, strategy)
                   VALUES (?, ?, ?)""",
                (user_id, market_id, strategy)
            )
            await db.commit()

    async def was_signal_sent(self, user_id: int, market_id: str, strategy: str, hours: int = 24) -> bool:
        """Check if signal was already sent to user within time window."""
        async with aiosqlite.connect(self.db_path) as db:
            since = datetime.now() - timedelta(hours=hours)
            cursor = await db.execute(
                """SELECT 1 FROM sent_signals
                   WHERE user_id = ? AND market_id = ? AND strategy = ?
                   AND sent_at > ?""",
                (user_id, market_id, strategy, since.isoformat())
            )
            return await cursor.fetchone() is not None

    async def save_signal_history(self, market_id: str, market_name: str,
                                   strategy: str, price_at_signal: float,
                                   expected_outcome: str, confidence: float) -> int:
        """Save signal to history. Returns signal_id."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT INTO signal_history
                   (market_id, market_name, strategy, price_at_signal,
                    expected_outcome, confidence)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (market_id, market_name, strategy, price_at_signal,
                 expected_outcome, confidence)
            )
            await db.commit()
            return cursor.lastrowid

    async def update_signal_outcome(self, signal_id: int, actual_outcome: str):
        """Update signal with actual outcome."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """UPDATE signal_history
                   SET outcome_verified = 1, actual_outcome = ?
                   WHERE id = ?""",
                (actual_outcome, signal_id)
            )
            await db.commit()

    async def get_signal_stats(self, days: int = 30) -> Dict[str, Any]:
        """Get signal statistics for last N days."""
        async with aiosqlite.connect(self.db_path) as db:
            since = datetime.now() - timedelta(days=days)

            cursor = await db.execute(
                """SELECT COUNT(*) as total,
                          SUM(CASE WHEN outcome_verified = 1 THEN 1 ELSE 0 END) as verified,
                          strategy
                   FROM signal_history
                   WHERE sent_at > ?
                   GROUP BY strategy""",
                (since.isoformat(),)
            )

            stats = {}
            async for row in cursor:
                stats[row["strategy"]] = {
                    "total": row["total"],
                    "verified": row["verified"]
                }

            return stats


db = Database()
