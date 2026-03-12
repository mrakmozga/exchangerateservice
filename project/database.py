"""
database.py — SQLite persistence layer for CNB exchange rates.
"""
import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def db_session(db_path: str):
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str) -> None:
    """Create tables if they do not exist. Parent directory is created if absent."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with db_session(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                currency    TEXT    NOT NULL,
                amount      INTEGER NOT NULL,
                rate        REAL    NOT NULL,
                created_at  TEXT    DEFAULT (datetime('now')),
                UNIQUE (date, currency)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date_currency
            ON exchange_rates (date, currency)
        """)
    logger.info("Database initialised at %s", db_path)


def upsert_rates(db_path: str, rows: list[dict]) -> int:
    """
    Insert or replace rate rows.
    Each dict must have keys: date, currency, amount, rate
    Returns number of rows affected.
    """
    if not rows:
        return 0
    with db_session(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO exchange_rates (date, currency, amount, rate)
            VALUES (:date, :currency, :amount, :rate)
            ON CONFLICT(date, currency) DO UPDATE SET
                amount = excluded.amount,
                rate   = excluded.rate
            """,
            rows,
        )
    return len(rows)


def get_report(db_path: str, start_date: str, end_date: str, currencies: list[str]) -> list[dict]:
    """
    Return min/max/avg for each requested currency in [start_date, end_date].
    Rates are normalised to 1 unit (rate / amount).
    """
    placeholders = ",".join("?" * len(currencies))
    query = f"""
        SELECT
            currency,
            MIN(rate / amount)  AS min_rate,
            MAX(rate / amount)  AS max_rate,
            AVG(rate / amount)  AS avg_rate,
            COUNT(*)            AS data_points
        FROM exchange_rates
        WHERE date BETWEEN ? AND ?
          AND currency IN ({placeholders})
        GROUP BY currency
        ORDER BY currency
    """
    params = [start_date, end_date, *currencies]
    with db_session(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(r) for r in rows]
