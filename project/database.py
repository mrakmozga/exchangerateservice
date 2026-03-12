import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # доступ к колонкам по имени
    conn.execute("PRAGMA journal_mode=WAL")  # WAL позволяет читать БД во время записи
    return conn


@contextmanager
def db_session(db_path: str):
    # контекстный менеджер: commit при успехе, rollback при ошибке
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
    # создаём директорию для файла БД, если её нет (актуально при первом запуске в Docker)
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with db_session(db_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT    NOT NULL,
                currency    TEXT    NOT NULL,
                amount      INTEGER NOT NULL,  -- за сколько единиц валюты указан курс
                rate        REAL    NOT NULL,
                created_at  TEXT    DEFAULT (datetime('now')),
                UNIQUE (date, currency)         -- один курс на валюту в день
            )
        """)
        # индекс ускоряет выборки по дате и валюте в отчётах
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_date_currency
            ON exchange_rates (date, currency)
        """)
    logger.info("Database initialised at %s", db_path)


def upsert_rates(db_path: str, rows: list[dict]) -> int:
    # INSERT с обновлением при конфликте — повторная синхронизация не создаёт дублей
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
    # делим rate на amount, чтобы получить курс за 1 единицу (JPY, HUF публикуются за 100)
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
