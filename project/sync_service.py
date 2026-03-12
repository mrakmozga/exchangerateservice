import logging
from datetime import date

from cnb_client import fetch_daily, fetch_range
from database import upsert_rates

logger = logging.getLogger(__name__)


def sync_daily(db_path: str, currencies: list[str], target_date: date | None = None,
               daily_url: str | None = None) -> int:
    """Синхронизирует курсы валют за один день.
    Если target_date не передан — используется сегодняшняя дата.
    Запрашивает данные у ČNB через fetch_daily и сохраняет в БД через upsert_rates.
    Возвращает количество сохранённых записей.
    """
    target_date = target_date or date.today()
    logger.info("Daily sync for %s, currencies: %s", target_date, currencies)

    kwargs = {}
    if daily_url:
        kwargs["base_url"] = daily_url

    rows = fetch_daily(target_date, currencies, **kwargs)
    saved = upsert_rates(db_path, rows)
    logger.info("Daily sync complete: %d rows saved", saved)
    return saved


def sync_range(db_path: str, start_date: date, end_date: date, currencies: list[str],
               daily_url: str | None = None, year_url: str | None = None) -> int:
    """Синхронизирует курсы валют за диапазон дат.
    Делегирует выбор стратегии запросов в fetch_range:
    полные годы запрашиваются одним запросом, частичные — по дням.
    Все полученные данные сохраняются в БД через upsert_rates.
    Возвращает количество сохранённых записей.
    """
    logger.info("Range sync %s → %s, currencies: %s", start_date, end_date, currencies)

    kwargs = {}
    if daily_url:
        kwargs["daily_url"] = daily_url
    if year_url:
        kwargs["year_url"] = year_url

    rows = fetch_range(start_date, end_date, currencies, **kwargs)
    saved = upsert_rates(db_path, rows)
    logger.info("Range sync complete: %d rows saved", saved)
    return saved
