"""
Форматы данных ČNB:

  daily.txt — первые две строки заголовки, далее одна валюта в строке:
    27 Jul 2019 #143
    Country|Currency|Amount|Code|Rate
    Australia|dollar|1|AUD|15.727
    Japan|yen|100|JPY|21.171

  year.txt — первая строка заголовок с кодами валют, далее строка на каждый день:
    Date|AUD|BGN|JPY (100)|USD
    02.01.2019|15.861|13.108|21.600|22.508
"""
import logging
from datetime import date, timedelta

import requests

logger = logging.getLogger(__name__)

DAILY_URL = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt"
YEAR_URL  = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/year.txt"

# Session переиспользует TCP-соединение между запросами (keep-alive)
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "CNB-Sync-App/1.0"})
TIMEOUT = 15


def _get(url: str, params: dict) -> str | None:
    """Выполняет GET-запрос и возвращает тело ответа как строку.
    При любой сетевой ошибке логирует предупреждение и возвращает None,
    чтобы вызывающий код мог пропустить этот день без падения всей синхронизации.
    """
    try:
        resp = _SESSION.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.warning("HTTP error fetching %s %s: %s", url, params, exc)
        return None


def _parse_daily(text: str, target_date: date, currencies: set[str]) -> list[dict]:
    """Парсит формат daily.txt.
    Пропускает первые две строки-заголовки, затем разбивает каждую строку по '|'.
    Фильтрует только нужные валюты из множества currencies.
    Пропускает строки с некорректными данными, не прерывая обработку.
    """
    rows: list[dict] = []
    lines = text.splitlines()
    for line in lines[2:]:  # первые две строки — заголовки
        parts = line.strip().split("|")
        if len(parts) != 5:
            continue
        _country, _name, amount_str, code, rate_str = parts
        if code not in currencies:
            continue
        try:
            rows.append({
                "date":     target_date.isoformat(),
                "currency": code,
                "amount":   int(amount_str),
                "rate":     float(rate_str.replace(",", ".")),
            })
        except ValueError as exc:
            logger.debug("Skipping malformed daily row %r: %s", line, exc)
    return rows


def _parse_year(text: str, currencies: set[str]) -> list[dict]:
    """Парсит формат year.txt.
    Из первой строки извлекает коды валют и их amount (например, 'JPY (100)' → amount=100).
    Запоминает индексы только нужных колонок, чтобы не перебирать все валюты в каждой строке.
    Пропускает ячейки со значениями 'N/A' и '-' — это выходные и праздничные дни.
    """
    rows: list[dict] = []
    lines = text.splitlines()
    if not lines:
        return rows

    # разбираем заголовок: строка вида "Date|AUD|BGN|JPY (100)|USD"
    col_meta: list[tuple[str, int]] = []  # список (код_валюты, amount)
    for h in lines[0].split("|")[1:]:
        h = h.strip()
        if "(" in h:
            # формат "JPY (100)" — курс дан за 100 единиц
            code, rest = h.split("(", 1)
            try:
                amount = int(rest.rstrip(")").strip())
            except ValueError:
                amount = 1
            code = code.strip()
        else:
            code, amount = h, 1
        col_meta.append((code, amount))

    # сохраняем только индексы нужных нам валют
    wanted_indices = [i for i, (code, _) in enumerate(col_meta) if code in currencies]

    for line in lines[1:]:
        parts = line.strip().split("|")
        if len(parts) < 2:
            continue
        try:
            d = date.fromisoformat(_cnb_date_to_iso(parts[0].strip()))
        except ValueError:
            logger.debug("Skipping bad date %r", parts[0])
            continue

        for idx in wanted_indices:
            code, amount = col_meta[idx]
            raw = parts[idx + 1].strip() if idx + 1 < len(parts) else ""
            if not raw or raw in ("N/A", "-"):
                continue  # нет данных за этот день — пропускаем
            try:
                rows.append({
                    "date":     d.isoformat(),
                    "currency": code,
                    "amount":   amount,
                    "rate":     float(raw.replace(",", ".")),
                })
            except ValueError as exc:
                logger.debug("Skipping bad value %r for %s on %s: %s", raw, code, parts[0], exc)

    return rows


def _cnb_date_to_iso(s: str) -> str:
    """Конвертирует дату из формата ČNB (DD.MM.YYYY) в ISO-формат (YYYY-MM-DD).
    ISO-формат используется для хранения в БД и корректной сортировки строк.
    """
    day, month, year = s.split(".")
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"


def fetch_daily(target_date: date, currencies: list[str], base_url: str = DAILY_URL) -> list[dict]:
    """Загружает курсы валют за один конкретный день с эндпоинта daily.txt.
    Возвращает пустой список если запрос не удался.
    """
    date_str = target_date.strftime("%d.%m.%Y")
    text = _get(base_url, {"date": date_str})
    if text is None:
        return []
    return _parse_daily(text, target_date, set(currencies))


def fetch_year(year: int, currencies: list[str], base_url: str = YEAR_URL) -> list[dict]:
    """Загружает все курсы за календарный год одним запросом к year.txt.
    Значительно эффективнее, чем 365 отдельных запросов через fetch_daily.
    Возвращает пустой список если запрос не удался.
    """
    text = _get(base_url, {"year": str(year)})
    if text is None:
        return []
    return _parse_year(text, set(currencies))


def fetch_range(start: date, end: date, currencies: list[str],
                daily_url: str = DAILY_URL, year_url: str = YEAR_URL) -> list[dict]:
    """Загружает курсы за произвольный диапазон дат с оптимизацией запросов.
    Если год полностью входит в диапазон — один запрос к year.txt.
    Если год входит частично (начало или конец диапазона) — итерация по дням через daily.txt.
    Пример: запрос с 15.03.2023 по 20.06.2024 сделает:
      - daily.txt для 15.03–31.12.2023 (частичный год)
      - year.txt для 2024 не подойдёт, daily.txt для 01.01–20.06.2024
    """
    all_rows: list[dict] = []

    if start > end:
        logger.warning("fetch_range: start %s > end %s, nothing to fetch", start, end)
        return all_rows

    for year in range(start.year, end.year + 1):
        year_start = date(year, 1, 1)
        year_end   = date(year, 12, 31)

        if start <= year_start and end >= year_end:
            # весь год покрыт диапазоном — один запрос вместо 365
            logger.info("Fetching full year %d", year)
            rows = fetch_year(year, currencies, year_url)
        else:
            # частичный год — идём по дням
            chunk_start = max(start, year_start)
            chunk_end   = min(end,   year_end)
            logger.info("Fetching daily from %s to %s", chunk_start, chunk_end)
            rows = []
            current = chunk_start
            while current <= chunk_end:
                rows.extend(fetch_daily(current, currencies, daily_url))
                current += timedelta(days=1)

        all_rows.extend(rows)

    return all_rows
