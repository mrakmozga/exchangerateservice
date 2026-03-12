"""
cnb_client.py — fetch and parse exchange-rate data from the Czech National Bank.

CNB daily format (first two lines are headers):
  27 Jul 2019 #143
  Country|Currency|Amount|Code|Rate
  Australia|dollar|1|AUD|15.727
  ...

CNB year format:
  Date|AUD|BGN|BRL|...
  02.01.2019|15.861|13.108|...
"""
import logging
from datetime import date, timedelta
from io import StringIO

import requests

logger = logging.getLogger(__name__)

DAILY_URL = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/daily.txt"
YEAR_URL  = "https://www.cnb.cz/en/financial_markets/foreign_exchange_market/exchange_rate_fixing/year.txt"

_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "CNB-Sync-App/1.0"})
TIMEOUT = 15


# ---------------------------------------------------------------------------
# Low-level HTTP helpers
# ---------------------------------------------------------------------------

def _get(url: str, params: dict) -> str | None:
    try:
        resp = _SESSION.get(url, params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as exc:
        logger.warning("HTTP error fetching %s %s: %s", url, params, exc)
        return None


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_daily(text: str, target_date: date, currencies: set[str]) -> list[dict]:
    """Parse the daily.txt format."""
    rows: list[dict] = []
    lines = text.splitlines()
    # Skip first two header lines
    for line in lines[2:]:
        parts = line.strip().split("|")
        if len(parts) != 5:
            continue
        _country, _currency_name, amount_str, code, rate_str = parts
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
    """Parse the year.txt format."""
    rows: list[dict] = []
    lines = text.splitlines()
    if not lines:
        return rows

    # Header line: Date|AUD|BGN|...  (amounts encoded as "AUD (100)|")
    header_raw = lines[0].split("|")
    # header_raw[0] == "Date"
    # rest may look like "AUD" or "JPY (100)" etc.
    col_meta: list[tuple[str, int]] = []  # (code, amount)
    for h in header_raw[1:]:
        h = h.strip()
        if "(" in h:
            code, rest = h.split("(", 1)
            try:
                amount = int(rest.rstrip(")").strip())
            except ValueError:
                amount = 1
            code = code.strip()
        else:
            code   = h
            amount = 1
        col_meta.append((code, amount))

    wanted_indices = [i for i, (code, _) in enumerate(col_meta) if code in currencies]

    for line in lines[1:]:
        parts = line.strip().split("|")
        if len(parts) < 2:
            continue
        date_str = parts[0].strip()  # DD.MM.YYYY
        try:
            d = date.fromisoformat(_cnb_date_to_iso(date_str))
        except ValueError:
            logger.debug("Skipping bad date %r", date_str)
            continue

        for idx in wanted_indices:
            code, amount = col_meta[idx]
            raw = parts[idx + 1].strip() if idx + 1 < len(parts) else ""
            if not raw or raw == "N/A" or raw == "-":
                continue
            try:
                rows.append({
                    "date":     d.isoformat(),
                    "currency": code,
                    "amount":   amount,
                    "rate":     float(raw.replace(",", ".")),
                })
            except ValueError as exc:
                logger.debug("Skipping bad value %r for %s on %s: %s", raw, code, date_str, exc)

    return rows


def _cnb_date_to_iso(s: str) -> str:
    """Convert DD.MM.YYYY → YYYY-MM-DD."""
    day, month, year = s.split(".")
    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_daily(target_date: date, currencies: list[str], base_url: str = DAILY_URL) -> list[dict]:
    """Fetch exchange rates for a single day."""
    date_str = target_date.strftime("%d.%m.%Y")
    text = _get(base_url, {"date": date_str})
    if text is None:
        return []
    return _parse_daily(text, target_date, set(currencies))


def fetch_year(year: int, currencies: list[str], base_url: str = YEAR_URL) -> list[dict]:
    """Fetch all exchange rates for a given year."""
    text = _get(base_url, {"year": str(year)})
    if text is None:
        return []
    return _parse_year(text, set(currencies))


def fetch_range(start: date, end: date, currencies: list[str],
                daily_url: str = DAILY_URL, year_url: str = YEAR_URL) -> list[dict]:
    """
    Fetch exchange rates for [start, end].
    Uses year endpoint for full years, daily endpoint for partial years.
    """
    all_rows: list[dict] = []

    if start > end:
        logger.warning("fetch_range: start %s > end %s, nothing to fetch", start, end)
        return all_rows

    # Collect which years are spanned
    years = list(range(start.year, end.year + 1))

    for year in years:
        year_start = date(year, 1, 1)
        year_end   = date(year, 12, 31)

        # Full year covered → use efficient year endpoint
        if start <= year_start and end >= year_end:
            logger.info("Fetching full year %d", year)
            rows = fetch_year(year, currencies, year_url)
        else:
            # Partial year → day by day (CNB has no partial-year endpoint)
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
