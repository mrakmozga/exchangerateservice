"""
tests.py — offline unit tests for parser and database layer.
Run with: python tests.py
"""
import sys
import os
import tempfile
import unittest
from datetime import date

# Make sure we can import our modules
sys.path.insert(0, os.path.dirname(__file__))

from cnb_client import _parse_daily, _parse_year, _cnb_date_to_iso
from database import init_db, upsert_rates, get_report


DAILY_SAMPLE = """\
27 Jul 2019 #143
Country|Currency|Amount|Code|Rate
Australia|dollar|1|AUD|15.727
Brazil|real|1|BRL|5.557
Bulgaria|lev|1|BGN|13.089
Canada|dollar|1|CAD|17.092
China|renminbi|1|CNY|3.268
Denmark|krone|1|DKK|3.421
EMU|euro|1|EUR|25.525
Hongkong|dollar|1|HKD|2.881
Hungary|forint|100|HUF|7.757
Iceland|krona|100|ISK|18.375
IMF|SDR|1|XDR|31.367
India|rupee|100|INR|32.718
Indonesia|rupiah|1000|IDR|1.618
Japan|yen|100|JPY|21.171
Malaysia|ringgit|1|MYR|5.471
Mexico|peso|1|MXN|1.178
New Zealand|dollar|1|NZD|14.531
Norway|krone|1|NOK|2.528
Philippines|peso|100|PHP|44.047
Poland|zloty|1|PLN|5.929
Romania|leu|1|RON|5.255
Russia|rouble|100|RUB|35.661
Singapore|dollar|1|SGD|16.631
South Africa|rand|1|ZAR|1.578
South Korea|won|100|KRW|1.934
Sweden|krona|1|SEK|2.389
Switzerland|franc|1|CHF|23.097
Thailand|baht|100|THB|73.513
Turkey|lira|1|TRY|4.156
United Kingdom|pound|1|GBP|28.279
USA|dollar|1|USD|22.744
"""

YEAR_SAMPLE = """\
Date|AUD|BRL|EUR|JPY (100)|USD
02.01.2019|15.861|5.557|25.720|21.600|22.508
03.01.2019|15.624|5.481|25.540|21.383|22.333
07.01.2019|15.803|5.568|25.590|21.573|22.351
08.01.2019|15.922|5.593|25.615|21.701|22.385
09.01.2019|15.926|5.557|25.630|21.784|22.334
10.01.2019|15.932|5.503|25.600|21.872|22.284
"""


class TestCnbDateConversion(unittest.TestCase):
    def test_normal(self):
        self.assertEqual(_cnb_date_to_iso("27.07.2019"), "2019-07-27")

    def test_zero_padding(self):
        self.assertEqual(_cnb_date_to_iso("02.01.2019"), "2019-01-02")


class TestParseDaily(unittest.TestCase):
    def test_parses_known_currencies(self):
        rows = _parse_daily(DAILY_SAMPLE, date(2019, 7, 27), {"USD", "EUR", "GBP"})
        self.assertEqual(len(rows), 3)
        codes = {r["currency"] for r in rows}
        self.assertEqual(codes, {"USD", "EUR", "GBP"})

    def test_correct_values(self):
        rows = _parse_daily(DAILY_SAMPLE, date(2019, 7, 27), {"USD"})
        self.assertEqual(rows[0]["rate"], 22.744)
        self.assertEqual(rows[0]["amount"], 1)
        self.assertEqual(rows[0]["date"], "2019-07-27")

    def test_amount_100(self):
        rows = _parse_daily(DAILY_SAMPLE, date(2019, 7, 27), {"RUB"})
        self.assertEqual(rows[0]["amount"], 100)
        self.assertAlmostEqual(rows[0]["rate"], 35.661)

    def test_ignores_unknown_currencies(self):
        rows = _parse_daily(DAILY_SAMPLE, date(2019, 7, 27), {"XYZ"})
        self.assertEqual(rows, [])

    def test_empty_text(self):
        rows = _parse_daily("", date(2019, 7, 27), {"USD"})
        self.assertEqual(rows, [])


class TestParseYear(unittest.TestCase):
    def test_parses_multiple_currencies(self):
        rows = _parse_year(YEAR_SAMPLE, {"USD", "EUR"})
        currencies = {r["currency"] for r in rows}
        self.assertIn("USD", currencies)
        self.assertIn("EUR", currencies)

    def test_row_count(self):
        rows = _parse_year(YEAR_SAMPLE, {"USD"})
        self.assertEqual(len(rows), 6)

    def test_jpy_amount(self):
        rows = _parse_year(YEAR_SAMPLE, {"JPY"})
        self.assertTrue(all(r["amount"] == 100 for r in rows))

    def test_correct_date(self):
        rows = _parse_year(YEAR_SAMPLE, {"USD"})
        dates = sorted(r["date"] for r in rows)
        self.assertEqual(dates[0], "2019-01-02")

    def test_correct_value(self):
        rows = _parse_year(YEAR_SAMPLE, {"USD"})
        first = next(r for r in rows if r["date"] == "2019-01-02")
        self.assertAlmostEqual(first["rate"], 22.508)

    def test_ignores_missing_currency(self):
        rows = _parse_year(YEAR_SAMPLE, {"CHF"})
        self.assertEqual(rows, [])


class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.db  = self.tmp.name
        init_db(self.db)

    def tearDown(self):
        os.unlink(self.db)

    def _seed(self):
        rows = [
            {"date": "2024-01-02", "currency": "USD", "amount": 1, "rate": 22.5},
            {"date": "2024-01-03", "currency": "USD", "amount": 1, "rate": 23.0},
            {"date": "2024-01-04", "currency": "USD", "amount": 1, "rate": 22.0},
            {"date": "2024-01-02", "currency": "EUR", "amount": 1, "rate": 25.0},
            {"date": "2024-01-03", "currency": "EUR", "amount": 1, "rate": 25.5},
        ]
        upsert_rates(self.db, rows)

    def test_upsert_insert(self):
        rows = [{"date": "2024-01-01", "currency": "USD", "amount": 1, "rate": 22.0}]
        n = upsert_rates(self.db, rows)
        self.assertEqual(n, 1)

    def test_upsert_update(self):
        upsert_rates(self.db, [{"date": "2024-01-01", "currency": "USD", "amount": 1, "rate": 22.0}])
        upsert_rates(self.db, [{"date": "2024-01-01", "currency": "USD", "amount": 1, "rate": 23.5}])
        report = get_report(self.db, "2024-01-01", "2024-01-01", ["USD"])
        self.assertAlmostEqual(report[0]["avg_rate"], 23.5)

    def test_report_min_max_avg(self):
        self._seed()
        report = get_report(self.db, "2024-01-02", "2024-01-04", ["USD"])
        self.assertEqual(len(report), 1)
        r = report[0]
        self.assertAlmostEqual(r["min_rate"], 22.0)
        self.assertAlmostEqual(r["max_rate"], 23.0)
        self.assertAlmostEqual(r["avg_rate"], (22.5 + 23.0 + 22.0) / 3)
        self.assertEqual(r["data_points"], 3)

    def test_report_multiple_currencies(self):
        self._seed()
        report = get_report(self.db, "2024-01-02", "2024-01-04", ["USD", "EUR"])
        codes = {r["currency"] for r in report}
        self.assertIn("USD", codes)
        self.assertIn("EUR", codes)

    def test_report_missing_currency(self):
        self._seed()
        report = get_report(self.db, "2024-01-02", "2024-01-04", ["CHF"])
        self.assertEqual(report, [])

    def test_report_normalises_amount(self):
        upsert_rates(self.db, [{"date": "2024-01-01", "currency": "JPY", "amount": 100, "rate": 15.0}])
        report = get_report(self.db, "2024-01-01", "2024-01-01", ["JPY"])
        self.assertAlmostEqual(report[0]["avg_rate"], 0.15)  # 15.0 / 100

    def test_upsert_empty(self):
        self.assertEqual(upsert_rates(self.db, []), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
