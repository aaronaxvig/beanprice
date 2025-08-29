import datetime
import unittest
from os import environ
from decimal import Decimal

from unittest import mock
from dateutil import tz

import requests

from beanprice import source
from beanprice.sources import alphavantage

timezone = tz.gettz("America/NewYork")

# From
# https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=demo
# Truncated from 100 entries in typical response.
response_tsda = {
    "Meta Data": {
        "1. Information": "Daily Time Series with Splits and Dividend Events",
        "2. Symbol": "IBM",
        "3. Last Refreshed": "2025-04-08",
        "4. Output Size": "Compact",
        "5. Time Zone": "US/Eastern"
    },
    "Time Series (Daily)": {
        "2025-04-08": {
            "1. open": "232.56",
            "2. high": "233.05",
            "3. low": "217.28",
            "4. close": "221.03",
            "5. adjusted close": "221.03",
            "6. volume": "6374209",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0"
        },
        "2025-04-07": {
            "1. open": "219.24",
            "2. high": "232.29",
            "3. low": "214.5",
            "4. close": "225.78",
            "5. adjusted close": "225.78",
            "6. volume": "7797889",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0"
        },
        "2025-04-04": {
            "1. open": "238.0",
            "2. high": "240.16",
            "3. low": "226.88",
            "4. close": "227.48",
            "5. adjusted close": "227.48",
            "6. volume": "7407096",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0"
        },
        "2025-04-03": {
            "1. open": "242.71",
            "2. high": "250.61",
            "3. low": "242.53",
            "4. close": "243.49",
            "5. adjusted close": "243.49",
            "6. volume": "5309626",
            "7. dividend amount": "0.0000",
            "8. split coefficient": "1.0"
        }
    }
}

def response(contents, status_code=requests.codes.ok):
    """Return a context manager to patch a JSON response."""
    response = mock.Mock()
    response.status_code = status_code
    response.text = ""
    response.json.return_value = contents
    return mock.patch("requests.get", return_value=response)


class AlphavantagePriceFetcher(unittest.TestCase):
    def setUp(self):
        environ["ALPHAVANTAGE_API_KEY"] = "foo"

    def tearDown(self):
        del environ["ALPHAVANTAGE_API_KEY"]

    def test_get_historical_price(self):
        with response(contents=response_tsda):
            srcprice = alphavantage.Source().get_historical_price(
                "price:IBM:USD", datetime.datetime(2025, 4, 4).replace(tzinfo=timezone)
            )
            self.assertEqual(Decimal("227.48"), srcprice.price)
            self.assertEqual(
                datetime.datetime(2025, 4, 4)
                .replace(tzinfo=datetime.timezone.utc)
                .date(),
                srcprice.time.date(),
            )
            self.assertEqual("USD", srcprice.quote_currency)


    def test_error_invalid_ticker(self):
        with self.assertRaises(ValueError):
            alphavantage.Source().get_latest_price("INVALID")

    def test_error_network(self):
        with response("Foobar", 404):
            with self.assertRaises(alphavantage.AlphavantageApiError):
                alphavantage.Source().get_latest_price("price:IBM:USD")

    def test_error_response(self):
        contents = {"Error Message": "Something wrong"}
        with response(contents):
            with self.assertRaises(alphavantage.AlphavantageApiError):
                alphavantage.Source().get_latest_price("price:IBM:USD")

    def test_valid_response_price(self):
        contents = {
            "Global Quote": {
                "05. price": "144.7400",
                "07. latest trading day": "2021-01-21",
            }
        }
        with response(contents):
            srcprice = alphavantage.Source().get_latest_price("price:FOO:USD")
            self.assertIsInstance(srcprice, source.SourcePrice)
            self.assertEqual(Decimal("144.7400"), srcprice.price)
            self.assertEqual("USD", srcprice.quote_currency)
            self.assertEqual(
                datetime.datetime(2021, 1, 21, 0, 0, 0, tzinfo=tz.tzutc()), srcprice.time
            )

    def test_valid_response_fx(self):
        contents = {
            "Realtime Currency Exchange Rate": {
                "5. Exchange Rate": "108.94000000",
                "6. Last Refreshed": "2021-02-21 20:32:25",
                "7. Time Zone": "UTC",
            }
        }
        with response(contents):
            srcprice = alphavantage.Source().get_latest_price("fx:USD:CHF")
            self.assertIsInstance(srcprice, source.SourcePrice)
            self.assertEqual(Decimal("108.94000000"), srcprice.price)
            self.assertEqual("CHF", srcprice.quote_currency)
            self.assertEqual(
                datetime.datetime(2021, 2, 21, 20, 32, 25, tzinfo=tz.tzutc()), srcprice.time
            )


if __name__ == "__main__":
    unittest.main()
