import os

from common.middleware.worker_base import WorkerBase

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"
CONVERSION_API_REQUESTS = "CONVERSION_API_REQUESTS"

CURRENCY_CODES = {
    "US Dollar": "USD", "Euro": "EUR", "Yuan": "CNY",
    "Ruble": "RUB", "Yen": "JPY", "UK Pound": "GBP",
    "Swiss Franc": "CHF", "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD", "Mexican Peso": "MXN",
    "Brazil Real": "BRL", "Rupee": "INR", "Saudi Riyal": "SAR",
}

class MoneyConverter(WorkerBase):

    def __init__(self):
        super().__init__()

        # Get environment variables
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]
        self._conversor_api_channel = os.environ[CONVERSION_API_REQUESTS]

        # Currency rates
        self._currency_rates_by_date = {}

    def _consult_currency_rates_api(self, date, currency):
        pass

    def _get_date_from_datetime(self, datetime_str):
        pass

    def process(self, data):
        date = self._get_date_from_datetime(data["Timestamp"])

        # Get currency rates of dates
        currency_rates = self._currency_rates_by_date.get(date, {})

        # If currency not included
        currency = data["Payment Currency"]
        if currency not in currency_rates:
            currency_rates[currency] = self._consult_currency_rates_api(date, currency)


    def on_eof(self):
        pass