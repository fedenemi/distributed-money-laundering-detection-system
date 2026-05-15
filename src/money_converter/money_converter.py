import os

from common.middleware.worker_base import WorkerBase

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"

class MoneyConverter(WorkerBase):

    def __init__(self):
        super().__init__()

        # Get environment variables
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]

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

        # If not currency included
        currency = data["Payment Currency"]
        if currency not in currency_rates:
            currency_rates[currency] = self._consult_currency_rates_api(date, currency)


    def on_eof(self):
        pass