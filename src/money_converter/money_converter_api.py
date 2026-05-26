from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import internal
from common.middleware.worker_base import WorkerBase

import logging
import os
import requests
import signal


class MoneyConversionClient(WorkerBase):
    def __init__(self):
        super().__init__()
        self._currency_rates_by_date = {}

    def _request_api(self, day, from_currency, to_currency):
        url = f"https://api.frankfurter.dev/v2/rate/{from_currency}/{to_currency}?date={day}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["rate"]

    def process(self, data):
        if "Type" not in data:
            datetime = data["timestamp"]
            origin_curr = data["origin"]
            dest_curr = data["destination"]
            if datetime not in self._currency_rates_by_date or \
                    (origin_curr, dest_curr) not in self._currency_rates_by_date[datetime]:
                conversion_rate = self._request_api(datetime, origin_curr, dest_curr)
                self._currency_rates_by_date.setdefault(datetime, {})
                self._currency_rates_by_date[datetime][(origin_curr, dest_curr)] = conversion_rate
            else:
                conversion_rate = self._currency_rates_by_date[datetime][(origin_curr, dest_curr)]
            data_copy = data.copy()
            data_copy["conversion_rate"] = conversion_rate
            return [data_copy]

        return [{"Type" : "eob"}]

    def on_eof(self, client_id=None):
        return []


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    conversion_client = MoneyConversionClient()
    conversion_client.run()