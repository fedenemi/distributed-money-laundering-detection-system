import os
import queue
import threading

from common.middleware.worker_base import WorkerBase
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import internal

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"
CONVERSION_API_REQUESTS = "CONVERSION_API_REQUESTS"
INPUT_REQ_QUEUE_TAG = "INPUT_REQ_QUEUE"
CONVERSOR_ID_TAG = "CONVERSOR_ID"
CONVERTER_PREFIX = os.environ["CONVERTER_PREFIX"]
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")

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
        self._input_reqs_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, os.environ[INPUT_REQ_QUEUE_TAG])
        self._conversor_id = os.environ[CONVERSOR_ID_TAG]

        # Currency rates by date
        self._currency_rates_by_date = {}

        # Thread for requests results
        self._reqs_results_channel = queue.Queue()
        self._reqs_responses = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, f"{CONVERTER_PREFIX}_{self._conversor_id}")
        self._results_thread = threading.Thread(target=self._handle_request_response, args=())
        self._results_thread.start()

    def _process_req_response(self, data: bytes, ack, nack):
        response = internal.deserialize(data)
        self._reqs_results_channel.put(response[2])
        ack()

    def _handle_request_response(self):
        self._reqs_responses.start_consuming(self._process_req_response)

    def _consult_currency_rates_api(self, date, origin_currency, dest_currency):
        # Send data
        self._input_reqs_queue.send(
            internal.serialize(
                    [
                        self._conversor_id,
                        date,
                        origin_currency,
                        dest_currency
                    ]
                )
            )

        # Wait for result
        req_response = self._reqs_results_channel.get()
        return req_response[2]

    def process(self, data):
        date = data["Timestamp"]

        # Get currency rates of dates
        currency_rates = self._currency_rates_by_date.get(date, {})

        # If currency not included
        origin_currency = data["Payment Currency"]
        if origin_currency not in currency_rates:
            currency_rates[origin_currency] = self._consult_currency_rates_api(date, origin_currency, self._target_currency)

        amount = float(data["Amount Paid"])
        data["Amount Paid"] = str(amount * currency_rates[origin_currency])

        del data["Payment Currency"]

        return [data]


    def on_eof(self):
        return []