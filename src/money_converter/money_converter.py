import os
import multiprocessing

from common.middleware.double_io_worker_base import WorkerBaseDoubleIO
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

class MoneyConverter(WorkerBaseDoubleIO):

    def __init__(self):
        super().__init__()

        # Get environment variables
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]
        self._conversor_api_channel = os.environ[CONVERSION_API_REQUESTS]
        self._input_reqs_queue = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, os.environ[INPUT_REQ_QUEUE_TAG])
        self._conversor_id = os.environ[CONVERSOR_ID_TAG]

        # Currency rates by date
        self._currency_rates_by_date = {}
        self._current_prev_row_analyzed = 0

    def _generate_consult_currency_rate(self, datetime, origin_curr, dest_curr):
        return {"datetime" : datetime, "origin" : origin_curr, "destination" : dest_curr}

    def process_main_input(self, data: dict) -> tuple[list, list]:
        # Get data elements
        data_copy = data.copy()
        datetime = data["Timestamp"]
        origin_curr = data["Received Currency"]
        dest_curr = data["Payment Currency"]
        currency_rate_req = []

        if datetime not in self._currency_rates_by_date or \
                (origin_curr, dest_curr) not in self._currency_rates_by_date[datetime]:
            currency_rate_req.append(self._generate_consult_currency_rate(datetime, origin_curr, self._target_currency))
        else:
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * self._currency_rates_by_date[datetime][(origin_curr, dest_curr)]

        return (currency_rate_req, [data_copy])


    def process_secondary_input(self, data: dict, prev_stage_data: list) -> tuple[list, list]:
        new_data_list = []

        if "Type" not in data:
            prev_stage_data_row = prev_stage_data[self._current_prev_row_analyzed].copy()
            while self._current_prev_row_analyzed < len(prev_stage_data):
                datetime = data["timestamp"]
                origin_curr = data["origin"]
                dest_curr = data["destination"]

                if prev_stage_data_row["Timestamp"] == datetime and \
                        prev_stage_data_row["Received Currency"] == origin_curr and \
                        prev_stage_data_row["Payment Currency"] == dest_curr:
                    currency_rate = data["conversion_rate"]

                    # Change data to send
                    prev_stage_data_row["Amount Paid"] = str(currency_rate * float(prev_stage_data["Amount Paid"]))
                    prev_stage_data_row["Payment Currency"] = self._target_currency

                    # If data was not stored, store it
                    self._currency_rates_by_date.setdefault(datetime, {})
                    self._currency_rates_by_date[datetime].setdefault((origin_curr, dest_curr), {})
                    self._currency_rates_by_date[datetime][(origin_curr, dest_curr)] = currency_rate

                    new_data_list.append(prev_stage_data_row)
                    self._current_prev_row_analyzed += 1
                    break

                new_data_list.append(prev_stage_data_row)
                self._current_prev_row_analyzed += 1

        return ([], new_data_list)

    def on_eof(self):
        return []