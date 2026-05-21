from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.message_protocol import internal

import logging
import os
import requests
import signal

INPUT_QUEUE = os.environ["INPUT_QUEUE"]
CONVERTER_PREFIX = os.environ["CONVERTER_PREFIX"]
TOTAL_CONVERTERS = os.environ["TOTAL_CONVERTERS"]
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")

class MoneyConversionClient():
    def __init__(self):
        self._currency_rates = {}
        self._input_queue = MessageMiddlewareQueueRabbitMQ(host=RABBITMQ_HOST, queue_name=INPUT_QUEUE)
        self._results_queues = []

        # Add converter's results queues
        for i in range(TOTAL_CONVERTERS):
            self._results_queues.append(
                MessageMiddlewareQueueRabbitMQ(host=RABBITMQ_HOST, queue_name=f"{CONVERTER_PREFIX}_{i}")
                )
        
        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, *_):
        logger.info("SIGTERM recibido -> cerrando")
        self._running = False
        try:
            self._input_queue.close()
            # Close all output queues
            for queue in self._results_queues:
                queue.close()
        except Exception:
            pass

    def _request_api(self, day, from_currency, to_currency):
        url = f"https://api.frankfurter.app/{day}?from={from_currency}&to={to_currency}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["rates"]["USD"]


    def on_message(self, body: bytes, ack, nack):
        cmd = internal.deserialize(body)

        # Get message
        conversor_id = cmd[0]
        date = cmd[1]
        origin_currency = cmd[2]
        dest_currency = cmd[3]

        # Check if currency is already stored
        if date in self._currency_rates and (origin_currency, dest_currency) in self._currency_rates[date]:
            currency_rate = self._currency_rates[date][(origin_currency, dest_currency)]
        else:
            currency_rate = self._request_api(date, origin_currency, dest_currency)

        # Send rate to requester
        self._results_queues[conversor_id].send([
            internal.serialize([origin_currency, dest_currency, currency_rate])
        ])
        ack()


    def start(self):
        self._input_queue.start_consuming(self.on_message)


if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    conversion_client = MoneyConversionClient()
    conversion_client.start()