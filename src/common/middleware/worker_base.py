"""
WorkerBase: clase base para todos los workers.


MessageMiddlewareQueueRabbitMQ: para colas simples
ShardedExchangeConsumer: para consumir un shard de exchange
ShardedExchangeProducer: para publicar con sharding

Variables de entorno:
  RABBITMQ_HOST: host de RabbitMQ (default: rabbitmq)
  INPUT_QUEUE: cola de entrada (si consume de cola simple)
  INPUT_EXCHANGE: exchange de entrada (si consume de shard)
  SHARD_ID        : id del shard de este worker
  N_UPSTREAM      : cantidad de EOFs a esperar
  OUTPUT_QUEUE    : cola de salida simple
  OUTPUT_EXCHANGE : exchange de salida con sharding
  OUTPUT_SHARDS   : cantidad de shards de salida (default 1)
  BATCH_SIZE      : filas por batch de salida (default 500)
"""
import json
import logging
import os
import signal
import time
import sys

from middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from middleware.middleware_sharded import ShardedExchangeConsumer, ShardedExchangeProducer
from middleware.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RECONNECT_DELAY = 2


def _wait_for_rabbitmq():
    while True:
        try:
            import pika
            conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            conn.close()
            return
        except Exception:
            logger.warning(f"RabbitMQ no disponible, reintentando en {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)


class WorkerBase:

    def __init__(self):
        self.input_queue     = os.environ.get("INPUT_QUEUE", "")
        self.input_exchange  = os.environ.get("INPUT_EXCHANGE", "")
        self.shard_id        = int(os.environ.get("SHARD_ID", "-1"))
        self.n_upstream      = int(os.environ.get("N_UPSTREAM", "1"))
        self.output_queue    = os.environ.get("OUTPUT_QUEUE", "")
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "")
        self.output_shards   = int(os.environ.get("OUTPUT_SHARDS", "1"))
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))

        self._buffer: dict = {}
        self._running = True

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        _wait_for_rabbitmq()
        self._setup_connections()

    def _setup_connections(self):
        # Input
        if self.input_exchange and self.shard_id >= 0:
            self._consumer = ShardedExchangeConsumer(RABBITMQ_HOST, self.input_exchange, self.shard_id)
        elif self.input_queue:
            self._consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.input_queue)
        else:
            raise ValueError("Se requiere INPUT_QUEUE o INPUT_EXCHANGE + SHARD_ID")

        # Output
        if self.output_exchange and self.output_shards > 1:
            self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
        elif self.output_queue:
            self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
        else:
            self._producer = None

    def _handle_sigterm(self, *_):
        logger.info("SIGTERM recibido -> cerrando")
        self._running = False
        try:
            self._consumer.stop_consuming()
        except Exception:
            pass

    # --- Para implementar en subclases -------------------------------------------

    def process(self, data: dict) -> list:
        raise NotImplementedError

    def on_eof(self) -> list:
        return []

    def shard_key_for(self, msg: dict) -> str:
        """Clave de sharding para el mensaje de salida."""
        return ""

    # --- Emisión con Buffer y flush --------------------------------------------------------

    def _emit(self, results: list):
        if not results or self._producer is None:
            return
        for msg in results:
            if self.output_exchange and self.output_shards > 1:
                shard = hash(self.shard_key_for(msg)) % self.output_shards
                buf_key = str(shard)
            else:
                buf_key = "__queue__"
            self._buffer.setdefault(buf_key, []).append(msg)
            if len(self._buffer[buf_key]) >= self.batch_size:
                self._flush_key(buf_key)

    def _flush_key(self, buf_key: str):
        rows = self._buffer.pop(buf_key, [])
        if not rows:
            return
        body = json.dumps({"rows": rows}).encode()
        if self.output_exchange and self.output_shards > 1:
            self._producer.send_to_shard(body, int(buf_key))
        else:
            self._producer.send(body)

    def _flush_all(self):
        for key in list(self._buffer.keys()):
            self._flush_key(key)

    def _send_eof(self):
        if self._producer is None:
            return
        eof_body = json.dumps({"type": "eof"}).encode()
        if self.output_exchange and self.output_shards > 1:
            self._producer.send_eof_to_all(eof_body)
        else:
            self._producer.send(eof_body)

    # --- Loop principal ---------------------------------------------------------

    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")
        eof_count = [0]

        def on_message(body: bytes, ack, nack):
            try:
                msg = json.loads(body)
                if msg.get("type") == "eof":
                    eof_count[0] += 1
                    ack()
                    if eof_count[0] >= self.n_upstream:
                        self._emit(self.on_eof())
                        self._flush_all()
                        self._send_eof()
                        self._consumer.stop_consuming()
                        logger.info(f"{self.__class__.__name__} terminado")
                else:
                    for row in msg.get("rows", []):
                        self._emit(self.process(row))
                    ack()
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        try:
            self._consumer.start_consuming(on_message)
        except MessageMiddlewareDisconnectedError:
            if self._running:
                logger.error("Conexion perdida con RabbitMQ")
