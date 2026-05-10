"""
WorkerBase: clase base para todos los workers.

Variables de entorno comunes:
  INPUT_QUEUE: cola de entrada (o vacío si consume shard)
  INPUT_EXCHANGE: exchange de entrada (si consume shard)
  SHARD_ID        : id del shard de este worker
  N_UPSTREAM      : cantidad de EOFs a esperar
  OUTPUT_QUEUE    : cola de salida simple
  OUTPUT_EXCHANGE : exchange de salida sharded
  OUTPUT_SHARDS   : cantidad de shards de salida (default 1)
"""
import logging
import os
import signal
import sys
sys.path.insert(0, "/app")

from middleware.middleware import Middleware

logger = logging.getLogger(__name__)


class WorkerBase:

    def __init__(self):
        self.input_queue     = os.environ.get("INPUT_QUEUE", "")
        self.input_exchange  = os.environ.get("INPUT_EXCHANGE", "")
        self.shard_id        = int(os.environ.get("SHARD_ID", "-1"))
        self.n_upstream      = int(os.environ.get("N_UPSTREAM", "1"))
        self.output_queue    = os.environ.get("OUTPUT_QUEUE", "")
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "")
        self.output_shards   = int(os.environ.get("OUTPUT_SHARDS", "1"))

        self._running = True
        signal.signal(signal.SIGTERM, self._handle_sigterm)

        self.mw = Middleware()
        self._declare()

    def _declare(self):
        if self.input_queue:
            self.mw.declare_queue(self.input_queue)
        if self.input_exchange and self.shard_id >= 0:
            self.mw.declare_sharded_queues(self.input_exchange,
                                           self.output_shards)
        if self.output_queue:
            self.mw.declare_queue(self.output_queue)
        if self.output_exchange:
            self.mw.declare_sharded_queues(self.output_exchange,
                                           self.output_shards)

    def _handle_sigterm(self, *_):
        logger.info("SIGTERM → cerrando")
        self._running = False
        self.mw.close()

    # --- Para implementar en subclases -------------------------------------------

    def process(self, data: dict) -> list:
        raise NotImplementedError

    def on_eof(self) -> list:
        return []

    def shard_key_for(self, msg: dict) -> str:
        """Clave de sharding para el mensaje de salida."""
        return ""

    # --- Emisión --------------------------------------------------------------

    def _emit(self, results: list):
        for msg in results:
            if self.output_exchange and self.output_shards > 1:
                self.mw.publish_shard(
                    self.output_exchange, self.output_shards,
                    self.shard_key_for(msg), msg
                )
            elif self.output_queue:
                self.mw.publish(self.output_queue, msg)

    def _emit_eof(self):
        if self.output_exchange and self.output_shards > 1:
            self.mw.publish_eof_sharded(self.output_exchange, self.output_shards)
        elif self.output_queue:
            self.mw.publish_eof(self.output_queue)

    # --- Loop principal ---------------------------------------------------------

    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")

        def on_data(data):
            self._emit(self.process(data))

        def on_eof():
            self._emit(self.on_eof())
            self._emit_eof()
            logger.info(f"{self.__class__.__name__} terminado")

        if self.input_exchange and self.shard_id >= 0:
            self.mw.consume_shard(
                self.input_exchange, self.shard_id,
                on_data, on_eof, self.n_upstream
            )
        else:
            self.mw.consume(
                self.input_queue, on_data, on_eof, self.n_upstream
            )
