"""
WorkerBase: clase base para todos los workers.


MessageMiddlewareQueueRabbitMQ: para colas simples
ShardedExchangeConsumer: para consumir un shard de exchange
ShardedExchangeProducer: para publicar con sharding

Variables de entorno:
  RABBITMQ_HOST: host de RabbitMQ (default: rabbitmq)
  MAIN_INPUT_QUEUE: cola de entrada principal (si consume de cola simple)
  MAIN_INPUT_EXCHANGE: exchange de entrada principal (si consume de shard)
  SECONDARU_INPUT_QUEUE: cola de entrada secundaria (si consume de cola simple)
  SECONDARU_INPUT_EXCHANGE: exchange de entrada secundaria (si consume de shard)
  SHARD_ID        : id del shard de este worker
  N_UPSTREAM_MAIN : cantidad de EOFs a esperar de la entrada principal
  N_UPSTREAM_SECONDARY : cantidad de EOFs a esperar de la entrada secundaria
  OUTPUT_QUEUE    : cola de salida simple principal
  OUTPUT_QUEUE    : cola de salida simple secundaria
  MAIN_OUTPUT_EXCHANGE : exchange de salida con sharding principal
  SECONDARY_OUTPUT_EXCHANGE : exchange de salida con sharding secundaria
  OUTPUT_SHARDS   : cantidad de shards de salida (default 1)
  BATCH_SIZE      : filas por batch de salida (default 500)
"""
import json
import logging
import os
import signal
import time
import sys
import multiprocessing
import queue

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
        # Input
        self.main_input_queue     = os.environ.get("MAIN_INPUT_QUEUE", "")
        self.main_input_exchange  = os.environ.get("MAIN_INPUT_EXCHANGE", "")
        self.sec_input_queue     = os.environ.get("SECONDARY_INPUT_QUEUE", "")
        self.sec_input_exchange  = os.environ.get("SECONDARY_INPUT_EXCHANGE", "")
        self.shard_id        = int(os.environ.get("SHARD_ID", "-1"))
        self.main_n_upstream      = int(os.environ.get("MAIN_N_UPSTREAM", "1"))
        self.sec_n_upstream      = int(os.environ.get("SECONDARY_N_UPSTREAM", "1"))
        
        # Output
        self.main_output_queue    = os.environ.get("MAIN_OUTPUT_QUEUE", "")
        self.main_output_exchange = os.environ.get("MAIN_OUTPUT_EXCHANGE", "")
        self.main_output_shards   = int(os.environ.get("MAIN_OUTPUT_SHARDS", "1"))
        self.sec_output_queue    = os.environ.get("SECONDARY_OUTPUT_QUEUE", "")
        self.sec_output_exchange = os.environ.get("SECONDARY_OUTPUT_EXCHANGE", "")
        self.sec_output_shards   = int(os.environ.get("SECONDARY_OUTPUT_SHARDS", "1"))
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))
        self.total_clients   = int(os.environ.get("TOTAL_CLIENTS", "0"))
        self._main_out_buffer: dict = {}
        self._sec_out_buffer: dict = {}

        # Configuration
        self._operation_mode = os.environ["OP_MODE"]
        self._channel_stages = queue.Queue()
        self._results_buffer_next_stage = []
        self._clients_eof_main_input = {}
        self._clients_eof_sec_input = {}
        self._running = True

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        _wait_for_rabbitmq()
        self._setup_connections()
        self._define_type()

    def _setup_connections(self):
        # Main input
        if self.main_input_exchange and self.shard_id >= 0:
            self._main_consumer = ShardedExchangeConsumer(RABBITMQ_HOST, self.main_input_exchange, self.shard_id)
        elif self.main_input_queue:
            self._main_consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_input_queue)
        else:
            raise ValueError("Se requiere MAIN_INPUT_QUEUE o MAIN_INPUT_EXCHANGE + SHARD_ID")

        # Secondary input
        if self.sec_input_exchange and self.shard_id >= 0:
            self._sec_consumer = ShardedExchangeConsumer(RABBITMQ_HOST, self.sec_input_exchange, self.shard_id)
        elif self.sec_input_queue:
            self._sec_consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.sec_input_queue)
        else:
            raise ValueError("Se requiere SECONDARY_INPUT_QUEUE o SECONDARY_INPUT_EXCHANGE + SHARD_ID")

        # Main output
        if self.main_output_exchange and self.main_output_shards > 1:
            self._main_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.main_output_exchange, self.main_output_shards)
        elif self.main_output_queue:
            self._main_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_output_queue)
        else:
            self._main_producer = None
        
        # Secondary output
        if self.sec_output_exchange and self.sec_output_shards > 1:
            self._sec_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.sec_output_exchange, self.sec_output_shards)
        elif self.sec_output_queue:
            self._sec_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.sec_output_queue)
        else:
            self._sec_producer = None

    def _define_operation_mode(self):
        if self._operation_mode != "PIPELINE" and self._operation_mode != "JOINER":
            raise ValueError("Modo de operación incorrecto. Debe ser PIPELINE o JOINER")

    def _handle_sigterm(self, *_):
        logger.info("SIGTERM recibido -> cerrando")
        self._running = False
        try:
            self._main_consumer.stop_consuming()
            self._sec_consumer.stop_consuming()
        except Exception:
            pass

    # --- Para implementar en subclases -------------------------------------------

    def process_main_input(self, data: dict) -> tuple[list, list]:
        raise NotImplementedError

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        raise NotImplementedError

    def on_main_input_eof(self, client_id=None) -> list:
        return []
    
    def on_secondary_input_eof(self, client_id=None) -> list:
        return []
    
    def on_both_eof_received(self, client_id=None) -> list:
        return []

    def _routing_key(self, msg: dict) -> str:
        """Clave de particion del mensaje. Override en Splitter."""
        return "__queue__"

    def _buffer_key(self, msg: dict) -> str:
        if self.output_exchange and self.output_shards > 1:
            return self._routing_key(msg)
        if isinstance(msg, dict):
            client_id = msg.get("client_id")
            if client_id is not None:
                return f"client:{client_id}"
        return "__queue__"

    # --- Emisión con Buffer y flush --------------------------------------------------------

    def _send_data_batch_to_next_stage(self):
        new_data_batch = self._results_buffer_next_stage
        self._channel_stages.put(new_data_batch)

    def _send_data_to_next_stage(self, results: list):
        for res in results:
            self._results_buffer_next_stage.append(res)
            if len(self._results_buffer_next_stage) >= self.batch_size:
                self._send_data_batch_to_next_stage()

    def _emit_results_main_stage(self, results: tuple[list, list]):
        self._emit_main_output(results[0])
        self._send_data_to_next_stage(results[1])

    def _emit_main_output(self, results: list):
        if not results or self._main_producer is None:
            return
        for msg in results:
            buf_key = self._buffer_key(msg)
            self._main_out_buffer.setdefault(buf_key, []).append(msg)
            if len(self._main_out_buffer[buf_key]) >= self.batch_size:
                self._flush_main_buffer_key(buf_key)

    def _emit_sec_output(self, results: list):
        if not results or self._sec_producer is None:
            return
        for msg in results:
            buf_key = self._buffer_key(msg)
            self._sec_out_buffer.setdefault(buf_key, []).append(msg)
            if len(self._sec_out_buffer[buf_key]) >= self.batch_size:
                self._flush_sec_buffer_key(buf_key)

    def _flush_main_buffer_key(self, buf_key: str):
        rows = self._main_out_buffer.pop(buf_key, [])
        if not rows:
            return
        body = json.dumps({"rows": rows}).encode()
        if self.main_output_exchange and self.main_output_shards > 1:
            self._main_producer.send_to_shard(body, int(buf_key))
        else:
            self._main_producer.send(body)

    def _flush_sec_buffer_key(self, buf_key: str):
        rows = self._sec_out_buffer.pop(buf_key, [])
        if not rows:
            return
        body = json.dumps({"rows": rows}).encode()
        if self.sec_output_exchange and self.sec_output_shards > 1:
            self._sec_producer.send_to_shard(body, int(buf_key))
        else:
            self._sec_producer.send(body)

    def _flush_all_main_buffer(self):
        for key in list(self._main_out_buffer.keys()):
            self._flush_main_buffer_key(key)
    
    def _flush_all_sec_buffer(self):
        for key in list(self._sec_out_buffer.keys()):
            self._flush_sec_buffer_key(key)

    def _flush_all_next_stage(self):
        self._flush_all_main_buffer()
        self._send_data_batch_to_next_stage()

    def _send_main_output_eof(self, client_id=None):
        if self._producer is None:
            return
        eof_msg = {"type": "eof"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = json.dumps(eof_msg).encode()
        if self.main_output_exchange and self.main_output_shards > 1:
            self._main_producer.send_eof_to_all(eof_body)
        else:
            self._main_producer.send(eof_body)

    # --- Loop principal ---------------------------------------------------------

    def handle_message_main_input(self):
        eof_count = [0]

        def on_message(body: bytes, ack, nack):
            try:
                msg = json.loads(body)
                if msg.get("type") == "eof":
                    client_id = msg.get("client_id")
                    if client_id is None:
                        eof_count[0] += 1
                        ack()
                        if eof_count[0] >= self.main_n_upstream:
                            for result in self.on_eof(None):
                                self._emit_results_main_stage([result])
                            self._flush_all()
                            self._send_eof()
                            self._main_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    self._clients_eof_main_input[client_id] = self._clients_eof_main_input.get(client_id, 0) + 1
                    ack()
                    # For pipeline send data to the next stage
                    if self._operation_mode == "PIPELINE":
                        if self._clients_eof_main_input[client_id] >= self.main_n_upstream:
                            for result in self.on_main_input_eof(client_id):
                                self._emit_results_main_stage([result])
                            self._flush_all_next_stage()
                        return
                    else: #For joiner, check if joiner action is necessary
                        if self._clients_eof_main_input[client_id] >= self.main_n_upstream and \
                                self._clients_eof_sec_input[client_id] >= self.sec_n_upstream:
                            for result in self.on_both_eof_received(client_id):
                                self._emit_main_output([result])
                            self._flush_all_main_buffer()
                            self._send_main_output_eof(client_id)
                        return

                else:
                    for row in msg.get("rows", []):
                        self._emit(self.process_main_input(row))
                    ack()
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        try:
            self._main_consumer.start_consuming(on_message)
        except MessageMiddlewareDisconnectedError:
            if self._running:
                logger.error("Conexion perdida con RabbitMQ")

    def handle_message_sec_input(self):
        eof_count = [0]
        eof_per_client = {}
        done_clients = set()

        def on_message(body: bytes, ack, nack):
            try:
                msg = json.loads(body)
                if msg.get("type") == "eof":
                    client_id = msg.get("client_id")
                    if client_id is None:
                        eof_count[0] += 1
                        ack()
                        if eof_count[0] >= self.n_upstream:
                            for result in self.on_eof(None):
                                self._emit([result])
                            self._flush_all()
                            self._send_eof()
                            self._sec_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    eof_per_client[client_id] = eof_per_client.get(client_id, 0) + 1
                    ack()
                    if eof_per_client[client_id] >= self.n_upstream and client_id not in done_clients:
                        for result in self.on_eof(client_id):
                            self._emit([result])
                        self._flush_all()
                        self._send_eof(client_id)
                        done_clients.add(client_id)

                    if self.total_clients > 0 and len(done_clients) >= self.total_clients:
                        self._sec_consumer.stop_consuming()
                        logger.info(f"{self.__class__.__name__} terminado")
                    return
                else:
                    for row in msg.get("rows", []):
                        self._emit(self.process(row))
                    ack()
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        try:
            self._sec_consumer.start_consuming(on_message)
        except MessageMiddlewareDisconnectedError:
            if self._running:
                logger.error("Conexion perdida con RabbitMQ")


    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")

        with multiprocessing.Manager() as manager:
            client_sockets = manager.dict()
            bank_maps = manager.dict()
            client_query_eofs = manager.dict()
            client_ready = manager.dict()
            send_lock = manager.Lock()

            with multiprocessing.Pool(processes=os.cpu_count()) as processes_pool:
                processes_pool.apply_async(
                    self.handle_message_main_input, (),
                )

                processes_pool.apply_async(
                    self.handle_message_sec_input, (),
                )
