"""
WorkerBase: clase base para todos los workers.


MessageMiddlewareQueueRabbitMQ: para colas simples
ShardedExchangeConsumer: para consumir un shard de exchange
ShardedExchangeProducer: para publicar con sharding

Variables de entorno:
  RABBITMQ_HOST: host de RabbitMQ (default: rabbitmq)
  MAIN_INPUT_QUEUE: cola de entrada principal (si consume de cola simple)
  MAIN_INPUT_EXCHANGE: exchange de entrada principal (si consume de shard)
  SECONDARY_INPUT_QUEUE: cola de entrada secundaria (si consume de cola simple)
  SECONDARY_INPUT_EXCHANGE: exchange de entrada secundaria (si consume de shard)
  SHARD_ID        : id del shard de este worker
  N_UPSTREAM_MAIN : cantidad de EOFs a esperar de la entrada principal
  N_UPSTREAM_SECONDARY : cantidad de EOFs a esperar de la entrada secundaria
  MAIN_OUTPUT_QUEUE    : cola de salida simple principal
  MAIN_OUTPUT_QUEUE    : cola de salida simple secundaria
  MAIN_OUTPUT_EXCHANGE : exchange de salida con sharding principal
  SECONDARY_OUTPUT_EXCHANGE : exchange de salida con sharding secundaria
  OUTPUT_SHARDS   : cantidad de shards de salida (default 1)
  BATCH_SIZE      : filas por batch de salida (default 500)
  OP_MODE           : Modo de operación del worker. JOINER si se quiere que se use como joiner de
                        de dos entradas o PIPELINE si se quieren realizar acciones primero con la
                        entrada principal y luego con la entrada secundaria.
"""
import json
import logging
import os
import signal
import time
import multiprocessing

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ
from common.middleware.middleware_sharded import ShardedExchangeConsumer, ShardedExchangeProducer
from common.middleware.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

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


class WorkerBaseDoubleIO:

    def __init__(self):
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))
        self.total_clients   = int(os.environ.get("TOTAL_CLIENTS", "0"))

        self.main_n_upstream = int(os.environ.get("MAIN_N_UPSTREAM", "1"))
        self.sec_n_upstream  = int(os.environ.get("SECONDARY_N_UPSTREAM", "1"))

        # Configuration
        self._operation_mode = os.environ["OP_MODE"]
        self._results_buffer_next_stage = []
        self._running = True

        _wait_for_rabbitmq()
        self._define_operation_mode()


    def _define_operation_mode(self):
        if self._operation_mode != "PIPELINE" and self._operation_mode != "JOINER":
            raise ValueError("Modo de operación incorrecto. Debe ser PIPELINE o JOINER")

    def _handle_main_process_sigterm(self, *_):
        self._main_consumer.stop_consuming()
        self._main_consumer.close()
        self._main_producer.close()

    def _handle_sec_process_sigterm(self, *_):
        self._sec_consumer.stop_consuming()
        self._sec_consumer.close()
        self._sec_producer.close()

    # --- Para implementar en subclases -------------------------------------------

    def process_main_input(self, data: dict) -> tuple[list, list]:
        raise NotImplementedError

    def process_secondary_input(self, data: dict, prev_stage_data: list) -> tuple[list, list]:
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

    def _buffer_key(self, msg: dict, output_exchange, output_shards) -> str:
        if output_exchange and output_shards > 1:
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
            buf_key = self._buffer_key(msg, self.main_output_exchange, self.main_output_shards)
            self._main_out_buffer.setdefault(buf_key, []).append(msg)
            if len(self._main_out_buffer[buf_key]) >= self.batch_size:
                self._flush_main_buffer_key(buf_key)

    def _emit_sec_output(self, results: list):
        if not results or self._sec_producer is None:
            return
        for msg in results:
            buf_key = self._buffer_key(msg, self.sec_output_exchange, self.sec_output_shards)
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
        if self._main_producer is None:
            return
        eof_msg = {"type": "eof"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = json.dumps(eof_msg).encode()
        if self.main_output_exchange and self.main_output_shards > 1:
            self._main_producer.send_eof_to_all(eof_body)
        else:
            self._main_producer.send(eof_body)
    
    def _send_sec_output_eof(self, client_id=None):
        if self._sec_producer is None:
            return
        eof_msg = {"type": "eof"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = json.dumps(eof_msg).encode()
        if self.sec_output_exchange and self.sec_output_shards > 1:
            self._sec_producer.send_eof_to_all(eof_body)
        else:
            self._sec_producer.send(eof_body)

    def _execute_eof_main_input(self, client_id=None):
        if self._operation_mode == "PIPELINE":
            if self._clients_eof_main_input[client_id] >= self.main_n_upstream:
                for result in self.on_main_input_eof(client_id):
                    self._emit_results_main_stage([result])
                self._flush_all_next_stage()
        else: #For joiner, check if joiner action is necessary
            with self._eof_lock:
                if self._clients_joined.get(client_id, False):
                    return

                main_ready = self._clients_eof_main_input.get(client_id, 0) >= self.main_n_upstream
                sec_ready = self._clients_eof_sec_input.get(client_id, 0) >= self.sec_n_upstream
                
                if main_ready and sec_ready:
                    self._clients_joined[client_id] = True
                    
                    for result in self.on_both_eof_received(client_id):
                        self._emit_main_output([result])
                    self._flush_all_main_buffer()
                    self._send_main_output_eof(client_id)

    def _execute_eof_sec_input(self, client_id=None):
        if self._operation_mode == "PIPELINE":
            if self._clients_eof_sec_input[client_id] >= self.sec_n_upstream:
                for result in self.on_secondary_input_eof(client_id):
                    self._emit_sec_output([result])
                self._flush_all_sec_buffer()
                self._send_sec_output_eof(client_id)
        else: # For joiner, check if joiner action is necessary
            with self._eof_lock:
                if self._clients_joined.get(client_id, False):
                    return
                
                main_ready = self._clients_eof_main_input.get(client_id, 0) >= self.main_n_upstream
                sec_ready = self._clients_eof_sec_input.get(client_id, 0) >= self.sec_n_upstream
                
                if main_ready and sec_ready:
                    self._clients_joined[client_id] = True
                    
                    for result in self.on_both_eof_received(client_id):
                        self._emit_main_output([result])
                    self._flush_all_main_buffer()
                    self._send_main_output_eof(client_id)

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
                            self._execute_eof_main_input(None)
                            self._main_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    self._clients_eof_main_input[client_id] = self._clients_eof_main_input.get(client_id, 0) + 1
                    self._execute_eof_main_input(client_id)
                    ack()
                    return

                else:
                    for row in msg.get("rows", []):
                        if self._operation_mode == "PIPELINE":
                            self._emit_results_main_stage(self.process_main_input(row))
                        else:
                            self.process_main_input(row)
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

        def on_message(body: bytes, ack, nack):
            try:
                msg = json.loads(body)
                if msg.get("type") == "eof":
                    client_id = msg.get("client_id")
                    if client_id is None:
                        eof_count[0] += 1
                        ack()
                        if eof_count[0] >= self.sec_n_upstream:
                            self._execute_eof_sec_input(None)
                            self._sec_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    self._clients_eof_sec_input[client_id] = self._clients_eof_sec_input.get(client_id, 0) + 1
                    self._execute_eof_sec_input(client_id)
                    ack()
                    return


                else:
                    for row in msg.get("rows", []):
                        if self._operation_mode == "PIPELINE":
                            prev_stage_data = self._channel_stages.get()
                            self._emit_sec_output(self.process_secondary_input(row, prev_stage_data)[1])
                        else:
                            self.process_secondary_input(row, None)
                    ack()
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        try:
            self._sec_consumer.start_consuming(on_message)
        except MessageMiddlewareDisconnectedError:
            if self._running:
                logger.error("Conexion perdida con RabbitMQ")

    def run_main_process(self, clients_eof_main, clients_eof_sec, clients_joined, eof_lock, channel_stages):
        logging.basicConfig(level=logging.INFO)
        # Shared variables
        self._clients_eof_main_input = clients_eof_main
        self._clients_eof_sec_input = clients_eof_sec
        self._clients_joined = clients_joined
        self._eof_lock = eof_lock
        self._channel_stages = channel_stages

        # Input
        self.main_input_queue     = os.environ.get("MAIN_INPUT_QUEUE", "")
        self.main_input_exchange  = os.environ.get("MAIN_INPUT_EXCHANGE", "")
        self.shard_id             = int(os.environ.get("SHARD_ID", "-1"))

        # Output
        self.main_output_queue    = os.environ.get("MAIN_OUTPUT_QUEUE", "")
        self.main_output_exchange = os.environ.get("MAIN_OUTPUT_EXCHANGE", "")
        self.main_output_shards   = int(os.environ.get("MAIN_OUTPUT_SHARDS", "1"))
        self._main_out_buffer: dict = {}

        self.sec_output_queue    = os.environ.get("SECONDARY_OUTPUT_QUEUE", "")
        self.sec_output_exchange = os.environ.get("SECONDARY_OUTPUT_EXCHANGE", "")
        self.sec_output_shards   = int(os.environ.get("SECONDARY_OUTPUT_SHARDS", "1"))
        self._sec_out_buffer: dict = {}

        # Setup connections
        # Main input
        if self.main_input_exchange and self.shard_id >= 0:
            self._main_consumer = ShardedExchangeConsumer(RABBITMQ_HOST, self.main_input_exchange, self.shard_id)
        elif self.main_input_queue:
            self._main_consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_input_queue)
        else:
            raise ValueError("Se requiere MAIN_INPUT_QUEUE o MAIN_INPUT_EXCHANGE + SHARD_ID")

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

        # SIGTERM handler
        signal.signal(signal.SIGTERM, self._handle_main_process_sigterm)

        # Handle inputs
        self.handle_message_main_input()

    def run_sec_process(self, clients_eof_main, clients_eof_sec, clients_joined, eof_lock, channel_stages):
        logging.basicConfig(level=logging.INFO)
        # Shared variables
        self._clients_eof_main_input = clients_eof_main
        self._clients_eof_sec_input = clients_eof_sec
        self._clients_joined = clients_joined
        self._eof_lock = eof_lock
        self._channel_stages = channel_stages

        # Input
        self.sec_input_queue     = os.environ.get("SECONDARY_INPUT_QUEUE", "")
        self.sec_input_exchange  = os.environ.get("SECONDARY_INPUT_EXCHANGE", "")
        self.shard_id            = int(os.environ.get("SHARD_ID", "-1"))

        # Main output
        self.main_output_queue    = os.environ.get("MAIN_OUTPUT_QUEUE", "")
        self.main_output_exchange = os.environ.get("MAIN_OUTPUT_EXCHANGE", "")
        self.main_output_shards   = int(os.environ.get("MAIN_OUTPUT_SHARDS", "1"))
        self._main_out_buffer: dict = {}

        # Secondary output
        self.sec_output_queue    = os.environ.get("SECONDARY_OUTPUT_QUEUE", "")
        self.sec_output_exchange = os.environ.get("SECONDARY_OUTPUT_EXCHANGE", "")
        self.sec_output_shards   = int(os.environ.get("SECONDARY_OUTPUT_SHARDS", "1"))
        self._sec_out_buffer: dict = {}

        # Setup connections
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

        # SIGTERM handler
        signal.signal(signal.SIGTERM, self._handle_sec_process_sigterm)

        # Handle inputs
        self.handle_message_sec_input()


    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")

        # Create manager
        manager = multiprocessing.Manager()
        clients_eof_main = manager.dict()
        clients_eof_sec = manager.dict()
        clients_joined = manager.dict()
        eof_lock = multiprocessing.Lock()
        channel_stages = multiprocessing.Queue()

        # Create processes
        main_process = multiprocessing.Process(
            target=self.run_main_process, 
            args=(clients_eof_main, clients_eof_sec, clients_joined, eof_lock, channel_stages)
        )
        sec_process = multiprocessing.Process(
            target=self.run_sec_process,
            args=(clients_eof_main, clients_eof_sec, clients_joined, eof_lock, channel_stages)
        )

        def _handle_sigterm(*_):
            logger.info("SIGTERM recibido -> cerrando")
            main_process.terminate()
            sec_process.terminate()

        signal.signal(signal.SIGTERM, _handle_sigterm)

        # Start processes
        main_process.start()
        sec_process.start()

        # Wait for processes to end
        main_process.join()
        sec_process.join()
