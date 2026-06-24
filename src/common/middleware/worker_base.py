"""
WorkerBase: clase base para todos los workers.

MessageMiddlewareQueueRabbitMQ: para colas simples
ShardedExchangeConsumer: para consumir un shard de exchange
ShardedExchangeProducer: para publicar con sharding
"""
import logging
import os
import random
import signal
import time
import zlib
import hashlib

from common.logger.base_node_logger import BaseNodeLogger
from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ, _connection_parameters
from common.middleware.middleware_sharded import ShardedExchangeConsumer, ShardedExchangeProducer
from common.middleware.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
from common.message_protocol.internal import deserialize, serialize
from common.health.health_server import HealthCheckServer

logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RECONNECT_DELAY = 2
RECONNECT_MAX_DELAY = 30
PARTIAL_BATCH_CHECKPOINT_TOTAL = 500


def _wait_for_rabbitmq():
    while True:
        try:
            import pika
            conn = pika.BlockingConnection(_connection_parameters(RABBITMQ_HOST))
            conn.close()
            return
        except Exception:
            logger.info(f"RabbitMQ no disponible, reintentando en {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)


class WorkerBase(HealthCheckServer):

    def __init__(self):
        super().__init__()
        self.input_queue     = os.environ.get("INPUT_QUEUE", "")
        self.input_exchange  = os.environ.get("INPUT_EXCHANGE", "")
        self.consumer_group  = os.environ.get("CONSUMER_GROUP", self.__class__.__name__)
        self.shard_id        = int(os.environ.get("SHARD_ID", "-1"))
        self.n_upstream      = int(os.environ.get("N_UPSTREAM", "1"))
        self.output_queue    = os.environ.get("OUTPUT_QUEUE", "")
        self.output_exchange = os.environ.get("OUTPUT_EXCHANGE", "")
        self.output_shards   = int(os.environ.get("OUTPUT_SHARDS", "1"))
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))
        self.total_clients   = int(os.environ.get("TOTAL_CLIENTS", "0"))

        self._buffer: dict = {}
        self._running = True

        base_logs_dir = "/worker_logs"
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        worker_dir = os.path.join(base_logs_dir, worker_name)
        os.makedirs(worker_dir, exist_ok=True)
        logger_path = os.path.join(worker_dir, "data")

        self.node_logger = BaseNodeLogger(logger_path)

        (self.pending_batch_id, 
         self.processed_tx_count, 
         self.last_completed_batch,
         self.saved_buffer_sizes) = self.node_logger.recover_batch_state()

        recovered_buffers = self.node_logger.load_all_buffers()
        for (client_id, buf_key), msgs in recovered_buffers.items():
            self._buffer.setdefault(buf_key, []).extend(msgs)

        self._reconcile_state()

        self.eof_global_senders, self.eof_client_senders = self.node_logger.recover_eofs()
        self.completed_eofs = self.node_logger.recover_eof_done()
        self.completed_checkpoints = self.node_logger.recover_checkpoint_done()

        signal.signal(signal.SIGTERM, self._handle_sigterm)

        _wait_for_rabbitmq()
        self._setup_connections()
        self.start_health_server()

    def _setup_connections(self):
        # Input
        if self.input_exchange and self.shard_id >= 0:
            self._consumer = ShardedExchangeConsumer(
                RABBITMQ_HOST, self.input_exchange, self.shard_id, self.consumer_group
            )
        elif self.input_queue:
            self._consumer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.input_queue)
        else:
            raise ValueError("Se requiere INPUT_QUEUE o INPUT_EXCHANGE + SHARD_ID")

        # Output
        if self.output_exchange and self.output_shards >= 1:
            self._producer = ShardedExchangeProducer(RABBITMQ_HOST, self.output_exchange, self.output_shards)
        elif self.output_queue:
            self._producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.output_queue)
        else:
            self._producer = None

    def _close_resources(self, close_logger=True):
        try:
            if hasattr(self, "_consumer") and self._consumer is not None:
                self._consumer.stop_consuming()
                self._consumer.close()
        except Exception:
            pass
        try:
            if hasattr(self, "_producer") and self._producer is not None:
                self._producer.close()
        except Exception:
            pass
        if close_logger and hasattr(self, "node_logger") and self.node_logger is not None:
            self.node_logger.close()

    def _reconnect_backoff(self, attempt: int):
        delay = min(RECONNECT_DELAY * (2 ** attempt), RECONNECT_MAX_DELAY)
        logger.info(f"Reintentando conexion en {delay}s...")
        time.sleep(delay)

    def _reconcile_state(self):
        if not self.pending_batch_id:
            return

        for buf_key, msgs in self._buffer.items():
            expected_size = self.saved_buffer_sizes.get(buf_key, 0)
            real_size = len(msgs)

            if real_size > expected_size:
                orphans = real_size - expected_size
                logger.warning(
                    f"Reconciliación: Truncando {orphans} registros huérfanos en '{buf_key}'. "
                    f"Estado seguro: {expected_size}. Encontrados: {real_size}."
                )
                self._buffer[buf_key] = msgs[:expected_size]

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

    def on_eof(self, client_id=None) -> list:
        return []

    def on_worker_started(self):
        pass

    def supports_partial_batch_resume(self) -> bool:
        return True

    def on_batch_complete(self, batch_id: str):
        pass

    def on_eof_complete(self, client_id=None):
        pass

    def _routing_key(self, msg: dict) -> str:
        if self.output_exchange and self.output_shards >= 1:
            routing_field = os.environ.get("ROUTING_FIELD")
            if routing_field and routing_field in msg:
                val = str(msg[routing_field]).encode()
                return str(zlib.crc32(val) % self.output_shards)
            else:
                return str(random.randint(0, self.output_shards - 1))
        return "__queue__"

    def _buffer_key(self, msg: dict) -> str:
        if self.output_exchange and self.output_shards >= 1:
            return self._routing_key(msg)
        if isinstance(msg, dict):
            client_id = msg.get("client_id")
            if client_id is not None:
                return f"client:{client_id}"
        return "__queue__"

    # --- Emisión con Buffer y flush --------------------------------------------------------

    def _outbox_client_id(self, msg: dict):
        return msg.get("client_id") if isinstance(msg, dict) else None

    def _emit(self, results: list):
        if not results or self._producer is None:
            return

        bulk_data = {}
        client_id = self._outbox_client_id(results[0])
        for msg in results:
            buf_key = self._buffer_key(msg)
            bulk_data.setdefault(buf_key, []).append(msg)

        for buf_key, msgs in bulk_data.items():
            if hasattr(self, "node_logger"):
                self.node_logger.append_bulk_to_buffer(client_id, buf_key, msgs)

            for i, msg in enumerate(msgs):
                self._buffer.setdefault(buf_key, []).append(msg)
                
                if len(self._buffer[buf_key]) >= self.batch_size:
                    self._flush_key(buf_key)

                    remainder = msgs[i+1:]
                    if remainder and hasattr(self, "node_logger"):
                        self.node_logger.append_bulk_to_buffer(client_id, buf_key, remainder)

    def _flush_key(self, buf_key: str):
        records = self._buffer.pop(buf_key, [])
        if not records:
            return

        # Clear disk buffer
        clients_in_batch = {self._outbox_client_id(record) for record in records}
        for cid in clients_in_batch:
            if hasattr(self, "node_logger"):
                self.node_logger.clear_buffer(cid, buf_key)

        # Send elements
        if buf_key == "__control__":
            self._flush_control_records(records)
        else:
            body = serialize({
                "rows": records,
                "_worker_node_id": f"{self.consumer_group}_{self.shard_id}"
            })

            self._send_body(buf_key, body)

    def _send_body(self, buf_key: str, body: bytes):
        try:
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_to_shard(body, int(buf_key))
            else:
                self._producer.send(body)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            self._close_resources(close_logger=False)
            _wait_for_rabbitmq()
            self._setup_connections()
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_to_shard(body, int(buf_key))
            else:
                self._producer.send(body)

    def _send_control_body(self, body: bytes):
        try:
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(body)
            else:
                self._producer.send(body)
        except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
            self._close_resources(close_logger=False)
            _wait_for_rabbitmq()
            self._setup_connections()
            if self.output_exchange and self.output_shards >= 1:
                self._producer.send_eof_to_all(body)
            else:
                self._producer.send(body)

    def _flush_control_records(self, records: list):
        for record in records:
            control_msg = record.get("message", record)
            self._send_control_body(serialize(control_msg))

    def _emit_control(self, msg: dict):
        if self._producer is None:
            return
        record = {
            "__outbox_type": "control",
            "client_id": msg.get("client_id"),
            "message": msg,
        }
        buf_key = "__control__"
        client_id = self._outbox_client_id(record)
        self._buffer.setdefault(buf_key, []).append(record)
        self.node_logger.append_to_buffer(client_id, buf_key, record)
        self._flush_key(buf_key)

    def _flush_all(self):
        for key in list(self._buffer.keys()):
            self._flush_key(key)

    def _send_eof(self, client_id=None):
        if self._producer is None:
            return
            
        eof_msg = {
            "type": "eof",
            "_worker_node_id": f"{self.consumer_group}_{self.shard_id}"
        }
        if client_id is not None:
            eof_msg["client_id"] = client_id

        self._emit_control(eof_msg)

    def _send_checkpoint(self, client_id, checkpoint_id):
        if self._producer is None:
            return
        self._emit_control({
            "type": "checkpoint",
            "client_id": client_id,
            "checkpoint_id": checkpoint_id,
            "_worker_node_id": f"{self.consumer_group}_{self.shard_id}",
        })

    def _eof_done_key(self, client_id=None) -> str:
        return "__global__" if client_id is None else str(client_id)

    def _eof_is_done(self, client_id=None) -> bool:
        return self._eof_done_key(client_id) in self.completed_eofs

    def _mark_eof_done(self, client_id=None):
        key = self._eof_done_key(client_id)
        if key in self.completed_eofs:
            return
        self.node_logger.log_eof_done(client_id)
        self.completed_eofs.add(key)

    def _checkpoint_done_key(self, client_id, checkpoint_id) -> str:
        client_key = "__global__" if client_id is None else str(client_id)
        return f"{client_key}:{checkpoint_id}"

    def _checkpoint_is_done(self, client_id, checkpoint_id) -> bool:
        return self._checkpoint_done_key(client_id, checkpoint_id) in self.completed_checkpoints

    def _mark_checkpoint_done(self, client_id, checkpoint_id):
        key = self._checkpoint_done_key(client_id, checkpoint_id)
        if key in self.completed_checkpoints:
            return
        self.node_logger.log_checkpoint_done_key(key, client_id, checkpoint_id)
        self.completed_checkpoints.add(key)

    def _finish_eof(self, client_id=None):
        if self._eof_is_done(client_id):
            logger.info(f"{self.__class__.__name__} EOF ya finalizado para client_id={client_id}; no se reemite")
            return

        for result in self.on_eof(client_id):
            self._emit([result])
        self._flush_all()
        self._send_eof(client_id)
        self._mark_eof_done(client_id)
        self.on_eof_complete(client_id)

    # --- Loop principal ---------------------------------------------------------

    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")
        self.on_worker_started()
        
        eof_global_senders = self.eof_global_senders
        eof_client_senders = self.eof_client_senders
        done_clients = set()
        
        checkpoint_senders = {}

        for client_id, senders in list(eof_client_senders.items()):
            if len(senders) >= self.n_upstream and not self._eof_is_done(client_id):
                logger.info(f"{self.__class__.__name__} recupero EOF completo para client_id={client_id}; ejecutando cierre pendiente")
                self._finish_eof(client_id)
                done_clients.add(client_id)
                if client_id in eof_client_senders:
                    del eof_client_senders[client_id]

        def on_message(body: bytes, ack, nack):
            try:
                msg_hash = hashlib.md5(body).hexdigest()
                
                if msg_hash == self.last_completed_batch:
                    logger.info(f"DUPLICADO IGNORADO ({msg_hash}) en {self.__class__.__name__}. Haciendo ack silencioso.")
                    ack()
                    return

                msg = deserialize(body)
                sender_id = msg.get("_worker_node_id") or f"unknown:{msg_hash}"

                if msg.get("type") == "checkpoint":
                    client_id = msg.get("client_id")
                    checkpoint_id = msg.get("checkpoint_id")
                    chk_key = (client_id, checkpoint_id)

                    if self._checkpoint_is_done(client_id, checkpoint_id):
                        ack()
                        return

                    checkpoint_senders.setdefault(chk_key, set())
                    if sender_id in checkpoint_senders[chk_key]:
                        logger.info(f"CHECKPOINT DUPLICADO IGNORADO de {sender_id}. Haciendo ack silencioso.")
                        ack()
                        return

                    checkpoint_senders[chk_key].add(sender_id)

                    if len(checkpoint_senders[chk_key]) >= self.n_upstream:
                        self._flush_all()
                        self._send_checkpoint(client_id, checkpoint_id)
                        del checkpoint_senders[chk_key]
                        self._mark_checkpoint_done(client_id, checkpoint_id)
                    ack()
                    return
                    
                elif msg.get("type") == "eof":
                    client_id = msg.get("client_id")

                    if client_id is None:
                        if self._eof_is_done(None):
                            ack()
                            return

                        if sender_id in eof_global_senders:
                            if len(eof_global_senders) >= self.n_upstream:
                                self._finish_eof(None)
                                self._consumer.stop_consuming()
                            ack()
                            return
                        
                        self.node_logger.log_eof(client_id, sender_id)
                        eof_global_senders.add(sender_id)
                        current_eof_count = len(eof_global_senders)
                        logger.info(f"{self.__class__.__name__} EOF global recibido ({current_eof_count}/{self.n_upstream})")
                        
                        if current_eof_count >= self.n_upstream:
                            self._finish_eof(None)
                            self._consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado globalmente")
                        ack()
                        return

                    if client_id in done_clients or self._eof_is_done(client_id):
                        ack()
                        return

                    eof_client_senders.setdefault(client_id, set())
                    if sender_id in eof_client_senders[client_id]:
                        if len(eof_client_senders[client_id]) >= self.n_upstream:
                            self._finish_eof(client_id)
                            done_clients.add(client_id)
                            if client_id in eof_client_senders:
                                del eof_client_senders[client_id]
                        ack()
                        return

                    self.node_logger.log_eof(client_id, sender_id)
                    eof_client_senders[client_id].add(sender_id)
                    current_eof_count = len(eof_client_senders[client_id])
                    
                    logger.info(f"{self.__class__.__name__} EOF recibido para client_id={client_id} ({current_eof_count}/{self.n_upstream})")
                    
                    if current_eof_count >= self.n_upstream:
                        self._finish_eof(client_id)
                        done_clients.add(client_id)
                        if client_id in eof_client_senders:
                            del eof_client_senders[client_id]

                    ack()
                    return

                is_resuming = self.supports_partial_batch_resume() and (msg_hash == self.pending_batch_id)
                rows = msg.get("rows", [])

                if not is_resuming:
                    self.node_logger.save_batch_state(msg_hash, 0, self.last_completed_batch)
                    self.pending_batch_id = msg_hash
                    self.processed_tx_count = 0

                chunk_results = []                
                for i, row in enumerate(rows):
                    if is_resuming and i < self.processed_tx_count:
                        continue

                    self._current_msg_hash = msg_hash
                    self._current_row_index = i
                    processed_data = self.process(row)
                    
                    if processed_data:
                        chunk_results.extend(processed_data)

                    is_last_row = (i == len(rows) - 1)

                    if (self.supports_partial_batch_resume() and i % PARTIAL_BATCH_CHECKPOINT_TOTAL == 0 and i > 0) or is_last_row:

                        self._emit(chunk_results)

                        if self.supports_partial_batch_resume() and not is_last_row:
                            current_sizes = {k: len(v) for k, v in self._buffer.items()}
                            self.node_logger.save_batch_state(msg_hash, i, self.last_completed_batch, current_sizes)

                        chunk_results.clear()

                self.on_batch_complete(msg_hash)
                self.node_logger.save_batch_state(None, 0, msg_hash, {})
                self.pending_batch_id = None
                self.processed_tx_count = 0
                self.last_completed_batch = msg_hash

                ack()
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        attempt = 0
        while self._running:
            try:
                self._consumer.start_consuming(on_message)
                if self._running:
                    logger.info("El consumo finalizo inesperadamente; reconectando")
                    self._close_resources(close_logger=False)
                    _wait_for_rabbitmq()
                    self._reconnect_backoff(attempt)
                    self._setup_connections()
                    attempt += 1
                    continue
                break
            except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
                if not self._running:
                    break
                logger.error("Conexion perdida con RabbitMQ")
                self._close_resources(close_logger=False)
                _wait_for_rabbitmq()
                self._reconnect_backoff(attempt)
                self._setup_connections()
                attempt += 1
            except Exception as e:
                logger.error(f"Error inesperado en {self.__class__.__name__}: {e}")
                break