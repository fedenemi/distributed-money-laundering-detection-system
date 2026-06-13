import json
import logging
import os
import signal
import time
import multiprocessing
import hashlib
import random

from common.middleware.middleware_rabbitmq import MessageMiddlewareQueueRabbitMQ, _connection_parameters
from common.middleware.middleware_sharded import ShardedExchangeConsumer, ShardedExchangeProducer
from common.middleware.middleware import MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError
from common.message_protocol.internal import deserialize, serialize
from common.health.health_server import HealthCheckServer


logger = logging.getLogger(__name__)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RECONNECT_DELAY = 2
RECONNECT_MAX_DELAY = 30


def _wait_for_rabbitmq():
    while True:
        try:
            import pika
            conn = pika.BlockingConnection(_connection_parameters(RABBITMQ_HOST))
            conn.close()
            return
        except Exception:
            logger.warning(f"RabbitMQ no disponible, reintentando en {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)


class WorkerBaseDoubleIO(HealthCheckServer):

    def __init__(self):
        super().__init__()
        self.batch_size      = int(os.environ.get("BATCH_SIZE", "500"))
        self.sec_batch_size  = int(os.environ.get("SEC_BATCH_SIZE", str(self.batch_size)))
        self.total_clients   = int(os.environ.get("TOTAL_CLIENTS", "0"))
        self.consumer_group  = os.environ.get("CONSUMER_GROUP", self.__class__.__name__)

        self.main_n_upstream = int(os.environ.get("MAIN_N_UPSTREAM", "1"))
        self.sec_n_upstream  = int(os.environ.get("SECONDARY_N_UPSTREAM", "1"))

        # Configuration
        self._operation_mode = os.environ["OP_MODE"]
        self.main_eof_dest = os.environ.get("MAIN_EOF_DEST", "NONE")
        self.sec_eof_dest = os.environ.get("SEC_EOF_DEST", "NONE")
        self._results_buffer_next_stage = []
        self._running = True

        _wait_for_rabbitmq()
        self.start_health_server()
        self._define_operation_mode()




    def _define_operation_mode(self):
        if self._operation_mode != "PIPELINE" and self._operation_mode != "JOINER":
            raise ValueError("Modo de operación incorrecto. Debe ser PIPELINE o JOINER")

    def _reconnect_backoff(self, attempt: int):
        delay = min(RECONNECT_DELAY * (2 ** attempt), RECONNECT_MAX_DELAY)
        logger.warning(f"Reintentando conexion en {delay}s...")
        time.sleep(delay)

    def _handle_main_process_sigterm(self, *_):
        self._close_main_resources()

    def _handle_sec_process_sigterm(self, *_):
        self._close_sec_resources()

    def _close_main_resources(self):
        try:
            if hasattr(self, "_main_consumer") and self._main_consumer is not None:
                self._main_consumer.stop_consuming()
                self._main_consumer.close()
        except Exception:
            pass
        try:
            if hasattr(self, "_main_producer") and self._main_producer is not None:
                self._main_producer.close()
        except Exception:
            pass

    def _close_sec_resources(self):
        try:
            if hasattr(self, "_sec_consumer") and self._sec_consumer is not None:
                self._sec_consumer.stop_consuming()
                self._sec_consumer.close()
        except Exception:
            pass
        try:
            if hasattr(self, "_sec_producer") and self._sec_producer is not None:
                self._sec_producer.close()
        except Exception:
            pass

    def _producer_is_open(self, producer):
        if producer is None:
            return False
        conn = getattr(producer, "connection", None)
        if conn is None:
            return True
        return getattr(conn, "is_open", False)

    def _recreate_main_producer(self):
        if self.main_output_exchange and self.main_output_shards > 1:
            self._main_producer = ShardedExchangeProducer(
                RABBITMQ_HOST, self.main_output_exchange, self.main_output_shards
            )
        elif self.main_output_queue:
            self._main_producer = MessageMiddlewareQueueRabbitMQ(
                RABBITMQ_HOST, self.main_output_queue
            )

    def _recreate_sec_producer(self):
        if self.sec_output_exchange and self.sec_output_shards > 1:
            self._sec_producer = ShardedExchangeProducer(
                RABBITMQ_HOST, self.sec_output_exchange, self.sec_output_shards
            )
        elif self.sec_output_queue:
            self._sec_producer = MessageMiddlewareQueueRabbitMQ(
                RABBITMQ_HOST, self.sec_output_queue
            )

    def _ensure_main_producer(self):
        if self._main_producer is None:
            return
        if not self._producer_is_open(self._main_producer):
            try:
                self._main_producer.close()
            except Exception:
                pass
            self._recreate_main_producer()

    def _ensure_sec_producer(self):
        if self._sec_producer is None:
            return
        if not self._producer_is_open(self._sec_producer):
            try:
                self._sec_producer.close()
            except Exception:
                pass
            self._recreate_sec_producer()

    # --- Para implementar en subclases -------------------------------------------

    def process_main_input(self, data: dict) -> tuple[list, list]:
        raise NotImplementedError
    
    def on_main_batch_complete(self):
        return

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        raise NotImplementedError
    
    def on_sec_batch_complete(self):
        return

    def on_main_input_eof(self, client_id=None) -> list:
        return []
    
    def on_secondary_input_eof(self, client_id=None) -> list:
        return []
    
    def on_both_eof_received(self, client_id=None) -> list:
        return []
    
    def on_secondary_ready(self, client_id=None) -> list:
        return []

    def waits_for_both_pipeline_eofs(self) -> bool:
        return False

    def _routing_key(self, msg: dict) -> str:
        """Clave de particion del mensaje. Override en Splitter."""
        return "__queue__"

    def _buffer_key(self, msg: dict, output_exchange, output_shards) -> str:
        if output_exchange and output_shards > 1:
            if isinstance(msg, dict):
                routing_field = os.environ.get("ROUTING_FIELD")
                if routing_field and routing_field in msg:
                    val = str(msg[routing_field]).encode()
                    return str(int(hashlib.md5(val).hexdigest(), 16) % output_shards)
            
            # FIX: Enrutamiento determinístico para garantizar idempotencia
            if isinstance(msg, dict):
                val = json.dumps(msg, sort_keys=True).encode()
                return str(int(hashlib.md5(val).hexdigest(), 16) % output_shards)
            return str(0) # Fallback seguro
        if isinstance(msg, dict):
            client_id = msg.get("client_id")
            if client_id is not None:
                return f"client:{client_id}"
        return "__queue__"

    # --- Emisión con Buffer y flush --------------------------------------------------------

    def _emit_results_main_stage(self, results: tuple[list, list]):
        self._emit_main_output(results[0])
        self._emit_sec_output(results[1])

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
            if len(self._sec_out_buffer[buf_key]) >= self.sec_batch_size:
                self._flush_sec_buffer_key(buf_key)

    def _flush_main_buffer_key(self, buf_key: str):
        rows = self._main_out_buffer.pop(buf_key, [])
        if not rows:
            return
        body = serialize({
            "rows": rows, 
            "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_main"
        })
        self._ensure_main_producer()
        try:
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_to_shard(body, int(buf_key))
            else:
                self._main_producer.send(body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_main_producer()
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_to_shard(body, int(buf_key))
            else:
                self._main_producer.send(body)

    def _flush_sec_buffer_key(self, buf_key: str):
        rows = self._sec_out_buffer.pop(buf_key, [])
        if not rows:
            return
        body = serialize({
            "rows": rows, 
            "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_sec"
        })
        self._ensure_sec_producer()
        try:
            if self.sec_output_exchange and self.sec_output_shards > 1:
                self._sec_producer.send_to_shard(body, int(buf_key))
            else:
                self._sec_producer.send(body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_sec_producer()
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
        self._flush_all_sec_buffer()

    def _send_main_checkpoint(self, client_id, checkpoint_id):
        if self._main_producer is None:
            return
        chk_msg = {"type": "checkpoint", "client_id": client_id, "checkpoint_id": checkpoint_id, "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_main"}
        chk_body = serialize(chk_msg)
        self._ensure_main_producer()
        try:
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_eof_to_all(chk_body)
            else:
                self._main_producer.send(chk_body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_main_producer()
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_eof_to_all(chk_body)
            else:
                self._main_producer.send(chk_body)

    def _send_sec_checkpoint(self, client_id, checkpoint_id):
        if self._sec_producer is None:
            return
        chk_msg = {"type": "checkpoint", "client_id": client_id, "checkpoint_id": checkpoint_id, "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_sec"}
        chk_body = serialize(chk_msg)
        self._ensure_sec_producer()
        try:
            if self.sec_output_exchange and self.sec_output_shards > 1:
                self._sec_producer.send_eof_to_all(chk_body)
            else:
                self._sec_producer.send(chk_body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_sec_producer()
            if self.sec_output_exchange and self.sec_output_shards > 1:
                self._sec_producer.send_eof_to_all(chk_body)
            else:
                self._sec_producer.send(chk_body)

    def _send_main_output_eof(self, client_id=None):
        if self._main_producer is None:
            return
        eof_msg = {"type": "eof", "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_main"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = serialize(eof_msg)
        self._ensure_main_producer()
        try:
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_eof_to_all(eof_body)
            else:
                self._main_producer.send(eof_body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_main_producer()
            if self.main_output_exchange and self.main_output_shards > 1:
                self._main_producer.send_eof_to_all(eof_body)
            else:
                self._main_producer.send(eof_body)
    
    def _send_sec_output_eof(self, client_id=None):
        if self._sec_producer is None:
            return
        eof_msg = {"type": "eof", "_worker_node_id": f"{self.consumer_group}_{self.shard_id}_sec"}
        if client_id is not None:
            eof_msg["client_id"] = client_id
        eof_body = serialize(eof_msg)
        self._ensure_sec_producer()
        try:
            if self.sec_output_exchange and self.sec_output_shards > 1:
                self._sec_producer.send_eof_to_all(eof_body)
            else:
                self._sec_producer.send(eof_body)
        except MessageMiddlewareDisconnectedError:
            self._ensure_sec_producer()
            if self.sec_output_exchange and self.sec_output_shards > 1:
                self._sec_producer.send_eof_to_all(eof_body)
            else:
                self._sec_producer.send(eof_body)

    def _sender_id(self, msg: dict, msg_hash: str) -> str:
        return msg.get("_worker_node_id") or f"unknown:{msg_hash}"

    def _sender_set(self, mapping, key) -> set:
        value = mapping.get(key, set())
        if isinstance(value, set):
            return set(value)
        if isinstance(value, (list, tuple)):
            return set(value)
        if value in (None, 0):
            return set()
        return {str(value)}

    def _sender_count(self, mapping, key) -> int:
        return len(self._sender_set(mapping, key))

    def _execute_eof_main_input(self, client_id=None):
        if self._operation_mode == "PIPELINE":
            if self.waits_for_both_pipeline_eofs():
                self._execute_pipeline_both_eofs(client_id)
                return
            if self._sender_count(self._clients_eof_main_input, client_id) >= self.main_n_upstream:
                for result in self.on_main_input_eof(client_id):
                    self._emit_results_main_stage(result)
                self._flush_all_next_stage()
                self._send_main_output_eof(client_id)
        else: #For joiner, check if joiner action is necessary
            with self._eof_lock:
                if self._clients_joined.get(client_id, False):
                    return

                main_ready = self._sender_count(self._clients_eof_main_input, client_id) >= self.main_n_upstream
                sec_ready = self._sender_count(self._clients_eof_sec_input, client_id) >= self.sec_n_upstream
                
                # NUEVO: Vaciar el buffer del proceso main incondicionalmente
                if main_ready:
                    self._flush_all_main_buffer()
                    self._flush_all_sec_buffer()

                if main_ready and sec_ready:
                    self._clients_joined[client_id] = True
                    
                    for result in self.on_both_eof_received(client_id):
                        self._emit_main_output([result])
                    self._flush_all_main_buffer()
                    self._send_main_output_eof(client_id)
   
    def _execute_eof_sec_input(self, client_id=None):
        if self._operation_mode == "PIPELINE":
            if self.waits_for_both_pipeline_eofs():
                self._execute_pipeline_both_eofs(client_id)
                return
            if self._sender_count(self._clients_eof_sec_input, client_id) >= self.sec_n_upstream:
                for result in self.on_secondary_input_eof(client_id):
                    self._emit_sec_output([result])
                self._flush_all_sec_buffer()
                self._send_sec_output_eof(client_id)
        else: # For joiner, check if joiner action is necessary
            with self._eof_lock:
                if self._clients_joined.get(client_id, False):
                    return
                
                main_ready = self._sender_count(self._clients_eof_main_input, client_id) >= self.main_n_upstream
                sec_ready = self._sender_count(self._clients_eof_sec_input, client_id) >= self.sec_n_upstream
                
                if sec_ready and not self._clients_secondary_ready.get(client_id, False):
                    self._clients_secondary_ready[client_id] = True

                    for result in self.on_secondary_ready(client_id):
                        self._emit_main_output([result])
                    self._flush_all_main_buffer()
                    self._flush_all_sec_buffer() # NUEVO
                
                if main_ready and sec_ready:
                    self._clients_joined[client_id] = True
                    
                    for result in self.on_both_eof_received(client_id):
                        self._emit_main_output([result])
                    self._flush_all_main_buffer()
                    self._send_main_output_eof(client_id)

    def _execute_pipeline_both_eofs(self, client_id=None):
        with self._eof_lock:
            if self._clients_joined.get(client_id, False):
                return

            main_ready = self._sender_count(self._clients_eof_main_input, client_id) >= self.main_n_upstream
            sec_ready = self._sender_count(self._clients_eof_sec_input, client_id) >= self.sec_n_upstream
            if not main_ready or not sec_ready:
                return

            self._clients_joined[client_id] = True

            for result in self.on_main_input_eof(client_id):
                self._emit_results_main_stage([result])
            for result in self.on_secondary_input_eof(client_id):
                self._emit_sec_output([result])

            self._flush_all_next_stage()

            if self.main_eof_dest in ["MAIN", "BOTH"] or self.sec_eof_dest in ["MAIN", "BOTH"]:
                self._send_main_output_eof(client_id)
            if self.main_eof_dest in ["SECONDARY", "BOTH"] or self.sec_eof_dest in ["SECONDARY", "BOTH"]:
                self._send_sec_output_eof(client_id)

    # --- Loop principal ---------------------------------------------------------

    def handle_message_main_input(self):
        eof_global_senders = set()
        chk_acks = {}
        client_eof_acks = {}
        completed_checkpoints = set()

        def on_message(body: bytes, ack, nack):
            try:
                
                msg_hash = hashlib.md5(body).hexdigest()
                if msg_hash in self._processed_msgs_main:
                    logger.warning(f"DUPLICADO IGNORADO ({msg_hash}) en {self.__class__.__name__}. Haciendo ack silencioso.")
                    ack()
                    return
                t0 = time.perf_counter()
                msg = deserialize(body)
                t_deser = time.perf_counter() - t0
  
                if msg.get("type") == "checkpoint":
                    client_id = msg.get("client_id")
                    chk_id = msg.get("checkpoint_id")
                    chk_key = f"{client_id}_{chk_id}"
                    sender_id = self._sender_id(msg, msg_hash)

                    if chk_key in completed_checkpoints:
                        ack()
                        return
  
                    with self._eof_lock:
                        senders = self._sender_set(self._checkpoints_main, chk_key)
                        if sender_id in senders:
                            logger.warning(f"CHECKPOINT MAIN DUPLICADO IGNORADO de {sender_id}. Haciendo ack silencioso.")
                            ack()
                            return

                        senders.add(sender_id)
                        self._checkpoints_main[chk_key] = senders
                        
                        if self._operation_mode == "PIPELINE":
                            chk_acks.setdefault(chk_key, []).append(ack)
                            if len(senders) >= self.main_n_upstream:
                                self._flush_all_main_buffer()
                                self._send_main_checkpoint(client_id, chk_id)
                                for a in chk_acks[chk_key]: a()
                                del chk_acks[chk_key]
                                del self._checkpoints_main[chk_key]
                                completed_checkpoints.add(chk_key)
                        else: # JOINER
                            ack()
                            main_count = len(senders)
                            sec_count = self._sender_count(self._checkpoints_sec, chk_key)

                            # NUEVO: Obligar al proceso main a liberar mensajes atrapados
                            if main_count == self.main_n_upstream:
                                self._flush_all_main_buffer()
                                self._flush_all_sec_buffer()

                            if main_count >= self.main_n_upstream and sec_count >= self.sec_n_upstream:
                                self._send_main_checkpoint(client_id, chk_id)
                                try:
                                    del self._checkpoints_main[chk_key]
                                    del self._checkpoints_sec[chk_key]
                                except KeyError:
                                    pass
                                completed_checkpoints.add(chk_key)
                    return
                elif msg.get("type") == "eof":
                    client_id = msg.get("client_id")
                    sender_id = self._sender_id(msg, msg_hash)
                    if client_id is None:
                        if sender_id in eof_global_senders:
                            ack()
                            return

                        eof_global_senders.add(sender_id)
                        current_eof_count = len(eof_global_senders)
                        logger.info(f"{self.__class__.__name__} EOF main recibido ({current_eof_count}/{self.main_n_upstream})")
                        ack()
                        if current_eof_count >= self.main_n_upstream:
                            self._execute_eof_main_input(None)
                            self._main_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    if self._clients_joined.get(client_id, False):
                        ack()
                        return

                    senders = self._sender_set(self._clients_eof_main_input, client_id)
                    if sender_id in senders:
                        ack()
                        return
                    senders.add(sender_id)
                    self._clients_eof_main_input[client_id] = senders
                    current_eof_count = len(senders)
                    logger.info(f"{self.__class__.__name__} EOF main recibido para client_id={client_id} ({current_eof_count}/{self.main_n_upstream})")
                    
                    if self._operation_mode == "PIPELINE":
                        client_eof_acks.setdefault(client_id, []).append(ack)
                        self._execute_eof_main_input(client_id)
                        if current_eof_count >= self.main_n_upstream:
                            for a in client_eof_acks[client_id]: a()
                            client_eof_acks[client_id] = []
                    else:
                        self._execute_eof_main_input(client_id)
                        ack()
                    return

                else:
                    t_process = 0.0
                    t_emit = 0.0
                    for i, row in enumerate(msg.get("rows", [])):
                        t1 = time.perf_counter()
                        if self._operation_mode == "PIPELINE":
                            self._emit_results_main_stage(self.process_main_input(row))
                        else:
                            results, _ = self.process_main_input(row)
                            self._emit_main_output(results)
                        t2 = time.perf_counter()
                        t_process += (t2 - t1)

                        if i % 100 == 0:
                            self._main_consumer.process_events()
                            if self._main_producer:
                                self._main_producer.process_events()

                    self.on_main_batch_complete()
                    
                    self._flush_all_next_stage() 
                    self._processed_msgs_main.add(msg_hash)
                    self._processed_file_handle_main.write(msg_hash + "\n")
                    self._processed_file_handle_main.flush()
                    os.fsync(self._processed_file_handle_main.fileno())
                    
                    # -------------------
                    ack()
                    logger.info(f"Tiempos Main -> Deserializar: {t_deser:.4f}s | Process: {t_process:.4f}s")
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        attempt = 0
        while self._running:
            try:
                self._main_consumer.start_consuming(on_message)
                if self._running:
                    logger.warning("El consumo finalizo inesperadamente; reconectando")
                    self._close_main_resources()
                    _wait_for_rabbitmq()
                    self._reconnect_backoff(attempt)
                    self._main_consumer = self._create_main_consumer()
                    attempt += 1
                    continue
                break
            except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
                if not self._running:
                    break
                logger.error("Conexion perdida con RabbitMQ")
                self._close_main_resources()
                _wait_for_rabbitmq()
                self._reconnect_backoff(attempt)
                self._main_consumer = self._create_main_consumer()
                attempt += 1
            except Exception as e:
                logger.error(f"Error inesperado en {self.__class__.__name__}: {e}")
                break

    def handle_message_sec_input(self):
        eof_global_senders = set()
        chk_acks = {}
        client_eof_acks = {}
        completed_checkpoints = set()

        def on_message(body: bytes, ack, nack):
            try:
                msg_hash = hashlib.md5(body).hexdigest()
                if msg_hash in self._processed_msgs_sec:
                    logger.warning(f"DUPLICADO IGNORADO ({msg_hash}) en {self.__class__.__name__}. Haciendo ack silencioso.")
                    ack()
                    return
                t0 = time.perf_counter()
                msg = deserialize(body)
                t_deser = time.perf_counter() - t0

                if msg.get("type") == "checkpoint":
                    client_id = msg.get("client_id")
                    chk_id = msg.get("checkpoint_id")
                    chk_key = f"{client_id}_{chk_id}"
                    sender_id = self._sender_id(msg, msg_hash)

                    if chk_key in completed_checkpoints:
                        ack()
                        return
                    
                    with self._eof_lock:
                        senders = self._sender_set(self._checkpoints_sec, chk_key)
                        if sender_id in senders:
                            logger.warning(f"CHECKPOINT SEC DUPLICADO IGNORADO de {sender_id}. Haciendo ack silencioso.")
                            ack()
                            return

                        senders.add(sender_id)
                        self._checkpoints_sec[chk_key] = senders
                        
                        if self._operation_mode == "PIPELINE":
                            chk_acks.setdefault(chk_key, []).append(ack)
                            if len(senders) >= self.sec_n_upstream:
                                self._flush_all_sec_buffer()
                                self._send_sec_checkpoint(client_id, chk_id)
                                for a in chk_acks[chk_key]: a()
                                del chk_acks[chk_key]
                                del self._checkpoints_sec[chk_key]
                                completed_checkpoints.add(chk_key)
                        else: # JOINER
                            ack()
                            main_count = self._sender_count(self._checkpoints_main, chk_key)
                            sec_count = len(senders)
                            
                            # Obliga al proceso secundario a liberar mensajes
                            if sec_count == self.sec_n_upstream:
                                self._flush_all_main_buffer()
                                self._flush_all_sec_buffer()

                            if main_count >= self.main_n_upstream and sec_count >= self.sec_n_upstream:
                                self._send_main_checkpoint(client_id, chk_id)
                                try:
                                    del self._checkpoints_main[chk_key]
                                    del self._checkpoints_sec[chk_key]
                                except KeyError:
                                    pass
                                completed_checkpoints.add(chk_key)
                    return
                elif msg.get("type") == "eof":
                    client_id = msg.get("client_id")
                    sender_id = self._sender_id(msg, msg_hash)
                    if client_id is None:
                        if sender_id in eof_global_senders:
                            ack()
                            return

                        eof_global_senders.add(sender_id)
                        current_eof_count = len(eof_global_senders)
                        logger.info(f"{self.__class__.__name__} EOF secondary recibido ({current_eof_count}/{self.sec_n_upstream})")
                        ack()
                        if current_eof_count >= self.sec_n_upstream:
                            self._execute_eof_sec_input(None)
                            self._sec_consumer.stop_consuming()
                            logger.info(f"{self.__class__.__name__} terminado")
                        return

                    if self._clients_joined.get(client_id, False):
                        ack()
                        return

                    senders = self._sender_set(self._clients_eof_sec_input, client_id)
                    if sender_id in senders:
                        ack()
                        return
                    senders.add(sender_id)
                    self._clients_eof_sec_input[client_id] = senders
                    current_eof_count = len(senders)
                    logger.info(f"{self.__class__.__name__} EOF secondary recibido para client_id={client_id} ({current_eof_count}/{self.sec_n_upstream})")
                    
                    if self._operation_mode == "PIPELINE":
                        client_eof_acks.setdefault(client_id, []).append(ack)
                        self._execute_eof_sec_input(client_id)
                        if current_eof_count >= self.sec_n_upstream:
                            for a in client_eof_acks[client_id]: a()
                            client_eof_acks[client_id] = []
                    else:
                        self._execute_eof_sec_input(client_id)
                        ack()
                    return

                else:
                    t_process = 0.0
                    t_emit = 0.0
                    for i, row in enumerate(msg.get("rows", [])):
                        t1 = time.perf_counter()
                        if self._operation_mode == "PIPELINE":
                            self._emit_sec_output(self.process_secondary_input(row)[1])
                        else:
                            self.process_secondary_input(row)
                        t2 = time.perf_counter()
                        t_process += (t2 - t1)

                        if i % 100 == 0:
                            self._sec_consumer.process_events()
                            if self._sec_producer:
                                self._sec_producer.process_events()

                    self.on_sec_batch_complete()
                    
                    
                    self._flush_all_next_stage() 
                    self._processed_msgs_sec.add(msg_hash)
                    self._processed_file_handle_sec.write(msg_hash + "\n")
                    self._processed_file_handle_sec.flush()
                    os.fsync(self._processed_file_handle_sec.fileno())
                    # -------------------
                    ack()
                    logger.info(f"Tiempos Sec -> Deserializar: {t_deser:.4f}s | Process: {t_process:.4f}s")
            except Exception as e:
                logger.error(f"Error procesando mensaje: {e}")
                nack()

        attempt = 0
        while self._running:
            try:
                self._sec_consumer.start_consuming(on_message)
                if self._running:
                    logger.warning("El consumo finalizo inesperadamente; reconectando")
                    self._close_sec_resources()
                    _wait_for_rabbitmq()
                    self._reconnect_backoff(attempt)
                    self._sec_consumer = self._create_sec_consumer()
                    attempt += 1
                    continue
                break
            except (MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError):
                if not self._running:
                    break
                logger.error("Conexion perdida con RabbitMQ")
                self._close_sec_resources()
                _wait_for_rabbitmq()
                self._reconnect_backoff(attempt)
                self._sec_consumer = self._create_sec_consumer()
                attempt += 1
            except Exception as e:
                logger.error(f"Error inesperado en {self.__class__.__name__}: {e}")
                break

    def _create_main_consumer(self):
        if self.main_input_exchange and self.shard_id >= 0:
            return ShardedExchangeConsumer(
                RABBITMQ_HOST, self.main_input_exchange, self.shard_id, self.consumer_group
            )
        if self.main_input_queue:
            return MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_input_queue)
        raise ValueError("Se requiere MAIN_INPUT_QUEUE o MAIN_INPUT_EXCHANGE + SHARD_ID")

    def _create_sec_consumer(self):
        if self.sec_input_exchange and self.shard_id >= 0:
            return ShardedExchangeConsumer(
                RABBITMQ_HOST, self.sec_input_exchange, self.shard_id, self.consumer_group
            )
        if self.sec_input_queue:
            return MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.sec_input_queue)
        raise ValueError("Se requiere SECONDARY_INPUT_QUEUE o SECONDARY_INPUT_EXCHANGE + SHARD_ID")

    def run_main_process(self,
                         clients_eof_main,
                         clients_eof_sec,
                         clients_joined,
                         clients_secondary_ready,
                         eof_lock,
                         channel_stages,
                         shared_cache,
                         shared_pending,
                         shared_lock,
                         checkpoints_main,
                         checkpoints_sec,
                         ):
        logging.basicConfig(level=logging.INFO)
        self._clients_eof_main_input = clients_eof_main
        self._clients_eof_sec_input = clients_eof_sec
        self._clients_joined = clients_joined
        self._clients_secondary_ready = clients_secondary_ready
        self._eof_lock = eof_lock
        self._channel_stages = channel_stages
        self._shared_cache = shared_cache
        self._shared_pending = shared_pending
        self._shared_lock = shared_lock
        self._checkpoints_main = checkpoints_main
        self._checkpoints_sec = checkpoints_sec

        self.main_input_queue     = os.environ.get("MAIN_INPUT_QUEUE", "")
        self.main_input_exchange  = os.environ.get("MAIN_INPUT_EXCHANGE", "")
        self.shard_id             = int(os.environ.get("SHARD_ID", "-1"))

        self.main_output_queue    = os.environ.get("MAIN_OUTPUT_QUEUE", "")
        self.main_output_exchange = os.environ.get("MAIN_OUTPUT_EXCHANGE", "")
        self.main_output_shards   = int(os.environ.get("MAIN_OUTPUT_SHARDS", "1"))
        self._main_out_buffer: dict = {}

        self.sec_output_queue    = os.environ.get("SECONDARY_OUTPUT_QUEUE", "")
        self.sec_output_exchange = os.environ.get("SECONDARY_OUTPUT_EXCHANGE", "")
        self.sec_output_shards   = int(os.environ.get("SEC_OUTPUT_SHARDS", os.environ.get("SECONDARY_OUTPUT_SHARDS", "1")))
        self._sec_out_buffer: dict = {}
        self.main_eof_dest = os.environ.get("MAIN_EOF_DEST", "NONE")
        self.sec_eof_dest = os.environ.get("SEC_EOF_DEST", "NONE")

        self._main_consumer = self._create_main_consumer()

        if self.main_output_exchange and self.main_output_shards >= 1:
            self._main_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.main_output_exchange, self.main_output_shards)
        elif self.main_output_queue:
            self._main_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_output_queue)
        else:
            self._main_producer = None

        if self.sec_output_exchange and self.sec_output_shards >= 1:
            self._sec_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.sec_output_exchange, self.sec_output_shards)
        elif self.sec_output_queue:
            self._sec_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.sec_output_queue)
        else:
            self._sec_producer = None

        signal.signal(signal.SIGTERM, self._handle_main_process_sigterm)

        self._processed_msgs_file_main = f"/tmp/processed_main_{self.consumer_group}_{self.shard_id}.txt"
        self._processed_msgs_main = set()
        if os.path.exists(self._processed_msgs_file_main):
            with open(self._processed_msgs_file_main, "r") as f:
                for line in f:
                    self._processed_msgs_main.add(line.strip())
        self._processed_file_handle_main = open(self._processed_msgs_file_main, "a")

        self.handle_message_main_input()

    def run_sec_process(self,
                        clients_eof_main,
                        clients_eof_sec,
                        clients_joined,
                        clients_secondary_ready,
                        eof_lock, channel_stages,
                        shared_cache,
                        shared_pending,
                        shared_lock,
                        checkpoints_main,
                        checkpoints_sec,
                        ):
        logging.basicConfig(level=logging.INFO)
        self._clients_eof_main_input = clients_eof_main
        self._clients_eof_sec_input = clients_eof_sec
        self._clients_joined = clients_joined
        self._clients_secondary_ready = clients_secondary_ready
        self._eof_lock = eof_lock
        self._channel_stages = channel_stages
        self._shared_cache = shared_cache
        self._shared_pending = shared_pending
        self._shared_lock = shared_lock
        self._checkpoints_main = checkpoints_main
        self._checkpoints_sec = checkpoints_sec

        self.sec_input_queue     = os.environ.get("SECONDARY_INPUT_QUEUE", "")
        self.sec_input_exchange  = os.environ.get("SECONDARY_INPUT_EXCHANGE", "")
        self.shard_id            = int(os.environ.get("SHARD_ID", "-1"))

        self.main_output_queue    = os.environ.get("MAIN_OUTPUT_QUEUE", "")
        self.main_output_exchange = os.environ.get("MAIN_OUTPUT_EXCHANGE", "")
        self.main_output_shards   = int(os.environ.get("MAIN_OUTPUT_SHARDS", "1"))
        self._main_out_buffer: dict = {}

        self.sec_output_queue    = os.environ.get("SECONDARY_OUTPUT_QUEUE", "")
        self.sec_output_exchange = os.environ.get("SECONDARY_OUTPUT_EXCHANGE", "")
        self.sec_output_shards   = int(os.environ.get("SEC_OUTPUT_SHARDS", os.environ.get("SECONDARY_OUTPUT_SHARDS", "1")))
        self._sec_out_buffer: dict = {}
        self.main_eof_dest = os.environ.get("MAIN_EOF_DEST", "NONE")
        self.sec_eof_dest = os.environ.get("SEC_EOF_DEST", "NONE")

        self._sec_consumer = self._create_sec_consumer()

        if self.main_output_exchange and self.main_output_shards >= 1:
            self._main_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.main_output_exchange, self.main_output_shards)
        elif self.main_output_queue:
            self._main_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.main_output_queue)
        else:
            self._main_producer = None

        if self.sec_output_exchange and self.sec_output_shards >= 1:
            self._sec_producer = ShardedExchangeProducer(RABBITMQ_HOST, self.sec_output_exchange, self.sec_output_shards)
        elif self.sec_output_queue:
            self._sec_producer = MessageMiddlewareQueueRabbitMQ(RABBITMQ_HOST, self.sec_output_queue)
        else:
            self._sec_producer = None

        signal.signal(signal.SIGTERM, self._handle_sec_process_sigterm)
        self._processed_msgs_file_sec = f"/tmp/processed_sec_{self.consumer_group}_{self.shard_id}.txt"
        self._processed_msgs_sec = set()
        if os.path.exists(self._processed_msgs_file_sec):
            with open(self._processed_msgs_file_sec, "r") as f:
                for line in f:
                    self._processed_msgs_sec.add(line.strip())
        self._processed_file_handle_sec = open(self._processed_msgs_file_sec, "a")

        self.handle_message_sec_input()


    def run(self):
        logger.info(f"{self.__class__.__name__} iniciando")

        ctx = multiprocessing.get_context("fork")

        manager = ctx.Manager()
        clients_eof_main = manager.dict()
        clients_eof_sec = manager.dict()
        clients_joined = manager.dict()
        clients_secondary_ready = manager.dict()
        eof_lock = ctx.Lock()
        channel_stages = ctx.Queue()
        shared_cache = manager.dict()
        shared_pending = manager.dict()
        shared_lock = manager.Lock()
        checkpoints_main = manager.dict()
        checkpoints_sec = manager.dict()

        main_process = ctx.Process(
            target=self.run_main_process,
            args=(
                clients_eof_main,
                clients_eof_sec,
                clients_joined,
                clients_secondary_ready,
                eof_lock,
                channel_stages,
                shared_cache,
                shared_pending,
                shared_lock,
                checkpoints_main,
                checkpoints_sec,
            ),
        )
        sec_process = ctx.Process(
            target=self.run_sec_process,
            args=(
                clients_eof_main,
                clients_eof_sec,
                clients_joined,
                clients_secondary_ready,
                eof_lock,
                channel_stages,
                shared_cache,
                shared_pending,
                shared_lock,
                checkpoints_main,
                checkpoints_sec,
            ),
        )

        def _handle_sigterm(*_):
            logger.info("SIGTERM recibido -> cerrando")
            main_process.terminate()
            sec_process.terminate()

        signal.signal(signal.SIGTERM, _handle_sigterm)

        main_process.start()
        sec_process.start()

        main_process.join()
        sec_process.join()
