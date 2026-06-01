import logging
import traceback
import socket
import threading
import queue

from message_handlers import message_handler
from common import middleware, message_protocol
import random                         
import os   
from common.middleware.middleware_sharded import ShardedExchangeProducer 


def _normalize_bank_id(bank_id):
    if bank_id is None:
        return None
    normalized = str(bank_id).strip()
    normalized = normalized.lstrip("0")
    return normalized or "0"

def _update_bank_map(bank_maps, client_id, rows):
    bank_map = dict(bank_maps.get(client_id, {}))
    for row in rows:
        if len(row) < 2:
            continue
        bank_name, bank_id = row[0], row[1]
        bank_id = str(bank_id).strip()
        bank_map[bank_id] = bank_name
        bank_map[_normalize_bank_id(bank_id)] = bank_name
    bank_maps[client_id] = bank_map


def _rows_to_transactions(client_id, rows, transaction_columns):
    for row in rows:
        if len(row) != len(transaction_columns):
            logging.warning("Transaction row has unexpected length %s", len(row))
            continue
        transaction = dict(zip(transaction_columns, row))
        transaction["client_id"] = client_id
        yield transaction


def _chunks(items, size):
    chunk = []
    for item in items:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


from common.middleware.middleware_sharded import ShardedExchangeProducer

def _build_output_queue(mom_host, output_queue, output_exchange, output_shards=1):
    if output_queue:
        return middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)
    if output_exchange:
        return ShardedExchangeProducer(mom_host, output_exchange, output_shards)
    raise Exception("FATAL: no output given for data processing")


def client_dispatcher(client_id, client_socket, outbox, ready_event):
    ready_event.wait() 

    while True:
        try:
            msg_type, payload = outbox.get()

            if msg_type == "ROWS":
                query_id, rows = payload
                message_protocol.external.send_msg(
                    client_socket, 
                    message_protocol.external.MsgType.QUERY_RESULT_BATCH, 
                    client_id, 
                    query_id, 
                    rows
                )
                
            elif msg_type == "END_QUERY":
                query_id = payload
                message_protocol.external.send_msg(
                    client_socket, 
                    message_protocol.external.MsgType.END_QUERY, 
                    client_id, 
                    query_id
                )
                
            elif msg_type == "END_RESULTS":
                message_protocol.external.send_msg(
                    client_socket, 
                    message_protocol.external.MsgType.END_RESULTS, 
                    client_id
                )
                msg_type_ack, ack_payload = message_protocol.external.recv_msg(client_socket)
                break 

            msg_type_ack, ack_payload = message_protocol.external.recv_msg(client_socket)
            if msg_type_ack != message_protocol.external.MsgType.ACK or ack_payload != client_id:
                logging.error(f"Error de ACK en despachador para {client_id}")
                break

        except Exception as e:
            logging.error(f"Error enviando al cliente {client_id}: {e}")
            break

    client_socket.close()


def handle_client_request(
    client_socket,
    client_sockets,
    bank_maps,
    client_ready_events,
    client_outboxes,
    mom_host,
    output_queue,
    output_exchange,
    transaction_columns,
    client_checkpoints,
    client_semaphores,
    checkpoint_barriers,
    checkpoint_lock
):
    handler = message_handler.MessageHandler()
    client_id = None
    output_shards = int(os.environ.get("OUTPUT_SHARDS", "1"))
    gateway_output_batch_size = int(os.environ.get("GATEWAY_OUTPUT_BATCH_SIZE", "2000"))
    max_in_flight_batches = int(os.environ.get("MAX_IN_FLIGHT_BATCHES", "3"))
    output = _build_output_queue(mom_host, output_queue, output_exchange, output_shards)

    try:
        client_socket.setblocking(True)
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(client_socket)
            except Exception as e:
                client_socket.close() 
                raise e

            if msg_type in (
                message_protocol.external.MsgType.ACCOUNTS_BATCH,
                message_protocol.external.MsgType.TRANSACTIONS_BATCH,
            ):
                msg_client_id, rows = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready_events[client_id] = threading.Event()
                    client_outboxes[client_id] = queue.Queue()
                    client_semaphores[client_id] = threading.Semaphore(max_in_flight_batches)

                    t_disp = threading.Thread(
                        target=client_dispatcher,
                        args=(client_id, client_socket, client_outboxes[client_id], client_ready_events[client_id])
                    )
                    t_disp.daemon = True
                    t_disp.start()
                    
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in request stream")

            if msg_type == message_protocol.external.MsgType.ACCOUNTS_BATCH:
                _update_bank_map(bank_maps, client_id, rows)
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.END_ACCOUNTS:
                msg_client_id = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    
                    client_ready_events[client_id] = threading.Event()
                    client_outboxes[client_id] = queue.Queue()
                    client_semaphores[client_id] = threading.Semaphore(max_in_flight_batches)

                    t_disp = threading.Thread(
                        target=client_dispatcher,
                        args=(client_id, client_socket, client_outboxes[client_id], client_ready_events[client_id])
                    )
                    t_disp.daemon = True
                    t_disp.start()
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in end accounts")
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.TRANSACTIONS_BATCH:
                client_semaphores[client_id].acquire()

                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange)

                transactions = _rows_to_transactions(client_id, rows, transaction_columns)
                for transactions_chunk in _chunks(transactions, gateway_output_batch_size):
                    serialized_message = handler.serialize_rows_message(client_id, transactions_chunk)
                    if isinstance(output, ShardedExchangeProducer):
                        shard = random.randint(0, output_shards - 1)
                        output.send_to_shard(serialized_message, shard)
                    else:
                        output.send(serialized_message)

                with checkpoint_lock:
                    current_checkpoint = client_checkpoints.get(client_id, 0) + 1
                    client_checkpoints[client_id] = current_checkpoint
                    checkpoint_barriers[(client_id, current_checkpoint)] = set()

                checkpoint_msg = handler.serialize_checkpoint_message(
                    client_id,
                    current_checkpoint
                )

                if isinstance(output, ShardedExchangeProducer):
                    output.send_eof_to_all(checkpoint_msg)
                else:
                    output.send(checkpoint_msg)
                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                continue

            if msg_type == message_protocol.external.MsgType.END_TRANSACTIONS:
                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange)
                msg_client_id = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready_events[client_id] = threading.Event()
                    client_outboxes[client_id] = queue.Queue()
                    t_disp = threading.Thread(
                        target=client_dispatcher,
                        args=(client_id, client_socket, client_outboxes[client_id], client_ready_events[client_id])
                    )
                    t_disp.daemon = True
                    t_disp.start()

                serialized_message = handler.serialize_eof_message(client_id)
                if isinstance(output, ShardedExchangeProducer):
                    output.send_eof_to_all(serialized_message)
                else:
                    output.send(serialized_message)

                message_protocol.external.send_msg(client_socket, message_protocol.external.MsgType.ACK, client_id)
                client_ready_events[client_id].set()
                return 

            raise TypeError(f"Unexpected message type: {msg_type}")
    except Exception as e:
        logging.error(f"Handler error for client {client_id}: {e}")
        logging.error(traceback.format_exc())
        client_socket.close() 
    finally:
        if output is not None:
            output.close()
        logging.info(f"Terminó la recepción de datos de entrada para el cliente {client_id}.")
