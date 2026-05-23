import logging
import traceback
import socket

from message_handlers import message_handler
from common import middleware, message_protocol
import random                         
import os   
from common.middleware.middleware_sharded import ShardedExchangeProducer 

def _update_bank_map(bank_maps, client_id, rows):
    bank_map = dict(bank_maps.get(client_id, {}))
    for row in rows:
        if len(row) < 2:
            continue
        bank_name, bank_id = row[0], row[1]
        bank_map[bank_id] = bank_name
    bank_maps[client_id] = bank_map


def _rows_to_transactions(client_id, rows, transaction_columns):
    transactions = []
    for row in rows:
        if len(row) != len(transaction_columns):
            logging.warning("Transaction row has unexpected length %s", len(row))
            continue
        transaction = dict(zip(transaction_columns, row))
        transaction["client_id"] = client_id
        transactions.append(transaction)
    return transactions


from common.middleware.middleware_sharded import ShardedExchangeProducer

def _build_output_queue(mom_host, output_queue, output_exchange, output_shards=1):
    if output_queue:
        return middleware.MessageMiddlewareQueueRabbitMQ(mom_host, output_queue)
    if output_exchange:
        return ShardedExchangeProducer(mom_host, output_exchange, output_shards)
    raise Exception("FATAL: no output given for data processing")


def handle_client_request(
    client_socket,
    client_sockets,
    bank_maps,
    client_ready,
    mom_host,
    output_queue,
    output_exchange,
    transaction_columns,
):
    handler = message_handler.MessageHandler()
    client_id = None
    output_shards = int(os.environ.get("OUTPUT_SHARDS", "1"))
    output = _build_output_queue(mom_host, output_queue, output_exchange, output_shards)

    try:
        client_socket.setblocking(True)
        while True:
            try:
                msg_type, payload = message_protocol.external.recv_msg(client_socket)
            except Exception as e:
                if client_id is None and "0 bytes" in str(e):
                    logging.warning("Conexión cerrada sin enviar datos (health probe o timeout del cliente).")
                    client_socket.close() # Cerramos acá porque el cliente se fue antes de identificarse
                    return
                raise e

            if msg_type in (
                message_protocol.external.MsgType.ACCOUNTS_BATCH,
                message_protocol.external.MsgType.TRANSACTIONS_BATCH,
            ):
                msg_client_id, rows = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready[client_id] = False
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in request stream")

            if msg_type == message_protocol.external.MsgType.ACCOUNTS_BATCH:
                logging.info(f"Received accounts batch from client {client_id}")
                _update_bank_map(bank_maps, client_id, rows)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK,
                    client_id,
                )
                continue

            if msg_type == message_protocol.external.MsgType.END_ACCOUNTS:
                logging.info(f"Received end accounts message from client {client_id}")
                msg_client_id = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready[client_id] = False
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in end accounts")
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK,
                    client_id,
                )
                continue

            if msg_type == message_protocol.external.MsgType.TRANSACTIONS_BATCH:
                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange)
                logging.info(f"Received transactions batch from client {client_id}")
                transactions = _rows_to_transactions(
                    client_id, rows, transaction_columns
                )
                serialized_message = handler.serialize_rows_message(
                    client_id, transactions
                )
                
                if isinstance(output, ShardedExchangeProducer):
                    shard = random.randint(0, output_shards - 1)
                    output.send_to_shard(serialized_message, shard)
                else:
                    output.send(serialized_message)

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK,
                    client_id,
                )
                continue

            if msg_type == message_protocol.external.MsgType.END_TRANSACTIONS:
                if output is None:
                    output = _build_output_queue(mom_host, output_queue, output_exchange)
                logging.info(f"Received end transactions message from client {client_id}")
                msg_client_id = payload
                if client_id is None:
                    client_id = msg_client_id
                    client_sockets[client_id] = client_socket
                    client_ready[client_id] = False
                elif msg_client_id != client_id:
                    raise ValueError("Client id mismatch in end transactions")
                serialized_message = handler.serialize_eof_message(client_id)
                if isinstance(output, ShardedExchangeProducer):
                    output.send_eof_to_all(serialized_message)
                else:
                    output.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.ACK,
                    client_id,
                )
                # Avisamos al hilo de resultados que este cliente ya terminó de mandar todo
                client_ready[client_id] = True
                return # RETORNO EXITOSO: NO CERRAMOS EL SOCKET, queda vivo para result_handlers

            raise TypeError(f"Unexpected message type: {msg_type}")
    except Exception as e:
        logging.error(f"Handler error for client {client_id}: {e}")
        logging.error(traceback.format_exc())
        client_socket.close() # Si algo falló catastróficamente en la recepción, sí cerramos el socket
    finally:
        if output is not None:
            output.close()
        logging.info(f"Terminó la recepción de datos de entrada para el cliente {client_id}.")