import logging
import socket
import time

from message_handlers import message_handler
from common import middleware, message_protocol

ACCOUNT_BANK_NAME_COL = "Bank Name"


def _expect_client_ack(client_socket, client_id):
    msg_type, payload = message_protocol.external.recv_msg(client_socket)
    if msg_type != message_protocol.external.MsgType.ACK:
        raise TypeError(f"Expected ACK, got {msg_type}")
    if payload != client_id:
        raise ValueError("Client id mismatch in ACK")


def _extract_client_id(message, rows, client_sockets):
    client_id = None
    if isinstance(message, dict):
        client_id = message.get("client_id")
    if client_id is None and rows:
        first_row = rows[0]
        if isinstance(first_row, dict):
            client_id = first_row.get("client_id")

    if client_id is None:
        if len(client_sockets) == 1:
            client_id = next(iter(client_sockets.keys()))
    return client_id


def _normalize_result_rows(rows, query_id, bank_maps, client_id):
    normalized = []
    for row in rows:
        if isinstance(row, dict) and query_id == 2:
            bank_id = row.get("From Bank")
            bank_map = bank_maps.get(client_id, {})
            row = dict(row)
            row[ACCOUNT_BANK_NAME_COL] = bank_map.get(bank_id, "")

        if isinstance(row, dict) and "client_id" in row:
            row = dict(row)
            row.pop("client_id", None)

        if isinstance(row, dict) and "client_id" in row:
            if row["client_id"] != client_id:
                logging.warning(
                    f"Row client_id {row['client_id']} does not match expected client_id {client_id}"
                )

        if isinstance(row, dict):
            row_values = list(row.values())
        else:
            row_values = list(row)
        normalized.append([str(value) for value in row_values])
    return normalized


def _handle_query_eof(
    client_id,
    query_id,
    client_sockets,
    client_query_eofs,
    total_queries,
    send_lock,
):
    with send_lock:
        client_socket = client_sockets.get(client_id)
        if client_socket:
            message_protocol.external.send_msg(
                client_socket,
                message_protocol.external.MsgType.END_QUERY,
                client_id,
                query_id,
            )
            _expect_client_ack(client_socket, client_id)

    done = list(client_query_eofs.get(client_id, []))
    if query_id not in done:
        done.append(query_id)
        client_query_eofs[client_id] = done

    if len(done) >= total_queries:
        with send_lock:
            client_socket = client_sockets.get(client_id)
            if client_socket:
                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.END_RESULTS,
                    client_id,
                )
                _expect_client_ack(client_socket, client_id)


def handle_client_response(
    queue_name,
    query_id,
    mom_host,
    client_sockets,
    bank_maps,
    client_query_eofs,
    client_ready,
    total_queries,
    send_lock,
):
    logging.basicConfig(level=logging.INFO)
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, queue_name)
    handler = message_handler.MessageHandler()

    def _consume_result(message, ack, nack):
        try:
            payload = handler.deserialize_system_message(message)

            if isinstance(payload, dict) and payload.get("type") == "eof":
                rows = []
                client_id = _extract_client_id(payload, rows, client_sockets)
                if not client_id:
                    raise ValueError("Missing client_id in EOF message")
                if not client_ready.get(client_id, False):
                    time.sleep(0.2)
                    nack()
                    return
                logging.info(f"Received EOF for query {query_id} from client {client_id}")
                _handle_query_eof(
                    client_id,
                    query_id,
                    client_sockets,
                    client_query_eofs,
                    total_queries,
                    send_lock,
                )
                ack()
                return

            if isinstance(payload, dict):
                rows = payload.get("rows", [])
                client_id = _extract_client_id(payload, rows, client_sockets)
            elif isinstance(payload, list):
                if len(payload) == 1:
                    client_id = payload[0]
                    _handle_query_eof(
                        client_id,
                        query_id,
                        client_sockets,
                        client_query_eofs,
                        total_queries,
                        send_lock,
                    )
                    ack()
                    return

                client_id = payload[0] if payload else None
                rows = payload[1] if len(payload) > 1 else []
            else:
                raise TypeError("Unsupported result payload")

            if not client_id:
                raise ValueError("Missing client_id in result payload")

            done_queries = client_query_eofs.get(client_id, [])
            if query_id in done_queries:
                logging.info(f"Ignorando mensaje residual de query {query_id} (el cliente ya terminó)")
                ack()
                return

            if not client_ready.get(client_id, False):
                time.sleep(0.2)
                nack()
                return

            normalized_rows = _normalize_result_rows(
                rows, query_id, bank_maps, client_id
            )

            if normalized_rows:
                with send_lock:
                    client_socket = client_sockets.get(client_id)
                    logging.info(f"Sending {len(normalized_rows)} rows for query {query_id} to client {client_id}")
                    if client_socket:
                        message_protocol.external.send_msg(
                            client_socket,
                            message_protocol.external.MsgType.QUERY_RESULT_BATCH,
                            client_id,
                            query_id,
                            normalized_rows,
                        )
                        _expect_client_ack(client_socket, client_id)
            ack()
        except socket.error:
            logging.error("The connection with the client was lost")
            ack()
        except Exception as e:
            logging.error(e)
            nack()

    input_queue.start_consuming(_consume_result)
    input_queue.close()
