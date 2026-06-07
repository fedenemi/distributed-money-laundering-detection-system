import logging
import socket
import time

from message_handlers import message_handler
from common import middleware, message_protocol

ACCOUNT_BANK_NAME_COL = "Bank Name"


def _normalize_bank_id(bank_id):
    if bank_id is None:
        return None
    normalized = str(bank_id).strip()
    normalized = normalized.lstrip("0")
    return normalized or "0"


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


def _normalize_result_rows(rows, query_id, client_id):
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        # Mapeo de columnas según la query
        if query_id == 1:
            # Q1: from_bank, from_account, to_bank, to_account, amount
            mapped = {
                "from_bank": row.get("From Bank", ""),
                "from_account": row.get("Account", ""),
                "to_bank": row.get("To Bank", ""),
                "to_account": row.get("Account.1", ""),
                "amount": row.get("Amount Paid", 0),
            }
            
        elif query_id == 2:
            # Q2: bank_name, from_account, amount
            mapped = {
                "bank_name": row.get("Bank Name", ""),
                "from_account": row.get("Account", ""),
                "amount": row.get("Amount Paid", 0),
            }
        
        elif query_id == 3:
            # Q3: from_bank, from_account, payment_format, amount            
            mapped = {
                "from_bank": row.get("From Bank", ""),
                "from_account": row.get("Account", ""),
                "payment_format": row.get("Payment Format", ""),
                "amount": row.get("Amount Paid", 0),
            }

        elif query_id == 4:
            # Q4: cuentas endpoint que cumplen scatter-gather
            mapped = {
                "bank": row.get("Bank", ""),
                "account": row.get("Account", ""),
            }

        elif query_id == 5:
            # Q5: count o sum_count
            count = row.get("sum_count", row.get("count", 0))
            mapped = {
                "count": int(float(count)),
            }

        else:
            mapped = {k: v for k, v in row.items() if k != "client_id"}

        row_values = [str(mapped[key]) for key in mapped]
        normalized.append(row_values)

    return normalized

def _handle_query_eof(
    client_id,
    query_id,
    client_outboxes,
    client_query_eofs,
    total_queries,
):
    outbox = client_outboxes.get(client_id)
    if not outbox:
        return

    outbox.put(("END_QUERY", query_id))

    done = list(client_query_eofs.get(client_id, []))
    if query_id not in done:
        done.append(query_id)
        client_query_eofs[client_id] = done

    if len(done) >= total_queries:
        outbox.put(("END_RESULTS", None))


def handle_client_response(
    queue_name,
    query_id,
    mom_host,
    client_sockets,
    client_query_eofs,
    client_outboxes,
    total_queries,
    n_upstream,
    client_semaphores,
    checkpoint_barriers,
    checkpoint_lock
):
    logging.basicConfig(level=logging.INFO)
    eof_count = {}
    checkpoint_counts = {} 
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, queue_name)
    handler = message_handler.MessageHandler()

    def _consume_result(message, ack, nack):
        try:
            payload = handler.deserialize_system_message(message)

            if isinstance(payload, dict) and payload.get("type") == "checkpoint":
                client_id = payload.get("client_id")
                chk_id = payload.get("checkpoint_id")
                local_chk_key = (client_id, chk_id)
                checkpoint_counts[local_chk_key] = checkpoint_counts.get(local_chk_key, 0) + 1
                if checkpoint_counts[local_chk_key] >= n_upstream:
                    with checkpoint_lock:
                        barrier_key = (client_id, chk_id)
                        
                        if barrier_key not in checkpoint_barriers:
                            checkpoint_barriers[barrier_key] = set()
                            
                        checkpoint_barriers[barrier_key].add(query_id)

                        if len(checkpoint_barriers[barrier_key]) == total_queries:
                            if client_id in client_semaphores:
                                client_semaphores[client_id].release()
                            
                            del checkpoint_barriers[barrier_key]
                    del checkpoint_counts[local_chk_key]

                ack()
                return

            if isinstance(payload, dict) and payload.get("type") == "eof":
                rows = []
                client_id = _extract_client_id(payload, rows, client_sockets)
                
                if client_id not in client_outboxes:
                    ack()
                    return

                eof_count[client_id] = eof_count.get(client_id, 0) + 1
                logging.info(f"Received EOF for query {query_id} from client {client_id} ({eof_count[client_id]}/{n_upstream})")

                if eof_count[client_id] >= n_upstream:
                    _handle_query_eof(
                        client_id,
                        query_id,
                        client_outboxes,
                        client_query_eofs,
                        total_queries,
                    )
                ack()
                return

            if isinstance(payload, dict):
                rows = payload.get("rows", [])
                client_id = _extract_client_id(payload, rows, client_sockets)
            elif isinstance(payload, list):
                if len(payload) == 1:
                    client_id = payload[0]
                    if client_id in client_outboxes:
                        _handle_query_eof(client_id, query_id, client_outboxes, client_query_eofs, total_queries)
                    ack()
                    return

                client_id = payload[0] if payload else None
                rows = payload[1] if len(payload) > 1 else []
            else:
                raise TypeError("Unsupported result payload")

            if client_id not in client_outboxes:
                ack()
                return

            done_queries = client_query_eofs.get(client_id, [])
            if query_id in done_queries:
                ack()
                return

            normalized_rows = _normalize_result_rows(rows, query_id, client_id)

            if normalized_rows:
                client_outboxes[client_id].put(("ROWS", (query_id, normalized_rows)))
                
            ack()

        except Exception as e:
            logging.error(f"Error en _consume_result: {e}")
            nack()

    input_queue.start_consuming(_consume_result)
    input_queue.close()