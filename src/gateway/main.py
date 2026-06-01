import logging
import socket
import threading
import os

from message_handlers import client_handlers, result_handlers


def main():
    logging.basicConfig(level=logging.INFO)

    SERVER_HOST = os.environ.get("SERVER_HOST")
    PORT = int(os.environ.get("SERVER_PORT"))
    MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
    OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "")
    OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "")

    transaction_columns_raw = os.environ.get("TRANSACTION_COLUMNS", "")
    TRANSACTION_COLUMNS = [
        col.strip()
        for col in transaction_columns_raw.split(",")
        if col.strip()
    ]

    TOTAL_QUERIES = int(os.environ.get("TOTAL_QUERIES", "1"))
    INPUT_QUERY_QUEUE_PREFIX = os.environ.get(
        "INPUT_QUEUE_PREFIX",
        "results"
    )

    result_queues = [
        f"{INPUT_QUERY_QUEUE_PREFIX}_{i}"
        for i in range(1, TOTAL_QUERIES + 1)
    ]

    logging.info(f"TRANSACTION_COLUMNS loaded: {TRANSACTION_COLUMNS}")

    # Diccionarios estándar y el Lock
    client_sockets = {}
    bank_maps = {}
    client_query_eofs = {}
    client_ready_events = {} 
    client_outboxes = {}
    client_checkpoints = {}
    client_semaphores = {}
    checkpoint_barriers = {}
    checkpoint_lock = threading.Lock()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((SERVER_HOST, PORT))
    server_socket.listen()

    logging.info(f"Listening to connections on port {PORT}")

    # Results handler
    for index, queue_name in enumerate(result_queues, start=1):
        n_upstream = int(os.environ.get(f"QUERY_{index}_N_UPSTREAM", "1"))
        t = threading.Thread(
            target=result_handlers.handle_client_response,
            kwargs={
                "queue_name": queue_name,
                "client_sockets": client_sockets,
                "bank_maps": bank_maps,
                "client_query_eofs": client_query_eofs,
                "client_outboxes": client_outboxes,
                "mom_host": MOM_HOST,
                "query_id": index,
                "total_queries": TOTAL_QUERIES,
                "n_upstream": n_upstream,
                "client_semaphores": client_semaphores,
                "checkpoint_barriers": checkpoint_barriers,
                "checkpoint_lock": checkpoint_lock,
            }
        )
        t.daemon = True
        t.start()

        logging.info(f"Started result handler for queue: {queue_name}")

    # Main clients loop
    while True:
        try:
            client_socket, addr = server_socket.accept()
            logging.info(f"A new client has connected from {addr}")

            t = threading.Thread(
                target=client_handlers.handle_client_request,
                args=(
                    client_socket,
                    client_sockets,
                    bank_maps,
                    client_ready_events,
                    client_outboxes,
                    MOM_HOST,
                    OUTPUT_QUEUE,
                    OUTPUT_EXCHANGE,
                    TRANSACTION_COLUMNS,
                    client_checkpoints,
                    client_semaphores,
                    checkpoint_barriers,
                    checkpoint_lock,
                ),
            )

            t.daemon = True
            t.start()

        except Exception as e:
            logging.exception(f"Accept error: {e}")


if __name__ == "__main__":
    main()