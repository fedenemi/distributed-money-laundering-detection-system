import logging
import socket
import threading
import os

from message_handlers import client_handlers, result_handlers


def main():
    logging.basicConfig(level=logging.INFO)

    # Buscamos ambas variables por si tu compose usa SERVER_PORT en lugar de PORT
    PORT = int(os.environ.get("SERVER_PORT", os.environ.get("PORT", 12345)))
    MOM_HOST = os.environ.get("MOM_HOST", "rabbitmq")
    OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "")
    OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "")

    # FIX: limpiamos espacios y evitamos lista [""] si no existe la variable
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
    client_ready = {}
    client_query_eofs = {}
    send_lock = threading.Lock()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR evita bloqueos del puerto al reiniciar Docker
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server_socket.bind(("0.0.0.0", PORT))
    server_socket.listen(5)

    logging.info(f"Listening to connections on port {PORT}")

    # Lanzamos handlers de resultados
    for index, queue_name in enumerate(result_queues, start=1):
        n_upstream = int(os.environ.get(f"QUERY_{index}_N_UPSTREAM", "1"))
        t = threading.Thread(
            target=result_handlers.handle_client_response,
            kwargs={
                "queue_name": queue_name,
                "client_sockets": client_sockets,
                "bank_maps": bank_maps,
                "client_query_eofs": client_query_eofs,
                "client_ready": client_ready,
                "mom_host": MOM_HOST,
                "query_id": index,
                "total_queries": TOTAL_QUERIES,
                "send_lock": send_lock,
                "n_upstream": n_upstream
            }
        )

        t.daemon = True
        t.start()

        logging.info(f"Started result handler for queue: {queue_name}")

    # Bucle principal de clientes
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
                    client_ready,
                    MOM_HOST,
                    OUTPUT_QUEUE,
                    OUTPUT_EXCHANGE,
                    TRANSACTION_COLUMNS,
                ),
            )

            t.daemon = True
            t.start()

        except Exception as e:
            logging.exception(f"Accept error: {e}")


if __name__ == "__main__":
    main()