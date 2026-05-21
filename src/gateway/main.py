import logging
import multiprocessing
import os
import signal
import socket

from message_handlers import client_handlers, result_handlers
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ.get("OUTPUT_QUEUE", "")
OUTPUT_EXCHANGE = os.environ.get("OUTPUT_EXCHANGE", "")
INPUT_QUEUE_PREFIX = os.environ["INPUT_QUEUE_PREFIX"]
TOTAL_QUERIES = int(os.environ["TOTAL_QUERIES"])

TRANSACTION_COLUMNS = [
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account.1",
    "Amount Received",
    "Receiving Currency",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
]



def _get_result_queues():
    return [f"{INPUT_QUEUE_PREFIX}_{i}" for i in range(1, TOTAL_QUERIES + 1)]


def handle_sigterm(server_socket, client_sockets, sigterm_received):
    server_socket.shutdown(socket.SHUT_RDWR)
    for client_socket in client_sockets.values():
        client_socket.shutdown(socket.SHUT_RDWR)
    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    result_queues = _get_result_queues()
    if not result_queues:
        raise RuntimeError("No result queues configured")

    with multiprocessing.Manager() as manager:
        client_sockets = manager.dict()
        bank_maps = manager.dict()
        client_query_eofs = manager.dict()
        client_ready = manager.dict()
        send_lock = manager.Lock()
        sigterm_received = manager.Value("c_short", 0)

        with multiprocessing.Pool(processes=os.cpu_count()) as processes_pool:
            for index, queue_name in enumerate(result_queues, start=1):
                processes_pool.apply_async(
                    result_handlers.handle_client_response,
                    (
                        queue_name,
                        index,
                        MOM_HOST,
                        client_sockets,
                        bank_maps,
                        client_query_eofs,
                        client_ready,
                        TOTAL_QUERIES,
                        send_lock,
                    ),
                )

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_sockets, sigterm_received
                    ),
                )
                while True:
                    try:
                        client_socket, _ = server_socket.accept()
                        logging.info("A new client has connected")
                        processes_pool.apply_async(
                            client_handlers.handle_client_request,
                            (
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
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        return 0
                    except Exception as e:
                        logging.error(e)
                        return 2
    return 0


if __name__ == "__main__":
    main()