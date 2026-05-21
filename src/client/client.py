import csv
import logging
import os
import signal
import socket
import time

from common import message_protocol

TRANSACTIONS_FILE = os.environ["TRANSACTIONS_FILE"]
ACCOUNTS_FILE = os.environ["ACCOUNTS_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
CLIENT_ID = os.environ["CLIENT_ID"]
RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))


class Client:

    def __init__(self):
        self.transactions_file = TRANSACTIONS_FILE
        self.accounts_file = ACCOUNTS_FILE
        self.server_host = SERVER_HOST
        self.server_port = SERVER_PORT
        self.client_id = CLIENT_ID
        self.results_dir = RESULTS_DIR
        self.batch_size = BATCH_SIZE
        self.closed = False
        self.server_socket = None
        self._writers = {}
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recieved SIGTERM signal")
        self.closed = True
        self._close_writers()
        self.disconnect()

        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self):
        attempts = 0
        while True:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.server_socket.connect((self.server_host, self.server_port))
                return
            except socket.error:
                self.server_socket.close()
                attempts += 1
                if attempts >= 10:
                    raise
                logging.warning("Gateway not ready, retrying...")
                time.sleep(1)

    def disconnect(self):
        if not self.server_socket:
            return

        try:
            self.server_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        finally:
            self.server_socket.close()
            self.server_socket = None

    def _close_writers(self):
        for csvfile, _ in self._writers.values():
            csvfile.close()
        self._writers = {}

    def _expect_ack(self):
        msg_type, payload = message_protocol.external.recv_msg(self.server_socket)
        if msg_type != message_protocol.external.MsgType.ACK:
            raise TypeError(f"Expected ACK, got {msg_type}")
        if payload != self.client_id:
            raise ValueError("Client id mismatch in ACK")
        
        logging.info("Received ACK from gateway")

    def _send_rows_in_batches(self, rows, msg_type):
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= self.batch_size:
                logging.info(f"Sending batch of {len(batch)} rows to gateway")
                message_protocol.external.send_msg(
                    self.server_socket, msg_type, self.client_id, batch
                )
                self._expect_ack()
                batch = []

        if batch:
            logging.info(f"Sending final batch of {len(batch)} rows to gateway")
            message_protocol.external.send_msg(
                self.server_socket, msg_type, self.client_id, batch
            )
            self._expect_ack()

    def send_accounts_and_transactions(self):
        logging.info("Sending accounts in batches")
        with open(self.accounts_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            rows = ([row[0], row[1]] for row in csv_reader if len(row) >= 2)
            self._send_rows_in_batches(
                rows, message_protocol.external.MsgType.ACCOUNTS_BATCH
            )

        logging.info("Finished sending accounts")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_ACCOUNTS,
            self.client_id,
        )
        self._expect_ack()

        logging.info("Sending transactions in batches")
        with open(self.transactions_file, newline="\n") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            rows = (row[:-1] for row in csv_reader if len(row) >= 2)
            self._send_rows_in_batches(
                rows, message_protocol.external.MsgType.TRANSACTIONS_BATCH
            )

        logging.info("Finished sending transactions")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_TRANSACTIONS,
            self.client_id,
        )
        self._expect_ack()

    def recv_query_results(self):
        logging.info("Receiving query results")
        output_dir = os.path.join(self.results_dir, f"client_{self.client_id}")
        os.makedirs(output_dir, exist_ok=True)

        while True:
            msg_type, payload = message_protocol.external.recv_msg(self.server_socket)

            if msg_type == message_protocol.external.MsgType.QUERY_RESULT_BATCH:
                msg_client_id, query_id, rows = payload
                if msg_client_id != self.client_id:
                    raise ValueError("Client id mismatch in query result batch")

                logging.info(f"Received batch of {len(rows)} rows for query {query_id} from gateway")

                if query_id not in self._writers:
                    file_path = os.path.join(
                        output_dir, f"results_q{query_id}.csv"
                    )
                    csvfile = open(file_path, "w", newline="")
                    self._writers[query_id] = (csvfile, csv.writer(csvfile))

                _, csv_writer = self._writers[query_id]
                csv_writer.writerows(rows)
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ACK,
                    self.client_id,
                )
                continue

            if msg_type == message_protocol.external.MsgType.END_QUERY:
                msg_client_id, query_id = payload
                if msg_client_id != self.client_id:
                    raise ValueError("Client id mismatch in end query")
                
                logging.info(f"Received end of results for query {query_id} from gateway")
                
                if query_id in self._writers:
                    csvfile, _ = self._writers.pop(query_id)
                    csvfile.close()
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ACK,
                    self.client_id,
                )
                continue

            if msg_type == message_protocol.external.MsgType.END_RESULTS:
                if payload != self.client_id:
                    raise ValueError("Client id mismatch in end results")
                
                logging.info(f"Received end of all results from gateway for client {self.client_id}")
                
                message_protocol.external.send_msg(
                    self.server_socket,
                    message_protocol.external.MsgType.ACK,
                    self.client_id,
                )
                break

            raise TypeError(f"Unexpected message type: {msg_type}")

        self._close_writers()

    def run(self):
        try:
            self.connect()
            self.send_accounts_and_transactions()
            self.recv_query_results()
        finally:
            if not self.closed:
                self._close_writers()
                self.disconnect()
