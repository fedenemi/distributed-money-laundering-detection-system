import csv
import io
import logging
import os
import signal
import socket
import time
import traceback

from common import message_protocol

TRANSACTIONS_FILE = os.environ["TRANSACTIONS_FILE"]
ACCOUNTS_FILE = os.environ["ACCOUNTS_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

# normalizamos a string
CLIENT_ID = str(os.environ["CLIENT_ID"])

RESULTS_DIR = os.environ.get("RESULTS_DIR", "/results")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
PROGRESS_LOG_EVERY = int(os.environ.get("PROGRESS_LOG_EVERY", "500000"))
TRANSACTION_COLUMNS = [
    "Timestamp",
    "From Bank",
    "Account",
    "To Bank",
    "Account.1",
    "Amount Paid",
    "Payment Currency",
    "Payment Format",
]


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
                logging.info("Connected to gateway")
                return

            except socket.error as e:
                logging.warning(f"Gateway not ready, retrying... ({e})")
                self.server_socket.close()
                attempts += 1
                if attempts >= 10:
                    raise
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

    def _clear_previous_results(self, output_dir):
        self._close_writers()
        for filename in os.listdir(output_dir):
            if filename.startswith("results_q") and filename.endswith(".csv"):
                os.remove(os.path.join(output_dir, filename))

    # ACK con traceback    
    def _expect_ack(self):
        try:
            msg_type, payload = message_protocol.external.recv_msg(
                self.server_socket
            )

            if msg_type != message_protocol.external.MsgType.ACK:
                raise TypeError(f"Expected ACK, got {msg_type}")

            if payload != self.client_id:
                raise ValueError(
                    f"Client id mismatch in ACK "
                    f"(got={payload}, expected={self.client_id})"
                )

            logging.debug("Received ACK from gateway")

        except Exception as e:
            logging.error(f"ACK failure: {e}")
            logging.error(traceback.format_exc())
            raise

    def _send_csv_batch(self, msg_type, csv_buffer, row_count):
        logging.debug(f"Sending batch of {row_count} rows to gateway")
        message_protocol.external.send_client_csv_batch(
            self.server_socket,
            msg_type,
            self.client_id,
            csv_buffer.getvalue(),
        )
        self._expect_ack()

    def _send_csv_rows_in_batches(self, rows, msg_type, progress_label=None):
        csv_buffer = io.StringIO(newline="")
        csv_writer = csv.writer(csv_buffer, lineterminator="\n")
        batch_count = 0
        total_sent = 0
        next_progress_log = PROGRESS_LOG_EVERY
        for row in rows:
            csv_writer.writerow(row)
            batch_count += 1
            if batch_count >= self.batch_size:
                self._send_csv_batch(msg_type, csv_buffer, batch_count)
                total_sent += batch_count
                if progress_label and total_sent >= next_progress_log:
                    logging.info(f"Sent a total of {total_sent} {progress_label} rows to gateway")
                    next_progress_log += PROGRESS_LOG_EVERY
                csv_buffer = io.StringIO(newline="")
                csv_writer = csv.writer(csv_buffer, lineterminator="\n")
                batch_count = 0

        if batch_count:
            self._send_csv_batch(msg_type, csv_buffer, batch_count)
            total_sent += batch_count

        if progress_label:
            logging.info(f"Finished sending {total_sent} {progress_label} rows to gateway")

    def _transaction_rows(self, csv_reader):
        raw_headers = next(csv_reader, [])
        seen_headers = {}
        headers = []
        for header in raw_headers:
            count = seen_headers.get(header, 0)
            headers.append(header if count == 0 else f"{header}.{count}")
            seen_headers[header] = count + 1

        column_indexes = [headers.index(column) for column in TRANSACTION_COLUMNS]

        for row in csv_reader:
            if len(row) < len(headers):
                logging.warning("Transaction row has unexpected length %s", len(row))
                continue
            yield [row[index] for index in column_indexes]

    def send_accounts_and_transactions(self):
        logging.info("Sending accounts in batches")
        with open(self.accounts_file, newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            next(csv_reader, None)
            rows = ([row[0], row[1]] for row in csv_reader if len(row) >= 2)
            self._send_csv_rows_in_batches(
                rows,
                message_protocol.external.MsgType.ACCOUNTS_BATCH,
                "account",
            )

        logging.info("Finished sending accounts")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_ACCOUNTS,
            self.client_id,
        )
        self._expect_ack()

        logging.info("Sending transactions in batches")
        with open(self.transactions_file, newline="") as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
            self._send_csv_rows_in_batches(
                self._transaction_rows(csv_reader),
                message_protocol.external.MsgType.TRANSACTIONS_BATCH,
                "transaction",
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
        self._clear_previous_results(output_dir)

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
            start_time = time.time()
            logging.info(f"Envío datos desde el cliente")
            self.send_accounts_and_transactions()
            self.recv_query_results()
            logging.info(f"Resultados recibidos. Tiempo total: {time.time() - start_time}")
        finally:
            if not self.closed:
                self._close_writers()
                self.disconnect()
