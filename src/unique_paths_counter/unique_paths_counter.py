import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from common.middleware.worker_base import WorkerBase

# Environment variables
OUTPUT_BATCH_SIZE = os.environ["OUTPUT_BATCH_SIZE"]

# Constants
START_ACC_DATA_POS = 0
INTERMEDIATE_ACC_DATA_POS = 1
END_ACC_DATA_POS = 2

TRANSACTION_BANK_POS = 0
TRANSACTION_ACC_POS = 1

class UniquePathsCounter(WorkerBase):

    def __init__(self):
        # Create storage for intermediate nodes
        self.intermediate_nodes = {}

    # Process data message
    def _process_data_batch(self, transactions_batch):
        logging.info("Batch de datos recibido")
        # For each transaction
        for transaction in transactions_batch:
            # Path start
            start_acc_data = transaction[START_ACC_DATA_POS]
            start_node = transaction_id.TransactionID(
                            start_acc_data[TRANSACTION_BANK_POS],
                            start_acc_data[TRANSACTION_ACC_POS])

            # Path intermediate node
            intermediate_acc_data = transaction[INTERMEDIATE_ACC_DATA_POS]
            intermediate_node = transaction_id.TransactionID(
                                intermediate_acc_data[TRANSACTION_BANK_POS],
                                intermediate_acc_data[TRANSACTION_ACC_POS])

            # Path end
            end_acc_data = transaction[END_ACC_DATA_POS]
            end_node = transaction_id.TransactionID(
                            end_acc_data[TRANSACTION_BANK_POS],
                            end_acc_data[TRANSACTION_ACC_POS])

            # Add intermediate node
            intermediate_accs_set = self.intermediate_nodes.get((start_node, end_node), set())
            intermediate_accs_set.add(intermediate_node)

        logging.info("Batch de datos procesado")

    # Serialize and send output batch
    def _send_output_batch(self, transactions_batch):
        message = message_protocol.internal.serialize(transactions_batch)
        transactions_batch.clear()
        self.output_queue.send(message)

    # Process EOF
    def _process_eof(self):
        logging.info("EOF recibido")

        # For each node with incoming edges
        batch_data = []
        for (start_node, end_node) in self.intermediate_nodes:
            # Get total of unique paths
            total_unique_paths = len(self.intermediate_nodes[(start_node, end_node)])
            batch_data.append((start_node, end_node, total_unique_paths))

            # Check if total batch length is reached
            if len(batch_data) == OUTPUT_BATCH_SIZE:
                self._send_output_batch(batch_data)

        if len(batch_data) > 0:
            self._send_output_batch(batch_data)

        logging.info("EOF procesado: datos enviados")

    # Process message that arrived
    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)

        if len(fields) > 1:
            self._process_data_batch(fields)
            ack()
        elif len(fields) == 1:
            self._process_eof(*fields)
            ack()
        else:
            nack()
