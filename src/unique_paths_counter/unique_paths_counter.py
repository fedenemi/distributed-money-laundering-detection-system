import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from common.middleware.worker_base import WorkerBase

# Environment variables
OUTPUT_BATCH_SIZE = os.environ["OUTPUT_BATCH_SIZE"]

# Constants
TRANSACTION_ORIGIN_BANK_KEY = "From Bank"
TRANSACTION_ORIGIN_ACC_KEY = "Account"
TRANSACTION_DESTINATION_BANK_KEY = "To Bank"
TRANSACTION_DESTINATION_ACC_KEY = "Account.1"
TRANSACTION_INTERMEDIATE_BANK_KEY = "Interm Bank"
TRANSACTION_INTERMEDIATE_ACC_KEY = "Interm Acc"

TOTAL_PATHS_KEY = "Total Paths"

class UniquePathsCounter(WorkerBase):

    def __init__(self):
        super().__init__()
        # Create storage for intermediate nodes
        self.intermediate_nodes_by_client_id = {}

    # Process data message
    def process(self, data):
        # Get paths counter
        logging.info("Leo nuevo camino")
        client_id = data["client_id"]
        intermediate_nodes = self.edges_by_client_id.get(client_id, {})

        # Path start
        start_node = transaction_id.TransactionID(
                        data[TRANSACTION_ORIGIN_BANK_KEY],
                        data[TRANSACTION_ORIGIN_ACC_KEY])

        # Path intermediate node
        intermediate_node = transaction_id.TransactionID(
                            data[TRANSACTION_INTERMEDIATE_BANK_KEY],
                            data[TRANSACTION_INTERMEDIATE_ACC_KEY])

        # Path end
        end_node = transaction_id.TransactionID(
                        data[TRANSACTION_DESTINATION_BANK_KEY],
                        data[TRANSACTION_DESTINATION_ACC_KEY])

        # Add intermediate node
        intermediate_accs_set = intermediate_nodes.get((start_node, end_node), set())
        intermediate_accs_set.add(intermediate_node)

        return []


    # Process EOF
    def on_eof(self, client_id=None):
        logging.info("EOF recibido")
        intermediate_nodes = self.edges_by_client_id.get(client_id, {})

        # For each node with incoming edges
        for (start_node, end_node) in intermediate_nodes:
            # Get start node ID elements
            start_bank, start_acc = start_node.as_tuple()

            # Get end node ID elements
            end_bank, end_acc = end_node.as_tuple()

            # Get total of unique paths
            yield {
                TRANSACTION_ORIGIN_BANK_KEY : start_bank,
                TRANSACTION_ORIGIN_ACC_KEY : start_acc,
                TRANSACTION_DESTINATION_BANK_KEY : end_bank,
                TRANSACTION_DESTINATION_ACC_KEY : end_acc,
                TOTAL_PATHS_KEY : len(intermediate_nodes[(start_node, end_node)]),
                }
            
        logging.info("EOF procesado: datos enviados")
