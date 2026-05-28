import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from common.middleware.worker_base import WorkerBase

# Constants
TRANSACTION_ORIGIN_BANK_KEY = "From Bank"
TRANSACTION_ORIGIN_ACC_KEY = "Account"
TRANSACTION_DESTINATION_BANK_KEY = "To Bank"
TRANSACTION_DESTINATION_ACC_KEY = "Account.1"
TRANSACTION_INTERMEDIATE_BANK_KEY = "Interm Bank"
TRANSACTION_INTERMEDIATE_ACC_KEY = "Interm Acc"

TOTAL_PATHS_KEY = "Total Paths"
MIN_TOTAL_PATHS = 5

class UniquePathsCounter(WorkerBase):

    def __init__(self):
        super().__init__()
        # Create storage for intermediate nodes
        self.intermediate_nodes_by_client_id = {}

    # Process data message
    def process(self, data):
        # Get paths counter
        logging.debug("Leo nuevo camino")
        client_id = data["client_id"]
        intermediate_nodes = self.intermediate_nodes_by_client_id.get(client_id)
        if intermediate_nodes is None:
            intermediate_nodes = {}
            self.intermediate_nodes_by_client_id[client_id] = intermediate_nodes

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
        intermediate_nodes[(start_node, end_node)] = intermediate_accs_set

        return []


    # Process EOF
    def on_eof(self, client_id=None):
        logging.info(f"EOF received for client_id={client_id}")
        intermediate_nodes = self.intermediate_nodes_by_client_id.pop(client_id, {})

        # For each node with incoming edges
        matching_accounts = set()
        for (start_node, end_node) in intermediate_nodes:
            if start_node == end_node:
                continue

            if len(intermediate_nodes[(start_node, end_node)]) <= MIN_TOTAL_PATHS:
                continue

            matching_accounts.add(start_node)
            matching_accounts.add(end_node)

        for account_node in sorted(matching_accounts, key=lambda node: node.as_tuple()):
            # Get start node ID elements
            bank, account = account_node.as_tuple()

            yield {
                "client_id" : client_id,
                "Bank" : bank,
                "Account" : account,
                }
            
        logging.info("EOF procesado: datos enviados")
