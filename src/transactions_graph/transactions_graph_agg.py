import os
import logging
import signal
import time

from common import middleware, message_protocol, transaction_id
from graph.graph_class import DirectedGraph
from common.middleware.worker_base import WorkerBase

# Constants
TRANSACTION_ORIGIN_BANK_KEY = "From Bank"
TRANSACTION_ORIGIN_ACC_KEY = "Account"
TRANSACTION_DESTINATION_BANK_KEY = "To Bank"
TRANSACTION_DESTINATION_ACC_KEY = "Account.1"

NEW_DATA_EDGE_TAG_KEY = "Edge Type"
EDGES_INPUT_TAG = "i"
EDGES_OUTPUT_TAG = "o"

class TransactionsGraphAgg(WorkerBase):

    def __init__(self):
        super().__init__()
        # Create graph
        self.graph_by_client_id = {}

    # Process data message
    def process(self, data):
        # Get client ID
        client_id = data["client_id"]
        client_graph : DirectedGraph = self.graph_by_client_id.get(client_id, DirectedGraph())

        # Get origin account
        origin = transaction_id.TransactionID(
                    data[TRANSACTION_ORIGIN_BANK_KEY],
                    data[TRANSACTION_ORIGIN_ACC_KEY]
                    )
        
        # Get destination account
        destination = transaction_id.TransactionID(
                    data[TRANSACTION_DESTINATION_BANK_KEY],
                    data[TRANSACTION_DESTINATION_ACC_KEY]
                    )

        # Check if edge already exists
        if client_graph.are_connected(origin, destination):
            # Add nodes and edge
            client_graph.add_node(origin)
            client_graph.add_node(destination)
            client_graph.add_edge(origin, destination)

            edge_as_input = {
                TRANSACTION_ORIGIN_BANK_KEY : data[TRANSACTION_ORIGIN_BANK_KEY],
                TRANSACTION_ORIGIN_ACC_KEY : data[TRANSACTION_ORIGIN_ACC_KEY],
                TRANSACTION_DESTINATION_BANK_KEY : data[TRANSACTION_DESTINATION_BANK_KEY],
                TRANSACTION_DESTINATION_ACC_KEY : data[TRANSACTION_DESTINATION_ACC_KEY],
                NEW_DATA_EDGE_TAG_KEY : EDGES_INPUT_TAG,
            }
            edge_as_output = {
                TRANSACTION_ORIGIN_BANK_KEY : data[TRANSACTION_ORIGIN_BANK_KEY],
                TRANSACTION_ORIGIN_ACC_KEY : data[TRANSACTION_ORIGIN_ACC_KEY],
                TRANSACTION_DESTINATION_BANK_KEY : data[TRANSACTION_DESTINATION_BANK_KEY],
                TRANSACTION_DESTINATION_ACC_KEY : data[TRANSACTION_DESTINATION_ACC_KEY],
                NEW_DATA_EDGE_TAG_KEY : EDGES_OUTPUT_TAG,
            }

            return [edge_as_input, edge_as_output]

        return []

    # Process EOF
    def on_eof(self, client_id=None):
        return []

    def _routing_key(self, msg: dict) -> str:
        """Clave de particion del mensaje."""
        if msg[NEW_DATA_EDGE_TAG_KEY] == "i":
            routing_key = f"{msg[TRANSACTION_DESTINATION_BANK_KEY]}{msg[TRANSACTION_DESTINATION_ACC_KEY]}"
        else:
            routing_key = f"{msg[TRANSACTION_ORIGIN_BANK_KEY]}{msg[TRANSACTION_ORIGIN_ACC_KEY]}"
        return routing_key
