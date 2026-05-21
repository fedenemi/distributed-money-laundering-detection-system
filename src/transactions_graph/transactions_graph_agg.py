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
EDGES_TAGS = ["i", "o"]

class TransactionsGraphAgg(WorkerBase):

    def __init__(self):
        # Create graph
        self.graph = DirectedGraph()

    # Process data message
    def process(self, transaction):
        logging.info("Batch de transacciones recibido")

        # Get origin account
        origin = transaction_id.TransactionID(
                    transaction[TRANSACTION_ORIGIN_BANK_KEY],
                    transaction[TRANSACTION_ORIGIN_ACC_KEY]
                    )
        
        # Get destination account
        destination = transaction_id.TransactionID(
                    transaction[TRANSACTION_DESTINATION_BANK_KEY],
                    transaction[TRANSACTION_DESTINATION_ACC_KEY]
                    )

        # Add nodes and edge
        self.graph.add_node(origin)
        self.graph.add_node(destination)
        self.graph.add_edge(origin, destination)
        
        logging.info("Batch de transacciones procesado")

    # Process EOF
    def on_eof(self, client_id=None):
        logging.info("EOF recibido")

        # For each node
        for origin in self.graph.get_nodes():
            # Get origin ID elements
            origin_bank, origin_acc = origin.as_tuple()

            # For each neighbour
            for destination in self.graph.get_neighbors(origin):
                # Get destination ID elements
                destination_bank, destination_acc = destination.as_tuple()

                # For each tag to send
                for tag in EDGES_TAGS:

                    # Generate new data edge
                    yield {
                        TRANSACTION_ORIGIN_BANK_KEY : origin_bank,
                        TRANSACTION_ORIGIN_ACC_KEY : origin_acc,
                        TRANSACTION_DESTINATION_BANK_KEY : destination_bank,
                        TRANSACTION_DESTINATION_ACC_KEY : destination_acc,
                        NEW_DATA_EDGE_TAG_KEY : tag
                        }

        logging.info("EOF procesado: datos enviados")

    def _routing_key(self, msg: dict) -> str:
        """Clave de particion del mensaje."""
        if msg[NEW_DATA_EDGE_TAG_KEY] == "i":
            routing_key = f"{msg[TRANSACTION_DESTINATION_BANK_KEY]}{msg[TRANSACTION_DESTINATION_ACC_KEY]}"
        else:
            routing_key = f"{msg[TRANSACTION_ORIGIN_BANK_KEY]}{msg[TRANSACTION_ORIGIN_ACC_KEY]}"
        return routing_key
