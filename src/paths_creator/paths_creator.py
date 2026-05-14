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
EDGE_TAG_KEY = "Edge Type"

TRANSACTION_INTERMEDIATE_BANK_KEY = "Interm Bank"
TRANSACTION_INTERMEDIATE_ACC_KEY = "Interm Acc"

class PathsCreator(WorkerBase):

    def __init__(self):
        super().__init__()
        # Create storage for edges of nodes
        self.incoming_edges = {}
        self.outgoing_edges = {}

    # Process data message
    def process(self, transaction):
        logging.info("Batch de datos recibido")

        # Transaction origin
        origin = transaction_id.TransactionID(
                            transaction[TRANSACTION_ORIGIN_BANK_KEY],
                            transaction[TRANSACTION_ORIGIN_ACC_KEY])

        # Transaction destination
        destination = transaction_id.TransactionID(
                            transaction[TRANSACTION_DESTINATION_BANK_KEY],
                            transaction[TRANSACTION_DESTINATION_ACC_KEY])

        # Get tag of edge
        tag = transaction[EDGE_TAG_KEY]

        # Store according if it is an "incoming" edge, where the destination node is stored here,
        # or if it is an "outgoing" edge, where the origin node is stored here
        if tag == "i":
            if destination not in self.incoming_edges:
                self.incoming_edges[destination] = set()
            self.incoming_edges[destination].add(origin)
        else:
            if origin not in self.outgoing_edges:
                self.outgoing_edges[origin] = set()
            self.outgoing_edges[origin].add(destination)

        logging.info("Batch de datos procesado")

    # Process EOF
    def on_eof(self, client_id=None):
        logging.info("EOF recibido")

        # For each node with incoming edges
        for node in self.incoming_edges:
            # Check if there are outgoing edges
            if node in self.outgoing_edges:
                # Get intermediate node ID elements
                interm_bank, interm_acc = node.as_tuple()

                # Get neighbours
                incoming_edges_neighbours = self.incoming_edges[node]
                outgoing_edges_neighbours = self.outgoing_edges[node]

                # Create paths
                for inc_neighbour in incoming_edges_neighbours:
                    # Get node ID elements
                    inc_bank, inc_acc = inc_neighbour.as_tuple()
                    for out_neighbour in outgoing_edges_neighbours:
                        # Get node ID elements
                        out_bank, out_acc = out_neighbour.as_tuple()

                        # Generate new data row
                        yield {
                            TRANSACTION_ORIGIN_BANK_KEY : inc_bank,
                            TRANSACTION_ORIGIN_ACC_KEY : inc_acc,
                            TRANSACTION_INTERMEDIATE_BANK_KEY : interm_bank,
                            TRANSACTION_INTERMEDIATE_ACC_KEY : interm_acc,
                            TRANSACTION_DESTINATION_BANK_KEY : out_bank,
                            TRANSACTION_DESTINATION_ACC_KEY : out_acc,
                        }

        logging.info("EOF procesado: datos enviados")
