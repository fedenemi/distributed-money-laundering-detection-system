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
        # Create storage for edges of nodes by client ID
        self.edges_by_client_id = {}

    # Process data message
    def process(self, data):
        logging.info("Arista recibida")
        # Get edge's dictionaries of according to client ID
        client_id = data["client_id"]
        edges_pair = self.edges_by_client_id.get(client_id, ({}, {}))
        incoming_edges = edges_pair[0]
        outgoing_edges = edges_pair[1]

        # Transaction origin
        origin = transaction_id.TransactionID(
                            data[TRANSACTION_ORIGIN_BANK_KEY],
                            data[TRANSACTION_ORIGIN_ACC_KEY])

        # Transaction destination
        destination = transaction_id.TransactionID(
                            data[TRANSACTION_DESTINATION_BANK_KEY],
                            data[TRANSACTION_DESTINATION_ACC_KEY])

        # Get tag of edge
        tag = data[EDGE_TAG_KEY]

        # Store according if it is an "incoming" edge, where the destination node is stored here,
        # or if it is an "outgoing" edge, where the origin node is stored here
        if tag == "i":
            if destination not in incoming_edges:
                incoming_edges[destination] = set()
            incoming_edges[destination].add(origin)
            logging.info("Arista de entrada guardada")
        else:
            if origin not in outgoing_edges:
                outgoing_edges[origin] = set()
            outgoing_edges[origin].add(destination)
            logging.info("Arista de salida guardada")

        return []


    # Process EOF
    def on_eof(self, client_id=None):
        logging.info("EOF recibido")
        edges_pair = self.edges_by_client_id[client_id]
        incoming_edges = edges_pair[0]
        outgoing_edges = edges_pair[1]

        # For each node with incoming edges
        for node in incoming_edges:
            # Check if there are outgoing edges
            if node in outgoing_edges:
                # Get intermediate node ID elements
                interm_bank, interm_acc = node.as_tuple()

                # Get neighbours
                incoming_edges_neighbours = incoming_edges[node]
                outgoing_edges_neighbours = outgoing_edges[node]

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
