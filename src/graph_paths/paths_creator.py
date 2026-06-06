import logging
import zlib
from collections import defaultdict
from common.middleware.worker_base import WorkerBase

class PathsCreator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.incoming_edges = {}
        self.outgoing_edges = {}

    def process(self, data):
        client_id = data.get("client_id")
        edge_type_for_interm = data.get("Type For Interm")

        origin = (data.get("From Bank"), data.get("Account"))
        dest = (data.get("To Bank"), data.get("Account.1"))

        new_paths = []

        # If for intermediate node it is outgoing, then origin is intermediate
        if edge_type_for_interm == "o":
            interm_node = origin
            final_dest = dest

            # Add edge
            if client_id not in self.outgoing_edges:
                self.outgoing_edges[client_id] = {}
            if interm_node not in self.outgoing_edges[client_id]:
                self.outgoing_edges[client_id][interm_node] = set()
            
            if final_dest not in self.outgoing_edges[client_id][interm_node]:
                self.outgoing_edges[client_id][interm_node].add(final_dest)

                # Create paths
                if client_id in self.incoming_edges and interm_node in self.incoming_edges[client_id]:
                    for start_node in self.incoming_edges[client_id][interm_node]:
                        if start_node != final_dest:
                            new_paths.append({
                                "client_id": client_id,
                                "Origin Bank": start_node[0],
                                "Origin Account": start_node[1],
                                "Dest Bank": final_dest[0],
                                "Dest Account": final_dest[1],
                            })

        # If for intermediate node it is incoming, then destination is intermediate
        elif edge_type_for_interm == "i":
            start_node = origin
            interm_node = dest

            # Add edge
            if client_id not in self.incoming_edges:
                self.incoming_edges[client_id] = {}
            if interm_node not in self.incoming_edges[client_id]:
                self.incoming_edges[client_id][interm_node] = set()
            
            if start_node not in self.incoming_edges[client_id][interm_node]:
                self.incoming_edges[client_id][interm_node].add(start_node)

                # Create paths
                if client_id in self.outgoing_edges and interm_node in self.outgoing_edges[client_id]:
                    for final_dest in self.outgoing_edges[client_id][interm_node]:
                        if start_node != final_dest:
                            new_paths.append({
                                "client_id": client_id,
                                "Origin Bank": start_node[0],
                                "Origin Account": start_node[1],
                                "Dest Bank": final_dest[0],
                                "Dest Account": final_dest[1],
                            })
        else:
            return []

        return new_paths

    def on_eof(self, client_id=None):
        if client_id is None:
            self.incoming_edges.clear()
            self.outgoing_edges.clear()
            return []

        self.incoming_edges.pop(client_id, None)
        self.outgoing_edges.pop(client_id, None)
        return []

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['Origin']}||{msg['Dest']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_creator = PathsCreator()
    paths_creator.run()