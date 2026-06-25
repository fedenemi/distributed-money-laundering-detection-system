import logging
import zlib
from common.middleware.worker_base import WorkerBase
from paths_state_logger import PathsStateLogger

class PathsCreator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.incoming_edges = {}
        self.outgoing_edges = {}
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        self._state_logger = PathsStateLogger(worker_name, "paths_creator_state.json")
        self._applied_batch_id = None

    def on_worker_started(self):
        state, self._applied_batch_id = self._state_logger.recover_state()
        self.incoming_edges = self._deserialize_edges(state.get("incoming_edges", {}))
        self.outgoing_edges = self._deserialize_edges(state.get("outgoing_edges", {}))
        logging.info(
            "PathsCreator recuperacion estado: incoming_clients=%s incoming_nodes=%s "
            "outgoing_clients=%s outgoing_nodes=%s applied_batch_id=%s",
            len(self.incoming_edges),
            sum(len(client_edges) for client_edges in self.incoming_edges.values()),
            len(self.outgoing_edges),
            sum(len(client_edges) for client_edges in self.outgoing_edges.values()),
            self._applied_batch_id,
        )

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_batch_complete(self, batch_id: str):
        self._applied_batch_id = batch_id
        self._persist_state()
        logging.info(
            "PathsCreator batch recibido: incoming_clients=%s incoming_nodes=%s "
            "outgoing_clients=%s outgoing_nodes=%s",
            len(self.incoming_edges),
            sum(len(client_edges) for client_edges in self.incoming_edges.values()),
            len(self.outgoing_edges),
            sum(len(client_edges) for client_edges in self.outgoing_edges.values()),
        )

    def on_eof_complete(self, client_id=None):
        self._persist_state()

    def _persist_state(self):
        self._state_logger.save_state({
            "incoming_edges": self._serialize_edges(self.incoming_edges),
            "outgoing_edges": self._serialize_edges(self.outgoing_edges),
        }, self._applied_batch_id)

    @classmethod
    def _serialize_edges(cls, edges):
        return {
            str(client_id): {
                cls._encode_node(interm_node): [
                    cls._encode_node(edge) for edge in sorted(edge_set)
                ]
                for interm_node, edge_set in client_edges.items()
            }
            for client_id, client_edges in edges.items()
        }

    @classmethod
    def _deserialize_edges(cls, state):
        return {
            client_id: {
                cls._decode_node(interm_node): {
                    cls._decode_node(edge) for edge in edge_list
                }
                for interm_node, edge_list in client_edges.items()
            }
            for client_id, client_edges in state.items()
            if isinstance(client_edges, dict)
        }

    @staticmethod
    def _encode_node(node):
        return f"{node[0]}|{node[1]}"

    @staticmethod
    def _decode_node(node):
        return tuple(node.split("|", 1))

    def process(self, data):
        if getattr(self, "_current_msg_hash", None) == self._applied_batch_id:
            return []

        client_id = str(data.get("client_id"))
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
                                "From Bank": start_node[0],
                                "Account": start_node[1],
                                "To Bank": final_dest[0],
                                "Account.1": final_dest[1],
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
                                "From Bank": start_node[0],
                                "Account": start_node[1],
                                "To Bank": final_dest[0],
                                "Account.1": final_dest[1],
                            })
        else:
            return []

        return new_paths

    def on_eof(self, client_id=None):
        if client_id is None:
            self.incoming_edges.clear()
            self.outgoing_edges.clear()
            return []

        client_key = str(client_id)
        self.incoming_edges.pop(client_key, None)
        self.outgoing_edges.pop(client_key, None)
        return []

    def _routing_key(self, msg: dict) -> str:
        key = (
            f"{msg['From Bank']}||{msg['Account']}||"
            f"{msg['To Bank']}||{msg['Account.1']}"
        )
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

    def on_clean_client_data(self, client_id=None):
        if client_id is None:
            return
            
        client_key = str(client_id)
        state_changed = False

        if client_key in self.incoming_edges:
            del self.incoming_edges[client_key]
            state_changed = True
            
        if client_key in self.outgoing_edges:
            del self.outgoing_edges[client_key]
            state_changed = True

        if state_changed:
            self._persist_state()
            
        logging.info(f"Limpieza completa para cliente {client_key}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_creator = PathsCreator()
    paths_creator.run()
