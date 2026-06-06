import logging
import os
import zlib
from common.middleware.worker_base import WorkerBase
from disk_storage_management import DiskSet

class OutgoingEdgesFilter(WorkerBase):

    def __init__(self):
        super().__init__()
        self._min_outgoing_edges = int(os.environ.get("MIN_OUTGOING", "5"))
        self._clients_files_by_id = {}

    @staticmethod
    def encode(elem):
        return f"{elem['From Bank']}|{elem['Account']}|{elem['To Bank']}|{elem['Account.1']}"

    @staticmethod
    def decode(elem):
        split_elems = elem.split("|")
        return {
            "From Bank" : split_elems[0],
            "Account" : split_elems[1],
            "To Bank" : split_elems[2],
            "Account.1" : split_elems[3]
            }

    def process(self, data):
        # Check client ID
        client_id = data["client_id"]
        if client_id not in self._clients_files_by_id:
            self._clients_files_by_id[client_id] = DiskSet(
                id=f"{self.shard_id}_{client_id}",
                encode_func=OutgoingEdgesFilter.encode,
                decode_func=OutgoingEdgesFilter.decode,
                )
        # Store element
        disk_set = self._clients_files_by_id[client_id]
        disk_set.add(data)
        return []

    def on_eof(self, client_id=None):
        # Check if ID is stored
        if client_id is None or client_id not in self._clients_files_by_id:
            return []

        # Iterate over elements
        disk_set = self._clients_files_by_id[client_id]
        prev_elem_id = (None, None)
        current_destinations = set()
        for elem in disk_set.elements():
            elem_id = (elem["From Bank"], elem["Account"])
            # If new key is found, restart counting of intermediaries
            if prev_elem_id != elem_id:
                # Check condition for sending edges to next stage
                if len(current_destinations) > self._min_outgoing_edges:
                    for sent_elem in current_destinations:
                        yield {
                            "client_id": client_id,
                            "Type For Interm": "i",
                            "From Bank" : prev_elem_id[0],
                            "Account" : prev_elem_id[1],
                            "To Bank" : sent_elem[0],
                            "Account.1" : sent_elem[1]
                            }
                current_destinations.clear()
            # Add elements to set
            prev_elem_id = elem_id
            current_destinations.add((elem["To Bank"], elem["Account.1"]))

        # Check after the loop
        if prev_elem_id is not None and len(current_destinations) > self._min_outgoing_edges:
            for sent_elem in current_destinations:
                yield {
                    "client_id": client_id,
                    "Type For Interm": "i",
                    "From Bank" : prev_elem_id[0],
                    "Account" : prev_elem_id[1],
                    "To Bank" : sent_elem[0],
                    "Account.1" : sent_elem[1]
                }

        del self._clients_files_by_id[client_id]

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['To Bank']}{msg['Account.1']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = OutgoingEdgesFilter()
    paths_aggregator.run()