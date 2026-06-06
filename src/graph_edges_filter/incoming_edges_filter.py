import logging
import os
import zlib
from common.middleware.worker_base import WorkerBase
from disk_storage_management import DiskSet

class IncomingEdgesFilter(WorkerBase):

    def __init__(self):
        super().__init__()
        self._min_incoming = int(os.environ.get("MIN_INCOMING", "5"))
        self._clients_files_by_id = {}

    @staticmethod
    def encode(elem):
        return f"{elem['To Bank']}|{elem['Account.1']}|{elem['From Bank']}|{elem['Account']}"

    @staticmethod
    def decode(elem):
        split_elems = elem.split("|")
        return {
            "To Bank" : split_elems[0],
            "Account.1" : split_elems[1],
            "From Bank" : split_elems[2],
            "Account" : split_elems[3]
        }

    def process(self, data):
        # Check client ID
        client_id = data.get("client_id")
        if client_id not in self._clients_files_by_id:
            self._clients_files_by_id[client_id] = DiskSet(
                id=f"{self.shard_id}_{client_id}",
                encode_func=IncomingEdgesFilter.encode,
                decode_func=IncomingEdgesFilter.decode,
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
        prev_dest_id = None
        current_origins = set()
        for elem in disk_set.elements():
            dest_id = (elem["To Bank"], elem["Account.1"])
            # If new key is found, restart counting of intermediaries
            if prev_dest_id is not None and prev_dest_id != dest_id:
                # Check condition for sending edges to next stage
                if len(current_origins) > self._min_incoming:
                    for origin in current_origins:
                        yield {
                            "client_id": client_id,
                            "Type For Interm": "o",
                            "From Bank" : origin[0],
                            "Account" : origin[1],
                            "To Bank" : prev_dest_id[0],
                            "Account.1" : prev_dest_id[1]
                        }
                current_origins.clear()
            # Add elements to set
            prev_dest_id = dest_id
            current_origins.add((elem["From Bank"], elem["Account"]))

        # Check after the loop
        if prev_dest_id is not None and len(current_origins) > self._min_incoming:
            for origin in current_origins:
                yield {
                    "client_id": client_id,
                    "Type For Interm": "o",
                    "From Bank" : origin[0],
                    "Account" : origin[1],
                    "To Bank" : prev_dest_id[0],
                    "Account.1" : prev_dest_id[1]
                }

        del self._clients_files_by_id[client_id]

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['From Bank']}{msg['Account']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = IncomingEdgesFilter()
    paths_aggregator.run()