import logging
import os
import zlib
from pathlib import Path
from common.middleware.worker_base import WorkerBase
from disk_storage_management import DiskSet

class IncomingEdgesFilter(WorkerBase):

    def __init__(self):
        super().__init__()
        self._min_incoming = int(os.environ.get("MIN_INCOMING", "5"))
        self._clients_files_by_id = {}
        self._storage_base_dir = Path("/worker_logs") / f"{self.consumer_group}_{self.shard_id}" / "edge_storage"
        os.makedirs(self._storage_base_dir, exist_ok=True)

    def on_worker_started(self):
        prefix = f"storage_{self.shard_id}_"
        for storage_dir in self._storage_base_dir.glob(f"{prefix}*"):
            if not storage_dir.is_dir():
                continue
            client_id = storage_dir.name.removeprefix(prefix)
            self._clients_files_by_id[client_id] = self._create_disk_set(client_id)
        logging.info(
            "IncomingEdgesFilter recuperacion estado: clients=%s storage=%s",
            len(self._clients_files_by_id),
            self._storage_base_dir,
        )

    def _create_disk_set(self, client_id):
        return DiskSet(
            id=f"{self.shard_id}_{client_id}",
            encode_func=IncomingEdgesFilter.encode,
            decode_func=IncomingEdgesFilter.decode,
            base_dir=self._storage_base_dir,
        )

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
        client_id = str(data.get("client_id"))
        if client_id not in self._clients_files_by_id:
            self._clients_files_by_id[client_id] = self._create_disk_set(client_id)
        # Store element
        disk_set = self._clients_files_by_id[client_id]
        disk_set.add(data)
        return []

    def on_eof(self, client_id=None):
        # Check if ID is stored
        client_id = str(client_id) if client_id is not None else None
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

        disk_set.cleanup()
        del self._clients_files_by_id[client_id]

    def _routing_key(self, msg: dict) -> str:
        key = f"{msg['From Bank']}{msg['Account']}"
        return str(zlib.crc32(key.encode('utf-8')) % self.output_shards)
    
    def on_clean_client_data(self, client_id=None):
        if client_id is None:
            return
            
        client_key = str(client_id)

        if client_key in self._clients_files_by_id:
            disk_set = self._clients_files_by_id[client_key]
            disk_set.cleanup()
            del self._clients_files_by_id[client_key]

            logging.info(f"Limpieza completa para cliente {client_key}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = IncomingEdgesFilter()
    paths_aggregator.run()
