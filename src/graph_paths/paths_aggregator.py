import logging
from common.middleware.worker_base import WorkerBase
from paths_state_logger import PathsStateLogger

MIN_TOTAL_PATHS = 5


class PathsAggregator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.total_pair_counts = {}
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        self._state_logger = PathsStateLogger(worker_name, "paths_aggregator_state.json")
        self._applied_batch_id = None

    def on_worker_started(self):
        state, self._applied_batch_id = self._state_logger.recover_state()
        self.total_pair_counts = self._deserialize_state(state)
        logging.info(
            "PathsAggregator recuperacion estado: clients=%s pairs=%s applied_batch_id=%s",
            len(self.total_pair_counts),
            sum(len(client_state) for client_state in self.total_pair_counts.values()),
            self._applied_batch_id,
        )

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_batch_complete(self, batch_id: str):
        self._applied_batch_id = batch_id
        self._persist_state()
        logging.info(
            "PathsAggregator batch recibido: clients=%s pairs=%s",
            len(self.total_pair_counts),
            sum(len(client_state) for client_state in self.total_pair_counts.values()),
        )

    def on_eof_complete(self, client_id=None):
        self._persist_state()

    def _persist_state(self):
        self._state_logger.save_state(self._serialize_state(), self._applied_batch_id)

    def _serialize_state(self):
        return {
            str(client_id): {
                self._encode_pair(pair): count
                for pair, count in pair_counts.items()
            }
            for client_id, pair_counts in self.total_pair_counts.items()
        }

    @classmethod
    def _deserialize_state(cls, state):
        return {
            client_id: {
                cls._decode_pair(pair): count
                for pair, count in pair_counts.items()
            }
            for client_id, pair_counts in state.items()
            if isinstance(pair_counts, dict)
        }

    @staticmethod
    def _encode_pair(pair):
        return "|".join(pair)

    @staticmethod
    def _decode_pair(pair):
        return tuple(pair.split("|", 3))

    def process(self, data):
        if getattr(self, "_current_msg_hash", None) == self._applied_batch_id:
            return []

        client_id = str(data["client_id"])
        origin_and_dest = (data["From Bank"], data["Account"], data["To Bank"], data["Account.1"])
        if client_id not in self.total_pair_counts:
            self.total_pair_counts[client_id] = {}
        if origin_and_dest not in self.total_pair_counts[client_id]:
            self.total_pair_counts[client_id][origin_and_dest] = 1
        else:
            self.total_pair_counts[client_id][origin_and_dest] += 1

        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            return []

        pair_counts = self.total_pair_counts.pop(str(client_id), {})
        unique_accounts = set()
        qualified_pairs = 0
        max_paths = 0

        for (origin_bank, origin_acc, dest_bank, dest_acc), count in pair_counts.items():
            max_paths = max(max_paths, count)
            if count <= MIN_TOTAL_PATHS:
                continue

            qualified_pairs += 1
            unique_accounts.add((origin_bank, origin_acc))
            unique_accounts.add((dest_bank, dest_acc))

        for bank, account in sorted(unique_accounts):
            yield {
                "client_id": client_id,
                "Bank": bank,
                "Account": account
            }

        logging.info(
            "EOF procesado: "
            f"pairs={len(pair_counts)} max_paths_for_pair={max_paths} "
            f"qualified_pairs={qualified_pairs} emitted_accounts={len(unique_accounts)}"
        )
    
    def on_clean_client_data(self, client_id=None):
        if client_id is None:
            return
            
        client_key = str(client_id)
        if client_key in self.total_pair_counts:
            del self.total_pair_counts[client_key]
            self._persist_state()
            logging.info(f"Limpieza completa para cliente {client_key}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = PathsAggregator()
    paths_aggregator.run()
