import logging
from common.middleware.worker_base import WorkerBase

class PathsAggregator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.total_pair_counts = {}

    def process(self, data):
        client_id = data["client_id"]
        origin_and_dest = (data["From Bank"], data["Account"], data["To Bank"], data["Account.1"])
        if client_id not in self.total_pair_counts:
            self.total_pair_counts[client_id] = {}
        if origin_and_dest not in self.total_pair_counts[client_id]:
            self.total_pair_counts[client_id][origin_and_dest] = 1
        else:
            self.total_pair_counts[client_id][origin_and_dest] += 1

        return []

    def on_eof(self, client_id=None):
        if client_id is not None and client_id in self.total_pair_counts:
            unique_accounts = set()

            for (origin_bank, origin_acc, dest_bank, dest_acc), count in self.total_pair_counts[client_id].items():
                unique_accounts.add((origin_bank, origin_acc))
                unique_accounts.add((dest_bank, dest_acc))

            for bank, account in unique_accounts:
                yield {
                    "client_id": client_id,
                    "Bank": bank,
                    "Account": account
                }

            del self.total_pair_counts[client_id]
            
        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_aggregator = PathsAggregator()
    paths_aggregator.run()