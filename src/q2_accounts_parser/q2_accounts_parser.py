import logging
import os
from common.middleware.worker_base import WorkerBase

logger = logging.getLogger(__name__)

def _normalize_bank_id(bank_id):
    if bank_id is None:
        return "0"
    normalized = str(bank_id).strip().lstrip("0")
    return normalized if normalized else "0"

class Q2AccountsParser(WorkerBase):
    def __init__(self):
        super().__init__()
        self.sent_bank_ids = set()
        self.processed_count = 0

    def process(self, data: dict):
        client_id = data.get("client_id")
        bank_id = _normalize_bank_id(data.get("Bank ID"))
        bank_name = data.get("Bank Name")

        cache_key = f"{client_id}:{bank_id}"
        if cache_key in self.sent_bank_ids:
            return []
        self.sent_bank_ids.add(cache_key)

        self.processed_count += 1
        if self.processed_count % 1000 == 0:
            logger.info(f"Parseadas {self.processed_count} cuentas únicas")

        return [{
            "client_id": client_id,
            "bank_id": bank_id,
            "bank_name": bank_name
        }]

    def on_eof(self, client_id=None):
        logger.info(f"Q2AccountsParser finalizó parseo. Total de cuentas únicas enviadas a joiner: {self.processed_count}")
        if client_id is None:
            self.sent_bank_ids.clear()
        else:
            self.sent_bank_ids = {k for k in self.sent_bank_ids if not k.startswith(f"{client_id}:")}
        self.processed_count = 0
        return []
    
    def on_clean_client_data(self, client_id=None):
        if client_id is None:
            return

        prefix = f"{client_id}:"
        original_size = len(self.sent_bank_ids)
        self.sent_bank_ids = {k for k in self.sent_bank_ids if not k.startswith(prefix)}
        removed_count = original_size - len(self.sent_bank_ids)
        if removed_count > 0:
            self.processed_count = max(0, self.processed_count - removed_count)
            
        logger.info(f"Limpieza completa de RAM para cliente {client_id}. Se purgaron {removed_count} IDs cacheados.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    worker = Q2AccountsParser()
    worker.run()