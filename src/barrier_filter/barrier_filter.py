from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

import json
import logging
import multiprocessing
import os
import tempfile


class BarrierFilter(WorkerBaseDoubleIO):
    def __init__(self):
        super().__init__()

        self._coeficient_comparison_value = float(os.environ["COEF"])
        self._spool_dir = os.environ.get(
            "BARRIER_FILTER_SPOOL_DIR",
            os.path.join(tempfile.gettempdir(), "barrier_filter"),
        )
        os.makedirs(self._spool_dir, exist_ok=True)

        manager = multiprocessing.Manager()
        self._comparison_values_by_client = manager.dict()
        self._thresholds_ready_by_client = manager.dict()
        self._spool_lock = manager.RLock()

    def _spool_path(self, client_id):
        shard_id = os.environ.get("SHARD_ID", "unknown")
        return os.path.join(self._spool_dir, f"transactions_{shard_id}_{client_id}.jsonl")

    def _snapshot_path(self, client_id):
        shard_id = os.environ.get("SHARD_ID", "unknown")
        return os.path.join(self._spool_dir, f"transactions_{shard_id}_{client_id}.ready.jsonl")

    def _store_transaction(self, client_id, data):
        with open(self._spool_path(client_id), "a", encoding="utf-8") as spool_file:
            spool_file.write(json.dumps(data, separators=(",", ":")) + "\n")

    def _snapshot_spool(self, client_id):
        path = self._spool_path(client_id)
        if not os.path.exists(path):
            return None

        snapshot_path = self._snapshot_path(client_id)
        os.replace(path, snapshot_path)
        return snapshot_path

    def _iter_transactions_from_path(self, path):
        with open(path, "r", encoding="utf-8") as spool_file:
            for line in spool_file:
                if line.strip():
                    yield json.loads(line)

    def _delete_path(self, path):
        try:
            os.remove(path)
        except FileNotFoundError:
            pass

    def _filter_transaction(self, client_id, transaction, comparison_values):
        payment_method = transaction["Payment Format"]
        threshold = comparison_values.get(payment_method)
        if threshold is None:
            return None

        if float(transaction["Amount Paid"]) >= threshold:
            return None

        return {
            "client_id": client_id,
            "From Bank": transaction["From Bank"],
            "Account": transaction["Account"],
            "Payment Format": payment_method,
            "Amount Paid": transaction["Amount Paid"],
        }

    def process_main_input(self, data):
        logging.debug("Nueva transaccion recibida")
        client_id = data["client_id"]

        with self._spool_lock:
            comparison_values = self._comparison_values_by_client.get(client_id, {})
            if self._thresholds_ready_by_client.get(client_id, False):
                result = self._filter_transaction(client_id, data, comparison_values)
                return ([result], []) if result is not None else ([], [])

            self._store_transaction(client_id, data)
            return ([], [])

    def process_secondary_input(self, data):
        logging.debug("Nuevo promedio recibido")

        client_id = data["client_id"]
        payment_format = data["Payment Format"]
        avg_value = float(data["avg_Amount Paid"])

        threshold = avg_value * self._coeficient_comparison_value
        client_values = self._comparison_values_by_client.get(client_id, {})
        client_values[payment_format] = threshold
        self._comparison_values_by_client[client_id] = client_values

        logging.debug("Promedio guardado")
        return ([], [])

    def on_secondary_ready(self, client_id=None):
        logging.info(f"Promedios listos para cliente {client_id}")

        with self._spool_lock:
            self._thresholds_ready_by_client[client_id] = True
            comparison_values = self._comparison_values_by_client.get(client_id, {})
            snapshot_path = self._snapshot_spool(client_id)

        if snapshot_path is None:
            logging.info("No hay transacciones en spool para procesar.")
            return

        sent_transactions = 0
        scanned_transactions = 0
        for transaction in self._iter_transactions_from_path(snapshot_path):
            scanned_transactions += 1
            result = self._filter_transaction(client_id, transaction, comparison_values)
            if result is not None:
                sent_transactions += 1
                yield result

        self._delete_path(snapshot_path)
        logging.info(f"Se leyeron {scanned_transactions} transacciones desde spool.")
        logging.info(f"Se enviaron {sent_transactions} transacciones desde spool.")

    def on_both_eof_received(self, client_id=None):
        logging.info(f"Ambos EOF recibidos de cliente {client_id}")
        self._thresholds_ready_by_client.pop(client_id, None)
        self._comparison_values_by_client.pop(client_id, None)
        logging.info("Estado del cliente limpiado")
        return iter([])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    barrier_filter = BarrierFilter()
    barrier_filter.run()
