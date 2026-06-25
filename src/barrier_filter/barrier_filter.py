from common.middleware.double_io_worker_base import WorkerBaseDoubleIO
from barrier_filter_logger import BarrierFilterLogger

import json
import logging
import multiprocessing
import os


class BarrierFilter(WorkerBaseDoubleIO):
    def __init__(self):
        super().__init__()

        self._coeficient_comparison_value = float(os.environ["COEF"])
        self._worker_name = f"{self.consumer_group}_{os.environ.get('SHARD_ID', '-1')}"
        self._state_logger = BarrierFilterLogger(self._worker_name)
        self._spool_dir = os.environ.get(
            "BARRIER_FILTER_SPOOL_DIR",
            os.path.join("/worker_logs", self._worker_name, "barrier_spool"),
        )
        os.makedirs(self._spool_dir, exist_ok=True)

        # Processes manager
        manager = multiprocessing.Manager()
        self._comparison_values_by_client = manager.dict()
        self._thresholds_ready_by_client = manager.dict()
        self._spool_lock = multiprocessing.Lock()

        # Local elements
        self._local_thresholds_ready = set()
        self._local_comparison_values = {}
        self._spool_buffer = {}

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_main_worker_started(self):
        self._recover_persistent_state()
        self._process_recovered_ready_spools()

    def on_sec_worker_started(self):
        self._recover_persistent_state()

    def on_sec_batch_complete(self):
        self._save_persistent_state()

    def _client_key(self, client_id):
        return str(client_id)

    def _recover_persistent_state(self):
        comparison_values, thresholds_ready = self._state_logger.recover_state()
        if not comparison_values and not thresholds_ready:
            logging.info("BarrierFilter no encontro estado persistido para recuperar")
            return

        for client_id, values in comparison_values.items():
            self._comparison_values_by_client[client_id] = values
            self._local_comparison_values[client_id] = values

        for client_id in thresholds_ready:
            self._thresholds_ready_by_client[client_id] = True
            self._local_thresholds_ready.add(client_id)

        logging.info(
            "BarrierFilter recupero estado: clients=%s thresholds_ready=%s",
            len(comparison_values),
            len(thresholds_ready),
        )

    def _save_persistent_state(self):
        comparison_values = {
            str(client_id): dict(values)
            for client_id, values in dict(self._comparison_values_by_client).items()
        }
        thresholds_ready = {
            str(client_id)
            for client_id, ready in dict(self._thresholds_ready_by_client).items()
            if ready
        }
        self._state_logger.save_state(comparison_values, thresholds_ready)
        logging.info(
            "BarrierFilter estado persistido: clients=%s thresholds_ready=%s",
            len(comparison_values),
            len(thresholds_ready),
        )

    def _spool_path(self, client_id):
        return os.path.join(self._spool_dir, f"transactions_{self._client_key(client_id)}.jsonl")

    def _snapshot_path(self, client_id):
        return os.path.join(self._spool_dir, f"transactions_{self._client_key(client_id)}.ready.jsonl")

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

    def _pending_spool_snapshots(self, client_id):
        snapshot_path = self._snapshot_path(client_id)
        active_path = self._spool_path(client_id)

        if os.path.exists(snapshot_path):
            yield snapshot_path
            if os.path.exists(active_path):
                yield active_path
            return

        new_snapshot_path = self._snapshot_spool(client_id)
        if new_snapshot_path is not None:
            yield new_snapshot_path

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

    def _emit_spool_for_ready_client(self, client_id, comparison_values):
        total_scanned = 0
        total_sent = 0

        for snapshot_path in self._pending_spool_snapshots(client_id):
            sent_transactions = 0
            scanned_transactions = 0
            for transaction in self._iter_transactions_from_path(snapshot_path):
                scanned_transactions += 1
                result = self._filter_transaction(client_id, transaction, comparison_values)
                if result is not None:
                    sent_transactions += 1
                    self._emit_main_output([result])

            self._delete_path(snapshot_path)
            total_scanned += scanned_transactions
            total_sent += sent_transactions

        if total_scanned:
            self._flush_all_main_buffer()
            logging.info(
                "BarrierFilter proceso spool recuperado para cliente %s: leidas=%s enviadas=%s",
                client_id,
                total_scanned,
                total_sent,
            )

    def _process_recovered_ready_spools(self):
        for client_id in list(self._local_thresholds_ready):
            comparison_values = self._local_comparison_values.get(client_id, {})
            self._emit_spool_for_ready_client(client_id, comparison_values)

    def _filter_transaction(self, client_id, transaction, comparison_values):
        payment_method = transaction["Payment Format"]
        threshold = comparison_values.get(payment_method)
        if threshold is None:
            return None

        if float(transaction["Amount Paid"]) >= threshold:
            return None

        return {
            "client_id": transaction.get("client_id", client_id),
            "From Bank": transaction["From Bank"],
            "Account": transaction["Account"],
            "Payment Format": payment_method,
            "Amount Paid": transaction["Amount Paid"],
        }

    def process_main_input(self, data):
        logging.debug("Nueva transaccion recibida")
        client_id = data["client_id"]
        client_key = self._client_key(client_id)

        if client_key in self._local_thresholds_ready:
            result = self._filter_transaction(client_id, data, self._local_comparison_values[client_key])
            return ([result], []) if result is not None else ([], [])

        self._spool_buffer.setdefault(client_key, []).append(data)
        return ([], [])

    def on_main_batch_complete(self):
        if not self._spool_buffer:
            return

        with self._spool_lock:
            for client_id, rows in list(self._spool_buffer.items()):
                if not rows:
                    continue

                if client_id not in self._local_thresholds_ready and self._thresholds_ready_by_client.get(client_id, False):
                    self._local_thresholds_ready.add(client_id)
                    self._local_comparison_values[client_id] = self._comparison_values_by_client.get(client_id, {})

                if client_id in self._local_thresholds_ready:
                    results_to_emit = []
                    for data in rows:
                        result = self._filter_transaction(client_id, data, self._local_comparison_values[client_id])
                        if result is not None:
                            results_to_emit.append(result)
                    if results_to_emit:
                        self._emit_main_output(results_to_emit)
                else:
                    lines = [json.dumps(r, separators=(",", ":")) for r in rows]
                    with open(self._spool_path(client_id), "a", encoding="utf-8") as f:
                        f.write("\n".join(lines) + "\n")
                        
        self._spool_buffer.clear()

    def process_secondary_input(self, data):
        logging.debug("Nuevo promedio recibido")

        client_id = data["client_id"]
        client_key = self._client_key(client_id)
        payment_format = data["Payment Format"]
        avg_value = float(data["avg_Amount Paid"])

        threshold = avg_value * self._coeficient_comparison_value
        client_values = self._comparison_values_by_client.get(client_key, {})
        client_values[payment_format] = threshold
        self._comparison_values_by_client[client_key] = client_values

        logging.debug("Promedio guardado")
        return ([], [])

    def on_secondary_ready(self, client_id=None):
        logging.info(f"Promedios listos para cliente {client_id}")
        client_key = self._client_key(client_id)

        with self._spool_lock:
            self._thresholds_ready_by_client[client_key] = True
            comparison_values = self._comparison_values_by_client.get(client_key, {})
            self._save_persistent_state()
            snapshot_paths = list(self._pending_spool_snapshots(client_id))

        if not snapshot_paths:
            logging.info("No hay transacciones en spool para procesar.")
            return

        total_sent = 0
        total_scanned = 0
        for snapshot_path in snapshot_paths:
            for transaction in self._iter_transactions_from_path(snapshot_path):
                total_scanned += 1
                result = self._filter_transaction(client_id, transaction, comparison_values)
                if result is not None:
                    total_sent += 1
                    yield result
            self._delete_path(snapshot_path)

        logging.info(f"Se leyeron {total_scanned} transacciones desde spool.")
        logging.info(f"Se enviaron {total_sent} transacciones desde spool.")

    def on_both_eof_received(self, client_id=None):
        logging.info(f"Ambos EOF recibidos de cliente {client_id}")
        client_key = self._client_key(client_id)
        self._thresholds_ready_by_client.pop(client_key, None)
        self._comparison_values_by_client.pop(client_key, None)
        self._local_thresholds_ready.discard(client_key)
        self._local_comparison_values.pop(client_key, None)
        self._save_persistent_state()
        logging.info("Estado del cliente limpiado")
        return iter([])
    
    def on_clean_client_data(self, client_id=None):
        if client_id is None:
            return

        client_key = self._client_key(client_id)
        state_changed = False

        with self._spool_lock:
            self._local_thresholds_ready.discard(client_key)
            if client_key in self._local_comparison_values:
                del self._local_comparison_values[client_key]
            if client_key in self._spool_buffer:
                del self._spool_buffer[client_key]

            if client_key in self._thresholds_ready_by_client:
                del self._thresholds_ready_by_client[client_key]
                state_changed = True

            if client_key in self._comparison_values_by_client:
                del self._comparison_values_by_client[client_key]
                state_changed = True

        if state_changed:
            self._save_persistent_state()

        active_path = self._spool_path(client_id)
        snapshot_path = self._snapshot_path(client_id)

        for path in [active_path, snapshot_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError as e:
                    logging.error(f"Error borrando archivo de spool {path}: {e}")

        logging.info(f"Limpieza completa para {client_key} (RAM, JSON y archivos de Spool)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    barrier_filter = BarrierFilter()
    barrier_filter.run()
