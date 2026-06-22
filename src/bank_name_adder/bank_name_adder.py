import logging

from common.middleware.double_io_worker_base import WorkerBaseDoubleIO
from bank_name_adder_logger import BankNameAdderLogger

def _normalize_bank_id(bank_id):
    if bank_id is None:
        return "0"
    normalized = str(bank_id).strip().lstrip("0")
    return normalized if normalized else "0"

class BankNameAdder(WorkerBaseDoubleIO):

    def waits_for_both_pipeline_eofs(self) -> bool:
        return True

    def __init__(self):
        super().__init__()
        self._persistent_state_loaded = False
        self._processed_row_ids = set()
        self._main_row_to_mark = None
        self._main_rows_to_mark = []
        self._sec_cache_updates = {}
        self._main_state_dirty = False
        self._sec_state_dirty = False

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_main_worker_started(self):
        self._recover_persistent_state()

    def on_sec_worker_started(self):
        self._recover_persistent_state()

    def on_main_row_complete(self):
        if self._main_row_to_mark is not None:
            self._main_rows_to_mark.append(self._main_row_to_mark)
        self._main_row_to_mark = None

    def on_main_batch_complete(self):
        if self._main_state_dirty:
            self._save_persistent_state()
        self._mark_rows_processed(self._main_rows_to_mark)
        self._flush_all_sec_buffer()
        self._main_rows_to_mark = []
        self._main_state_dirty = False

    def on_sec_row_complete(self):
        return

    def on_sec_batch_complete(self):
        if self._sec_cache_updates:
            self._state_logger().append_cache_entries(self._sec_cache_updates)
        if self._sec_state_dirty:
            self._save_persistent_state()
        self._flush_all_sec_buffer()
        self._sec_cache_updates = {}
        self._sec_state_dirty = False

    def _state_logger(self) -> BankNameAdderLogger:
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        return BankNameAdderLogger("/worker_logs", worker_name)

    def _pending_row_id(self) -> str:
        msg_hash = getattr(self, "_current_msg_hash", "unknown")
        row_index = getattr(self, "_current_row_index", "unknown")
        return f"{msg_hash}:{row_index}"

    def _mark_row_processed(self, row_id: str):
        if row_id in self._processed_row_ids:
            return
        self._processed_row_ids.add(row_id)
        self._state_logger().append_processed_row_id(row_id)

    def _mark_rows_processed(self, row_ids):
        new_row_ids = [
            row_id
            for row_id in row_ids
            if row_id is not None and row_id not in self._processed_row_ids
        ]
        if not new_row_ids:
            return

        self._processed_row_ids.update(new_row_ids)
        self._state_logger().append_processed_row_ids(new_row_ids)

    def _normalize_pending(self, pending):
        return BankNameAdderLogger.normalize_pending(pending)

    def _recover_persistent_state(self):
        if self._persistent_state_loaded:
            return

        cache, pending, processed_row_ids = self._state_logger().recover_state()
        with self._shared_lock:
            for bank_id, bank_name in cache.items():
                self._shared_cache[bank_id] = bank_name

            for bank_id, rows_by_id in pending.items():
                current = self._normalize_pending(self._shared_pending.get(bank_id, {}))
                current.update(rows_by_id)
                self._shared_pending[bank_id] = current

            self._processed_row_ids.update(processed_row_ids)

        self._persistent_state_loaded = True
        logging.info(
            "BankNameAdder recupero estado: cache=%s pending_keys=%s processed_rows=%s",
            len(cache),
            len(pending),
            len(processed_row_ids),
        )

    def _save_persistent_state(self):
        if not hasattr(self, "_shared_lock"):
            return

        with self._shared_lock:
            cache = dict(self._shared_cache)
            pending = {
                bank_id: self._normalize_pending(rows_by_id)
                for bank_id, rows_by_id in dict(self._shared_pending).items()
            }

        self._state_logger().save_pending(pending)

    def process_main_input(self, data: dict) -> tuple[list, list]:
        self._main_row_to_mark = None
        data_copy = data.copy()
        row_id = self._pending_row_id()
        if row_id in self._processed_row_ids:
            return ([], [])

        bank_id = _normalize_bank_id(data.get("From Bank"))

        # Get shared lock for shared elements
        with self._shared_lock:
            # Check if bank name is in cache
            if bank_id in self._shared_cache:
                data_copy["Bank Name"] = self._shared_cache[bank_id]
                self._main_row_to_mark = row_id
                return ([], [data_copy])
            else:   # If not, add element as pending data
                pending_rows = self._normalize_pending(self._shared_pending.get(bank_id, {}))
                pending_rows[row_id] = data_copy
                self._shared_pending[bank_id] = pending_rows
                self._main_row_to_mark = row_id
                self._main_state_dirty = True
                return ([], [])

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        self._sec_state_dirty = False
        bank_id = _normalize_bank_id(data.get("bank_id", data.get("From Bank")))
        bank_name = data.get("bank_name", data.get("Bank Name"))

        resolved_messages = []

        # Get shared lock for shared elements
        with self._shared_lock:
            self._shared_cache[bank_id] = bank_name
            self._sec_cache_updates[bank_id] = bank_name

            # Send elements if there are any pending name changes
            if bank_id in self._shared_pending:
                pending_rows = self._normalize_pending(self._shared_pending.pop(bank_id))
                self._sec_state_dirty = True
                for msg in pending_rows.values():
                    msg["Bank Name"] = bank_name
                    resolved_messages.append(msg)

            # Publicar antes de liberar el lock evita que EOF adelante filas resueltas.
            self._emit_sec_output(resolved_messages)

        return ([], [])

    def on_main_input_eof(self, client_id=None) -> list:
        unmatched = []
        with self._shared_lock:
            for bank_id, rows in self._shared_pending.items():
                for row in self._normalize_pending(rows).values():
                    row["Bank Name"] = bank_id
                    unmatched.append(row)
            self._shared_pending.clear()

        if unmatched:
            self._emit_sec_output(unmatched)
            self._save_persistent_state()

        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bank_name_adder = BankNameAdder()
    bank_name_adder.run()
