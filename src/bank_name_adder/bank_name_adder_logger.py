import json
import logging
import os
import zlib

logger = logging.getLogger(__name__)


class BankNameAdderLogger:
    def __init__(self, base_logs_dir: str, worker_name: str):
        worker_dir = os.path.join(base_logs_dir, worker_name)
        os.makedirs(worker_dir, exist_ok=True)
        self.state_filepath = os.path.join(worker_dir, "bank_name_adder_state.json")
        self.cache_filepath = os.path.join(worker_dir, "bank_name_adder_cache.log")
        self.processed_filepath = os.path.join(worker_dir, "bank_name_adder_processed.log")

    def _write_record(self, file, data: dict, sync: bool = True):
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        file.write(len(payload).to_bytes(4, "big"))
        file.write(payload)
        file.write(zlib.crc32(payload).to_bytes(4, "big"))
        file.flush()
        if sync:
            os.fsync(file.fileno())

    def _read_records(self, filepath: str):
        if not os.path.exists(filepath):
            return

        with open(filepath, "rb") as file:
            while True:
                length_bytes = file.read(4)
                if not length_bytes or len(length_bytes) < 4:
                    break

                length = int.from_bytes(length_bytes, "big")
                payload = file.read(length)
                if len(payload) < length:
                    break

                checksum = file.read(4)
                if len(checksum) < 4:
                    break

                if zlib.crc32(payload).to_bytes(4, "big") != checksum:
                    break

                try:
                    yield json.loads(payload.decode("utf-8"))
                except json.JSONDecodeError:
                    break

    @staticmethod
    def normalize_pending(pending):
        if isinstance(pending, dict):
            return dict(pending)

        normalized = {}
        for index, row in enumerate(pending or []):
            normalized[f"legacy:{index}"] = row
        return normalized

    def recover_state(self) -> tuple[dict, dict, set[str]]:
        if not os.path.exists(self.state_filepath):
            return {}, {}, set()

        try:
            with open(self.state_filepath, "r", encoding="utf-8") as file:
                state = json.load(file)
        except (OSError, json.JSONDecodeError):
            logger.exception("No se pudo recuperar estado persistido de BankNameAdder")
            return {}, {}, set()

        cache = dict(state.get("cache", {}))
        cache.update(self.recover_cache())
        pending = {
            bank_id: self.normalize_pending(rows_by_id)
            for bank_id, rows_by_id in state.get("pending", {}).items()
        }
        legacy_processed_row_ids = set(str(row_id) for row_id in state.get("processed_row_ids", []))
        processed_row_ids = self.recover_processed_row_ids()
        missing_legacy_ids = legacy_processed_row_ids - processed_row_ids
        self.append_processed_row_ids(missing_legacy_ids)
        processed_row_ids.update(legacy_processed_row_ids)
        return cache, pending, processed_row_ids

    def recover_cache(self) -> dict:
        cache = {}
        for record in self._read_records(self.cache_filepath) or []:
            entries = record.get("entries", {})
            if isinstance(entries, dict):
                cache.update({str(bank_id): bank_name for bank_id, bank_name in entries.items()})
        return cache

    def append_cache_entries(self, entries: dict):
        if not entries:
            return

        normalized_entries = {str(bank_id): bank_name for bank_id, bank_name in entries.items()}
        with open(self.cache_filepath, "ab") as file:
            self._write_record(file, {"entries": normalized_entries}, sync=True)

    def recover_processed_row_ids(self) -> set[str]:
        processed_row_ids = set()
        for record in self._read_records(self.processed_filepath) or []:
            row_id = record.get("row_id")
            if row_id is not None:
                processed_row_ids.add(str(row_id))
        return processed_row_ids

    def append_processed_row_id(self, row_id: str):
        with open(self.processed_filepath, "ab") as file:
            self._write_record(file, {"row_id": str(row_id)}, sync=False)

    def append_processed_row_ids(self, row_ids):
        row_ids = list(row_ids)
        if not row_ids:
            return

        with open(self.processed_filepath, "ab") as file:
            for row_id in row_ids:
                self._write_record(file, {"row_id": str(row_id)}, sync=False)
            file.flush()
            os.fsync(file.fileno())

    def save_state(self, cache: dict, pending: dict):
        self.save_pending(pending)

    def save_pending(self, pending: dict):
        tmp_path = f"{self.state_filepath}.{os.getpid()}.tmp"
        normalized_pending = {
            bank_id: self.normalize_pending(rows_by_id)
            for bank_id, rows_by_id in pending.items()
        }

        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(
                {
                    "pending": normalized_pending,
                },
                file,
                separators=(",", ":"),
            )
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, self.state_filepath)
