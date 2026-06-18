import json
import logging
import os
import zlib

logger = logging.getLogger(__name__)


class MoneyConverterLogger:
    def __init__(self, base_logs_dir: str, worker_name: str):
        worker_dir = os.path.join(base_logs_dir, worker_name)
        os.makedirs(worker_dir, exist_ok=True)
        self.state_filepath = os.path.join(worker_dir, "money_converter_state.json")
        self.processed_filepath = os.path.join(worker_dir, "money_converter_processed.log")

    def _write_record(self, file, data: dict, sync: bool = True):
        payload = json.dumps(data, separators=(",", ":")).encode("utf-8")
        length = len(payload).to_bytes(4, "big")
        checksum = zlib.crc32(payload).to_bytes(4, "big")
        file.write(length)
        file.write(payload)
        file.write(checksum)
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
            logger.exception("No se pudo recuperar estado persistido de MoneyConverter")
            return {}, {}, set()

        cache = dict(state.get("cache", {}))
        pending = {
            rate_key: self.normalize_pending(rows_by_id)
            for rate_key, rows_by_id in state.get("pending", {}).items()
        }
        legacy_processed_row_ids = set(str(row_id) for row_id in state.get("processed_row_ids", []))
        processed_row_ids = self.recover_processed_row_ids()
        missing_legacy_ids = legacy_processed_row_ids - processed_row_ids
        self.append_processed_row_ids(missing_legacy_ids)
        processed_row_ids.update(legacy_processed_row_ids)
        return cache, pending, processed_row_ids

    def recover_processed_row_ids(self) -> set[str]:
        processed_row_ids = set()
        for record in self._read_records(self.processed_filepath) or []:
            row_id = record.get("row_id")
            if row_id is not None:
                processed_row_ids.add(str(row_id))
        return processed_row_ids

    def append_processed_row_id(self, row_id: str) -> None:
        with open(self.processed_filepath, "ab") as file:
            self._write_record(file, {"row_id": str(row_id)}, sync=False)

    def append_processed_row_ids(self, row_ids) -> None:
        row_ids = list(row_ids)
        if not row_ids:
            return

        with open(self.processed_filepath, "ab") as file:
            for row_id in row_ids:
                self._write_record(file, {"row_id": str(row_id)}, sync=False)
            file.flush()
            os.fsync(file.fileno())

    def save_state(self, cache: dict, pending: dict) -> None:
        tmp_path = f"{self.state_filepath}.{os.getpid()}.tmp"
        normalized_pending = {
            rate_key: self.normalize_pending(rows_by_id)
            for rate_key, rows_by_id in pending.items()
        }

        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(
                {
                    "cache": dict(cache),
                    "pending": normalized_pending,
                },
                file,
                separators=(",", ":"),
            )
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp_path, self.state_filepath)
