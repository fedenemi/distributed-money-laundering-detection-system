import json
import os
from typing import Any


class BarrierFilterLogger:
    def __init__(self, worker_name: str, base_dir: str = "/worker_logs"):
        self.worker_dir = os.path.join(base_dir, worker_name)
        os.makedirs(self.worker_dir, exist_ok=True)
        self.state_path = os.path.join(self.worker_dir, "barrier_filter_state.json")

    def recover_state(self) -> tuple[dict[str, dict[str, Any]], set[str]]:
        if not os.path.exists(self.state_path):
            return {}, set()

        try:
            with open(self.state_path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (json.JSONDecodeError, OSError):
            return {}, set()

        comparison_values = payload.get("comparison_values_by_client", {})
        if not isinstance(comparison_values, dict):
            comparison_values = {}

        thresholds_ready = payload.get("thresholds_ready_by_client", [])
        return comparison_values, {str(client_id) for client_id in thresholds_ready}

    def save_state(self, comparison_values_by_client: dict, thresholds_ready_by_client: set):
        tmp_path = f"{self.state_path}.{os.getpid()}.tmp"
        payload = {
            "comparison_values_by_client": comparison_values_by_client,
            "thresholds_ready_by_client": sorted(str(client_id) for client_id in thresholds_ready_by_client),
        }

        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())

        os.replace(tmp_path, self.state_path)
