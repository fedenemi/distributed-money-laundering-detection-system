import json
import os
from typing import Any, Optional


class AggregatorLogger:
    def __init__(self, worker_name: str, base_dir: str = "/worker_logs"):
        self.worker_dir = os.path.join(base_dir, worker_name)
        os.makedirs(self.worker_dir, exist_ok=True)
        self.state_path = os.path.join(self.worker_dir, "aggregator_state.json")

    def recover_state(self) -> tuple[dict[str, dict[str, Any]], Optional[str]]:
        if not os.path.exists(self.state_path):
            return {}, None

        try:
            with open(self.state_path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (json.JSONDecodeError, OSError):
            return {}, None

        state = payload.get("state", {})
        if not isinstance(state, dict):
            state = {}

        applied_batch_id = payload.get("applied_batch_id")
        return state, applied_batch_id

    def save_state(self, state: dict[str, dict[str, Any]], applied_batch_id: Optional[str]):
        tmp_path = f"{self.state_path}.{os.getpid()}.tmp"
        payload = {
            "state": state,
            "applied_batch_id": applied_batch_id,
        }

        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(payload, file, separators=(",", ":"))
            file.flush()
            os.fsync(file.fileno())

        os.replace(tmp_path, self.state_path)

    def clear_client_state(self, client_id):
        if client_id is None:
            return

        str_key = str(client_id)
        state, applied_batch_id = self.recover_state()
        if str_key in state:
            del state[str_key]
            self.save_state(state, applied_batch_id)