import json
import os
from typing import Any


class PathsStateLogger:
    def __init__(self, worker_name: str, filename: str, base_dir: str = "/worker_logs"):
        worker_dir = os.path.join(base_dir, worker_name)
        os.makedirs(worker_dir, exist_ok=True)
        self.state_path = os.path.join(worker_dir, filename)

    def recover_state(self) -> tuple[dict[str, Any], str | None]:
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
        return state, payload.get("applied_batch_id")

    def save_state(self, state: dict[str, Any], applied_batch_id: str | None = None) -> None:
        tmp_path = f"{self.state_path}.{os.getpid()}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as file:
            json.dump(
                {"state": state, "applied_batch_id": applied_batch_id},
                file,
                separators=(",", ":"),
            )
            file.flush()
            os.fsync(file.fileno())

        os.replace(tmp_path, self.state_path)
