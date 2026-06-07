import logging

from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

def _normalize_bank_id(bank_id):
    if bank_id is None:
        return "0"
    normalized = str(bank_id).strip().lstrip("0")
    return normalized if normalized else "0"

class BankNameAdder(WorkerBaseDoubleIO):

    def process_main_input(self, data: dict) -> tuple[list, list]:
        bank_id = _normalize_bank_id(data.get("From Bank"))

        # Get shared lock for shared elements
        with self._shared_lock:
            # Check if bank name is in cache
            if bank_id in self._shared_cache:
                data["Bank Name"] = self._shared_cache[bank_id]
                return ([], [data]) 
            else:   # If not, add element as pending data
                pending_list = self._shared_pending.get(bank_id, [])
                pending_list.append(data)
                self._shared_pending[bank_id] = pending_list
                return ([], [])

    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        bank_id = _normalize_bank_id(data.get("From Bank"))
        bank_name = data.get("bank_name")

        resolved_messages = []

        # Get shared lock for shared elements
        with self._shared_lock:
            self._shared_cache[bank_id] = bank_name

            # Send elements if there are any pending name changes
            if bank_id in self._shared_pending:
                pending_list = self._shared_pending.pop(bank_id)
                for msg in pending_list:
                    msg["Bank Name"] = bank_name
                    resolved_messages.append(msg)

        return ([], resolved_messages)

    def on_main_input_eof(self, client_id=None) -> list:
        unmatched = []
        with self._shared_lock:
            for bank_id, rows in self._shared_pending.items():
                for row in rows:
                    row["Bank Name"] = bank_id 
                    unmatched.append(row)
            self._shared_pending.clear()
            
        if unmatched:
            self._emit_sec_output(unmatched)

        return []

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bank_name_adder = BankNameAdder()
    bank_name_adder.run()