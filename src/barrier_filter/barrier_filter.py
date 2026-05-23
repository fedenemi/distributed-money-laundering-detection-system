from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

import os

class BarrierFilter(WorkerBaseDoubleIO):
    def __init__(self):
        super().__init__()
        # Environment variables
        self._coeficient_comparison_value = os.environ["COEF"]

        # Initial state
        self._transactions_by_client = {}
        self._comparison_values_by_client = {}

    def process_main_input(self, data):
        client_id = data["client_id"]
        stored_transactions = self._transactions_by_client.setdefault(client_id, [])
        stored_transactions.append(data.copy())
        return ([], [])

    def process_secondary_input(self, data, prev_stage_data):
        client_id = data["client_id"]
        comparison_values = data["values"]
        new_stored_comparison_values = {}
        for method in comparison_values:
            value = float(comparison_values[method])
            new_stored_comparison_values[method] = self._coeficient_comparison_value * value
        self._comparison_values_by_client[client_id] = new_stored_comparison_values
        return ([], [])

    def on_both_eof_received(self, client_id=None):
        transactions = self._transactions_by_client[client_id]
        comparison_values = self._comparison_values_by_client[client_id]

        for transaction in transactions:
            payment_method = transaction["Payment Method"]

            if transaction["Amount Paid"] < comparison_values[payment_method]:
                yield {
                    transaction["From Bank"],
                    transaction["Account"],
                    transaction["Amount Paid"]
                }