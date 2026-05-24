from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

import logging
import multiprocessing
import os

class BarrierFilter(WorkerBaseDoubleIO):
    def __init__(self):
        super().__init__()
        # Environment variables
        self._coeficient_comparison_value = float(os.environ["COEF"])

        # Initial state
        manager = multiprocessing.Manager()
        self._transactions_by_client = manager.dict()
        self._comparison_values_by_client = manager.dict()

    def process_main_input(self, data):
        logging.info(f"Nueva transacción recibida")
        client_id = data["client_id"]
        stored_transactions = self._transactions_by_client.get(client_id, [])
        stored_transactions.append(data)
        self._transactions_by_client[client_id] = stored_transactions
        return ([], [])

    def process_secondary_input(self, data, prev_stage_data):
        logging.info(f"Nuevo promedio recibido")
        # Getting values
        client_id = data["client_id"]
        payment_format = data["Payment Format"]
        avg_value = float(data["avg_Amount Paid"])

        # Calculating values
        threshold = avg_value * self._coeficient_comparison_value
        client_values = self._comparison_values_by_client.get(client_id, {})
        client_values[payment_format] = threshold
        self._comparison_values_by_client[client_id] = client_values

        logging.info(f"Promedio guardado")
        return ([], [])

    def on_both_eof_received(self, client_id=None):
        logging.info(f"Ambos EOF recibidos de cliente {client_id}")
        transactions = self._transactions_by_client.get(client_id, [])
        comparison_values = self._comparison_values_by_client.get(client_id, {})
        logging.info(f"Se tienen {len(transactions)} transacciones.")
        logging.info(f"Se tienen {len(comparison_values)} promedios.")

        for transaction in transactions:
            payment_method = transaction["Payment Format"]

            if payment_method in comparison_values:
                if float(transaction["Amount Paid"]) < comparison_values[payment_method]:
                    yield {
                        "From Bank": transaction["From Bank"],
                        "Account": transaction["Account"],
                        "Amount Paid": transaction["Amount Paid"]
                    }

        logging.info(f"Todas las transacciones enviadas")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    barrier_filter = BarrierFilter()
    barrier_filter.run()