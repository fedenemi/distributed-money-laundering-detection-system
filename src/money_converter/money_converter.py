import csv
import logging
import os

from common.middleware.double_io_worker_base import WorkerBaseDoubleIO

TARGET_CURRENCY_TAG = "TARGET_CURRENCY"
BTC_RATES_PATH_TAG = "BTC_RATES_PATH"
CONVERSION_LOG_SAMPLES_TAG = "CONVERSION_LOG_SAMPLES"

CURRENCY_CODES = {
    "US Dollar": "USD", "Euro": "EUR", "Yuan": "CNY",
    "Ruble": "RUB", "Yen": "JPY", "UK Pound": "GBP",
    "Swiss Franc": "CHF", "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD", "Mexican Peso": "MXN",
    "Brazil Real": "BRL", "Rupee": "INR", "Saudi Riyal": "SAR",
    "Bitcoin": "BTC",
    "Shekel": "ILS",
}

class MoneyConverter(WorkerBaseDoubleIO):

    def __init__(self):
        super().__init__()

        # Get environment variables
        self._target_currency = os.environ[TARGET_CURRENCY_TAG]

        # Currency rates by date
        self._currency_rates_by_date = {}
        self._btc_rates_by_day = self._load_btc_rates()
        self._log_samples_remaining = int(os.environ.get(CONVERSION_LOG_SAMPLES_TAG, "20"))

    def _log_conversion(self, day, origin_code, target_code, amount_in, rate, amount_out):
        if self._log_samples_remaining <= 0:
            return
        logging.info(
            "Conversion sample: day=%s origin=%s target=%s amount_in=%s rate=%s amount_out=%s",
            day,
            origin_code,
            target_code,
            amount_in,
            rate,
            amount_out,
        )
        self._log_samples_remaining -= 1

    def _load_btc_rates(self):
        path = os.environ.get(BTC_RATES_PATH_TAG, "/btc_rates.csv")
        rates = {}
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    day = str(row.get("date", "")).strip().replace("/", "-")
                    rate = row.get("rate")
                    if day and rate:
                        rates[day] = float(rate)
        except Exception:
            logging.exception("No se pudo cargar BTC rates")
        return rates

    def _generate_consult_currency_rate(self, timestamp, origin_curr, dest_curr):
        return {"timestamp": timestamp, "origin": origin_curr, "destination": dest_curr, "sender_id": self.shard_id}

    def process_main_input(self, data: dict) -> tuple[list, list]:
        # Get data elements
        data_copy = data.copy()
        timestamp = data["Timestamp"]
        day = str(timestamp).split(" ")[0].replace("/", "-")
        origin_curr = data["Payment Currency"]
        origin_code = CURRENCY_CODES.get(origin_curr, origin_curr)
        target_code = CURRENCY_CODES.get(self._target_currency, self._target_currency)
        
        rate_key = f"{day}_{origin_code}_{target_code}"

        if origin_code == target_code:
            data_copy["Payment Currency"] = self._target_currency
            return ([], [data_copy])

        if origin_code == "BTC" and target_code == "USD":
            rate = self._btc_rates_by_day.get(day)
            if rate is None:
                logging.info("BTC rate no disponible para %s", day)
                return ([], [])
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            return ([], [data_copy])

        if day in self._currency_rates_by_date and (origin_code, target_code) in self._currency_rates_by_date[day]:
            rate = self._currency_rates_by_date[day][(origin_code, target_code)]
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            return ([], [data_copy])

        if not hasattr(self, "_local_pending"):
            self._local_pending = {}

        with self._shared_lock:
            if rate_key in self._shared_cache:
                rate = self._shared_cache[rate_key]

                self._currency_rates_by_date.setdefault(day, {})
                self._currency_rates_by_date[day][(origin_code, target_code)] = rate
                data_copy["Payment Currency"] = self._target_currency
                data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
                self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
                return ([], [data_copy])
            
            else:
                is_first_request = False
                if rate_key not in self._shared_pending and rate_key not in self._local_pending:
                    is_first_request = True

                self._local_pending.setdefault(rate_key, []).append(data_copy)

                if len(self._local_pending[rate_key]) >= 100:
                    pending_list = self._shared_pending.get(rate_key, [])
                    pending_list.extend(self._local_pending.pop(rate_key))
                    self._shared_pending[rate_key] = pending_list

        if is_first_request:
            req = self._generate_consult_currency_rate(day, origin_code, target_code)
            return ([req], [])

        return ([], [])


    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        new_data_list = []

        if "Type" not in data:
            day = data["timestamp"]
            origin_code = data["origin"]
            target_code = data["destination"]
            currency_rate = data["conversion_rate"]
            rate_key = f"{day}_{origin_code}_{target_code}"

            with self._shared_lock:
                self._shared_cache[rate_key] = currency_rate
                pending_txs = self._shared_pending.pop(rate_key, [])

            for row in pending_txs:
                amount_in = row["Amount Paid"]
                amount_out = currency_rate * float(amount_in)
                row["Amount Paid"] = str(amount_out)
                row["Payment Currency"] = self._target_currency
                self._log_conversion(day, origin_code, target_code, amount_in, currency_rate, amount_out)
                new_data_list.append(row)

        return ([], new_data_list)

    def on_main_input_eof(self, client_id=None) -> list:
        if hasattr(self, "_local_pending") and self._local_pending:
            with self._shared_lock:
                for rate_key, rows in self._local_pending.items():
                    if rate_key in self._shared_cache:
                        rate = self._shared_cache[rate_key]
                        out_rows = []
                        for row in rows:
                            row["Payment Currency"] = self._target_currency
                            row["Amount Paid"] = float(row["Amount Paid"]) * rate
                            out_rows.append(row)
                        self._emit_sec_output(out_rows)

                    else:
                        pending_list = self._shared_pending.get(rate_key, [])
                        pending_list.extend(rows)
                        self._shared_pending[rate_key] = pending_list

            self._local_pending.clear()
            
        return []
    
if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    money_converter = MoneyConverter()
    money_converter.run()