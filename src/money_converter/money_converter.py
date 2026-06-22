import csv
import logging
import os

from common.middleware.double_io_worker_base import WorkerBaseDoubleIO
from money_converter_logger import MoneyConverterLogger

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
        self._persistent_state_loaded = False
        self._processed_row_ids = set()
        self._main_row_to_mark = None
        self._main_pending_rows_to_mark = []
        self._main_state_dirty = False
        self._main_batch_state_dirty = False
        self._sec_state_dirty = False

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_main_worker_started(self):
        self._recover_persistent_state()

    def on_sec_worker_started(self):
        self._recover_persistent_state()

    def on_main_batch_complete(self):
        if self._main_batch_state_dirty:
            self._save_persistent_state()
        self._mark_rows_processed(self._main_pending_rows_to_mark)
        self._flush_all_main_buffer()
        self._main_pending_rows_to_mark = []
        self._main_batch_state_dirty = False

    def on_sec_batch_complete(self):
        return

    def on_main_row_complete(self):
        if self._main_row_to_mark is not None:
            if self._main_state_dirty:
                self._main_pending_rows_to_mark.append(self._main_row_to_mark)
                self._main_batch_state_dirty = True
            else:
                self._mark_row_processed(self._main_row_to_mark)
        self._main_row_to_mark = None
        self._main_state_dirty = False

    def on_sec_row_complete(self):
        if self._sec_state_dirty:
            self._save_persistent_state()
        self._sec_state_dirty = False

    def _state_logger(self) -> MoneyConverterLogger:
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        return MoneyConverterLogger("/worker_logs", worker_name)

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
        return MoneyConverterLogger.normalize_pending(pending)

    def _recover_persistent_state(self):
        if self._persistent_state_loaded:
            return

        cache, pending, processed_row_ids = self._state_logger().recover_state()
        with self._shared_lock:
            for rate_key, rate in cache.items():
                self._shared_cache[rate_key] = rate

            for rate_key, rows_by_id in pending.items():
                current = self._normalize_pending(self._shared_pending.get(rate_key, {}))
                current.update(rows_by_id)
                self._shared_pending[rate_key] = current

            self._processed_row_ids.update(processed_row_ids)

        self._persistent_state_loaded = True
        logging.info(
            "MoneyConverter recupero estado: cache=%s pending_keys=%s processed_rows=%s",
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
                rate_key: self._normalize_pending(rows_by_id)
                for rate_key, rows_by_id in dict(self._shared_pending).items()
            }

        self._state_logger().save_state(cache, pending)

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
        self._main_row_to_mark = None
        self._main_state_dirty = False
        data_copy = data.copy()
        row_id = self._pending_row_id()
        if row_id in self._processed_row_ids:
            return ([], [])

        timestamp = data["Timestamp"]
        day = str(timestamp).split(" ")[0].replace("/", "-")
        origin_curr = data["Payment Currency"]
        origin_code = CURRENCY_CODES.get(origin_curr, origin_curr)
        target_code = CURRENCY_CODES.get(self._target_currency, self._target_currency)
        
        rate_key = f"{day}_{origin_code}_{target_code}"

        if origin_code == target_code:
            data_copy["Payment Currency"] = self._target_currency
            self._main_row_to_mark = row_id
            return ([], [data_copy])

        if origin_code == "BTC" and target_code == "USD":
            rate = self._btc_rates_by_day.get(day)
            if rate is None:
                logging.info("BTC rate no disponible para %s", day)
                return ([], [])
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) / rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            self._main_row_to_mark = row_id
            return ([], [data_copy])

        if day in self._currency_rates_by_date and (origin_code, target_code) in self._currency_rates_by_date[day]:
            rate = self._currency_rates_by_date[day][(origin_code, target_code)]
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            self._main_row_to_mark = row_id
            return ([], [data_copy])

        with self._shared_lock:
            if rate_key in self._shared_cache:
                rate = self._shared_cache[rate_key]

                self._currency_rates_by_date.setdefault(day, {})
                self._currency_rates_by_date[day][(origin_code, target_code)] = rate
                data_copy["Payment Currency"] = self._target_currency
                data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
                self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
                self._main_row_to_mark = row_id
                should_emit_cached = True
            else:
                should_emit_cached = False

            if should_emit_cached:
                pass
            else:
                is_first_request = rate_key not in self._shared_pending
                pending_rows = self._normalize_pending(self._shared_pending.get(rate_key, {}))
                pending_rows[row_id] = data_copy
                self._shared_pending[rate_key] = pending_rows
                self._main_row_to_mark = row_id
                self._main_state_dirty = True

        if should_emit_cached:
            return ([], [data_copy])
            
        if is_first_request:
            req = self._generate_consult_currency_rate(day, origin_code, target_code)
            return ([req], [])

        return ([], [])


    def process_secondary_input(self, data: dict) -> tuple[list, list]:
        self._sec_state_dirty = False
        new_data_list = []

        if "Type" not in data:
            day = data["timestamp"]
            origin_code = data["origin"]
            target_code = data["destination"]
            currency_rate = data["conversion_rate"]
            rate_key = f"{day}_{origin_code}_{target_code}"

            with self._shared_lock:
                self._shared_cache[rate_key] = currency_rate
                pending_txs = list(
                    self._normalize_pending(self._shared_pending.pop(rate_key, {})).values()
                )
                self._sec_state_dirty = True

            for row in pending_txs:
                amount_in = row["Amount Paid"]
                amount_out = currency_rate * float(amount_in)
                row["Amount Paid"] = str(amount_out)
                row["Payment Currency"] = self._target_currency
                self._log_conversion(day, origin_code, target_code, amount_in, currency_rate, amount_out)
                new_data_list.append(row)

        return ([], new_data_list)

if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    money_converter = MoneyConverter()
    money_converter.run()
