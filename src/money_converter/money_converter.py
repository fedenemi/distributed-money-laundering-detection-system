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
        self._current_prev_row_analyzed = 0
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
        return {"timestamp": timestamp, "origin": origin_curr, "destination": dest_curr}

    def process_main_input(self, data: dict) -> tuple[list, list]:
        # Get data elements
        data_copy = data.copy()
        timestamp = data["Timestamp"]
        day = str(timestamp).split(" ")[0].replace("/", "-")
        origin_curr = data["Payment Currency"]
        origin_code = CURRENCY_CODES.get(origin_curr, origin_curr)
        target_code = CURRENCY_CODES.get(self._target_currency, self._target_currency)
        rate_key = (origin_code, target_code)
        currency_rate_req = []

        if origin_code == target_code:
            data_copy["Payment Currency"] = self._target_currency
            self._emit_sec_output([data_copy])
            return ([], [])

        if origin_code == "BTC" and target_code == "USD":
            rate = self._btc_rates_by_day.get(day)
            if rate is None:
                logging.warning("BTC rate no disponible para %s", day)
                return ([], [])
            data_copy["Payment Currency"] = self._target_currency
            data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
            self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
            self._emit_sec_output([data_copy])
            return ([], [])

        if day not in self._currency_rates_by_date or \
                rate_key not in self._currency_rates_by_date[day]:
            currency_rate_req.append(
                self._generate_consult_currency_rate(day, origin_code, target_code)
            )
            return (currency_rate_req, [data_copy])

        rate = self._currency_rates_by_date[day][rate_key]
        data_copy["Payment Currency"] = self._target_currency
        data_copy["Amount Paid"] = float(data["Amount Paid"]) * rate
        self._log_conversion(day, origin_code, target_code, data["Amount Paid"], rate, data_copy["Amount Paid"])
        self._emit_sec_output([data_copy])
        return ([], [])


    def process_secondary_input(self, data: dict, prev_stage_data: list) -> tuple[list, list]:
        new_data_list = []

        if "Type" not in data:
            day = data["timestamp"]
            origin_code = data["origin"]
            target_code = data["destination"]
            currency_rate = data["conversion_rate"]

            self._currency_rates_by_date.setdefault(day, {})
            self._currency_rates_by_date[day][(origin_code, target_code)] = currency_rate

            for row in prev_stage_data:
                row_copy = row.copy()
                row_day = str(row_copy["Timestamp"]).split(" ")[0].replace("/", "-")
                row_origin = CURRENCY_CODES.get(row_copy["Payment Currency"], row_copy["Payment Currency"])

                if row_day == day and row_origin == origin_code:
                    amount_in = row_copy["Amount Paid"]
                    amount_out = currency_rate * float(row_copy["Amount Paid"])
                    row_copy["Amount Paid"] = str(amount_out)
                    row_copy["Payment Currency"] = self._target_currency
                    self._log_conversion(day, origin_code, target_code, amount_in, currency_rate, amount_out)

                new_data_list.append(row_copy)

        return ([], new_data_list)

    def on_eof(self):
        return []
    
if __name__ == "__main__":
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    money_converter = MoneyConverter()
    money_converter.run()