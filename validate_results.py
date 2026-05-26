#!/usr/bin/env python3
"""
Valida los resultados del sistema distribuido comparando con una
ejecucion serial del mismo CSV.

Uso:
  python validate_results.py \
      --transactions data/LI-Small_Trans.csv \
      --accounts data/LI-Small_accounts.csv \
      --results-dir results \
      [--show-diff]
"""
import argparse
import csv
import os
import sys
import requests
from datetime import datetime, date
from collections import defaultdict
from functools import lru_cache


# Helpers ----------------------------------------------------------

def parse_date(ts):
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(ts.strip(), fmt).date()
        except ValueError:
            pass
    return None

def in_period(ts, s, e):
    d = parse_date(ts)
    return d and date.fromisoformat(s) <= d <= date.fromisoformat(e)

CURRENCY_CODES = {
    "US Dollar": "USD", "Euro": "EUR", "Yuan": "CNY",
    "Ruble": "RUB", "Yen": "JPY", "UK Pound": "GBP",
    "Swiss Franc": "CHF", "Australian Dollar": "AUD",
    "Canadian Dollar": "CAD", "Mexican Peso": "MXN",
    "Brazil Real": "BRL", "Rupee": "INR", "Saudi Riyal": "SAR",
    "Bitcoin": "BTC",
    "Shekel": "ILS",
}

BTC_RATES_PATH = os.path.join(
    os.path.dirname(__file__), "src", "money_converter", "btc_rates.csv"
)


@lru_cache(maxsize=1)
def _load_btc_rates():
    rates = {}
    try:
        with open(BTC_RATES_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                day = str(row.get("date", "")).strip().replace("/", "-")
                rate = row.get("rate")
                if day and rate:
                    rates[day] = float(rate)
    except Exception:
        return {}
    return rates


def _get_btc_rate(day):
    return _load_btc_rates().get(day)

@lru_cache(maxsize=1024)
def get_rate(from_code, day):
    if from_code == "USD":
        return 1.0
    try:
        url = f"https://api.frankfurter.dev/v2/rate/{from_code}/USD?date={day}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()["rate"]
    except Exception:
        return None

def to_usd(amount, currency, timestamp):
    code = CURRENCY_CODES.get(currency)
    if code is None:
        return None
    if code == "USD":
        return amount
    if code == "BTC":
        day = parse_date(timestamp).isoformat()
        rate = _get_btc_rate(day)
        return amount * rate if rate else None
    day = parse_date(timestamp).isoformat()
    rate = get_rate(code, day)
    return amount * rate if rate else None


# Carga de datos ----------------------------------------------------------

def load_transactions(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            try:
                rows.append({
                    "timestamp":        r["Timestamp"],
                    "from_bank":        r["From Bank"],
                    "from_account":     r["Account"],
                    "to_bank":          r["To Bank"],
                    "to_account":       r["Account.1"],
                    "amount":           float(r["Amount Paid"]),
                    "payment_currency": r["Payment Currency"],
                    "payment_format":   r["Payment Format"],
                })
            except Exception:
                pass
    print(f"  {len(rows)} transacciones cargadas")
    return rows

def load_accounts(path):
    mapping = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            mapping[r["Bank ID"]] = r["Bank Name"]
    print(f"  {len(mapping)} cuentas cargadas")
    return mapping


# Calculos seriales ----------------------------------------------------------

def serial_q1(rows):
    return [
        {
        "from_bank":    r["from_bank"],
        "from_account": r["from_account"],
        "to_bank":      r["to_bank"],
        "to_account":   r["to_account"],
        "amount":       r["amount"]}
        for r in rows
        if r["payment_currency"] == "US Dollar" and r["amount"] < 50
    ]

def serial_q2(rows, accounts):
    best = {}
    for r in rows:
        if r["payment_currency"] != "US Dollar":
            continue
        raw_bank_id = r["from_bank"]
        bank_id = str(raw_bank_id).strip()
        normalized_bank_id = bank_id.lstrip("0") or "0"
        bank_name = accounts.get(bank_id)
        if bank_name is None:
            bank_name = accounts.get(normalized_bank_id, raw_bank_id)
        if bank_id not in best or r["amount"] > best[bank_id]["amount"]:
            best[bank_id] = {
                "bank_name":    bank_name,
                "from_account": r["from_account"],
                "amount":       r["amount"],
            }
    return list(best.values())

def serial_q3(rows):
    usd = [r for r in rows if r["payment_currency"] == "US Dollar"]
    period_a = [r for r in usd if in_period(r["timestamp"], "2022-09-01", "2022-09-05")]
    period_b = [r for r in usd if in_period(r["timestamp"], "2022-09-06", "2022-09-15")]
    acc = defaultdict(lambda: {"s": 0.0, "n": 0})
    for r in period_a:
        acc[r["payment_format"]]["s"] += r["amount"]
        acc[r["payment_format"]]["n"] += 1
    avgs = {f: v["s"] / v["n"] for f, v in acc.items() if v["n"]}
    return [
        {
            "from_bank": r["from_bank"],
            "from_account": r["from_account"],
            "payment_format": r["payment_format"],
            "amount": r["amount"],
        }
        for r in period_b
        if r["payment_format"] in avgs
        and r["amount"] < avgs[r["payment_format"]] * 0.01
    ]

def serial_q4(rows):
    usd_a = [r for r in rows
             if r["payment_currency"] == "US Dollar"
             and in_period(r["timestamp"], "2022-09-01", "2022-09-05")]
    out_e = defaultdict(set)
    in_e  = defaultdict(set)
    for r in usd_a:
        out_e[r["from_account"]].add(r["to_account"])
        in_e[r["to_account"]].add(r["from_account"])
    pairs = defaultdict(set)
    for b, origins in in_e.items():
        for a in origins:
            for c in out_e.get(b, set()):
                if a != c:
                    pairs[(a, c)].add(b)
    return [
        {"origin": a, "destination": c, "n_intermediaries": len(bs)}
        for (a, c), bs in pairs.items()
        if len(bs) >= 5 and len(out_e.get(a, set())) >= 5
    ]

def serial_q5(rows):
    cnt = 0
    for r in rows:
        if not in_period(r["timestamp"], "2022-09-01", "2022-09-05"):
            continue
        if r["payment_format"] not in ("Wire", "ACH"):
            continue
        usd = to_usd(r["amount"], r["payment_currency"], r["timestamp"])
        if usd is not None and usd < 1:
            cnt += 1
    return [{"count": cnt}]


# Comparacion -----------------------------------------------------------

def normalize(rows):
    result = set()
    for r in rows:
        items = []
        for k, v in r.items():
            if isinstance(v, float):
                v = round(v, 4)
            items.append((k, str(v)))
        result.add(tuple(sorted(items)))
    return result

def compare(q, expected, actual, show_diff):
    es  = normalize(expected)
    as_ = normalize(actual)
    if es == as_:
        print(f"  OK {q}: {len(expected)} filas")
        return True
    miss  = es - as_
    extra = as_ - es
    print(f"  FALLO {q}: esperados={len(expected)} obtenidos={len(actual)}")
    if show_diff:
        for r in list(miss)[:3]:
            print(f"    FALTA: {dict(r)}")
        for r in list(extra)[:3]:
            print(f"    SOBRA: {dict(r)}")
    return False


def _parse_float(value):
    try:
        return float(str(value).replace(",", "."))
    except Exception:
        return value


def _is_numeric_key(key):
    if key in {"amount", "count", "n_intermediaries"}:
        return True
    if key.startswith("avg_") or key.startswith("sum_"):
        return True
    return False


def _find_results_csv(results_dir, q):
    qnum = q[1:] if q.startswith("q") else q
    path = os.path.join(results_dir, f"results_q{qnum}.csv")
    return [path] if os.path.exists(path) else []


def _csv_row_to_result(q, row, expected_keys):
    if expected_keys:
        values = row[:len(expected_keys)]
        mapped = {}
        for k, v in zip(expected_keys, values):
            mapped[k] = _parse_float(v) if _is_numeric_key(k) else v
        return mapped

    return {f"col_{i}": _parse_float(v) for i, v in enumerate(row)}


def load_results_from_csv(results_paths, q, expected):
    expected_keys = list(expected[0].keys()) if expected else []
    rows = []
    for path in results_paths:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                if expected_keys and row == expected_keys:
                    continue
                rows.append(_csv_row_to_result(q, row, expected_keys))
    return rows


#----------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--transactions", required=True, help="Path a LI-Small_Trans.csv")
    ap.add_argument("--accounts",     required=True, help="Path a LI-Small_accounts.csv")
    ap.add_argument("--results-dir",  required=True, help="Directorio con los CSV de resultados (results_q*.csv)")
    ap.add_argument("--show-diff",    action="store_true")
    args = ap.parse_args()

    print("Cargando datos...")
    rows     = load_transactions(args.transactions)
    accounts = load_accounts(args.accounts)

    print("\nCalculando resultados seriales...")
    serial = {
        "q1": serial_q1(rows),
        "q2": serial_q2(rows, accounts),
        "q3": serial_q3(rows),
        "q4": serial_q4(rows),
        "q5": serial_q5(rows),
    }
    for q, res in serial.items():
        print(f"  {q}: {len(res)} filas")

    print("\nComparando con resultados del sistema distribuido...")
    all_ok = True
    for q, expected in serial.items():
        paths = _find_results_csv(args.results_dir, q)
        if not paths:
            expected_path = os.path.join(args.results_dir, f"results_q{q[1:]}.csv")
            print(f"  FALTA {q}: archivo no encontrado ({expected_path})")
            all_ok = False
            continue
        actual = load_results_from_csv(paths, q, expected)
        ok = compare(q, expected, actual, args.show_diff)
        all_ok = all_ok and ok

    print()
    if all_ok:
        print("Todos los resultados son correctos.")
        sys.exit(0)
    else:
        print("Hay diferencias en los resultados.")
        sys.exit(1)


if __name__ == "__main__":
    main()
