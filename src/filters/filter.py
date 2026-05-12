import os
import logging
import datetime

from common.middleware.worker_base import WorkerBase
from common.utils.dates import parse_date


FILTER_FIELD = os.environ["FILTER_FIELD"]        # either an int index (e.g. "2") or a dict key name (e.g. "country")
FILTER_OP = os.environ.get("FILTER_OP", "eq")    # eq, neq, lt, le, gt, ge, contains, startswith, endswith
FILTER_VALUE = os.environ["FILTER_VALUE"]        # comparison value (string/number)


class FilterWorker(WorkerBase):

    def __init__(self):
        # parse filter field (index or key)
        self.filter_field_index = None
        self.filter_field_key = None
        try:
            self.filter_field_index = int(FILTER_FIELD)
        except Exception:
            self.filter_field_key = FILTER_FIELD
        
        # parse filter value: int/float -> datetime (ISO) -> string
        try:
            if "." in FILTER_VALUE:
                self.filter_value = float(FILTER_VALUE)
            else:
                self.filter_value = int(FILTER_VALUE)
        except Exception:
            d = parse_date(FILTER_VALUE)
            if isinstance(d, datetime.date):
                self.filter_value = d
            else:
                self.filter_value = FILTER_VALUE
        
        # operator mapping
        ops = {
            "eq": lambda a, b: a == b,
            "neq": lambda a, b: a != b,
            "lt": lambda a, b: a < b,
            "le": lambda a, b: a <= b,
            "gt": lambda a, b: a > b,
            "ge": lambda a, b: a >= b,
            "contains": lambda a, b: (b in a) if a is not None else False,
            "startswith": lambda a, b: str(a).startswith(str(b)),
            "endswith": lambda a, b: str(a).endswith(str(b)),
        }
        
        self.op = ops.get(FILTER_OP.lower(), ops["eq"])

        super().__init__()


    def process(self, data: dict):
        """Receives a row as dict. Returns [data] if it passes the filter, [] if not."""
        
        target = None

        if self.filter_field_index is None and self.filter_field_key is None:
            raise RuntimeError("No FILTER_FIELD configurado (ni index ni key)")

        # detect target to evaluate by index
        if self.filter_field_index is not None:
            idx = self.filter_field_index
            if isinstance(data, dict):
                vals = list(data.values())
                if 0 <= idx < len(vals):
                    target = vals[idx]

        # detect target to evaluate by key
        elif self.filter_field_key is not None:
            key = self.filter_field_key
            if isinstance(data, dict):
                target = data.get(key)
        
        if not target:
            raise RuntimeError("Target to evaluate was not found.")

        # cast target to filter_value type
        try:
            if isinstance(self.filter_value, int):
                target_cast = int(target)
            elif isinstance(self.filter_value, float):
                target_cast = float(target)
            elif isinstance(self.filter_value, datetime.date):
                dt = parse_date(target)
                target_cast = dt if isinstance(dt, datetime.date) else target
            else:
                target_cast = target
        except Exception:
            target_cast = target

        try:
            if self.op(target_cast, self.filter_value):
                return [data]
            return []
        except Exception:
            logging.exception("Error evaluating filter")
            return []


    def on_eof(self):
        # no produce filas extra; WorkerBase propagará EOF según su política
        return []


if __name__ == "__main__":
    FilterWorker().run()