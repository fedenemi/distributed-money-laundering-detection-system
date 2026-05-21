"""
FilterWorker: filtra filas según una condición configurable por variables de entorno.

Recibe filas en formato dict y decide si reenviarlas o descartarlas según:
  - FILTER_FIELD: clave del dict a evaluar
  - FILTER_OP   : operador (eq, neq, lt, le, gt, ge, contains, startswith, endswith, in)
  - FILTER_VALUE: valor de comparación (número, fecha 'YYYY/MM/DD HH:MM'|'YYYY/MM/DD', o JSON list)
  - DROP_FILTER_FIELD: indica si se dropea la clave evaluada de la data transmitida. Solo acepta ("True", "False").
  
Comportamiento:
  - Stateless: escala libremente.
  - Si falta el campo objetivo lanza KeyError/IndexError/TypeError (WorkerBase decide nack/retry).
  - Si FILTER_VALUE es un intervalo de fechas o un conjunto de valores, requiere FILTER_OP == "in".
"""

import os
import logging
import datetime
import json
from typing import Any, Optional, Set, Tuple

from common.middleware.worker_base import WorkerBase
from common.utils.dates import parse_date

logger = logging.getLogger(__name__)

FILTER_FIELD = os.environ["FILTER_FIELD"]
FILTER_OP = os.environ.get("FILTER_OP", "eq").lower()
FILTER_VALUE = os.environ["FILTER_VALUE"]
DROP_FILTER_FIELD = os.environ.get("DROP_FILTER_FIELD", "false")


def _parse_filter_value(raw: str) -> Tuple[Any, Optional[Set[str]], bool, Optional[datetime.date], Optional[datetime.date]]:
    """
    Parse FILTER_VALUE raw string.
    Returns (filter_value, value_set, is_date_range, date_start, date_end).

    - If numeric -> filter_value = int/float
    - If JSON list:
        * length 2 and both parseable as dates -> is_date_range True and date_start/date_end set
        * otherwise -> value_set is set(parsed)
    - Else try single date -> filter_value = datetime.date
    - Else -> filter_value = raw string
    """
    # defaults
    value_set = None
    date_start = date_end = None

    # try numeric
    try:
        if "." in raw:
            return float(raw), None, False, None, None
        return int(raw), None, False, None, None
    except Exception:
        pass

    # try JSON list
    parsed = None
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        if len(parsed) == 2:
            d0 = parse_date(parsed[0])
            d1 = parse_date(parsed[1])
            if isinstance(d0, datetime.date) and isinstance(d1, datetime.date):
                # normalize
                if d0 <= d1:
                    date_start, date_end = d0, d1
                else:
                    date_start, date_end = d1, d0
                return ( (date_start, date_end), None, True, date_start, date_end )
        # fallback to value set
        value_set = set(parsed)
        return parsed, value_set, False, None, None

    # try single date
    d = parse_date(raw)
    if isinstance(d, datetime.date):
        return d, None, False, None, None

    # fallback plain string
    return raw, None, False, None, None


def _extract_target(data: dict, field_key: Optional[str]) -> Any:
    """Extract target value from dict by key only.
    Raises TypeError/KeyError/RuntimeError on missing/unexpected input.
    """
    if field_key is None:
        raise RuntimeError("FILTER_FIELD debe ser una clave (string)")

    if not isinstance(data, dict):
        raise TypeError("Se esperaba dict cuando FILTER_FIELD es una clave")
    if field_key not in data:
        raise KeyError(f"Clave '{field_key}' no encontrada en la fila")
    return data[field_key]


def _target_matches_date_range(target: Any, start: datetime.date, end: datetime.date) -> bool:
    tdate = parse_date(target)
    if not isinstance(tdate, datetime.date):
        return False
    return start <= tdate <= end


def _target_in_value_set(target: Any, value_set: Set[Any]) -> bool:
    if target in value_set:
        return True
    # fallback string comparison
    target_s = str(target)
    return any(target_s == str(v) for v in value_set)


class FilterWorker(WorkerBase):

    def __init__(self):
        # parse filter field (key)
        self.filter_field_key = FILTER_FIELD

        # whether to drop the filter field from the row when forwarding
        self.drop_filter_field = str(DROP_FILTER_FIELD).strip().lower() == "true"

        # parse FILTER_VALUE into structured form
        (self.filter_value,
         self._value_set,
         self._is_date_range,
         self._date_start,
         self._date_end) = _parse_filter_value(FILTER_VALUE)

        # if date range was provided, require operator "in"
        if self._is_date_range and FILTER_OP != "in":
            raise ValueError("FILTER_VALUE es un intervalo de fechas: use FILTER_OP='in' para habilitar el filtrado por rango")

        # if value set was provided, require operator "in"
        if self._value_set is not None and FILTER_OP != "in":
            raise ValueError("FILTER_VALUE es un conjunto de valores: use FILTER_OP='in' para habilitar el filtrado por pertenencia")

        # operator mapping (regular comparisons)
        self._ops = {
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
        self.op_func = self._ops.get(FILTER_OP, self._ops["eq"])

        logger.info(f"Initialized FilterWorker with FILTER_FIELD='{FILTER_FIELD}', FILTER_OP='{FILTER_OP}', FILTER_VALUE='{FILTER_VALUE}', DROP_FILTER_FIELD={self.drop_filter_field}")

        super().__init__()


    def _maybe_drop_field(self, data: dict):
        """Drop the configured filter field from data in-place if enabled."""
        if not self.drop_filter_field:
            return
        try:
            data.pop(self.filter_field_key, None)
        except Exception:
            logger.exception("Error al dropear el campo de filtro")


    def process(self, data: dict):
        """Recibe fila dict. Devuelve [data] si cumple, [] si no. Lanza errores para WorkerBase."""
        
        if not isinstance(data, dict):
            raise TypeError("Se esperaba dict como input para el filtro")
        
        if "client_id" not in data:
            logger.warning("client_id no encontrado en data")
        
        target = _extract_target(data, self.filter_field_key)

        # date-range matching (requires FILTER_OP == 'in' by init check)
        if self._is_date_range:
            if _target_matches_date_range(target, self._date_start, self._date_end):
                self._maybe_drop_field(data)
                logger.info(f"Row matches date range filter: {target} in [{self._date_start}, {self._date_end}]")
                return [data]
            return []

        # explicit value set membership
        if self._value_set is not None:
            if _target_in_value_set(target, self._value_set):
                self._maybe_drop_field(data)
                logger.info(f"Row matches value set filter: {target} in {self._value_set}")
                return [data]
            return []

        # regular comparison
        target_cast = target
        try:
            if isinstance(self.filter_value, int):
                try:
                    target_cast = int(target)
                except Exception:
                    target_cast = float(str(target).replace(",", "."))
            elif isinstance(self.filter_value, float):
                target_cast = float(str(target).replace(",", "."))
            elif isinstance(self.filter_value, datetime.date):
                dt = parse_date(target)
                if isinstance(dt, datetime.date):
                    target_cast = dt
        except Exception:
            target_cast = target

        try:
            if self.op_func(target_cast, self.filter_value):
                self._maybe_drop_field(data)
                logger.info(f"Row matches filter: {target_cast} {FILTER_OP} {self.filter_value}")
                return [data]
            return []
        except Exception:
            logger.exception("Error evaluating filter")
            return []


    def on_eof(self, client_id=None):
        return []
