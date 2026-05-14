import datetime
from typing import Optional, Union


def parse_date(value: Union[str, datetime.date, datetime.datetime]) -> Optional[datetime.date]:
    """Parsea value y devuelve datetime.date (día sin horario).

    Soporta los formatos:
      - 'YYYY/MM/DD HH:MM'
      - 'YYYY/MM/DD'

    Devuelve None si no coincide o el tipo no es soportado.
    """
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.datetime):
        return value.date()

    if isinstance(value, str):
        for fmt in ("%Y/%m/%d %H:%M", "%Y/%m/%d"):
            try:
                return datetime.datetime.strptime(value, fmt).date()
            except Exception:
                pass
        return None

    return None