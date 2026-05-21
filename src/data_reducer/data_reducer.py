"""
DataReducer: reduce el tamaño de cada fila dejando solo las columnas requeridas.

Recibe mensajes con filas en formato dict, elimina las claves que no están en
KEEP_COLUMNS y reenvía solo las claves seleccionadas.

Variables de entorno:
  KEEP_COLUMNS: lista de columnas a mantener, separadas por comas
                (ej: "from_bank,to_bank,amount").

Comportamiento:
  - Stateless: escala libremente.
  - Si falta alguna clave especificada en KEEP_COLUMNS se lanza KeyError
    (esto permite que WorkerBase decida nack/retry).
"""

import os
import logging

from common.middleware.worker_base import WorkerBase

KEEP_COLUMNS = os.environ.get("KEEP_COLUMNS")

logger = logging.getLogger(__name__)


class DataReducer(WorkerBase):
    def __init__(self):
        if not KEEP_COLUMNS:
            raise RuntimeError("KEEP_COLUMNS no definida (ej: 'from_bank,to_bank,amount')")
        
        # lista de claves a conservar (orden preservado)
        self.keep_columns = [c.strip() for c in KEEP_COLUMNS.split(",") if c.strip()]
        
        if not self.keep_columns:
            raise RuntimeError("KEEP_COLUMNS está vacía")
        
        logger.info(f"DataReducer initialized with keep_columns: {self.keep_columns}")

        super().__init__()

    def process(self, data: dict):
        """Recibe una fila (dict). Devuelve [reduced_dict] con solo las claves indicadas."""
        if not isinstance(data, dict):
            raise TypeError("Data debe ser dict")

        # Si alguna clave no existe, propagamos KeyError para que WorkerBase pueda manejarla
        reduced = {k: data[k] for k in self.keep_columns}
        if "client_id" in data:
            reduced["client_id"] = data["client_id"]
        else:
            logger.warning("client_id no encontrado en data, no se incluirá en el resultado reducido")
        
        logger.info(f"Reducing data {data} to {reduced}")
        return [reduced]

    def on_eof(self, client_id=None):
        return []
