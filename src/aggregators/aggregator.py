"""
Aggregator genérico configurable por variables de entorno.
Stateful: recibe datos ya shardados por clave (el Splitter upstream
garantiza que todos los mensajes con la misma clave lleguen aquí).

Variables de entorno:
  AGG_OP: "max" | "avg" | "sum" | "count"
  AGG_FIELD: campo numérico a agregar (no requerido para "count")
  KEY_FIELD: campo que identifica la clave de acumulación (vacío = acumulación global)
  CARRY_FIELDS: campos adicionales a emitir junto al resultado, separados por coma
                (para "max": emite los campos del registro con el valor máximo)
  OUTPUT_TAG: si se define, agrega {"source": OUTPUT_TAG} al resultado

Ejemplos:
  # Max por banco (Q2), emitir también from_account
  AGG_OP=max  AGG_FIELD=amount  KEY_FIELD=from_bank  CARRY_FIELDS=from_account

  # Promedio por formato de pago (Q3), tagear como fuente A
  AGG_OP=avg  AGG_FIELD=amount  KEY_FIELD=payment_format  OUTPUT_TAG=A

  # Contador global (Q5)
  AGG_OP=count
"""
import logging
import os
import sys

from middleware.worker_base import WorkerBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Aggregator(WorkerBase):

    def __init__(self):
        super().__init__()
        self.op           = os.environ["AGG_OP"]
        self.agg_field    = os.environ.get("AGG_FIELD", "")
        self.key_field    = os.environ.get("KEY_FIELD", "")
        self.carry_fields = [
            f.strip() for f in os.environ.get("CARRY_FIELDS", "").split(",") if f.strip()
        ]
        self.output_tag   = os.environ.get("OUTPUT_TAG", "")
        self._state       = {}  # {key: accumulator}
        logger.info(
            f"Aggregator op={self.op} field={self.agg_field} "
            f"key={self.key_field or '(global)'}"
        )

    def _key(self, data: dict) -> str:
        return str(data.get(self.key_field, "__global__")) if self.key_field else "__global__"

    def process(self, data: dict) -> list:
        k = self._key(data)

        if self.op == "max":
            val = float(data.get(self.agg_field, float("-inf")))
            current = self._state.get(k)
            if current is None or val > current["val"]:
                self._state[k] = {"val": val, "row": data}

        elif self.op == "avg":
            val = float(data.get(self.agg_field, 0))
            if k not in self._state:
                self._state[k] = {"sum": 0.0, "count": 0}
            self._state[k]["sum"]   += val
            self._state[k]["count"] += 1

        elif self.op == "sum":
            val = float(data.get(self.agg_field, 0))
            self._state[k] = self._state.get(k, 0.0) + val

        elif self.op == "count":
            self._state[k] = self._state.get(k, 0) + 1

        return []

    def on_eof(self):
        count = 0
        for k, acc in self._state.items():
            result = {}
            if self.key_field:
                result[self.key_field] = k

            if self.op == "max":
                result[self.agg_field] = acc["val"]
                for f in self.carry_fields:
                    result[f] = acc["row"].get(f)

            elif self.op == "avg":
                avg = acc["sum"] / acc["count"] if acc["count"] else 0.0
                result[f"avg_{self.agg_field}"] = avg

            elif self.op == "sum":
                result[f"sum_{self.agg_field}"] = acc

            elif self.op == "count":
                result["count"] = acc

            if self.output_tag:
                result["source"] = self.output_tag

            count += 1
            yield result

        logger.info(f"Aggregator {self.op}: {count} claves emitidas")


if __name__ == "__main__":
    Aggregator().run()
