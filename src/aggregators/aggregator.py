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
sys.path.insert(0, "/app") 
sys.path.insert(0, "/app/common") 
from aggregator_logger import AggregatorLogger
from common.middleware.worker_base import WorkerBase

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
        self._state       = {}  # ahora será {client_id: {key: accumulator}}
        worker_name = f"{self.consumer_group}_{self.shard_id}"
        self._state_logger = AggregatorLogger(worker_name)
        self._applied_batch_id = None
        logger.info(
            f"Aggregator op={self.op} field={self.agg_field} "
            f"key={self.key_field or '(global)'}"
        )

    def on_worker_started(self):
        self._state, self._applied_batch_id = self._state_logger.recover_state()
        if self._state:
            logger.info(
                "Aggregator recupero estado: clients=%s applied_batch_id=%s",
                len(self._state),
                self._applied_batch_id,
            )

    def supports_partial_batch_resume(self) -> bool:
        return False

    def on_batch_complete(self, batch_id: str):
        self._applied_batch_id = batch_id
        self._state_logger.save_state(self._state, self._applied_batch_id)

    def on_eof_complete(self, client_id=None):
        if client_id is None:
            self._state.clear()
        else:
            self._state.pop(self._client_key(client_id), None)
        self._state_logger.save_state(self._state, self._applied_batch_id)

    def _key(self, data: dict) -> str:
        return str(data.get(self.key_field, "__global__")) if self.key_field else "__global__"

    def _client_key(self, client_id) -> str:
        return "__global__" if client_id is None else str(client_id)

    def _ensure_client_state(self, client_key: str):
        if client_key not in self._state:
            self._state[client_key] = {}

    def process(self, data: dict) -> list:
        if getattr(self, "_current_msg_hash", None) == self._applied_batch_id:
            return []

        client_key = self._client_key(data.get("client_id", "__global__"))
        self._ensure_client_state(client_key)
        state = self._state[client_key]
        k = self._key(data)

        if self.op == "max":
            val = float(data.get(self.agg_field, float("-inf")))
            current = state.get(k)
            if current is None or val > current["val"]:
                state[k] = {"val": val, "row": data}

        elif self.op == "avg":
            val = float(data.get(self.agg_field, 0))
            if k not in state:
                state[k] = {"sum": 0.0, "count": 0}
            state[k]["sum"]   += val
            state[k]["count"] += 1

        elif self.op == "sum":
            val = float(data.get(self.agg_field, 0))
            logger.info(f"Adding to sum key: {k} value: {val} (current sum: {state.get(k, 0.0)})")
            state[k] = state.get(k, 0.0) + val

        elif self.op == "count":
            logger.info(f"Counting key: {k} (current count: {state.get(k, 0)})")
            state[k] = state.get(k, 0) + 1

        return []

    def on_eof(self, client_id=None):
        if client_id is None:
            # EOF global (sin cliente): combina todos los clientes
            all_results = []
            for cid, cstate in self._state.items():
                for k, acc in cstate.items():
                    all_results.append(self._build_result(k, acc, cid))
            self._state.clear()
            yield from all_results
        else:
            # EOF de un cliente específico
            cstate = self._state.get(self._client_key(client_id), {})
            for k, acc in cstate.items():
                yield self._build_result(k, acc, client_id)

    def on_clean_client_data(self, client_id=None):
        if client_id is not None:
            pass

    def _build_result(self, key, acc, client_id):
        result = {}
        if self.key_field:
            result[self.key_field] = key

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

        result["client_id"] = client_id  

        return result

if __name__ == "__main__":
    Aggregator().run()
