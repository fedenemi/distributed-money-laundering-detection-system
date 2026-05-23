"""
Splitter generico configurable por variables de entorno.
Sobreescribe _routing_key para decidir a que particion va cada mensaje.
Stateless: escala libremente.

Variables de entorno:
  SHARD_KEY_FIELD: campo del mensaje usado como clave de sharding
  SHARD_KEY_FIELDS: campos separados por coma (se concatenan como clave)
  TAG_SOURCE: si se define, agrega {"source": TAG_SOURCE} a cada msg
"""
import logging
import os
import hashlib


from common.middleware.worker_base import WorkerBase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Splitter(WorkerBase):

    def __init__(self):
        super().__init__()
        single = os.environ.get("SHARD_KEY_FIELD", "")
        multi  = os.environ.get("SHARD_KEY_FIELDS", "")
        if multi:
            self._key_fields = [f.strip() for f in multi.split(",") if f.strip()]
        elif single:
            self._key_fields = [single]
        else:
            raise ValueError("Se requiere SHARD_KEY_FIELD o SHARD_KEY_FIELDS")
        self._tag_source = os.environ.get("TAG_SOURCE", "")

    def _shard_key(self, msg: dict) -> str:
        return "".join(str(msg.get(f, "")) for f in self._key_fields)

    def _routing_key(self, msg: dict) -> str:
        key = self._shard_key(msg).encode()
        shard = int(hashlib.md5(key).hexdigest(), 16) % self.output_shards
        return str(shard)

    def process(self, data: dict) -> list:
        if self._tag_source:
            data = {**data, "source": self._tag_source}
        return [data]

    def on_eof(self, client_id=None):
        return iter([])


if __name__ == "__main__":
    Splitter().run()
