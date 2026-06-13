import docker
import logging
import os
import signal
import socket
import time
import threading

from ring_election import RingElection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

HEALTH_PORT    = int(os.environ.get("HEALTH_PORT", "8888"))
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", "15"))
TIMEOUT        = float(os.environ.get("HEALTH_TIMEOUT", "50.0"))
MONITOR_ID     = int(os.environ.get("MONITOR_ID", "0"))
WORKERS        = [w.strip() for w in os.environ.get("WORKERS", "").split(",") if w.strip()]
SUCCESSORS     = [s.strip() for s in os.environ.get("SUCCESSORS", "").split(",") if s.strip()]

_docker = docker.from_env()
_failed_counts: dict[str, int] = {}
MAX_FAILURES = int(os.environ.get("MAX_FAILURES", "2"))


def _ping(host: str) -> bool:
    """Verifica si un worker está vivo via TCP — SIN docker."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        s.connect((host, HEALTH_PORT))
        data = s.recv(8)
        s.close()
        return data.strip() == b"OK"
    except Exception:
        return False


def _restart(container_name: str):
    """Reinicia el contenedor usando Docker API (permitido por la consigna)."""
    try:
        c = _docker.containers.get(container_name)
        c.restart()
        logger.info(f"[MONITOR] ↺ Restarted: {container_name}")
        _failed_counts[container_name] = 0
    except Exception as e:
        logger.error(f"[MONITOR] ✗ No se pudo reiniciar {container_name}: {e}")


def _check_all():
    for worker in WORKERS:
        try:
            c = _docker.containers.get(worker)
            if c.status == "exited" and c.attrs.get("State", {}).get("ExitCode") == 0:
                continue  # Ignorar workers que cerraron con éxito y terminaron su flujo
        except docker.errors.NotFound:
            pass
        except Exception as e:
            logger.debug(f"[MONITOR] No se pudo inspeccionar {worker}: {e}")

        alive = _ping(worker)
        if alive:
            _failed_counts[worker] = 0
        else:
            count = _failed_counts.get(worker, 0) + 1
            _failed_counts[worker] = count
            logger.warning(f"[MONITOR] ✗ {worker} sin respuesta ({count}/{MAX_FAILURES})")
            if count >= MAX_FAILURES:
                _restart(worker)


class Monitor:
    def __init__(self):
        self.ring     = RingElection(MONITOR_ID, SUCCESSORS)
        self.running  = True

    def run(self):
        threading.Thread(target=self.ring.run, daemon=True).start()
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)
        logger.info(f"[MONITOR] ID={MONITOR_ID} | Workers monitoreados: {len(WORKERS)}")

        while self.running:
            time.sleep(CHECK_INTERVAL)
            if self.ring.is_leader():
                _check_all()

    def _stop(self, *_):
        logger.info("[MONITOR] Cerrando...")
        self.running = False


if __name__ == "__main__":
    Monitor().run()
