import logging
import os
import shutil


WORKER_LOGS_DIR = os.environ.get("WORKER_LOGS_DIR", "/worker_logs")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not os.path.isdir(WORKER_LOGS_DIR):
        os.makedirs(WORKER_LOGS_DIR, exist_ok=True)
        logging.info("Worker logs dir creado: %s", WORKER_LOGS_DIR)
        return 0

    removed_entries = 0
    for entry in os.listdir(WORKER_LOGS_DIR):
        path = os.path.join(WORKER_LOGS_DIR, entry)
        try:
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
            removed_entries += 1
        except FileNotFoundError:
            continue

    logging.info(
        "Worker logs limpiados para nueva ejecucion: dir=%s entries=%s",
        WORKER_LOGS_DIR,
        removed_entries,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
