#!/usr/bin/env python3
"""
Chaos monkey simple para probar tolerancia a fallos en servicios de docker compose.

El script lee una configuracion YAML, elige periodicamente un servicio permitido y
lo interrumpe. Esta pensado para ejecutarse desde la raiz del proyecto mientras el
sistema distribuido esta levantado.
"""

from __future__ import annotations

import argparse
import logging
import random
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG = Path("scripts/chaos_monkey.yaml")
DEFAULT_COMPOSE_FILE = Path("docker-compose.yaml")


@dataclass(frozen=True)
class ServiceCandidate:
    name: str
    weight: float = 1.0


class ChaosConfigError(ValueError):
    pass


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ChaosConfigError("La configuracion debe ser un objeto YAML.")
    return data


def _load_compose_services(compose_file: Path) -> list[str]:
    data = _load_yaml(compose_file)
    services = data.get("services", {})
    if not isinstance(services, dict):
        raise ChaosConfigError(f"{compose_file} no contiene una seccion services valida.")
    return sorted(services.keys())


def _as_range(value: Any, default: tuple[float, float], field_name: str) -> tuple[float, float]:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        number = float(value)
        return number, number
    if isinstance(value, list) and len(value) == 2:
        low, high = float(value[0]), float(value[1])
        if low > high:
            raise ChaosConfigError(f"{field_name} debe tener minimo <= maximo.")
        return low, high
    raise ChaosConfigError(f"{field_name} debe ser un numero o una lista [min, max].")


def _compile_patterns(values: Any, field_name: str) -> list[re.Pattern[str]]:
    if values is None:
        return []
    if not isinstance(values, list):
        raise ChaosConfigError(f"{field_name} debe ser una lista.")
    return [re.compile(str(value)) for value in values]


def _matches_any(service: str, patterns: list[re.Pattern[str]]) -> bool:
    return any(pattern.search(service) for pattern in patterns)


def _build_candidates(config: dict[str, Any], compose_services: list[str]) -> list[ServiceCandidate]:
    allowed = config.get("allowed_services", [])
    allowed_patterns = _compile_patterns(config.get("allowed_patterns", []), "allowed_patterns")
    excluded = set(str(service) for service in config.get("exclude_services", []))
    excluded_patterns = _compile_patterns(config.get("exclude_patterns", []), "exclude_patterns")

    if not isinstance(allowed, list):
        raise ChaosConfigError("allowed_services debe ser una lista.")

    weights = config.get("weights", {})
    if weights is None:
        weights = {}
    if not isinstance(weights, dict):
        raise ChaosConfigError("weights debe ser un diccionario servicio -> peso.")

    service_set = set(compose_services)
    selected: set[str] = set()

    for service in allowed:
        service_name = str(service)
        if service_name not in service_set:
            raise ChaosConfigError(f"El servicio permitido '{service_name}' no existe en el compose.")
        selected.add(service_name)

    for service_name in compose_services:
        if _matches_any(service_name, allowed_patterns):
            selected.add(service_name)

    candidates = []
    for service_name in sorted(selected):
        if service_name in excluded or _matches_any(service_name, excluded_patterns):
            continue
        weight = float(weights.get(service_name, 1.0))
        if weight <= 0:
            continue
        candidates.append(ServiceCandidate(service_name, weight))

    if not candidates:
        raise ChaosConfigError("No hay servicios candidatos. Revisar allowed_services/allowed_patterns.")

    return candidates


def _docker_compose_cmd(compose_file: Path, project_name: str | None, *args: str) -> list[str]:
    command = ["docker", "compose"]
    if project_name:
        command.extend(["-p", project_name])
    command.extend(args)
    return command


def _run(command: list[str], dry_run: bool, cwd: Path | None = None) -> None:
    logging.info("Ejecutando: %s", " ".join(command))
    if dry_run:
        return
    subprocess.run(command, check=True, cwd=cwd)


def _interrupt_service(
    service: str,
    action: str,
    compose_file: Path,
    project_name: str | None,
    downtime_seconds: float,
    dry_run: bool,
) -> None:
    cwd = compose_file.parent
    if action == "restart":
        _run(_docker_compose_cmd(compose_file, project_name, "restart", service), dry_run, cwd)
        return

    if action == "stop":
        _run(_docker_compose_cmd(compose_file, project_name, "stop", service), dry_run, cwd)
        logging.info("Servicio %s detenido sin reinicio automatico", service)
        return
    if action == "kill":
        _run(_docker_compose_cmd(compose_file, project_name, "kill", "-s", "SIGKILL", service), dry_run, cwd)
        logging.info("Servicio %s matado abruptamente sin reinicio automatico", service)
        return
    if action == "stop_start":
        _run(_docker_compose_cmd(compose_file, project_name, "stop", service), dry_run, cwd)
    elif action == "kill_start":
        _run(_docker_compose_cmd(compose_file, project_name, "kill", "-s", "SIGKILL", service), dry_run, cwd)
    else:
        raise ChaosConfigError("action debe ser restart, stop, kill, stop_start o kill_start.")

    logging.info("Servicio %s detenido por %.1fs", service, downtime_seconds)
    if not dry_run:
        time.sleep(downtime_seconds)

    _run(_docker_compose_cmd(compose_file, project_name, "start", service), dry_run, cwd)

def _pick(candidates: list[ServiceCandidate]) -> ServiceCandidate:
    return random.choices(
        candidates,
        weights=[candidate.weight for candidate in candidates],
        k=1,
    )[0]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chaos monkey para servicios de docker compose.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG, help="Archivo YAML de configuracion.")
    parser.add_argument("--compose-file", type=Path, default=None, help="Override del docker-compose.yaml.")
    parser.add_argument(
        "--service",
        action="append",
        default=[],
        help=(
            "Servicio exacto a interrumpir. Puede repetirse. "
            "Si se indica, reemplaza allowed_services/allowed_patterns del YAML."
        ),
    )
    parser.add_argument(
        "--pattern",
        action="append",
        default=[],
        help=(
            "Regex de servicios candidatos. Puede repetirse. "
            "Si se indica, reemplaza allowed_services/allowed_patterns del YAML."
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Muestra acciones sin ejecutarlas.")
    parser.add_argument("--once", action="store_true", help="Ejecuta una sola interrupcion y termina.")
    parser.add_argument("--seed", type=int, default=None, help="Semilla para reproducibilidad.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if args.seed is not None:
        random.seed(args.seed)

    config = _load_yaml(args.config)
    if not config.get("enabled", True):
        logging.info("Chaos monkey deshabilitado por configuracion.")
        return 0

    compose_file = args.compose_file or Path(config.get("compose_file", DEFAULT_COMPOSE_FILE))
    compose_file = compose_file.resolve()
    project_name = config.get("project_name")
    action = str(config.get("action", "stop_start"))
    interval_range = _as_range(config.get("interval_seconds"), (30.0, 60.0), "interval_seconds")
    downtime_range = _as_range(config.get("downtime_seconds"), (5.0, 15.0), "downtime_seconds")
    initial_delay = float(config.get("initial_delay_seconds", 0))
    max_events = config.get("max_events")
    max_events = None if max_events is None else int(max_events)
    if args.dry_run and not args.once and max_events is None:
        max_events = 1
        logging.info("Dry-run sin max_events: se ejecutara un solo evento de prueba.")

    if args.service or args.pattern:
        config["allowed_services"] = args.service
        config["allowed_patterns"] = args.pattern
        logging.info(
            "Candidatos override por CLI: services=%s patterns=%s",
            args.service or [],
            args.pattern or [],
        )

    compose_services = _load_compose_services(compose_file)
    candidates = _build_candidates(config, compose_services)
    stop_requested = False

    def _handle_signal(signum: int, _frame: Any) -> None:
        nonlocal stop_requested
        logging.info("Senal %s recibida; finalizando luego del evento actual.", signum)
        stop_requested = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logging.info("Servicios candidatos: %s", ", ".join(candidate.name for candidate in candidates))

    if initial_delay > 0 and not args.dry_run:
        logging.info("Esperando %.1fs antes de iniciar.", initial_delay)
        time.sleep(initial_delay)

    events = 0
    while not stop_requested:
        candidate = _pick(candidates)
        downtime = random.uniform(*downtime_range)
        logging.info("Evento #%s: interrumpiendo %s con action=%s", events + 1, candidate.name, action)
        _interrupt_service(candidate.name, action, compose_file, project_name, downtime, args.dry_run)
        events += 1

        if args.once or (max_events is not None and events >= max_events):
            break

        interval = random.uniform(*interval_range)
        if args.dry_run:
            logging.info("Dry-run: se omite espera de %.1fs", interval)
        else:
            logging.info("Proximo evento en %.1fs", interval)
            time.sleep(interval)

    logging.info("Chaos monkey finalizado despues de %s eventos.", events)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ChaosConfigError as error:
        logging.error("Configuracion invalida: %s", error)
        raise SystemExit(2)
    except subprocess.CalledProcessError as error:
        logging.error("Comando fallo con exit code %s: %s", error.returncode, " ".join(error.cmd))
        raise SystemExit(error.returncode)
