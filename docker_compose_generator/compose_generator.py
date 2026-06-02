import argparse
import yaml

from system.system_docker_generator import generate_system_docker_compose
from client.client_docker_service import get_clients_docker_services

def _normalize_container_path(path, default_root):
    if not path:
        return path
    normalized = path.replace("\\", "/")
    if normalized.endswith("/") and len(normalized) > 1:
        normalized = normalized.rstrip("/")
    if normalized.startswith("/"):
        return normalized
    if normalized.startswith("datasets/"):
        return f"/datasets/{normalized[len('datasets/'):] }"
    if normalized.startswith("results"):
        suffix = normalized[len("results"):].lstrip("/")
        return f"/results/{suffix}" if suffix else "/results"
    if default_root:
        return f"{default_root.rstrip('/')}/{normalized}"
    return normalized


def generate_docker_compose_file(
    total_clients,
    transactions_file,
    accounts_file,
    results_dir,
):
    services = {}

    # Create system
    system_services = generate_system_docker_compose(total_clients=total_clients)
    services = services | system_services

    # Create clients
    if total_clients > 0:
        if not transactions_file or not accounts_file or not results_dir:
            raise ValueError(
                "transactions_file, accounts_file y results_dir son requeridos"
            )
        transactions_file = _normalize_container_path(
            transactions_file, "/datasets"
        )
        accounts_file = _normalize_container_path(accounts_file, "/datasets")
        results_dir = _normalize_container_path(results_dir, "/results")
        clients_services = get_clients_docker_services(
            transactions_file,
            accounts_file,
            results_dir,
            total_clients=total_clients,
            batch_size="20000"
        )
        services = services | clients_services

    # Store YAML file
    docker_file = {"services" : services}
    with open("docker-compose.yaml", "w") as output_file:
        yaml.safe_dump(docker_file, output_file)

if __name__ == "__main__":
    # Create arguments parser
    argsparser = argparse.ArgumentParser()

    # Add number of clients
    argsparser.add_argument("--total_clients", type=int, default=0)
    argsparser.add_argument("--transactions", default="")
    argsparser.add_argument("--accounts", default="")
    argsparser.add_argument("--results_dir", default="/results")

    # Get arguments
    args = argsparser.parse_args()
    total_clients = args.total_clients

    # Execute generator
    generate_docker_compose_file(
        total_clients,
        args.transactions,
        args.accounts,
        args.results_dir,
    )