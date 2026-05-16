import argparse
import yaml

from system.system_docker_generator import generate_system_docker_compose
from client.client_docker_service import get_clients_docker_services

def generate_docker_compose_file(total_clients):
    services = {}

    # Create system
    system_services = generate_system_docker_compose()
    services = services | system_services

    # Create clients
    if total_clients > 0:
        raise Exception("TODO: Clients services creation")
        clients_services = get_clients_docker_services("", "", total_clients=total_clients)
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

    # Get arguments
    args = argsparser.parse_args()
    total_clients = args.total_clients

    # Execute generator
    generate_docker_compose_file(total_clients)