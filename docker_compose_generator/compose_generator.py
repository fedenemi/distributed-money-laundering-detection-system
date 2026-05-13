import argparse
import yaml

from system.system_docker_generator import generate_systems_docker
from client.client_docker_service import get_clients_docker_services

def generate_docker_compose_file(total_clients):
    docker_file = {}

    # Create system
    system_services = generate_systems_docker()
    docker_file = docker_file | system_services

    # Create clients
    if total_clients > 0:
        clients_services = get_clients_docker_services("", "", total_clients=total_clients)
        docker_file = docker_file | clients_services

    # Store YAML file
    with open("../docker-compose.yaml") as output_file:
        yaml.safe_dump(docker_file, output_file)

if __name__ == "__main__":
    # Create arguments parser
    argsparser = argparse.ArgumentParser()

    # Add number of clients
    argsparser.add_argument("--total_clients", default=0)

    # Get arguments
    args = argsparser.parse_args()
    total_clients = args["total_clients"]

    # Execute generator
    generate_docker_compose_file(total_clients)