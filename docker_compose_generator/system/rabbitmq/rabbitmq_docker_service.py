import os
import yaml

CONFIG_FILE = "rabbitmq_config.yaml"

def get_rabbitmq_docker_service():
    base_path = os.path.dirname(__file__)
    config_file_path = os.path.join(base_path, CONFIG_FILE)
    with open(config_file_path, "r") as config_file:
        rabbitmq_service = yaml.safe_load(config_file)
    return rabbitmq_service