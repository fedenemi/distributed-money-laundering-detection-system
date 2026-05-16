import yaml

CONFIG_FILE = "rabbitmq_config.yaml"

def get_rabbitmq_docker_service():
    with open(CONFIG_FILE, "r") as config_file:
        rabbitmq_service = yaml.safe_load(config_file)
    return rabbitmq_service