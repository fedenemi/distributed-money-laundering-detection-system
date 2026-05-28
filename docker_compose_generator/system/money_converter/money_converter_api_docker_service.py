import yaml
import copy
import os

# Config file
BASE_DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(BASE_DIR, "api_money_conversion_client_config.yaml")

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src"

# Container name
CONTAINER_NAME_TAG = "container_name"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
INPUT_QUEUE_TAG = "INPUT_QUEUE"
INPUT_EXCHANGE_TAG = "INPUT_EXCHANGE"
CONSUMER_GROUP_TAG = "CONSUMER_GROUP"
SHARD_ID_TAG="SHARD_ID"
N_UPSTREAM_TAG="N_UPSTREAM"
OUTPUT_QUEUE_TAG = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE_TAG = "OUTPUT_EXCHANGE"


def get_money_conversion_api_client_docker_services(service_prefix, total_instances,
                                    input_queue=None, input_exchange=None,
                                    output_queue=None, output_exchange=None,
                                    n_upstream=1, output_shards=1):
    with open(CONFIG_FILE, "r") as config_file:
        base_money_conversion_service = yaml.safe_load(config_file)

    # Create all services
    money_conversion_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_money_conversion_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{N_UPSTREAM_TAG}={n_upstream}")

        ## I/O
        if input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_QUEUE_TAG}={input_queue}")
        elif input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_EXCHANGE_TAG}={input_exchange}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{CONSUMER_GROUP_TAG}={service_prefix}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_ID_TAG}={i}")

        if output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE_TAG}={output_queue}")
        elif output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_EXCHANGE_TAG}={output_exchange}")
            if output_shards >= 1:
                new_service_config[DOCKER_ENV_VARS_NAME].append(f"OUTPUT_SHARDS={output_shards}")

        # Add service in services dictionary
        money_conversion_services[new_service_name] = new_service_config

    return money_conversion_services
