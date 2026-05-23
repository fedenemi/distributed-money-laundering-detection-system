import yaml
import copy
import os

BASE_DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(BASE_DIR, "splitter_config.yaml")

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

# Container name
CONTAINER_NAME_TAG = "container_name"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
INPUT_QUEUE_TAG = "INPUT_QUEUE"
INPUT_EXCHANGE_TAG = "INPUT_EXCHANGE"
OUTPUT_QUEUE_TAG = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE_TAG = "OUTPUT_EXCHANGE"

## Aggregation operation
SHARD_KEY_FIELD_TAG = "SHARD_KEY_FIELD"
SHARD_KEY_FIELDS_TAG = "SHARD_KEY_FIELDS"
TAG_SOURCE_TAG = "TAG_SOURCE"
TOTAL_CLIENTS_TAG = "TOTAL_CLIENTS"


def get_splitter_docker_services(service_prefix, total_instances,
                               input_queue=None, input_exchange=None,
                               output_queue=None, output_exchange=None,
                               key_field=None, key_fields=None,
                               source_tag=None, output_shards=None,
                               n_upstream=None, total_clients=0):
    
    # Open config file
    with open(CONFIG_FILE, "r") as config_file:
        base_splitter_service = yaml.safe_load(config_file)

    # Create all services
    splitter_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_splitter_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = "./src"
        new_service_config[DOCKER_BUILD_SECTION_NAME]["dockerfile"] = "aggregators/Dockerfile"
        new_service_config["entrypoint"] = ["python3", "/app/splitter.py"]

        # Add environment variables
        ## I/O
        if input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_QUEUE_TAG}={input_queue}")
        elif input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_EXCHANGE_TAG}={input_exchange}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"SHARD_ID={i}")

        if output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE_TAG}={output_queue}")
        elif output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_EXCHANGE_TAG}={output_exchange}")

        if n_upstream is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"N_UPSTREAM={n_upstream}")
        if output_shards is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"OUTPUT_SHARDS={output_shards}")

        ## Aggregation operation
        if key_field is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_KEY_FIELD_TAG}={key_field}")
        elif key_fields is not None:
            shard_key_fields_value = ",".join(key_fields)
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_KEY_FIELDS_TAG}={shard_key_fields_value}")

        if source_tag is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TAG_SOURCE_TAG}={source_tag}")

        if total_clients > 0:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TOTAL_CLIENTS_TAG}={total_clients}")

        # Add service in services dictionary
        splitter_services[new_service_name] = new_service_config

    return splitter_services