import yaml
import copy
import os

# Config file
CONFIG_FILE = "filter_config.yaml"

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
OUTPUT_QUEUE_TAG = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE_TAG = "OUTPUT_EXCHANGE"

## Filter operation
FILTER_FIELD_TAG = "FILTER_FIELD"
FILTER_OP_TAG = "FILTER_OP"
FILTER_VALUE_TAG = "FILTER_VALUE"
TOTAL_CLIENTS_TAG = "TOTAL_CLIENTS"

def get_filters_docker_services(service_prefix, total_instances, filter_field,
                                filter_value, filter_op="eq",
                                input_queue=None, input_exchange=None,
                                output_queue=None, output_exchange=None,
                                n_upstream=1, output_shards=1,
                                total_clients=0):
    
    # Open config file
    base_path = os.path.dirname(__file__)
    config_file_path = os.path.join(base_path, CONFIG_FILE)

    with open(config_file_path, "r") as config_file:
        base_filter_service = yaml.safe_load(config_file)

    # Create all services
    filter_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_filter_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"N_UPSTREAM={n_upstream}")

        ## I/O
        if input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_QUEUE_TAG}={input_queue}")
        elif input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_EXCHANGE_TAG}={input_exchange}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{CONSUMER_GROUP_TAG}={service_prefix}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"SHARD_ID={i}")

        if output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE_TAG}={output_queue}")
        elif output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_EXCHANGE_TAG}={output_exchange}")
            if output_shards >= 1:
                new_service_config[DOCKER_ENV_VARS_NAME].append(f"OUTPUT_SHARDS={output_shards}")

        ## Filter operation
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{FILTER_FIELD_TAG}={filter_field}")
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{FILTER_OP_TAG}={filter_op}")
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{FILTER_VALUE_TAG}={filter_value}")

        if total_clients > 0:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TOTAL_CLIENTS_TAG}={total_clients}")

        # Add service in services dictionary
        filter_services[new_service_name] = new_service_config

    return filter_services
