import yaml
import copy

# Config file
CONFIG_FILE = "data_reducer_config.yaml"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src/data_cleaner"

# Container name
CONTAINER_NAME_TAG = "container_name"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
INPUT_QUEUE_TAG = "INPUT_QUEUE"
INPUT_EXCHANGE_TAG = "INPUT_EXCHANGE"
SHARD_ID_TAG="SHARD_ID"
N_UPSTREAM_TAG="N_UPSTREAM"
OUTPUT_QUEUE_TAG = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE_TAG = "OUTPUT_EXCHANGE"

## Columns kept
KEEP_COLUMNS_TAG = "KEEP_COLUMNS"

def get_data_reducer_docker_services(service_prefix, total_instances, columns_kept,
                                    input_queue=None, input_exchange=None,
                                    output_queue=None, output_exchange=None):
    with open(CONFIG_FILE, "r") as config_file:
        base_data_cleaner_service = yaml.safe_load(config_file)

    # Create all services
    data_cleaner_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_data_cleaner_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        ## I/O
        if input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_QUEUE_TAG}={input_queue}")
        elif input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{INPUT_EXCHANGE_TAG}={input_exchange}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{N_UPSTREAM_TAG}={1}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_ID_TAG}={i}")

        if output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE_TAG}={output_queue}")
        elif output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_EXCHANGE_TAG}={output_exchange}")
        
        # Columns to keep
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{KEEP_COLUMNS_TAG}={",".join(columns_kept)}")

        # Add service in services dictionary
        data_cleaner_services[new_service_name] = new_service_config

    return data_cleaner_services