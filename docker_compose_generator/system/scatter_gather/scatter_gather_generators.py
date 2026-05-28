import os
import yaml
import copy

# Configs files
SUB_GRAPH_CONFIG_FILE = "sub_graph_agg_config.yaml"
PATHS_CREATOR_CONFIG_FILE = "paths_creator_config.yaml"
UNIQUE_PATHS_COUNTER_CONFIG_FILE = "unique_paths_counter_config.yaml"

CONFIGS_FILES = {
    "sub_graph_agg" : SUB_GRAPH_CONFIG_FILE,
    "paths_creator" : PATHS_CREATOR_CONFIG_FILE,
    "unique_paths_count" : UNIQUE_PATHS_COUNTER_CONFIG_FILE
}

BASE_DIR = os.path.dirname(__file__)

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
CONSUMER_GROUP_TAG = "CONSUMER_GROUP"
N_UPSTREAM_TAG="N_UPSTREAM"
SHARD_ID_TAG="SHARD_ID"
OUTPUT_QUEUE_TAG = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE_TAG = "OUTPUT_EXCHANGE"

TOTAL_CLIENTS_TAG = "TOTAL_CLIENTS"


def get_scatter_gather_services(
        service_prefix, 
        total_instances, 
        service_type, 
        input_queue=None, input_exchange=None,
        output_queue=None, output_exchange=None,
        n_upstream=None, 
        output_shards=None, 
        total_clients=0
        ):
    # Open config file
    config_file_name = os.path.join(BASE_DIR, CONFIGS_FILES[service_type])
    with open(config_file_name, "r") as config_file:
        base_scatter_gather_service = yaml.safe_load(config_file)

    # Create all services
    scatter_gather_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_scatter_gather_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add environment variables
        if n_upstream is not None:
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

        if total_clients > 0:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TOTAL_CLIENTS_TAG}={total_clients}")

        # Add service in services dictionary
        scatter_gather_services[new_service_name] = new_service_config

    return scatter_gather_services
