import yaml
import copy

# Config file
CONFIG_FILE = "aggregator_config.yaml"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src/aggregator"

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

## Aggregation operation
AGG_OP_TAG = "AGG_OP"
AGG_FIELD_TAG = "AGG_FIELD"
KEY_FIELD_TAG = "KEY_FIELD"
CARRY_FIELDS_TAG = "CARRY_FIELDS"
OUTPUT_TAG_TAG = "OUTPUT_TAG"
TOTAL_CLIENTS_TAG = "TOTAL_CLIENTS"


def get_aggregator_docker_services(service_prefix, total_instances,
                               input_queue=None, input_exchange=None,
                               output_queue=None, output_exchange=None,
                               agg_op="count", agg_field=None, key_field=None,
                               carry_fields=None, output_tag=None,
                               total_clients=0):
    
    # Open config file
    with open(CONFIG_FILE, "r") as config_file:
        base_aggregator_service = yaml.safe_load(config_file)

    # Create all services
    aggregator_services = {}

    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_aggregator_service)

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

        ## Aggregation operation
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{AGG_OP_TAG}={agg_op}")
        if agg_field is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{AGG_FIELD_TAG}={agg_field}")
        if key_field is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{KEY_FIELD_TAG}={key_field}")
        if carry_fields is not None:
            carry_fields_value = ",".join(carry_fields)
            new_service_config[DOCKER_ENV_VARS_NAME].append(
                f"{CARRY_FIELDS_TAG}={carry_fields_value}"
            )
        if output_tag is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_TAG_TAG}={output_tag}")

        if total_clients > 0:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TOTAL_CLIENTS_TAG}={total_clients}")

        # Add service in services dictionary
        aggregator_services[new_service_name] = new_service_config

    return aggregator_services