import copy
import os
import yaml

# Config file
BASE_DIR = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(BASE_DIR, "money_converter_config.yaml")

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

# Container name
CONTAINER_NAME_TAG = "container_name"
# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
MAIN_INPUT_QUEUE_TAG = "MAIN_INPUT_QUEUE"
MAIN_INPUT_EXCHANGE_TAG = "MAIN_INPUT_EXCHANGE"
SEC_INPUT_QUEUE_TAG = "SECONDARY_INPUT_QUEUE"
SEC_INPUT_EXCHANGE_TAG = "SECONDARY_INPUT_EXCHANGE"
CONSUMER_GROUP_TAG = "CONSUMER_GROUP"
SHARD_ID_TAG="SHARD_ID"
MAIN_N_UPSTREAM_TAG="MAIN_N_UPSTREAM"
SEC_N_UPSTREAM_TAG="SECONDARY_N_UPSTREAM"
MAIN_OUTPUT_QUEUE_TAG = "MAIN_OUTPUT_QUEUE"
MAIN_OUTPUT_EXCHANGE_TAG = "MAIN_OUTPUT_EXCHANGE"
SEC_OUTPUT_QUEUE_TAG = "SECONDARY_OUTPUT_QUEUE"
SEC_OUTPUT_EXCHANGE_TAG = "SECONDARY_OUTPUT_EXCHANGE"

## Target currency
TARGET_CURRENCY_TAG = "TARGET_CURRENCY"

def get_money_converters_services(service_prefix, total_instances, target_currency,
                        main_input_queue=None, main_input_exchange=None, main_n_upstream=None,
                        sec_input_queue=None, sec_input_exchange=None, sec_n_upstream=None,
                        main_output_queue=None, main_output_exchange=None,
                        sec_output_queue=None, sec_output_exchange=None,
                        main_output_shards=1, sec_output_shards=1,
                        ):
    with open(CONFIG_FILE, "r") as config_file:
        base_money_converter_service = yaml.safe_load(config_file)

    # Create all services
    aggregator_services = {}
    for i in range(total_instances):
        # Copy service base configuration
        new_service_config = copy.deepcopy(base_money_converter_service)

        # Add container name
        new_service_name = f"{service_prefix}_{i}"
        new_service_config[CONTAINER_NAME_TAG] = new_service_name

        # Add context folder
        new_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = "./src"

        # Add environment variables
        ## I/O
        if main_input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{MAIN_INPUT_QUEUE_TAG}={main_input_queue}")
        elif main_input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{MAIN_INPUT_EXCHANGE_TAG}={main_input_exchange}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{CONSUMER_GROUP_TAG}={service_prefix}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_ID_TAG}={i}")

        if sec_input_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SEC_INPUT_QUEUE_TAG}={sec_input_queue}")
        elif sec_input_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SEC_INPUT_EXCHANGE_TAG}={sec_input_exchange}")
            if main_input_exchange is None:
                new_service_config[DOCKER_ENV_VARS_NAME].append(f"{CONSUMER_GROUP_TAG}={service_prefix}")
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SHARD_ID_TAG}={i}")

        if main_n_upstream is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{MAIN_N_UPSTREAM_TAG}={main_n_upstream}")
        if sec_n_upstream is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SEC_N_UPSTREAM_TAG}={sec_n_upstream}")

        if main_output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{MAIN_OUTPUT_QUEUE_TAG}={main_output_queue}")
        elif main_output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{MAIN_OUTPUT_EXCHANGE_TAG}={main_output_exchange}")
            if main_output_shards >= 1:
                new_service_config[DOCKER_ENV_VARS_NAME].append(f"MAIN_OUTPUT_SHARDS={main_output_shards}")

        if sec_output_queue is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SEC_OUTPUT_QUEUE_TAG}={sec_output_queue}")
        elif sec_output_exchange is not None:
            new_service_config[DOCKER_ENV_VARS_NAME].append(f"{SEC_OUTPUT_EXCHANGE_TAG}={sec_output_exchange}")
            if sec_output_shards >= 1:
                new_service_config[DOCKER_ENV_VARS_NAME].append(f"SEC_OUTPUT_SHARDS={sec_output_shards}")

        ## Target currency
        new_service_config[DOCKER_ENV_VARS_NAME].append(f"{TARGET_CURRENCY_TAG}={target_currency}")

        # Add service in services dictionary
        aggregator_services[new_service_name] = new_service_config

    return aggregator_services
