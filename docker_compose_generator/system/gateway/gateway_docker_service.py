import yaml
import os

# Config file
CONFIG_FILE = "gateway_config.yaml"

# Service configuration
SERVICE_NAME="gateway"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

OUTPUT_QUEUE = "OUTPUT_QUEUE"
OUTPUT_EXCHANGE = "OUTPUT_EXCHANGE"
BANK_OUTPUT_QUEUE = "BANK_OUTPUT_QUEUE"
BANK_OUTPUT_EXCHANGE = "BANK_OUTPUT_EXCHANGE"
INPUT_QUEUE_PREFIX = "INPUT_QUEUE_PREFIX"
TOTAL_QUERIES = "TOTAL_QUERIES"
MAX_IN_FLIGHT_BATCHES = "MAX_IN_FLIGHT_BATCHES"
TOTAL_MAX_IN_FLIGHT_BATCHES = 0

def get_gateway_docker_services(
    input_query_queue_prefix,
    total_queries,
    output_queue=None,
    output_exchange=None,
    banks_out_queue=None,
    banks_out_exch=None,
):
    # Open config file
    base_path = os.path.dirname(__file__)
    config_file_path = os.path.join(base_path, CONFIG_FILE)
    
    with open(config_file_path, "r") as config_file:
        gateway_service_config = yaml.safe_load(config_file)

    # Add context folder
    gateway_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

    # Add environment variables
    ## I/O
    if output_queue is not None:
        gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_QUEUE}={output_queue}")
    elif output_exchange is not None:
        gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{OUTPUT_EXCHANGE}={output_exchange}")

    if banks_out_queue is not None:
        gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{BANK_OUTPUT_QUEUE}={banks_out_queue}")
    elif banks_out_exch is not None:
        gateway_service_config[DOCKER_ENV_VARS_NAME].append(f"{BANK_OUTPUT_EXCHANGE}={banks_out_exch}")

    # Total queries and input queue prefix for result queues
    gateway_service_config[DOCKER_ENV_VARS_NAME].append(
        f"{INPUT_QUEUE_PREFIX}={input_query_queue_prefix}"
    )
    gateway_service_config[DOCKER_ENV_VARS_NAME].append(
        f"{TOTAL_QUERIES}={total_queries}"
    )

    # Total client's batches in flight
    gateway_service_config[DOCKER_ENV_VARS_NAME].append(
        f"{MAX_IN_FLIGHT_BATCHES}={TOTAL_MAX_IN_FLIGHT_BATCHES}"
    )

    # Add service name
    new_service_config = { SERVICE_NAME : gateway_service_config}

    return new_service_config
