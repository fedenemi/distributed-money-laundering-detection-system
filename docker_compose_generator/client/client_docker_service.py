import copy
import os
import yaml

CONFIG_FILE = "client_config.yaml"
CLIENT_NAME_PREFIX = "client"

# Build section
DOCKER_BUILD_SECTION_NAME = "build"
DOCKER_BUILD_CONTEXT_SUBSECTION_NAME = "context"

CONTEXT_FOLDER = "./src"

# Container name
CONTAINER_NAME_TAG = "container_name"

# Environment variable names
DOCKER_ENV_VARS_NAME = "environment"

## I/O
TRANSACTIONS_FILE_TAG = "TRANSACTIONS_FILE"
ACCOUNTS_FILE_TAG = "ACCOUNTS_FILE"
CLIENT_ID_TAG = "CLIENT_ID"
RESULTS_DIR_TAG = "RESULTS_DIR"
BATCH_SIZE_TAG = "BATCH_SIZE"


def get_clients_docker_services(
    transactions_file_path,
    accounts_file_path,
    results_dir,
    total_clients,
    batch_size="",
):
    # Open config file
    base_path = os.path.dirname(__file__)
    config_file_path = os.path.join(base_path, CONFIG_FILE)
    with open(config_file_path, "r") as config_file:
        base_client_service_config = yaml.safe_load(config_file)

    # Create empty services list
    new_clients_services = {}
    for i in range(total_clients):
        # Copy base configuration
        new_client_service_config = copy.deepcopy(base_client_service_config)

        # Add container name
        client_name = f"{CLIENT_NAME_PREFIX}_{i}"
        new_client_service_config[CONTAINER_NAME_TAG] = client_name

        # Add context folder
        new_client_service_config[DOCKER_BUILD_SECTION_NAME][DOCKER_BUILD_CONTEXT_SUBSECTION_NAME] = CONTEXT_FOLDER

        # Add environment variables
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(
            f"{TRANSACTIONS_FILE_TAG}={transactions_file_path}"
        )
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(
            f"{ACCOUNTS_FILE_TAG}={accounts_file_path}"
        )
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(
            f"{CLIENT_ID_TAG}={i}"
        )
        new_client_service_config[DOCKER_ENV_VARS_NAME].append(
            f"{RESULTS_DIR_TAG}={results_dir}"
        )
        if batch_size:
            new_client_service_config[DOCKER_ENV_VARS_NAME].append(
                f"{BATCH_SIZE_TAG}={batch_size}"
            )

        # Add service
        new_clients_services[client_name] = new_client_service_config

    return new_clients_services