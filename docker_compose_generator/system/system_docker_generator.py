from .aggregator.aggregator_docker_service import get_aggregator_docker_services
from .bank_name_adder.bank_name_adder_service import get_bank_name_adders_services
from .barrier_filter.barrier_filter_docker_service import get_barrier_filters_services
from .data_reducer.data_reducer_docker_service import get_data_reducer_docker_services
from .filter.filter_docker_service import get_filters_docker_services
from .gateway.gateway_docker_service import get_gateway_docker_services
from .money_converter.money_converter_api_docker_service import get_money_conversion_api_client_docker_services
from .money_converter.money_converter_docker_service import get_money_converters_services
from .rabbitmq.rabbitmq_docker_service import get_rabbitmq_docker_service
from .scatter_gather.scatter_gather_generators import get_scatter_gather_services
from .splitter.splitter_docker_service import get_splitter_docker_services


import csv
import os

WORKER_LOGS_CLEANER_SERVICE = "worker_logs_cleaner"

def _get_next_config_row(config_file):
    row = next(config_file)
    return row["prefix"], int(row["total_instances"])

def _get_worker_logs_cleaner_service():
    return {
        WORKER_LOGS_CLEANER_SERVICE: {
            "build": {
                "context": "./src/worker_logs_cleaner",
            },
            "environment": [
                "WORKER_LOGS_DIR=/worker_logs",
            ],
            "volumes": [
                "./worker_logs:/worker_logs",
            ],
            "restart": "no",
        }
    }

def _depends_on_worker_logs_cleaner(service_name: str, service_config: dict):
    if service_name in {"rabbitmq", WORKER_LOGS_CLEANER_SERVICE, "chaos_monkey"}:
        return

    depends_on = service_config.setdefault("depends_on", {})
    if isinstance(depends_on, list):
        depends_on = {dependency: {"condition": "service_started"} for dependency in depends_on}
        service_config["depends_on"] = depends_on

    depends_on[WORKER_LOGS_CLEANER_SERVICE] = {"condition": "service_completed_successfully"}

def _add_worker_logs_cleaner_dependency(system: dict):
    system = _get_worker_logs_cleaner_service() | system
    for service_name, service_config in system.items():
        _depends_on_worker_logs_cleaner(service_name, service_config)
    return system

def generate_system_docker_compose(total_clients=0):
    system = {}

    # Create rabbitmq
    rabbitmq = get_rabbitmq_docker_service()
    system = system | rabbitmq

    base_path = os.path.dirname(__file__)
    csv_path = os.path.join(base_path, "system_config.csv")

    with open(csv_path, mode="r") as config_file:
        # Get config file reader
        config_file_reader = csv.DictReader(config_file)
        
        usd_prefix, usd_instances = _get_next_config_row(config_file_reader)
        reducer_prefix, reducer_instances = _get_next_config_row(config_file_reader)
        filter_prefix, filter_instances = _get_next_config_row(config_file_reader)

        q2_splitter_prefix, q2_splitter_instances = _get_next_config_row(config_file_reader)
        q2_reducer_prefix, q2_reducer_instances = _get_next_config_row(config_file_reader)
        q2_aggregator_prefix, q2_aggregator_instances = _get_next_config_row(config_file_reader)
        q2_bank_names_adder_prefix, q2_bank_names_adder_instances = _get_next_config_row(config_file_reader)

        q3_data_reducer_prefix, q3_data_reducer_instances = _get_next_config_row(config_file_reader)
        q3_filter_06092022_15092022_prefix, q3_filter_06092022_15092022_instances = _get_next_config_row(config_file_reader)
        q3_filter_01092022_05092022_prefix, q3_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q3_splitter_by_payment_method_prefix, q3_splitter_by_payment_method_instances = _get_next_config_row(config_file_reader)
        q3_avg_aggregators_preceding_period_prefix, q3_avg_aggregators_preceding_period_instances = _get_next_config_row(config_file_reader)
        q3_avg_and_transactions_joiner_prefix, q3_avg_and_transactions_joiner_instances = _get_next_config_row(config_file_reader)

        q4_data_reducer_prefix, q4_data_reducer_instances = _get_next_config_row(config_file_reader)
        q4_filter_01092022_05092022_prefix, q4_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q4_splitters_by_origin_and_dest_inc_prefix, q4_splitters_by_origin_and_dest_instances = _get_next_config_row(config_file_reader)
        q4_inc_edges_filter_prefix, q4_edges_filter_instances = _get_next_config_row(config_file_reader)
        q4_splitters_by_origin_and_dest_out_prefix, q4_splitters_by_origin_and_dest_instances = _get_next_config_row(config_file_reader)
        q4_out_edges_filter_prefix, q4_edges_filter_instances = _get_next_config_row(config_file_reader)
        q4_paths_creators_prefix, q4_paths_creators_instances = _get_next_config_row(config_file_reader)
        q4_paths_aggregator_prefix, q4_paths_aggregator_instances = _get_next_config_row(config_file_reader)

        q5_data_reducer_prefix, q5_data_reducer_instances = _get_next_config_row(config_file_reader)
        q5_filter_01092022_05092022_prefix, q5_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q5_money_converters_prefix, q5_money_converters_instances = _get_next_config_row(config_file_reader)
        q5_money_converter_api_client_prefix, q5_money_converter_api_client_instances = _get_next_config_row(config_file_reader)
        q5_filter_lt_1_usd_prefix, q5_filter_lt_1_usd_instances = _get_next_config_row(config_file_reader)
        q5_payment_fmt_filters_prefix, q5_payment_fmt_filters_instances = _get_next_config_row(config_file_reader)
        q5_counter_prefix, q5_counter_instances = _get_next_config_row(config_file_reader)
        q5_totals_sumer_prefix, q5_totals_sumer_instances = _get_next_config_row(config_file_reader)

        # Create gateway
        gateway = get_gateway_docker_services(
            input_query_queue_prefix="results",
            total_queries=5,
            output_exchange="gateway_exc",
            banks_out_exch="q2_banks_exchange",
        )

        gateway["gateway"]["environment"].append(f"OUTPUT_SHARDS={usd_instances}")
        gateway["gateway"]["environment"].append(f"BANK_OUTPUT_SHARDS={q2_bank_names_adder_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_1_N_UPSTREAM={filter_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_2_N_UPSTREAM={q2_aggregator_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_3_N_UPSTREAM={q3_avg_and_transactions_joiner_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_4_N_UPSTREAM={q4_paths_aggregator_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_5_N_UPSTREAM={q5_totals_sumer_instances}")
        gateway["gateway"]["environment"].append(
            "TRANSACTION_COLUMNS=Timestamp,From Bank,Account,To Bank,Account.1,"
            "Amount Paid,Payment Currency,Payment Format"
        )
        system = system | gateway

        # USD filters
        usd_filters = get_filters_docker_services(
            usd_prefix, usd_instances,
            "Payment Currency", "US Dollar", "eq",
            input_exchange="gateway_exc",
            output_exchange="usd_transactions_exc",
            output_shards=reducer_instances,
            n_upstream=1,
            total_clients=total_clients,
        )
        system = system | usd_filters

        # Data reducers
        data_reducers_q1 = get_data_reducer_docker_services(
            reducer_prefix, reducer_instances,
            ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"],
            input_exchange="usd_transactions_exc",
            output_exchange="q1_reduced_exc",
            output_shards=filter_instances,
            n_upstream=usd_instances,
            total_clients=total_clients,
        )
        system = system | data_reducers_q1

        # Final amount filter
        q1_50_usd_filters = get_filters_docker_services(
            filter_prefix, filter_instances,
            "Amount Paid", 50, "lt",
            input_exchange="q1_reduced_exc",
            output_queue="results_1",
            n_upstream=reducer_instances,
            total_clients=total_clients,
        )
        for name, config in q1_50_usd_filters.items():
            config["environment"].append("BATCH_SIZE=5000")
        system = system | q1_50_usd_filters

        # =========================================================
        # QUERY 2
        # =========================================================

        q2_splitters = get_splitter_docker_services(
            q2_splitter_prefix, q2_splitter_instances,
            input_exchange="usd_transactions_exc",
            output_exchange="q2_by_bank_exc",
            key_field="From Bank",
            output_shards=q2_reducer_instances,
            n_upstream=usd_instances,
            total_clients=total_clients,
        )
        for config in q2_splitters.values():
            config["environment"].append("NORMALIZE_NUMERIC_KEY=true")
        system = system | q2_splitters

        q2_data_reducers = get_data_reducer_docker_services(
            q2_reducer_prefix, q2_reducer_instances,
            ["From Bank", "Account", "Amount Paid"],
            input_exchange="q2_by_bank_exc",
            output_exchange="q2_reduced_exc",
            output_shards=q2_aggregator_instances,
            n_upstream=q2_splitter_instances,
            total_clients=total_clients,
        )
        for name, config in q2_data_reducers.items():
            config["environment"].append("ROUTING_FIELD=From Bank")
        system = system | q2_data_reducers

        q2_aggregators = get_aggregator_docker_services(
            q2_aggregator_prefix, q2_aggregator_instances,
            input_exchange="q2_reduced_exc",
            output_exchange="q2_results_formatter",
            agg_op="max", agg_field="Amount Paid", key_field="From Bank",
            carry_fields=["Account"],
            n_upstream=q2_reducer_instances,
            total_clients=total_clients,
        )
        for name, config in q2_aggregators.items():
            config["environment"].append("BATCH_SIZE=5000")
            config["environment"].append("ROUTING_FIELD=From Bank")
            config["environment"].append(f"OUTPUT_SHARDS={q2_bank_names_adder_instances}")
        system = system | q2_aggregators

        q2_bank_names_adders = get_bank_name_adders_services(
            q2_bank_names_adder_prefix, q2_bank_names_adder_instances,
            main_input_exchange="q2_results_formatter",
            main_n_upstream=q2_aggregator_instances,
            sec_input_exchange="q2_banks_exchange",
            sec_n_upstream=1,
            sec_output_queue="results_2",
        )
        system = system | q2_bank_names_adders

        # =========================================================
        # QUERY 3
        # =========================================================

        # Reduce data
        data_reducers_q3 = get_data_reducer_docker_services(
            q3_data_reducer_prefix, q3_data_reducer_instances,
            ["Timestamp", "From Bank", "Account", "Amount Paid", "Payment Format"],
            input_exchange="usd_transactions_exc",
            output_exchange="q3_reduced_data_exc",
            n_upstream=usd_instances,
            output_shards=q3_filter_06092022_15092022_instances,
        )
        system = system | data_reducers_q3

        # El notebook compara strings con <= "2022/09/15", excluyendo timestamps del día 15.
        q3_filters_06092022_15092022 = get_filters_docker_services(
            q3_filter_06092022_15092022_prefix, q3_filter_06092022_15092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/06", "2022/09/14"]',
            input_exchange="q3_reduced_data_exc",
            output_exchange="q3_transactions_exc",
            output_shards=q3_avg_and_transactions_joiner_instances,
            n_upstream=q3_data_reducer_instances,
        )
        for name, config in q3_filters_06092022_15092022.items():
            config["environment"].append("ROUTING_FIELD=Payment Format")
        system = system | q3_filters_06092022_15092022

        # Filter dates between 01/09/2022 and 05/09/2022 included
        q3_filters_01092022_05092022 = get_filters_docker_services(
            q3_filter_01092022_05092022_prefix, q3_filter_01092022_05092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/01", "2022/09/05"]',
            input_exchange="q3_reduced_data_exc",
            output_exchange="q3_splitter_exc",
            n_upstream=q3_data_reducer_instances,
            output_shards=q3_splitter_by_payment_method_instances,
        )
        system = system | q3_filters_01092022_05092022

        # Split by payment format
        q3_splitters_by_payment_method = get_splitter_docker_services(
            q3_splitter_by_payment_method_prefix, q3_splitter_by_payment_method_instances,
            input_exchange="q3_splitter_exc",
            output_exchange="q3_split_by_payment_method_exc",
            key_field="Payment Format",
            n_upstream=q3_filter_01092022_05092022_instances,
            output_shards=q3_avg_aggregators_preceding_period_instances,
        )
        system = system | q3_splitters_by_payment_method

        # Average aggregator
        q3_avg_aggregators = get_aggregator_docker_services(
            q3_avg_aggregators_preceding_period_prefix, q3_avg_aggregators_preceding_period_instances,
            input_exchange="q3_split_by_payment_method_exc",
            output_exchange="q3_avg_preceding_period_exc",
            agg_op="avg", agg_field="Amount Paid", key_field="Payment Format",
            n_upstream=q3_splitter_by_payment_method_instances,
        )
        for name, config in q3_avg_aggregators.items():
            config["environment"].append("ROUTING_FIELD=Payment Format")
            config["environment"].append(f"OUTPUT_SHARDS={q3_avg_and_transactions_joiner_instances}")
        system = system | q3_avg_aggregators

        # Filter with barrier
        q3_avg_and_transactions_joiner = get_barrier_filters_services(
            q3_avg_and_transactions_joiner_prefix, q3_avg_and_transactions_joiner_instances,
            main_input_exchange="q3_transactions_exc",
            sec_input_exchange="q3_avg_preceding_period_exc",
            main_n_upstream=q3_filter_06092022_15092022_instances,
            sec_n_upstream=q3_avg_aggregators_preceding_period_instances,
            output_queue="results_3",
        )
        for name, config in q3_avg_and_transactions_joiner.items():
            config["environment"].append("BATCH_SIZE=10000")
        system = system | q3_avg_and_transactions_joiner

        # =========================================================
        # QUERY 4
        # =========================================================

        # Reduce data
        data_reducers_q4 = get_data_reducer_docker_services(
            q4_data_reducer_prefix, q4_data_reducer_instances,
            ["Timestamp", "From Bank", "Account", "To Bank", "Account.1"],
            input_exchange="usd_transactions_exc",
            output_exchange="q4_reduced_data_exc",
            n_upstream=usd_instances,
            output_shards=q4_filter_01092022_05092022_instances,
        )
        system = system | data_reducers_q4

        # Filter dates between 01/09/2022 and 05/09/2022 included
        q4_filters_01092022_05092022 = get_filters_docker_services(
            q4_filter_01092022_05092022_prefix,
            q4_filter_01092022_05092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/01", "2022/09/05"]',
            input_exchange="q4_reduced_data_exc",
            output_exchange="q4_splitters_exc",
            n_upstream=q4_data_reducer_instances,
            output_shards=q4_splitters_by_origin_and_dest_instances,
        )
        system = system | q4_filters_01092022_05092022

        # Outgoing edges
        ## Split by origin account so each worker can count distinct destinations for outgoing transactions.
        q4_splitters_by_origin_and_dest = get_splitter_docker_services(
            q4_splitters_by_origin_and_dest_out_prefix,
            q4_splitters_by_origin_and_dest_instances,
            input_exchange="q4_splitters_exc",
            output_exchange="q4_out_edges_filter_exc",
            key_fields=["From Bank", "Account"],
            n_upstream=q4_filter_01092022_05092022_instances,
            output_shards=q4_edges_filter_instances,
        )
        system = system | q4_splitters_by_origin_and_dest

        ## Create outgoing edges filter
        q4_transactions_graphs = get_scatter_gather_services(
            q4_out_edges_filter_prefix,
            q4_edges_filter_instances,
            "out_edges_filter",
            input_exchange="q4_out_edges_filter_exc",
            output_exchange="q4_candidates_edges_exc",
            n_upstream=q4_splitters_by_origin_and_dest_instances,
            output_shards=q4_paths_creators_instances,
        )
        system = system | q4_transactions_graphs

        # Incoming edges
        ## Split by origin account so each worker can count distinct destinations for incoming transactions.
        q4_splitters_by_origin_and_dest = get_splitter_docker_services(
            q4_splitters_by_origin_and_dest_inc_prefix,
            q4_splitters_by_origin_and_dest_instances,
            input_exchange="q4_splitters_exc",
            output_exchange="q4_inc_edges_filter_exc",
            key_fields=["To Bank", "Account.1"],
            n_upstream=q4_filter_01092022_05092022_instances,
            output_shards=q4_edges_filter_instances,
        )
        system = system | q4_splitters_by_origin_and_dest

        ## Create incoming edges filter
        q4_inc_edges_filters = get_scatter_gather_services(
            q4_inc_edges_filter_prefix,
            q4_edges_filter_instances,
            "inc_edges_filter",
            input_exchange="q4_inc_edges_filter_exc",
            output_exchange="q4_candidates_edges_exc",
            n_upstream=q4_splitters_by_origin_and_dest_instances,
            output_shards=q4_paths_creators_instances,
        )
        system = system | q4_inc_edges_filters

        # Paths creators
        q4_total_edges_filters = 2*q4_edges_filter_instances
        q4_paths_creators = get_scatter_gather_services(
            q4_paths_creators_prefix,
            q4_paths_creators_instances,
            "paths_creator",
            input_exchange="q4_candidates_edges_exc",
            output_exchange="q4_paths_exc",
            n_upstream=q4_total_edges_filters,
            output_shards=q4_paths_aggregator_instances,
        )
        system = system | q4_paths_creators

        # Unique paths counters
        q4_paths_aggregator = get_scatter_gather_services(
            q4_paths_aggregator_prefix,
            q4_paths_aggregator_instances,
            "unique_paths_count",
            input_exchange="q4_paths_exc",
            output_queue="results_4",
            n_upstream=q4_paths_creators_instances,
            total_clients=total_clients,
        )
        for name, config in q4_paths_aggregator.items():
            config["environment"].append("BATCH_SIZE=1000")
        system = system | q4_paths_aggregator

        # =========================================================
        # QUERY 5
        # =========================================================

        # Data reducers
        q5_data_reducers = get_data_reducer_docker_services(
            q5_data_reducer_prefix, q5_data_reducer_instances,
            ["Timestamp", "Amount Paid", "Payment Currency", "Payment Format"],
            input_exchange="gateway_exc",
            output_exchange="q5_reduced_exc",
            output_shards=q5_filter_01092022_05092022_instances,
        )
        system = system | q5_data_reducers

        # Filter dates between 01/09/2022 and 05/09/2022 included
        q5_filters_01092022_05092022 = get_filters_docker_services(
            q5_filter_01092022_05092022_prefix, q5_filter_01092022_05092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/01", "2022/09/05"]',
            input_exchange="q5_reduced_exc",
            output_exchange="q5_payment_format_exc",
            n_upstream=q5_data_reducer_instances,
            output_shards=q5_payment_fmt_filters_instances,
        )
        system = system | q5_filters_01092022_05092022

        # Filter by payment methods
        q5_payment_fmt_filters = get_filters_docker_services(
            q5_payment_fmt_filters_prefix, q5_payment_fmt_filters_instances,
            filter_field="Payment Format", filter_op="in", filter_value='["Wire", "ACH"]',
            input_exchange="q5_payment_format_exc",
            output_exchange="q5_converter_to_usd_exc",
            output_shards=q5_money_converters_instances,
            n_upstream=q5_filter_01092022_05092022_instances,
        )

        system = system | q5_payment_fmt_filters

        # Conversion to USD
        q5_money_converters = get_money_converters_services(
            q5_money_converters_prefix, q5_money_converters_instances, "US Dollar",
            main_input_exchange="q5_converter_to_usd_exc",
            sec_input_exchange="q5_currency_rates_from_api_exc",
            main_output_queue="q5_reqs_currency_rates_api",
            sec_output_exchange="q5_converted_amounts_exc",
            main_n_upstream=q5_payment_fmt_filters_instances,
            sec_n_upstream=q5_money_converter_api_client_instances,
            sec_output_shards=q5_filter_lt_1_usd_instances,
        )
        for name, config in q5_money_converters.items():
            config["environment"].append("BATCH_SIZE=1000")
            config["environment"].append("SEC_BATCH_SIZE=5000")
        system = system | q5_money_converters

        q5_money_converters_api_client = get_money_conversion_api_client_docker_services(
            q5_money_converter_api_client_prefix, q5_money_converter_api_client_instances,
            input_queue="q5_reqs_currency_rates_api",
            output_exchange="q5_currency_rates_from_api_exc",
            output_shards=q5_money_converters_instances,
            n_upstream=q5_money_converters_instances,
        )
        system = system | q5_money_converters_api_client

        # Filter of less than 1 USD
        q5_filters_lt_1_usd = get_filters_docker_services(
            q5_filter_lt_1_usd_prefix, q5_filter_lt_1_usd_instances,
            "Amount Paid", "1", "lt",
            input_exchange="q5_converted_amounts_exc",
            output_exchange="q5_small_amounts_exc",
            output_shards=q5_counter_instances,
            n_upstream=q5_money_converters_instances,
        )
        
        system = system | q5_filters_lt_1_usd

        # Count transactions that arrive
        q5_counters = get_aggregator_docker_services(
            q5_counter_prefix, q5_counter_instances,
            input_exchange="q5_small_amounts_exc",
            output_queue="q5_totals_reached",
            agg_op="count",
            n_upstream=q5_filter_lt_1_usd_instances,
        )
        system = system | q5_counters

        # Add results
        q5_totals_sumers = get_aggregator_docker_services(
            q5_totals_sumer_prefix, q5_totals_sumer_instances,
            input_queue="q5_totals_reached",
            output_queue="results_5",
            agg_op="sum", agg_field="count",
            n_upstream=q5_counter_instances,
            total_clients=total_clients,
        )
        system = system | q5_totals_sumers

                # --- MONITOR Y CHAOS MONKEY ---
        worker_names = [name for name in system.keys() if name not in ["rabbitmq","gateway"]]
        system = system | _get_monitor_services(worker_names)
        system = system | _get_chaos_monkey_service()

    system = _add_worker_logs_cleaner_dependency(system)
    return system

def _get_monitor_services(worker_names):
    workers_value = ",".join(name for name in worker_names if name.lower() not in ["rabbitmq","gateway"])
    return {
        "monitor_0": {
            "build": {"context": "./src/monitor"},
            "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
            "restart": "unless-stopped",
            "environment": [
                "MONITOR_ID=0",
                "SUCCESSORS=monitor_1,monitor_2",
                "HEALTH_PORT=8888",
                "CHECK_INTERVAL=10",
                "HEALTH_TIMEOUT=50",
                "MAX_FAILURES=4",
                f"WORKERS={workers_value}",
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "restart": "unless-stopped",
        },
        "monitor_1": {
            "build": {"context": "./src/monitor"},
            "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
            "restart": "unless-stopped",
            "environment": [
                "MONITOR_ID=1",
                "SUCCESSORS=monitor_2,monitor_0",
                "HEALTH_PORT=8888",
                "CHECK_INTERVAL=10",
                "HEALTH_TIMEOUT=50",
                "MAX_FAILURES=4",
                f"WORKERS={workers_value}",
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "restart": "unless-stopped",
        },
        "monitor_2": {
            "build": {"context": "./src/monitor"},
            "depends_on": {"rabbitmq": {"condition": "service_healthy"}},
            "restart": "unless-stopped",
            "environment": [
                "MONITOR_ID=2",
                "SUCCESSORS=monitor_0,monitor_1",
                "HEALTH_PORT=8888",
                "CHECK_INTERVAL=10",
                "HEALTH_TIMEOUT=50",
                "MAX_FAILURES=4",
                f"WORKERS={workers_value}",
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "restart": "unless-stopped",
        },
    }

def _get_chaos_monkey_service():
    return {
        "chaos_monkey": {
            "build": {"context": "./scripts"},
            "environment": [
                "CHAOS_TARGETS=usd_filter_0,q1_data_reducer_0,q2_aggregator_0,q2_banks_name_adder_0,q3_avg_and_transactions_joiner_0,q4_inc_edges_filter_0,q4_paths_creators_0,q5_money_converter_0",
                "CHAOS_INTERVAL=30",
                "CHAOS_MIN_WAIT=30",
            ],
            "volumes": ["/var/run/docker.sock:/var/run/docker.sock"],
            "profiles": ["chaos"],
        }
    }
