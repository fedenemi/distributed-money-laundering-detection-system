from .aggregator.aggregator_docker_service import get_aggregator_docker_services
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

def _get_next_config_row(config_file):
    row = next(config_file)
    return row["prefix"], int(row["total_instances"])

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

        # Create gateway (publica en exchange shardeado)
        gateway = get_gateway_docker_services(
            input_query_queue_prefix="results",
            total_queries=5,
            output_exchange="gateway_exc",
        )
        
        usd_prefix, usd_instances = _get_next_config_row(config_file_reader)
        reducer_prefix, reducer_instances = _get_next_config_row(config_file_reader)
        filter_prefix, filter_instances = _get_next_config_row(config_file_reader)

        q2_splitter_prefix, q2_splitter_instances = _get_next_config_row(config_file_reader)
        q2_reducer_prefix, q2_reducer_instances = _get_next_config_row(config_file_reader)
        q2_aggregator_prefix, q2_aggregator_instances = _get_next_config_row(config_file_reader)

        q3_data_reducer_prefix, q3_data_reducer_instances = _get_next_config_row(config_file_reader)
        q3_filter_06092022_15092022_prefix, q3_filter_06092022_15092022_instances = _get_next_config_row(config_file_reader)
        q3_filter_01092022_05092022_prefix, q3_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q3_splitter_by_payment_method_prefix, q3_splitter_by_payment_method_instances = _get_next_config_row(config_file_reader)
        q3_avg_aggregators_preceding_period_prefix, q3_avg_aggregators_preceding_period_instances = _get_next_config_row(config_file_reader)
        q3_avg_and_transactions_joiner_prefix, q3_avg_and_transactions_joiner_instances = _get_next_config_row(config_file_reader)

        q4_data_reducer_prefix, q4_data_reducer_instances = _get_next_config_row(config_file_reader)
        q4_filter_01092022_05092022_prefix, q4_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q4_splitters_by_origin_and_dest_prefix, q4_splitters_by_origin_and_dest_instances = _get_next_config_row(config_file_reader)
        q4_transaction_graph_prefix, q4_transaction_graph_instances = _get_next_config_row(config_file_reader)
        q4_graphs_edges_splitters_prefix, q4_graphs_edges_splitters_instances = _get_next_config_row(config_file_reader)
        q4_paths_creators_prefix, q4_paths_creators_instances = _get_next_config_row(config_file_reader)
        q4_paths_splitters_by_ends_prefix, q4_paths_splitters_by_ends_instances = _get_next_config_row(config_file_reader)
        q4_unique_paths_counters_prefix, q4_unique_paths_counters_instances = _get_next_config_row(config_file_reader)

        q5_data_reducer_prefix, q5_data_reducer_instances = _get_next_config_row(config_file_reader)
        q5_filter_01092022_05092022_prefix, q5_filter_01092022_05092022_instances = _get_next_config_row(config_file_reader)
        q5_money_converters_prefix, q5_money_converters_instances = _get_next_config_row(config_file_reader)
        q5_money_converter_api_client_prefix, q5_money_converter_api_client_instances = _get_next_config_row(config_file_reader)
        q5_filter_lt_1_usd_prefix, q5_filter_lt_1_usd_instances = _get_next_config_row(config_file_reader)
        q5_payment_fmt_filters_prefix, q5_payment_fmt_filters_instances = _get_next_config_row(config_file_reader)
        q5_counter_prefix, q5_counter_instances = _get_next_config_row(config_file_reader)
        q5_totals_sumer_prefix, q5_totals_sumer_instances = _get_next_config_row(config_file_reader)

        gateway["gateway"]["environment"].append(f"OUTPUT_SHARDS={usd_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_1_N_UPSTREAM={filter_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_2_N_UPSTREAM={q2_aggregator_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_3_N_UPSTREAM={q3_avg_and_transactions_joiner_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_4_N_UPSTREAM={q4_unique_paths_counters_instances}")
        gateway["gateway"]["environment"].append(f"QUERY_5_N_UPSTREAM={q5_totals_sumer_instances}")
        gateway["gateway"]["environment"].append(
            "TRANSACTION_COLUMNS=Timestamp,From Bank,Account,To Bank,Account.1,"
            "Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format"
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
            output_queue="results_2",
            agg_op="max", agg_field="Amount Paid", key_field="From Bank",
            carry_fields=["Account"],
            n_upstream=q2_reducer_instances,
            total_clients=total_clients,
        )
        for name, config in q2_aggregators.items():
            config["environment"].append("BATCH_SIZE=5000")
        system = system | q2_aggregators

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

        # Filter dates between 06/09/2022 and 15/09/2022 included
        q3_filters_06092022_15092022 = get_filters_docker_services(
            q3_filter_06092022_15092022_prefix, q3_filter_06092022_15092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/06", "2022/09/15"]',
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
            config["environment"].append("BATCH_SIZE=5000")
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
            q4_filter_01092022_05092022_prefix, q4_filter_01092022_05092022_instances,
            filter_field="Timestamp", filter_op="in", filter_value='["2022/09/01", "2022/09/05"]',
            input_exchange="q4_reduced_data_exc",
            output_exchange="q4_splitter_exc",
            n_upstream=q4_data_reducer_instances,
            output_shards=q4_splitters_by_origin_and_dest_instances,
        )
        system = system | q4_filters_01092022_05092022

        # Split by origin and destination accounts
        q4_splitters_by_origin_and_dest = get_splitter_docker_services(
            q4_splitters_by_origin_and_dest_prefix, q4_splitters_by_origin_and_dest_instances,
            input_exchange="q4_splitter_exc",
            output_exchange="q4_split_by_origin_and_dest_exc",
            key_fields=["From Bank", "Account", "To Bank", "Account.1"],
            n_upstream=q4_filter_01092022_05092022_instances,
            output_shards=q4_transaction_graph_instances,
        )
        system = system | q4_splitters_by_origin_and_dest

        # Create subgraphs of transactions
        q4_transactions_graphs = get_scatter_gather_services(
            q4_transaction_graph_prefix, q4_transaction_graph_instances,
            "sub_graph_agg",
            input_exchange="q4_split_by_origin_and_dest_exc",
            output_exchange="q4_edges_exc",
            n_upstream=q4_splitters_by_origin_and_dest_instances,
            output_shards=q4_paths_creators_instances,
        )
        system = system | q4_transactions_graphs

        # Paths creators
        q4_paths_creators = get_scatter_gather_services(
            q4_paths_creators_prefix, q4_paths_creators_instances,
            "paths_creator",
            input_exchange="q4_edges_exc",
            output_exchange="q4_paths_exc",
            n_upstream=q4_transaction_graph_instances,
            output_shards=q4_paths_splitters_by_ends_instances,
        )
        system = system | q4_paths_creators

        # Split by origin and destination nodes
        q4_paths_splitters_by_ends = get_splitter_docker_services(
            q4_paths_splitters_by_ends_prefix, q4_paths_splitters_by_ends_instances,
            input_exchange="q4_paths_exc",
            output_exchange="q4_unique_paths_counter_exc",
            key_fields=["From Bank", "Account", "To Bank", "Account.1"],
            n_upstream=q4_paths_creators_instances,
            output_shards=q4_unique_paths_counters_instances
        )
        system = system | q4_paths_splitters_by_ends

        # Unique paths counters
        q4_unique_paths_counters = get_scatter_gather_services(
            q4_unique_paths_counters_prefix,
            q4_unique_paths_counters_instances,
            "unique_paths_count",
            input_exchange="q4_unique_paths_counter_exc",
            output_queue="results_4",
            n_upstream=q4_paths_splitters_by_ends_instances,
        )
        for name, config in q4_unique_paths_counters.items():
            config["environment"].append("BATCH_SIZE=1000")
        system = system | q4_unique_paths_counters

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
            output_queue="q5_converter_to_usd",
            n_upstream=q5_data_reducer_instances,
        )
        system = system | q5_filters_01092022_05092022

        # Conversion to USD
        q5_money_converters = get_money_converters_services(
            q5_money_converters_prefix, q5_money_converters_instances, "US Dollar",
            main_input_queue="q5_converter_to_usd",
            sec_input_queue="q5_currency_rates_from_api",
            main_output_queue="q5_reqs_currency_rates_api",
            sec_output_exchange="q5_converted_amounts_exc",
            main_n_upstream=q5_filter_01092022_05092022_instances,
        )
        for name, config in q5_money_converters.items():
            config["environment"].append("BATCH_SIZE=1")
            config["environment"].append(f"SECONDARY_OUTPUT_SHARDS={q5_filter_lt_1_usd_instances}")
        system = system | q5_money_converters

        q5_money_converters_api_client = get_money_conversion_api_client_docker_services(
            q5_money_converter_api_client_prefix, q5_money_converter_api_client_instances,
            input_queue="q5_reqs_currency_rates_api",
            output_queue="q5_currency_rates_from_api"
        )
        system = system | q5_money_converters_api_client

        # Filter of less than 1 USD
        q5_filters_lt_1_usd = get_filters_docker_services(
            q5_filter_lt_1_usd_prefix, q5_filter_lt_1_usd_instances,
            "Amount Paid", "1", "lt",
            input_exchange="q5_converted_amounts_exc",
            output_exchange="q5_small_amounts_exc",
            output_shards=q5_payment_fmt_filters_instances,
            n_upstream=q5_money_converters_instances,
        )
        
        system = system | q5_filters_lt_1_usd

        # Filter by payment methods
        q5_payment_fmt_filters = get_filters_docker_services(
            q5_payment_fmt_filters_prefix, q5_payment_fmt_filters_instances,
            filter_field="Payment Format", filter_op="in", filter_value='["Wire", "ACH"]',
            input_exchange="q5_small_amounts_exc",
            output_exchange="q5_countable_exc",
            output_shards=q5_counter_instances,
            n_upstream=q5_filter_lt_1_usd_instances,
        )
        
        system = system | q5_payment_fmt_filters

        # Count transactions that arrive
        q5_counters = get_aggregator_docker_services(
            q5_counter_prefix, q5_counter_instances,
            input_exchange="q5_countable_exc",
            output_queue="q5_totals_reached",
            agg_op="count",
            n_upstream=q5_payment_fmt_filters_instances,
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

    return system
