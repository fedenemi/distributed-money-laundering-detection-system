from aggregator.aggregator_docker_service import get_aggregator_docker_services
from data_reducer.data_reducer_docker_service import get_data_reducer_docker_services
from filter.filter_docker_service import get_filters_docker_services
from gateway.gateway_docker_service import get_gateway_docker_services
from scatter_gather.scatter_gather_generators import get_scatter_gather_services
from splitter.splitter_docker_service import get_splitter_docker_services

def generate_system_docker_compose():
    system = {}

    # Create gateway
    gateway = get_gateway_docker_services(
                input_query_queue_prefix="results",
                total_queries=5, output_queue="raw_data_queue"
                )
    system = system | gateway

    # Create data cleaners
    raise Exception("TODO: Faltan los data cleaners")

    # Create data
    usd_filters = get_filters_docker_services("usd_filter", 1,
                                             "Payment Currency", "US Dollar", "eq",
                                             input_queue="cleanded_data_queue",
                                             output_exchange="usd_transactions_exc",
                                             )
    system = system | usd_filters


    # Query 1
    ## Reduce data
    data_reducers_q1 = get_data_reducer_docker_services("q1_data_reducer", 1,
                                                        ["From Bank", "Account", "To Bank", "Account.1", "Amount Paid"],
                                                        input_exchange="usd_transactions_exc",
                                                        output_queue="q1_reduced_data",
                                                        )
    system = system | data_reducers_q1

    ## Filter by amount
    q1_50_usd_filters = get_filters_docker_services("q1_filter_lt_50_usd", 1,
                                                    "Amount Paid", "lt", "50",
                                                    input_queue="q1_reduced_data",
                                                    output_queue="results_1",
                                                    )


    # Query 2
    ## Reduce data
    data_reducers_q2 = get_data_reducer_docker_services("q2_data_reducer", 1,
                                                        ["From Bank", "Account", "Amount Paid"],
                                                        input_exchange="usd_transactions_exc",
                                                        output_queue="q2_reduced_data",
                                                        )
    system = system | data_reducers_q2

    ## Splitter by bank
    q2_splitters_by_bank = get_splitter_docker_services("q2_splitter_by_bank", 1,
                                                        input_queue="q2_reduced_data",
                                                        output_exchange="q2_split_by_bank_exc",
                                                        key_field="From Bank",
                                                        )
    system = system | q2_splitters_by_bank

    ## Max aggregator
    q2_max_aggregators = get_aggregator_docker_services("q2_max_by_bank", 1,
                                                        input_exchange="q2_split_by_bank_exc",
                                                        output_queue="results_2",
                                                        agg_op="max", agg_field="Amount Paid", key_field="From Bank",
                                                        carry_fields=["From Bank", "Account", "Amount Paid"],
                                                        )
    system = system | q2_max_aggregators


    # Query 3
    ## Reduce data
    data_reducers_q3 = get_data_reducer_docker_services("q3_data_reducer", 1,
                                                        ["Timestamp", "From Bank", "Account", "Amount Paid", "Payment Format"],
                                                        input_exchange="usd_transactions_exc",
                                                        output_exchange="q3_reduced_data_exc",
                                                        )
    system = system | data_reducers_q3

    ## Filter data by date
    ### Filter dates between 06/09/2022 and 15/09/2022 included
    raise Exception("TODO: Ver que fecha caiga entre 06/09/2022 y 15/09/2022 inclusive")

    ### Filter dates between 01/09/2022 and 05/09/2022 included
    raise Exception("TODO: Ver que fecha caiga entre 01/09/2022 y 05/09/2022 inclusive")

    ### Split by payment method
    q3_splitters_by_payment_method = get_splitter_docker_services("q3_splitter_by_payment_method", 1,
                                                        input_queue="q3_filter_preceding_period",
                                                        output_exchange="q3_split_by_payment_method_exc",
                                                        key_field="Payment Method",
                                                        )
    system = system | q3_splitters_by_payment_method

    ### Average aggregator
    q3_avg_aggregators = get_aggregator_docker_services("q3_avg_aggregators_preceding_period", 1,
                                                        input_exchange="q3_split_by_payment_method_exc",
                                                        output_queue="q3_avg_preceding_period",
                                                        agg_op="avg", agg_field="Amount Paid", key_field="Payment Format",
                                                        )
    system = system | q3_avg_aggregators

    ## Filter with barrier
    raise Exception("TODO: Filtro con barrera")


    # Query 4
    ## Reduce data
    data_reducers_q4 = get_data_reducer_docker_services("q4_data_reducer", 1,
                                                        ["Timestamp", "From Bank", "Account", "To Bank", "Account.1"],
                                                        input_exchange="usd_transactions_exc",
                                                        output_exchange="q4_reduced_data_exc",
                                                        )
    system = system | data_reducers_q4

    ## Filter dates between 01/09/2022 and 05/09/2022 included
    raise Exception("TODO: Ver que fecha caiga entre 01/09/2022 y 05/09/2022 inclusive")

    ## Split by origin and destination accounts
    q4_splitters_by_origin_and_dest = get_splitter_docker_services("q4_splitter_by_origin_and_dest", 1,
                                                        input_queue="q4_filter_period",
                                                        output_exchange="q4_split_by_origin_and_dest_exc",
                                                        key_fields=["From Bank", "Account", "To Bank", "Account.1"],
                                                        )
    system = system | q4_splitters_by_origin_and_dest

    ## Create subgraphs of transactions
    q4_transactions_graphs = get_scatter_gather_services("transaction_graph", "sub_graph_agg",
                                                         "q4_split_by_origin_and_dest_exc",
                                                         "q4_subgraphs_edges",
                                                         1
                                                         )
    
    ## Send edges as "in" for destination node and "out" for origin node
    raise Exception("TODO: Splitter de aristas")

    ## Paths creators
    q4_paths_creators = get_scatter_gather_services("q4_path_creator", "paths_creator",
                                                    "q4_edges_exc",
                                                    "q4_split_by_origin_and_dest_queue",
                                                    1
                                                    )
    system = system | q4_paths_creators

    ## Split by origin and destination nodes
    q4_paths_splitters_by_ends = get_splitter_docker_services("q4_path_splitter", 1,
                                                              input_queue="q4_split_by_origin_and_dest_queue",
                                                              output_exchange="q4_unique_paths_counter_exc",
                                                              key_fields=["From Bank", "Account", "To Bank", "Account.1"],
                                                              )
    system = system | q4_paths_splitters_by_ends

    ## Unique paths counters
    q4_unique_paths_counters = get_scatter_gather_services("q4_unique_paths_counter", "unique_paths_count",
                                                           "q4_paths_exc",
                                                           "results_4",
                                                           1
                                                           )
    system = system | q4_unique_paths_counters


    # Query 5
    ## Data reducers
    data_reducers_q5 = get_data_reducer_docker_services("q5_data_reducer", 1,
                                                        ["Timestamp", "From Bank", "Account", "To Bank", "Account.1", "Amount Paid"],
                                                        input_queue="cleaned_data",
                                                        output_queue="q5_reduced_data",
                                                        )
    system = system | data_reducers_q5

    ## Filter dates between 01/09/2022 and 05/09/2022 included
    raise Exception("TODO: Ver que fecha caiga entre 01/09/2022 y 05/09/2022 inclusive")

    ## Convertion to USD
    raise Exception("TODO: Conversión a USD")

    ## Filter of less than 1 USD
    q5_filter_lt_1_usd = get_filters_docker_services("q5_filter_lt_1_usd", 1,
                                                     "Amount Paid", "1", "lt",
                                                     input_queue="q5_converted_amounts_transactions",
                                                     output_queue="q5_small_amounts_transactions"
                                                     )
    system = system | q5_filter_lt_1_usd

    ## Filter by payment methods
    raise Exception("TODO: Que se verifique con más de un método por worker")

    ## Count transactions that arrive
    q5_counters = get_aggregator_docker_services("transaction_counter", 1,
                                                 input_queue="q5_countable_transactions",
                                                 output_queue="q5_totals_reached",
                                                 agg_op="count"
                                                 )
    
    ## Add results
    raise Exception("TODO: Sumador que sume resultados de todo")

    # Return complete YAML system
    return system