import logging
from unique_paths_counter.unique_paths_counter import UniquePathsCounter

if __name__ == "__main__":
    logging.basicConfig(filename="transaction_graph_agg.log", level=logging.INFO)
    unique_paths_counter = UniquePathsCounter()
    unique_paths_counter.start()