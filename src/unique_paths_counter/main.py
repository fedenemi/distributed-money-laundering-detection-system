import logging
from unique_paths_counter.unique_paths_counter import UniquePathsCounter

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    unique_paths_counter = UniquePathsCounter()
    unique_paths_counter.run()