import logging
from paths_creator.paths_creator import PathsCreator

if __name__ == "__main__":
    logging.basicConfig(filename="transaction_graph_agg.log", level=logging.INFO)
    paths_creator = PathsCreator()
    paths_creator.start()