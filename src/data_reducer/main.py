import logging
from data_reducer.data_reducer import DataReducer

if __name__ == "__main__":
    logging.basicConfig(filename="data_reducer.log", level=logging.INFO)
    data_reducer = DataReducer()
    data_reducer.run()