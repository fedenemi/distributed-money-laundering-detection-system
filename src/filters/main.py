import logging
from filters.filter import FilterWorker

if __name__ == "__main__":
    logging.basicConfig(filename="filter_worker.log", level=logging.INFO)
    filter_worker = FilterWorker()
    filter_worker.run()