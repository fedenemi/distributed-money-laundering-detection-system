import logging
from transactions_graph_agg import TransactionsGraphAgg

if __name__ == "__main__":
    logging.basicConfig(filename="transaction_graph_agg.log", level=logging.INFO)
    transactions_graph_agg = TransactionsGraphAgg()
    transactions_graph_agg.start()