from common.middleware.worker_base import WorkerBase

class MoneyConverter(WorkerBase):

    def __init__(self):
        super().__init__()
        # Currency rates
        self._currency_rates_by_date = {}

    def process(self, data):
        pass
    
    def on_eof(self):
        pass