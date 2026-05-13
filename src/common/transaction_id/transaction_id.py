class TransactionID():
    def __init__(self, bank, account):
        self.bank = bank
        self.account = account

    def __del__(self):
        del self.bank
        del self.account

    def __eq__(self, other):
        return self.bank == other.bank and self.account == other.account

    def __hash__(self):
        return hash((self.bank, self.account))
    
    def as_tuple(self):
        return (self.bank, self.account)