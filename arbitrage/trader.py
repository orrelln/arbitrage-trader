import ccxt
from collections import namedtuple


class Trader:
    """
    WIP
    Performs inter-exchange trades, tracks information for wallets, and performs balancing mechanisms
    """

    def __init__(self):
        self.intra_pairs = {}
        self.inter_pairs = {}
        self.exchange_keys = {}
        self.exchanges = []
        self.exchange_pairs = []
        self.wallets = {}