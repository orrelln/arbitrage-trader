import ccxt
from collections import namedtuple


class Initializer:
    """
    Initializes exchanges and currency-pairs.
    """
    def __init__(self):
        self.intra_pairs = {}
        self.inter_pairs = {}
        self.exchanges = []
        self.exchange_pairs = []

    def initialize_exchanges(self):
        """Obtains exchanges from cryptocurrency websites and sets up exchange pairs."""
        self.exchanges, ids = [], []

        with open('input/exchanges.txt', 'r') as f:
            for line in f:
                ids.append(line.strip())

        for idx in ids:
            exchange = getattr(ccxt, idx)()
            exchange.load_markets()
            self.exchanges.append(exchange)
        self.exchange_pairs = self._create_exchange_pairs(self.exchanges)

    def initialize_pairs(self):
        """Initializes currency-pairs."""
        self.intra_pairs, self.inter_pairs = {}, {}
        for pair in self.exchange_pairs:
            exchange1 = pair.ex1.id
            exchange2 = pair.ex2.id

            symbols = self._get_common_symbols(pair.ex1, pair.ex2)
            key = (exchange1, exchange2)
            self.inter_pairs[key] = symbols

            if exchange1 not in self.intra_pairs:
                self.intra_pairs[exchange1] = []
            if exchange2 not in self.intra_pairs:
                self.intra_pairs[exchange2] = []
            for symbol in symbols:
                if symbol not in self.intra_pairs[exchange1]:
                    self.intra_pairs[exchange1].append(symbol)
                if symbol not in self.intra_pairs[exchange2]:
                    self.intra_pairs[exchange2].append(symbol)

    def reset_exchange(self, idx):
        """Resets an exchange when errored out."""
        exchange = getattr(ccxt, idx)()
        exchange.load_markets()
        self.exchanges = [exchange if exchange.id == x.id else x for x in self.exchanges]

    def _create_exchange_pairs(self, l):
        ExPairs = namedtuple('ExchangePairs', ['ex1', 'ex2'])
        result = []
        for i in range(len(l) - 1):
            for j in range(i + 1, len(l)):
                result.append(ExPairs(l[i], l[j]))
        return result

    def _get_common_symbols(self, ex1, ex2):
        a = ex1.symbols
        b = ex2.symbols
        both_list = []
        for x in a:
            if self._blacklist(x, ex1.id):
                continue
            for y in b:
                if self._blacklist(y, ex2.id):
                    continue
                if x == y:
                    if 'EUR' in x or 'GBP' in x:
                        break
                    both_list.append(x)
                    break
        return both_list

    def _blacklist(self, sym, id):
        str = id + ':' + sym
        return {
            'bittrex:BTS/BTC': True,
            'poloniex:XVC/BTC': True
        }.get(str, False)