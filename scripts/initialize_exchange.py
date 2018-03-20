import ccxt


def initialize_exchanges():
    """Obtains exchanges from cryptocurrency websites."""
    exchanges, ids = [], []
    # self.exchange_keys = read_exchange_keys()

    with open('input/exchanges.txt', 'r') as f:
        for line in f:
            ids.append(line.strip())

    for idx in ids:
        exchange = getattr(ccxt, idx)()
        exchange.load_markets()
        # if idx in self.exchange_keys:
        #     ex_key = self.exchange_keys[idx]
        #     exchange.apiKey = ex_key.apiKey
        #     exchange.secret = ex_key.secret
        exchanges.append(exchange)

    return exchanges

def initialize_exchange(idx):
    """Obtains exchange from cryptocurrency websites."""
    # self.exchange_keys = read_exchange_keys()

    exchange = getattr(ccxt, idx)()
    exchange.load_markets()
    # if idx in self.exchange_keys:
    #     ex_key = self.exchange_keys[idx]
    #     exchange.apiKey = ex_key.apiKey
    #     exchange.secret = ex_key.secret
    return exchange
