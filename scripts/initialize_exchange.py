import ccxt
from scripts.file_reader import read_exchange_keys


def initialize_exchanges():
    """Obtains exchanges from cryptocurrency websites."""
    exchanges, ids = [], []
    exchange_keys = read_exchange_keys()

    with open('input/exchanges.txt', 'r') as f:
        for line in f:
            ids.append(line.strip())

    for idx in ids:
        exchanges.append(initialize_exchange(idx))

    return exchanges


def initialize_exchange(idx):
    """Obtains exchange from cryptocurrency websites."""
    exchange_keys = read_exchange_keys()

    exchange = getattr(ccxt, idx)()
    exchange.load_markets()
    if idx in exchange_keys:
        ex_key = exchange_keys[idx]
        exchange.apiKey = ex_key.apiKey
        exchange.secret = ex_key.secret
    return exchange
