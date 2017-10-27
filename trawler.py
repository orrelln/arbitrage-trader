import ccxt
import pickle
import json
from time import time, sleep
from sys import argv


def main():
    iteration = 0
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    exchange = init_exchange(argv[1])
    while True:
        endtime = time() + 60
        iteration += 1
        try:
            write_tickers(exchange, intra_pairs[exchange.id])
        except Exception:
            file_name = 'log/' + exchange.id
            with open(file_name, 'a') as f:
                f.write(str(time()) + ' ' + str(iteration))
                f.write(str(Exception))
                f.write('\n')
        while endtime > time():
            sleep(1)


def write_tickers(exchange, symbols):
    if exchange.id == 'bitfinex' or exchange.id == 'gdax' or exchange.id == 'bitstamp':
        for s in symbols:
            ind_ticker = exchange.fetch_ticker(s)
            file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
            with open(file_name, 'a') as f:
                json.dump(ind_ticker, f)
                f.write('\n')
    else:
        all_tickers = exchange.fetch_tickers()
        for s in symbols:
            ind_ticker = all_tickers[s]
            file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
            with open(file_name, 'a') as f:
                json.dump(ind_ticker, f)
                f.write('\n')


def init_exchanges():
    ids = ['bittrex', 'bitfinex', 'gdax', 'poloniex', 'kraken', 'bitstamp', 'hitbtc2']
    exchanges = []
    for id in ids:
        exchange = getattr(ccxt, id)()
        exchange.load_markets()
        exchanges.append(exchange)
    return exchanges


def init_exchange(id):
    exchange = getattr(ccxt, id)()
    exchange.load_markets()
    return exchange


def format_pair(pair):
    split_pair = pair.split('/')
    formatted_pair = split_pair[0] + '_' + split_pair[1]
    return formatted_pair


if __name__ == '__main__':
    main()
