import ccxt
import pickle
import json
import os
import sched
import time
from sys import argv


def main():
   # s = sched.scheduler(time.time, time.sleep)
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    start_time = time.time()
    if len(argv) == 1:
        exchanges = init_exchanges()
        for exchange in exchanges:
            write_tickers(exchange, intra_pairs[exchange.id])
    else:
        exchange = init_exchange(argv[1])
        write_tickers(exchange, intra_pairs[exchange.id])
    print(time.time() - start_time)


def write_parent(intra_pairs, exchanges):
    for exchanges in exchanges:
        write_tickers(exchanges, intra_pairs[exchanges.id])


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
