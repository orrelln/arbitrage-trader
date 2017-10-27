import ccxt
import pickle
import json
from time import time, sleep, strftime, gmtime
from sys import argv
from time import mktime
from datetime import datetime



def main():
    iteration = 0
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    exchange = init_exchange('bittrex')
    while True:
        endtime = time() + 60
        iteration += 1
        try:
            write_tickers(exchange, intra_pairs[exchange.id])
        except Exception as e:
            file_name = 'logs/' + exchange.id + '.log'
            with open(file_name, 'a') as f:
                f.write('Iteration: ' +  str(iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exception: ' + str(e))
                f.write('\n')
        while endtime > time():
            sleep(1)


def write_tickers(exchange, symbols):
    value = 1/0
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
