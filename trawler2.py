import ccxt
import pickle
import json
from time import time, sleep, strftime, gmtime
from sys import argv
import simpleflock
from time import mktime
from datetime import datetime


def main():
    iteration = 0
    last_e = -1
    exchange = init_exchange(argv[1])
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    while True:
        end_time = time() + argv[2]
        iteration += 1
        try:
            write_tickers(exchange, intra_pairs[exchange.id])
        except Exception as e:
            file_name = 'logs/' + exchange.id + '.log'
            with open(file_name, 'a') as f:
                f.write('[' + argv[0] + '] ' + 'Iteration: ' + str(iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exception: ' + str(e))
                f.write('\n')
            if last_e == iteration - 1:
                exchange = init_exchange(argv[1])
            last_e = iteration
        if end_time > time():
            sleep(end_time-time())


def write_tickers(exchange, symbols):
    if exchange.id == 'bitfinex' or exchange.id == 'gdax' or exchange.id == 'bitstamp':
        for s in symbols:
            ind_ticker = exchange.fetch_ticker(s)
            flock_file = 'locks/' + exchange.id + format_pair(s)
            file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
            with simpleflock.SimpleFlock(flock_file, timeout=5):
                with open(file_name, 'a') as f:
                    json.dump(ind_ticker, f)
                    f.write('\n')
    else:
        all_tickers = exchange.fetch_tickers()
        for s in symbols:
            ind_ticker = all_tickers[s]
            flock_file = 'locks/' + exchange.id + format_pair(s)
            file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
            with simpleflock.SimpleFlock(flock_file, timeout=5):
                with open(file_name, 'a') as f:
                    json.dump(ind_ticker, f)
                    f.write('\n')


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
