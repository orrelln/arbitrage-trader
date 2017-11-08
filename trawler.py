import ccxt
import pickle
import urllib.request
import json
from time import time, sleep, strftime, gmtime
from sys import argv
import simpleflock
from time import mktime
from datetime import datetime
from bitfinex_tickers import bitfinex_tickers


class TrawlerHelper:
    def __init__(self, sleep_time, id):
        self.sleep_time = sleep_time
        self.id = id
        self.iteration = 0
        self.last_error = -1

    def exchange_malfunction(self):
        if self.last_error == self.iteration:
            return True
        else:
            return False


def main():
    helper = TrawlerHelper(argv[2], argv[1])
    exchange = init_exchange(helper.id)
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    while True:
        end_time = time() + float(helper.sleep_time)
        helper.iteration += 1
        write_tickers(exchange, intra_pairs[exchange.id], helper)
        if end_time > time():
            sleep(end_time-time())


def write_tickers(exchange, symbols, helper):
    if exchange.id == 'gdax' or exchange.id == 'bitstamp':
        individual_tickers(exchange, symbols, helper)
    else:
        batch_tickers(exchange, symbols, helper)


def individual_tickers(exchange, symbols, helper):
    for s in symbols:
        ind_ticker = None
        while ind_ticker is None:
            try:
                ind_ticker = exchange.fetch_ticker(s)
            except Exception as e:
                if not record_exception(exchange, helper, e):
                    continue
        flock_file = 'locks/' + exchange.id + format_pair(s)
        file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
        with simpleflock.SimpleFlock(flock_file, timeout=5):
            with open(file_name, 'a') as f:
                json.dump(ind_ticker, f)
                f.write('\n')


def batch_tickers(exchange, symbols, helper):
    all_tickers = None
    while all_tickers is None:
        try:
            if exchange.id == 'bitfinex':
                all_tickers = bitfinex_tickers()
            else:
                all_tickers = exchange.fetch_tickers()
        except Exception as e:
            if not record_exception(exchange, helper, e):
                return
    for s in symbols:
        ind_ticker = all_tickers[s]
        flock_file = 'locks/' + exchange.id + format_pair(s)
        file_name = 'data/' + exchange.id + '/' + format_pair(s) + '.json'
        with simpleflock.SimpleFlock(flock_file, timeout=5):
            with open(file_name, 'a') as f:
                json.dump(ind_ticker, f)
                f.write('\n')


def record_exception(exchange, helper, e):
    file_name = 'logs/' + exchange.id + '.log'
    with open(file_name, 'a') as f:
        f.write('[trawler]: ' + 'Iteration: ' + str(helper.iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exception: ' + str(e).strip()[:25])
        f.write('\n')
    if helper.exchange_malfunction():
        exchange = init_exchange(helper.id)
        return False
    helper.last_error = helper.iteration
    return True


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
