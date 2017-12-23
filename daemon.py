import threading
import sys, os
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time, sleep
from trader.arbitrager import Arbitrager
from trader.trawler import Trawler
from trader.initializer import Initializer
from ccxt import NetworkError, ExchangeError


def main():
    config = get_config()
    initializer, arbitrager, trawlers = create_objects()
    run_application(config, trawlers, initializer, arbitrager)


def run_application(config, trawlers, initializer, arbitrager):
    num_workers = trawlers.__len__() + 1
    init_time = time() + float(config['initializer_time'])

    trawl_time = time() + float(config['trawler_time'])
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        #executor.submit(arbitrager_fn, arbitrager)
        for result in executor.map(trawler_fn, trawlers):
            if result is not None:
                initializer.reset_exchange(result)
                print(result)
        # if trawl_time > time():
        #     sleep(trawl_time - time())
        # if init_time > time():
        #     init_time = time() + float(config['initializer_time'])
        #     initializer, arbitrager, trawlers = create_objects()


def trawler_fn(trawler):
    try:
        trawler.get_tickers()
    except NetworkError or ExchangeError:
        return trawler.exchange.id
    else:
        return None


def arbitrager_fn(arbitrager):
    arbitrager.load_tickers()
    arbitrager.calculate_arbitrage()
    print(arbitrager.arbitrage)


def get_config():
    config = {}
    with open('input/config.txt', 'r') as f:
        for line in f:
            (key, val) = line.strip().split('=')
            config[key] = val
    return config


def create_objects():
    trawlers = []
    initializer = Initializer()

    while initializer.exchange_pairs.__len__() == 0:
        try:
            initializer.initialize_exchanges()
        except NetworkError or ExchangeError:
            pass

    initializer.initialize_pairs()
    arbitrager = Arbitrager(initializer.inter_pairs, initializer.exchanges, initializer.exchange_pairs)
    for exchange in initializer.exchanges:
        trawlers.append(Trawler(initializer.intra_pairs, exchange))

    return initializer, arbitrager, trawlers


if __name__ == '__main__':
    main()
