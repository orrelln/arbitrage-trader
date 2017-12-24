import threading
import sys, os
from threading import Semaphore
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from time import time, sleep
from arbitrage.arbitrager import Arbitrager
from arbitrage.trawler import Trawler
from arbitrage.initializer import Initializer
from ccxt import NetworkError, ExchangeError


def main():
    """
    WIP
    Module for managing components for arbitrage trader. On hiatus until decision is made to do arbitrage trading.
    """
    config = get_config()
    initializer, arbitrager, trawlers = create_objects()
    run_application(config, trawlers, initializer, arbitrager)


def run_application(config, trawlers, initializer, arbitrager):
    init_time = time() + float(config['initializer_time'])

    trawler_time = float(config['trawler_time'])
    trawler_process = ProcessPoolExecutor()
    trawler_futures = []
    for trawler in trawlers:
        trawler_futures.append(trawler_process.submit(trawler_fn, trawler, trawler_time, init_time))

    while True:
        # need to refactor structure to not pass around all of these vars. Maybe make a daemon class??
        for idx, future in enumerate(trawler_futures):
            if future.done():
                if future.result() is not None:
                    exchange = initializer.reset_exchange(future.result())
                    trawler = reset_trawler(exchange, trawlers, initializer.intra_pairs)
                    trawler_futures[idx] = trawler_process.submit(trawler_fn, trawler, trawler_time, init_time)
        # possibly refactor here as it's getting messy
        # NEED TO DO HERE:
        # 1. Reset everything when init_time has elapsed
        # 2. Implement functionality of Trader class
        # 3. Probably make this shit a class

    # with ThreadPoolExecutor(max_workers=num_workers) as executor:
    #     #executor.submit(arbitrager_fn, arbitrager)
    #     for result in executor.map(trawler_fn, trawlers):
    #         if result is not None:
    #             initializer.reset_exchange(result)
    #             print(result)
    #     # if trawl_time > time():
    #     #     sleep(trawl_time - time())
    #     # if init_time > time():
    #     #     init_time = time() + float(config['initializer_time'])
    #     #     initializer, arbitrager, trawlers = create_objects()


def trawler_fn(trawler, t_time, init_time):
    while True:
        sleep_time = time() + t_time
        try:
            trawler.get_tickers()
        except NetworkError or ExchangeError:
            return trawler.exchange.id
        else:
            if init_time < time():
                return None
            if sleep_time > time():
                sleep(sleep_time-time())


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

# fix this function or change module to class
def reset_trawler(exchange, trawlers, intra_pairs):
    current_trawler = None
    for idx, trawlers in enumerate(trawlers):
        if exchange.id == trawlers.exchange.id:
            trawler = Trawler(intra_pairs, exchange)
    return trawler


if __name__ == '__main__':
    main()
