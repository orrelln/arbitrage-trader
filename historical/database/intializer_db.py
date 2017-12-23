import os
import pickle
from sys import argv
from time import time, sleep

import ccxt

from historical.database.exception_helper import ExceptionHelper


def main():
    helper = ExceptionHelper(argv[0], None, argv[1])
    intra_pairs = {}
    inter_pairs = {}
    create_directories()
    while True:
        end_time = time() + helper.sleep_time
        helper.iteration += 1
        exchanges = init_exchanges(helper)
        exchange_pairs = all_pairs(exchanges)
        init_symbols(intra_pairs, inter_pairs, exchange_pairs)
        with open('data/pairs/inter_pairs.p', 'wb+') as f:
            pickle.dump(inter_pairs, f, pickle.HIGHEST_PROTOCOL)
        with open('data/pairs/intra_pairs.p', 'wb+') as g:
            pickle.dump(intra_pairs, g, pickle.HIGHEST_PROTOCOL)
        if end_time > time():
            sleep(end_time - time())



def format_pair(pair):
    split_pair = pair.split('/')
    formatted_pair = split_pair[0] + '_' + split_pair[1]
    return formatted_pair


def create_directories():
    if not os.path.exists('logs'):
        os.mkdir('logs')
    if not os.path.exists('data'):
        os.mkdir('data')
        os.mkdir('data/pairs')
    if not os.path.exists('trades'):
        os.mkdir('trades')
        os.mkdir('trades/best')
        os.mkdir('trades/db')



def init_symbols(intra_pairs, inter_pairs, exchange_pairs):
    for p in exchange_pairs:
        symbols = get_common_symbols(p[0], p[1])
        key = (p[0].id, p[1].id)
        inter_pairs[key] = symbols
        for s in symbols:
            if p[0].id not in intra_pairs:
                intra_pairs[p[0].id] = []
            if p[1].id not in intra_pairs:
                intra_pairs[p[1].id] = []
            if s not in intra_pairs[p[0].id]:
                intra_pairs[p[0].id].append(s)
            if s not in intra_pairs[p[1].id]:
                intra_pairs[p[1].id].append(s)


def get_common_symbols(ex1, ex2):
    a = ex1.symbols
    b = ex2.symbols
    both_list = []
    for x in a:
        if blacklist(x, ex1.id):
            continue
        for y in b:
            if blacklist(y, ex2.id):
                continue
            if x == y:
                if 'EUR' in x or 'GBP' in x:
                    break
                both_list.append(x)
                break
    return both_list


def all_pairs(l):
    result = []
    for i in range(len(l) - 1):
        for j in range(i + 1, len(l)):
            result.append((l[i], l[j]))
    return result


def init_exchanges(helper):
    ids, exchanges = [], []
    with open('input/exchanges.txt', 'r') as f:
        for line in f:
            ids.append(line.strip())
    for idx in ids:
        exchange = init_exchange(helper, idx)
        exchanges.append(exchange)
    return exchanges


def init_exchange(helper, idx):
    while True:
        try:
            exchange = getattr(ccxt, idx)()
            exchange.load_markets()
            break
        except Exception as e:
            helper.record_exception(e)
    return exchange


def blacklist(sym, id):
    str = id + ':' + sym
    return {
        'bittrex:BTS/BTC': True,
        'poloniex:XVC/BTC': True
    }.get(str, False)


if __name__ == '__main__':
    main()
