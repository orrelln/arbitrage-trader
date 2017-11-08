import ccxt
import pickle
import os


def main():
    intra_pairs = {}
    inter_pairs = {}
    exchanges = init_exchanges()
    create_directories(exchanges)
    exchange_pairs = all_pairs(exchanges)
    init_symbols(intra_pairs, inter_pairs, exchange_pairs)
    with open('data/pairs/inter_pairs.p', 'wb+') as f:
        pickle.dump(inter_pairs, f, pickle.HIGHEST_PROTOCOL)
    with open('data/pairs/intra_pairs.p', 'wb+') as g:
        pickle.dump(intra_pairs, g, pickle.HIGHEST_PROTOCOL)
    for exchange in exchanges:
        for pair in intra_pairs[exchange.id]:
            file_name = 'data/' + exchange.id + '/' + format_pair(pair) + '.json'
            if not os.path.exists(file_name):
                open(file_name, 'a').close()


def format_pair(pair):
    split_pair = pair.split('/')
    formatted_pair = split_pair[0] + '_' + split_pair[1]
    return formatted_pair


def create_directories(exchanges):
    if not os.path.exists('logs'):
        os.mkdir('logs')
    if not os.path.exists('data'):
        os.mkdir('data')
        os.mkdir('data/pairs')
    if not os.path.exists('locks'):
        os.mkdir('locks')
    for exchange in exchanges:
        path = 'data/' + exchange.id
        path2 = 'logs/' + exchange.id + '.log'
        if not os.path.exists(path):
            os.mkdir(path)
        if not os.path.exists(path2):
            open(path2, 'a').close()


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


def init_exchanges():
    ids = ['bittrex', 'bitfinex', 'gdax', 'poloniex', 'kraken', 'bitstamp', 'hitbtc2']
    exchanges = []
    for id in ids:
        exchange = getattr(ccxt, id)()
        exchange.load_markets()
        exchanges.append(exchange)
    return exchanges


def blacklist(sym, id):
    str = id + ':' + sym
    return {
        'bittrex:BTS/BTC': True,
        'poloniex:XVC/BTC': True
    }.get(str, False)


if __name__ == '__main__':
    main()
