import ccxt
import pickle
from poloniex import poloniex
import bittrex
import simpleflock
from intializer import init_exchanges, all_pairs
from trawler import format_pair
import json
import tailer
from time import time, sleep, strftime, gmtime


def main():
    tickers = {}
    exchanges = None
    while exchanges is None:
        try:
            exchanges = init_exchanges()
        except Exception as e:
            pass
    exchange_pairs = all_pairs(exchanges)
    iteration = 0
    with open('data/pairs/inter_pairs.p', 'rb') as f:
        inter_pairs = pickle.load(f)
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    while True:
        error = False
        iteration += 1
        end_time = time() + 15
        for exchange in exchanges:
            if error is False:
                try:
                    get_tickers(tickers, exchange.id, intra_pairs[exchange.id])
                except Exception as e:
                    error = True
        for pair in exchange_pairs:
            if error is False:
                key = (pair[0].id, pair[1].id)
                symbols = inter_pairs[key]
                for symbol in symbols:
                    if 'USD' in symbol:
                        continue
                    key1 = (pair[0].id, symbol)
                    key2 = (pair[1].id, symbol)
                    if tickers[key1]['ask'] == 0.0 or tickers[key1]['bid'] == 0.0 or tickers[key2]['ask'] == 0.0 or tickers[key2]['bid'] == 0.0:
                        continue
                    try:
                        calc_arb(tickers, key1, key2, iteration)
                        calc_arb(tickers, key2, key1, iteration)
                    except Exception as e:
                        error = True
        if end_time > time():
            sleep(end_time-time())


def calc_arb(tickers, key1, key2, iteration):
    ex_id1 = key1[0]
    ex_id2 = key2[0]
    symbol = key2[1]
    arbitrage = (tickers[key1]['bid'] / tickers[key2]['ask']) * taker_fee(ex_id1) * taker_fee(ex_id2) - 1
    if arbitrage > 0.5:
        pass
    elif arbitrage > 0:
        cur_amt = convert_usd(tickers, ex_id2, symbol, 100, 'from')
        arb_made = make_trade(tickers, ex_id2, ex_id1, symbol, cur_amt)
        usd_made = convert_usd(tickers, ex_id2, symbol, arb_made, 'to')
        if usd_made > 101.0:
            if ex_id1 == 'hitbtc2' or ex_id2 == 'hitbtc2':
                file_path = 'trades/' + 'hitbtc2.log'
            else:
                file_path = 'trades/' + 'trades.log'
            with open(file_path, 'a') as f:
                f.write('[arbitrager]: ' + 'Iteration: ' + str(iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) +
                        ' Buy: ' + ex_id2 + ' Sell: ' + ex_id1 + ' Pair: ' + symbol + ' ' + ' Profit: ' + str(usd_made - 100)[:4] + '%')
                f.write('\n')


def get_tickers(tickers, ex_id, symbols):
    for symbol in symbols:
        key1 = (ex_id, symbol)
        flock_file = 'locks/' + ex_id + format_pair(symbol)
        file_name1 = 'data/' + ex_id + '/' + format_pair(symbol) + '.json'
        with simpleflock.SimpleFlock(flock_file, timeout=5):
            file_data = tailer.tail(open(file_name1), 1)
            tickers[key1] = json.loads(file_data[0])


def convert_usd(tickers, ex_id, s, amt, direction):
    split_pair = s.split('/')
    base_cur = split_pair[1] + usd_pair(ex_id)
    key = (ex_id, base_cur)
    if direction == 'from':
        conversion_rate = tickers[key]['ask']
        return amt/conversion_rate
    else:
        conversion_rate = tickers[key]['bid']
        return amt * conversion_rate


def make_trade(tickers, ex_id1, ex_id2, s, amt):
    split_pair = s.split('/')
    key1 = (ex_id1, s)
    key2 = (ex_id2, s)
    send_rate = tickers[key1]['ask']
    send_amt = (amt/send_rate)*taker_fee(ex_id1)
    send_amt -= get_fee(ex_id1, split_pair[1])
    receive_rate = tickers[key2]['bid']
    receive_amt = (send_amt*receive_rate)*taker_fee(ex_id2)
    return receive_amt


def taker_fee(ex_id):
    return {
        'bitfinex': 0.998,
        'bittrex': 0.9975,
        'kraken': 0.9974,
        'gdax': 0.9975,
        'poloniex': 0.9975,
        'bitstamp': 0.9975,
        'hitbtc2':  0.999
    }[ex_id]


def usd_pair(ex_id):
    return {
        'bitfinex': '/USD',
        'bittrex': '/USDT',
        'kraken': '/USD',
        'gdax': '/USD',
        'poloniex': '/USDT',
        'bitstamp': '/USD',
        'hitbtc2': '/USD'
    }[ex_id]


def get_fee(market, currency):
    if market == 'bittrex':
        if currency == 'BTC':
            return 0.001
        if currency == 'ETH':
            return 0.002
        if currency == 'XRP':
            return 5
        if currency == 'XLM':
            return 0.01
        if currency == 'LTC':
            return 0.01
        if currency == 'XDG':
            return 2.0
        if currency == 'ZEC':
            return 0.0001
        if currency == 'ICN':
            return 0.2
        if currency == 'REP':
            return 0.01
        if currency == 'ETC':
            return 0.005
        if currency == 'MLN':
            return 0.003
        if currency == 'XMR':
            return 0.05
        if currency == 'DASH':
            return 0.005
        if currency == 'GNO':
            return 0.01
        if currency == 'USDT':
            return 5.0
        if currency == 'EOS':
            return 0.5
        if currency == 'BCH':
            return 0.001

    if market ==  'bitstamp':
        return 0

    if market == 'kraken':
        if currency == 'BTC':
            return 0.001
        if currency == 'ETH':
            return 0.005
        if currency == 'XRP':
            return 0.02
        if currency == 'XLM':
            return 0.00002
        if currency == 'LTC':
            return 0.02
        if currency == 'XDG':
            return 2.0
        if currency == 'ZEC':
            return 0.0001
        if currency == 'ICN':
            return 0.2
        if currency == 'REP':
            return 0.01
        if currency == 'ETC':
            return 0.005
        if currency == 'MLN':
            return 0.003
        if currency == 'XMR':
            return 0.05
        if currency == 'DASH':
            return 0.005
        if currency == 'GNO':
            return 0.01
        if currency == 'USDT':
            return 5.0
        if currency == 'EOS':
            return 0.5
        if currency == 'BCH':
            return 0.001

    if market == 'bitfinex':
        if currency == 'BTC':
            return 0.0005
        if currency == 'ETH':
            return 0.01
        if currency == 'LTC':
            return 0.001
        if currency == 'ZEC':
            return 0.001
        if currency == 'OMG':
            return 0.1
        if currency == 'BCH':
            return 0.0005
        if currency == 'NEO':
            return 0.0
        if currency == 'MIOTA':
            return 0.0
        if currency == 'ETC':
            return 0.01
        if currency == 'DASH':
            return 0.01
        if currency == 'EOS':
            return 0.1
        if currency == 'XMR':
            return 0.04
        if currency == 'XRP':
            return 0.02
        if currency == 'ETP':
            return 0.01
        if currency == 'SAN':
            return 0.1
        if currency == 'USDT':
            return 5.0

    if market == 'poloniex':
        p = poloniex.Poloniex()
        currencies = p.returnCurrencies()

        for c in currencies.items():
            if c[0] == currency:
                return float(c[1].get('txFee'))

    return 0.01


if __name__ == '__main__':
    main()


