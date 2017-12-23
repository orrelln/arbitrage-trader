import os
import pickle
from time import time, sleep, strftime, gmtime

import mysql.connector
from intializer_db import init_exchanges, all_pairs
from poloniex import poloniex

from historical.database.exception_helper import ExceptionHelper


def main():
    helper = ExceptionHelper('arb', None, '15', None)
    update_time = time() + helper.update_time
    tickers = {}
    exchanges, exchange_pairs, inter_pairs = load_exchanges(helper)
    while True:
        best_pair = [0, 0, 0, 0, 0]
        sleep_time = time() + helper.sleep_time
        helper.iteration += 1
        for exchange in exchanges:
            read_from_database(exchange.id, helper, tickers)
        for pair in exchange_pairs:
            key = (pair[0].id, pair[1].id)
            symbols = inter_pairs[key]
            for symbol in symbols:
                key1 = (pair[0].id, symbol)
                key2 = (pair[1].id, symbol)
                if check_valid(symbol, key1, key2, tickers) is False:
                    continue
                try:
                    calc_arb(tickers, key1, key2, helper, best_pair)
                    calc_arb(tickers, key2, key1, helper, best_pair)
                except Exception as e:
                    helper.record_exception(e)
        if best_pair[0] is not 0:
            write_best_pair(best_pair, helper)
        if sleep_time > time():
            sleep(sleep_time-time())
        if time() > update_time:
            exchanges, exchange_pairs, inter_pairs = load_exchanges(helper)
            update_time = time() + helper.update_time


def calc_arb(tickers, key1, key2, helper, best_pair):
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
            write_to_txt(ex_id2, ex_id1, symbol, usd_made)
            if usd_made > float(best_pair[0]):
                best_pair[0] = usd_made
                best_pair[1] = helper.iteration
                best_pair[2] = ex_id2
                best_pair[3] = ex_id1
                best_pair[4] = symbol

def write_to_txt(ex_id2, ex_id1, symbol, usd_made):
    current_d = strftime("%d", gmtime(time()))
    current_ym = strftime("%Y-%m", gmtime(time()))
    if not os.path.exists('trades/db/' + current_ym):
        os.mkdir('trades/db/' + current_ym)
    file_path = 'trades/db/' + current_ym + '/' + current_d + '.txt'
    with open(file_path, 'a') as f:
        f.write(strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ',' + ex_id2 + ',' + ex_id1 + ',' + symbol + ','
                + str(usd_made - 100)[:4])
        f.write('\n')


def write_best_pair(best_pair, helper):
    usd_made, iteration, ex_id2, ex_id1, symbol = best_pair
    current_d = strftime("%d", gmtime(time()))
    current_ym = strftime("%Y-%m", gmtime(time()))
    if not os.path.exists('trades/best/' + current_ym):
        os.mkdir('trades/best/' + current_ym)
    file_path = 'trades/best/' + current_ym + '/' + current_d + '.log'
    with open(file_path, 'a') as f:
        f.write('[' + helper.program + ']: ' + 'Iteration: ' + str(iteration) + ' Datetime: '
                + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Buy: ' + ex_id2 + ' Sell: ' + ex_id1 + ' Pair: '
                + symbol + ' ' + ' Profit: ' + str(usd_made - 100)[:4] + '%')
        f.write('\n')


def load_exchanges(helper):
    exchanges = init_exchanges(helper)
    exchange_pairs = all_pairs(exchanges)
    with open('data/pairs/inter_pairs.p', 'rb') as f:
        inter_pairs = pickle.load(f)
    return exchanges, exchange_pairs, inter_pairs


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


def check_valid(symbol, key1, key2, tickers):
    if key1 not in tickers:
        return False
    if key2 not in tickers:
        return False
    if 'USD' in symbol:
        return False
    if tickers[key1]['ask'] == 0.0 or tickers[key1]['bid'] == 0.0 or tickers[key2]['ask'] == 0.0 or tickers[key2]['bid'] == 0.0:
        return False
    return True


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


def read_from_database(ex_id, helper, tickers):
    cnx = mysql.connector.connect(user='b1kxsssu6ej4', password='7sxgckl5ndg1wj7la8esqvxw',
                                  host='mytestdb.cg7oi5h8vvxm.us-east-2.rds.amazonaws.com',
                                  port='3306', database='mytestdb')
    cursor = cnx.cursor()
    exchange_db = 'mytestdb.' + ex_id
    add_file = ( "SELECT symbol, datetime, bid, ask FROM " + exchange_db +
                 " WHERE (symbol, datetime) IN ("
                 " SELECT symbol, MAX(datetime)"
                 " FROM " + exchange_db +
                 " GROUP BY symbol )")
    try:
        cursor.execute(add_file)
    except mysql.connector.Error as err:
        helper.record_sql_exception(err)
    for (symbol, datetime, bid, ask) in cursor:
        key = (ex_id, symbol)
        ind_ticker = {'symbol':symbol, 'datetime':datetime, 'bid':bid, 'ask':ask}
        tickers[key] = ind_ticker
    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    main()


