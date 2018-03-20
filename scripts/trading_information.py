def taker_fee(ex_id):
    return {
        'bitfinex': 0.998,
        'bittrex': 0.9975,
        'kraken': 0.9974,
        'gdax': 0.9975,
        'poloniex': 0.9975,
        'bitstamp': 0.9975,
        'hitbtc2':  0.999,
        'binance': 0.999,
        'kucoin': 0.999,
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

    if market == 'bitstamp':
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

    # if market == 'poloniex':
    #     p = poloniex.Poloniex()
    #     currencies = p.returnCurrencies()
    #
    #     for c in currencies.items():
    #         if c[0] == currency:
    #             return float(c[1].get('txFee'))

    return 0.01