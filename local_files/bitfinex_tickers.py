import urllib.request
import json
from datetime import datetime


def bitfinex_tickers():
    tickers = {}
    with urllib.request.urlopen("https://api.bitfinex.com/v1/tickers") as url:
        data = json.loads(url.read().decode())
    for d in data:
        symbol = get_symbol(d['pair'])
        tickers[symbol] = bitfinex_structure(d, symbol)
    return tickers


def bitfinex_structure(ticker, symbol):
    timestamp = float(ticker['timestamp']) * 1000
    return {
        'symbol': symbol,
        'timestamp': timestamp,
        'datetime': iso8601(timestamp),
        'high': float(ticker['high']),
        'low': float(ticker['low']),
        'bid': float(ticker['bid']),
        'ask': float(ticker['ask']),
        'vwap': None,
        'open': None,
        'close': None,
        'first': None,
        'last': float(ticker['last_price']),
        'change': None,
        'percentage': None,
        'average': float(ticker['mid']),
        'baseVolume': float(ticker['volume']),
        'quoteVolume': None,
        'info': ticker,
    }


def iso8601(timestamp):
    utc = datetime.utcfromtimestamp(int(round(timestamp / 1000)))
    return utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


def get_symbol(symbol):
    first_sym = common_currency_code(symbol[:3])
    last_sym = common_currency_code(symbol[3:])
    return first_sym + '/' + last_sym


def common_currency_code(currency):
    # issue  #4 Bitfinex names Dash as DSH, instead of DASH
    if currency == 'DSH':
        return 'DASH'
    if currency == 'QTM':
        return 'QTUM'
    return currency