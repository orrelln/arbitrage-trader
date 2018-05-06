import ccxt
from scripts.initialize_exchange import initialize_exchanges
from collections import namedtuple
from arbitrage.trader import Trader

Order = namedtuple('Order', ['buy_ex', 'sell_ex', 'symbol', 'buy_price', 'sell_price', 'volume'])

exchanges_init = initialize_exchanges()

exchanges = {}
exchanges['kucoin'] = exchanges_init[2]
exchanges['binance'] = exchanges_init[1]


order = Order('kucoin', 'binance', 'GAS/BTC', 0.002800, 0.002200, 1.0)

trader = Trader(exchanges, order)

trader.perform_arbitrage()