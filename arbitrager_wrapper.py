from arbitrage.initializer import Initializer
import sys
from arbitrage.arbitrager import Arbitrager
from scripts.wrappers import indef_call, timed_call
from scripts.decorators import exception_catch


@exception_catch('error')
def inner_loop(arbitrager_obj):
    arbitrager_obj.load_tickers()
    arbitrager_obj.ticker_percentages()
    arbitrager_obj.order_book_profit()
    arbitrager_obj.create_trader()


@exception_catch('error')
def arbitrager_loop(arbitrager_obj):
    timed_call(inner_loop, 30, (1200 / 30), arbitrager_obj)
    arbitrager_obj.exchanges = arbitrager_obj.load_exchanges()
    arbitrager_obj.exchange_pairs = arbitrager_obj.load_exchange_pairs()
    arbitrager_obj.inter_pairs = arbitrager_obj.load_inter_pairs()


def main():
    arbitrager_obj = Arbitrager()
    indef_call(arbitrager_loop, 0, arbitrager_obj)


if __name__ == '__main__':
    main()