from arbitrage.initializer import Initializer
import sys
from arbitrage.trawler import Trawler
from scripts.wrappers import indef_call, timed_call
from scripts.decorators import exception_catch


@exception_catch('error')
def inner_loop(trawler_obj):
    trawler_obj.get_tickers()
    trawler_obj.write_to_database()


@exception_catch('error')
def trawler_loop(trawler_obj):
    timed_call(inner_loop, 30, (1200 / 30), trawler_obj)
    trawler_obj.exchange = trawler_obj.load_exchange(trawler_obj.exchange.id)
    trawler_obj.intra_pairs = trawler_obj.load_intra_pairs()


def main():
    trawler_obj = Trawler(sys.argv[1])
    indef_call(trawler_loop, 0, trawler_obj)


if __name__ == '__main__':
    main()