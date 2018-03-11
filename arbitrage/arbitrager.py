from collections import namedtuple
import mysql.connector
from scripts.trading_information import taker_fee
from scripts.file_reader import read_sql_information
from arbitrage.trader import Trader
import pickle
import ccxt
import threading
import operator


class Arbitrager:
    """
    Reads ticker information and calculates arbitrage opportunities.
    """
    IdSymbol = namedtuple('IdSymbol', ['exchange_id', 'symbol'])
    Pair = namedtuple('Pair', ['buy_ex', 'sell_ex', 'symbol'])
    Order = namedtuple('Order', ['buy_ex', 'sell_ex', 'symbol', 'buy_price', 'sell_price', 'volume', 'profit'])

    def __init__(self):
        self.tickers = {}
        self.percentages = {}
        self.orders = []

        self.exchanges = self.load_exchanges()
        self.exchange_pairs = self.load_exchange_pairs()
        self.inter_pairs = self.load_inter_pairs()

    def load_tickers(self):
        """Obtains database information for most recent tickers."""
        for exchange_id in self.exchanges:
            self._read_from_database(exchange_id)

    def ticker_percentages(self):
        """Calculates percentage differences in prices between exchanges."""
        for pair in self.exchange_pairs:
            exchange_id1 = pair.ex1.id
            exchange_id2 = pair.ex2.id
            inter_key = (exchange_id1, exchange_id2)
            symbols = self.inter_pairs[inter_key]

            for symbol in symbols:
                exchange_key1 = (exchange_id1, symbol)
                exchange_key2 = (exchange_id2, symbol)

                # checks validity of potential comparision, and doesn't calculate arbitrage if false
                if self._check_valid(symbol, exchange_key1, exchange_key2) is False:
                    self.percentages[self.Pair(exchange_id1, exchange_id2, symbol)] = None
                    self.percentages[self.Pair(exchange_id2, exchange_id1, symbol)] = None
                    continue

                # calculates percentage at ticker level and stores potential
                pair1 = ((self.tickers[exchange_key2]['bid'] / self.tickers[exchange_key1]['ask'])
                              * taker_fee(exchange_key1) * taker_fee(exchange_key2) - 1)
                pair2 = ((self.tickers[exchange_key1]['bid'] / self.tickers[exchange_key2]['ask'])
                              * taker_fee(exchange_key1) * taker_fee(exchange_key2) - 1)
                self.percentages[self.Pair(exchange_id1, exchange_id2, symbol)] = pair1
                self.percentages[self.Pair(exchange_id2, exchange_id1, symbol)] = pair2

    def order_book_profit(self, percentage_threshold=20):
        """Calculates maximum possible buying opportunity."""
        order_books = {}

        for trading_pair in self.percentages:
            if self.percentages[trading_pair] < percentage_threshold:
                continue

            buy_exchange = self.exchanges[trading_pair.buy_ex]
            sell_exchange = self.exchanges[trading_pair.sell_ex]
            buy_pair = self.IdSymbol(trading_pair.buy_ex, trading_pair.symbol)
            sell_pair = self.IdSymbol(trading_pair.sell_ex, trading_pair.symbol)

            # save order books as they might be called multiple times in single iteration (lowers API calls)
            if buy_pair not in order_books:
                order_books[buy_pair] = buy_exchange.fetch_order_book(buy_pair.symbol)
            if sell_pair not in order_books:
                order_books[sell_pair] = sell_exchange.fetch_order_book(sell_pair.symbol)

            self.orders.append(self._order_book_calculation(buy_pair, sell_pair, order_books))

    def create_trader(self):
        max_profit = max(self.orders, key=operator.itemgetter(6))
        if max_profit > 0.1:
            trade_obj = Trader(self.exchanges, max_profit)
            trade_thr = threading.Thread(target=trade_obj.perform_arbitrage())
            trade_thr.start()
        self.orders = []


    def load_inter_pairs(self):
        with open('input/inter_pairs.p', 'rb') as f:
            inter_pairs = pickle.load(f)
        return inter_pairs

    def load_exchanges(self):
        with open('input/exchanges.p', 'rb') as f:
            exchanges = pickle.load(f)
        return exchanges

    def load_exchange_pairs(self):
        exchange_ids = [exchange.id for exchange in self.exchanges]
        return self._create_exchange_pairs(exchange_ids)

    def _create_exchange_pairs(self, l):
        ExPairs = namedtuple('ExchangePairs', ['ex1', 'ex2'])
        result = []
        for i in range(len(l) - 1):
            for j in range(i + 1, len(l)):
                result.append(ExPairs(l[i], l[j]))
        return result

    def _order_book_calculation(self, buy_pair, sell_pair, order_books):
        buy_volume, sell_volume, sell_price, buy_price = 0, 0, 0, 0
        for ask in order_books[buy_pair]:
            buy_volume += ask[1]
            buy_price = ask[0]
            sell_volume = 0
            for bid in order_books[sell_pair]:
                if bid[0] * taker_fee(sell_pair.exchange_id) > ask[0] / taker_fee(buy_pair.exchange_id):
                    sell_volume += bid[1]
                    sell_price = bid[0]
                else:
                    break
            if sell_volume > buy_volume:
                continue
            elif buy_volume > sell_volume:
                profit = sell_volume * (sell_price  * taker_fee(sell_pair.exchange_id) -
                                        buy_price / taker_fee(buy_pair.exchange_id))
                return self.Order(buy_pair.exchange_id, sell_pair.exchange_id, buy_pair.symbol, buy_price, sell_price,
                                  sell_volume, profit)

    def _read_from_database(self, exchange_id):
        """Grabs the most recent set of tickers for an exchange from the database."""
        cnx = self._connect_to_database()
        cursor = cnx.cursor()
        exchange_db = 'mytestdb.' + exchange_id

        SQL = ("SELECT symbol, datetime, bid, ask FROM {}"
               " WHERE (symbol, datetime) IN ("
               " SELECT symbol, MAX(datetime)"
               " FROM {}"
               " GROUP BY symbol )").format(exchange_db, exchange_db)
        cursor.execute(SQL)

        for (symbol, datetime, bid, ask) in cursor:
            key = (exchange_id, symbol)
            ind_ticker = {'symbol': symbol, 'datetime': datetime, 'bid': bid, 'ask': ask}
            self.tickers[key] = ind_ticker

        cnx.commit()
        cursor.close()
        cnx.close()

    def _connect_to_database(self):
        connector = read_sql_information()
        return mysql.connector.connect(**connector)

    def _check_valid(self, symbol, key1, key2):
        if key1 not in self.tickers:
            return False
        if key2 not in self.tickers:
            return False
        if 'USD' in symbol:
            return False
        if (self.tickers[key1]['ask'] == 0.0 or self.tickers[key1]['bid'] == 0.0 or
                self.tickers[key2]['ask'] == 0.0 or self.tickers[key2]['bid'] == 0.0):
            return False
        return True