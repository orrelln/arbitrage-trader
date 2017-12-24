from collections import namedtuple
import mysql.connector
from scripts.trading_information import taker_fee
from scripts.file_reader import read_sql_information


class Arbitrager:
    """
    Reads ticker information and calculates arbitrage opportunities.
    """
    def __init__(self, inter_pairs, exchanges, exchange_pairs):
        self.inter_pairs = inter_pairs
        self.exchanges = exchanges
        self.exchange_pairs = exchange_pairs
        self.tickers = {}
        self.arbitrage = {}

    def load_tickers(self):
        """Obtains database information for most recent tickers."""
        for exchange in self.exchanges:
            self._read_from_database(exchange.id)

    def calculate_arbitrage(self):
        """Calculates arbitrage opportunities."""
        Arb = namedtuple('Arb', ['buy', 'sell', 'symbol'])
        for pair in self.exchange_pairs:
            exchange1 = pair.ex1.id
            exchange2 = pair.ex2.id
            inter_key = (exchange1, exchange2)
            symbols = self.inter_pairs[inter_key]

            for symbol in symbols:
                exchange_key1 = (exchange1, symbol)
                exchange_key2 = (exchange2, symbol)

                if self._check_valid(symbol, exchange_key1, exchange_key2) is False:
                    self.arbitrage[Arb(exchange1, exchange2, symbol)] = None
                    self.arbitrage[Arb(exchange2, exchange1, symbol)] = None
                    continue

                arbitrage1 = ((self.tickers[exchange_key2]['bid'] / self.tickers[exchange_key1]['ask'])
                              * taker_fee(exchange_key1) * taker_fee(exchange_key2) - 1)
                arbitrage2 = ((self.tickers[exchange_key1]['bid'] / self.tickers[exchange_key2]['ask'])
                              * taker_fee(exchange_key1) * taker_fee(exchange_key2) - 1)
                self.arbitrage[Arb(exchange1, exchange2, symbol)] = arbitrage1
                self.arbitrage[Arb(exchange2, exchange1, symbol)] = arbitrage2

    def _read_from_database(self, exchange):
        cnx = self._connect_to_database()
        cursor = cnx.cursor()
        exchange_db = 'mytestdb.' + exchange

        SQL = ("SELECT symbol, datetime, bid, ask FROM {}"
               " WHERE (symbol, datetime) IN ("
               " SELECT symbol, MAX(datetime)"
               " FROM {}"
               " GROUP BY symbol )").format(exchange_db, exchange_db)
        cursor.execute(SQL)

        for (symbol, datetime, bid, ask) in cursor:
            key = (exchange, symbol)
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