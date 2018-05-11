import ccxt
import json
from collections import namedtuple
import mysql.connector
from scripts.file_reader import read_sql_information
import pickle
from scripts.initialize_exchange import initialize_exchanges


class Initializer:
    """
    Initializes exchanges and currency-pairs.
    """
    def __init__(self):
        self.intra_pairs = {}
        self.inter_pairs = {}
        self.exchange_keys = {}
        self.exchanges = []
        self.exchange_pairs = []

    def initialize_exchanges(self):
        """Obtains exchanges from cryptocurrency websites, sets up exchange pairs, and gets exchange keys."""
        self.exchanges = initialize_exchanges()
        self.exchange_pairs = self._create_exchange_pairs(self.exchanges)

    def initialize_pairs(self):
        """Initializes currency-pairs."""
        self.intra_pairs, self.inter_pairs = {}, {}

        for pair in self.exchange_pairs:
            exchange1 = pair.ex1.id
            exchange2 = pair.ex2.id

            symbols = self._get_common_symbols(pair.ex1, pair.ex2)
            key = "{}/{}".format(exchange1, exchange2)
            self.inter_pairs[key] = symbols

            if exchange1 not in self.intra_pairs:
                self.intra_pairs[exchange1] = []
            if exchange2 not in self.intra_pairs:
                self.intra_pairs[exchange2] = []

            for symbol in symbols:
                if symbol not in self.intra_pairs[exchange1]:
                    self.intra_pairs[exchange1].append(symbol)
                if symbol not in self.intra_pairs[exchange2]:
                    self.intra_pairs[exchange2].append(symbol)

    def reset_exchange(self, idx):
        """Resets an exchange when errored out."""
        exchange = getattr(ccxt, idx)()
        exchange.load_markets()
        if idx in self.exchange_keys:
            ex_key = self.exchange_keys[idx]
            exchange.apiKey = ex_key.apiKey
            exchange.secret = ex_key.secret
        self.exchanges = [exchange if exchange.id == x.id else x for x in self.exchanges]
        return exchange

    def write_to_file(self):
        with open('input/inter_pairs.p', 'wb+') as f:
            pickle.dump(self.inter_pairs, f, pickle.HIGHEST_PROTOCOL)
        with open('input/intra_pairs.p', 'wb+') as f:
            pickle.dump(self.intra_pairs, f, pickle.HIGHEST_PROTOCOL)

    def write_to_database(self):
        """Obtains database information and writes pair files to database."""
        cnx = self._connect_to_database()
        cursor = cnx.cursor()

        query = "REPLACE INTO PAIRS (label, body) VALUES (%s, %s)"
        inter_values = ('inter', json.dumps(self.inter_pairs))
        intra_values = ('intra', json.dumps(self.intra_pairs))

        cursor.execute(query, inter_values)
        cursor.execute(query, intra_values)

        cnx.commit()
        cursor.close()
        cnx.close()

    def _create_exchange_pairs(self, l):
        ExPairs = namedtuple('ExchangePairs', ['ex1', 'ex2'])
        result = []
        for i in range(len(l) - 1):
            for j in range(i + 1, len(l)):
                result.append(ExPairs(l[i], l[j]))
        return result

    def _get_common_symbols(self, ex1, ex2):
        a = ex1.symbols
        b = ex2.symbols
        both_list = []

        for x in a:
            if self._blacklist(x, ex1.id):
                continue
            for y in b:
                if self._blacklist(y, ex2.id):
                    continue
                if x == y:
                    if 'EUR' in x or 'GBP' in x:
                        break
                    both_list.append(x)
                    break

        return both_list

    def _blacklist(self, sym, id):
        str = id + ':' + sym
        return {
            'bittrex:BTS/BTC': True,
            'poloniex:XVC/BTC': True
        }.get(str, False)

    def _connect_to_database(self):
        connector = read_sql_information()
        return mysql.connector.connect(**connector)