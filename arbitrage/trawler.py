from datetime import datetime
import mysql.connector
from scripts.cd import cd
from ccxt import NetworkError
from scripts.file_reader import read_sql_information
import pickle


class Trawler:
    """
    Handles api calls to the cryptocurrency servers and writes ticker information to database.
    """
    def __init__(self, exchange_id):
        self.tickers = {}

        self.exchange = self.load_exchange(exchange_id)
        self.symbols = self.load_intra_pairs()

    def get_tickers(self):
        """Obtains the tickers and writes them to text files for future use."""
        if hasattr(self.exchange, 'fetch_tickers '):
            self._batch_tickers()
        else:
            self._individual_tickers()
        self._write_to_txt_file()

    def write_to_database(self):
        """Obtains database information and writes text file to database."""
        cnx = self._connect_to_database()
        cursor = cnx.cursor()
        file_path = self.exchange.id + '.txt'
        with cd('/data'):
            SQL = (
                "LOAD DATA LOCAL INFILE '{}' INTO TABLE {} FIELDS TERMINATED BY ','").format(file_path, self.exchange.id)
            cursor.execute(SQL)

        cnx.commit()
        cursor.close()
        cnx.close()

    def load_intra_pairs(self):
        with open('input/intra_pairs.p', 'rb') as f:
            intra_pairs = pickle.load(f)
        return intra_pairs[self.exchange.id]

    def load_exchange(self, idx):
        with open('input/exchanges.p', 'rb') as f:
            exchanges = pickle.load(f)
        for exchange in exchanges:
            if exchange.id == idx:
                return exchange

    def _individual_tickers(self, timeout=3):
        for s in self.symbols:
            iteration, ind_ticker = 0, None
            while iteration < timeout:
                try:
                    ind_ticker = self.exchange.fetch_ticker(s)
                except NetworkError:
                    iteration += 1
                else:
                    break
            if ind_ticker is not None:
                self.tickers[s] = ind_ticker

    def _batch_tickers(self, timeout=3):
        iteration, tickers = 0, None
        while iteration < timeout:
            try:
                self.tickers = self.exchange.fetch_tickers()
            except NetworkError:
                iteration += 1
            else:
                break

    def _write_to_txt_file(self):
        path = 'data/' + self.exchange.id + '.txt'
        with open(path, 'w') as f:
            for s in self.symbols:
                if s in self.tickers:
                    string = self._convert_string(**self.tickers[s])
                    f.write(string)
                    f.write('\n')

    def _convert_string(self, timestamp, symbol, high, low, bid, ask, baseVolume, quoteVolume, **kwargs):
        utc = datetime.utcfromtimestamp(int(round(timestamp / 1000)))
        dt = utc.strftime('%Y-%m-%d %H:%M:%S')
        string = '{},{},{},{},{},{},{},{}'.format(symbol, dt, high, low, bid, ask, baseVolume, quoteVolume)
        string = string.replace('None', 'NULL')
        return string

    def _connect_to_database(self):
        connector = read_sql_information()
        return mysql.connector.connect(**connector)