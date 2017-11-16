import ccxt
import pickle
import mysql.connector
from time import time, sleep, strftime, gmtime
from sys import argv
from datetime import datetime


class TrawlerHelper:
    def __init__(self, id, sleep_time):
        self.sleep_time = float(sleep_time)
        self.id = id
        self.iteration = 0
        self.sub_iteration = 0
        self.last_error = -1

    def exchange_malfunction(self):
        if self.last_error == self.iteration:
            self.sub_iteration += 1
            if self.sub_iteration == 3:
                return True
        else:
            self.sub_iteration = 0
        return False


def main():
    helper = TrawlerHelper(argv[1], argv[2])
    exchange = init_exchange(helper.id)
    with open('data/pairs/intra_pairs.p', 'rb') as f:
        intra_pairs = pickle.load(f)
    while True:
        end_time = time() + helper.sleep_time
        helper.iteration += 1
        write_tickers(exchange, intra_pairs[exchange.id], helper)
        if end_time > time():
            sleep(end_time-time())


def write_tickers(exchange, symbols, helper):
    if exchange.id == 'gdax' or exchange.id == 'bitstamp':
        individual_tickers(exchange, symbols, helper)
    else:
        batch_tickers(exchange, symbols, helper)


def individual_tickers(exchange, symbols, helper):
    all_tickers = {}
    for s in symbols:
        ind_ticker = None
        while ind_ticker is None:
            try:
                ind_ticker = exchange.fetch_ticker(s)
            except Exception as e:
                if not record_exception(exchange, helper, e):
                    continue
        all_tickers[s] = ind_ticker
    write_to_txt_file(exchange, all_tickers, symbols)
    write_to_database(exchange, helper)


def batch_tickers(exchange, symbols, helper):
    all_tickers = None
    while all_tickers is None:
        try:
            all_tickers = exchange.fetch_tickers()
        except Exception as e:
            if not record_exception(exchange, helper, e):
                return
    write_to_txt_file(exchange, all_tickers, symbols)
    write_to_database(exchange, helper)


def record_exception(exchange, helper, e, iteration=0):
    file_name = 'logs/' + exchange.id + '.log'
    with open(file_name, 'a') as f:
        f.write('[trawler]: ' + 'Iteration: ' + str(helper.iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exception: ' + str(e).strip()[:25])
        f.write('\n')
    if iteration == 3:
        with open(file_name, 'a') as f:
            f.write('[trawler]: ' + 'Too many attempts to reconnect to exchange, timeout for 5 minutes')
            f.write('\n')
        sleep(300)
    if helper.exchange_malfunction():
        try:
            exchange = init_exchange(helper.id)
        except Exception as e:
            record_exception(exchange, helper, e, iteration + 1)
    helper.last_error = helper.iteration


def record_sql_exception(exchange, helper, e):
    with open('logs/sql.log', 'a') as f:
        f.write('[trawler]: ' + 'Iteration: ' + str(helper.iteration) + ' Datetime: ' + strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exchange: ' + exchange.id + ' Exception: ' + str(e).strip()[:25])
        f.write('\n')


def init_exchange(id):
    exchange = getattr(ccxt, id)()
    exchange.load_markets()
    return exchange


def format_pair(pair):
    split_pair = pair.split('/')
    formatted_pair = split_pair[0] + '_' + split_pair[1]
    return formatted_pair


def convert_string(ind_ticker):
    timestamp = ind_ticker['timestamp']
    utc = datetime.utcfromtimestamp(int(round(timestamp / 1000)))
    dt = utc.strftime('%Y-%m-%d %H:%M:%S')
    string = ind_ticker['symbol'] + ',' + str(dt) + ',' + str(ind_ticker['high']) + ',' + \
             str(ind_ticker['low']) + ',' + str(ind_ticker['bid']) + ',' + str(ind_ticker['ask']) + ',' + \
             str(ind_ticker['baseVolume']) + ',' + str(ind_ticker['quoteVolume'])
    string = string.replace('None', 'NULL')
    return string


def write_to_txt_file(exchange, tickers, symbols):
    file_path = 'data/' + exchange.id + '.txt'
    with open(file_path, 'w') as f:
        for s in symbols:
            string = convert_string(tickers[s])
            f.write(string)
            f.write('\n')


def write_to_database(exchange, helper):
    cnx = mysql.connector.connect(user='b1kxsssu6ej4', password='7sxgckl5ndg1wj7la8esqvxw',
                                  host='mytestdb.cg7oi5h8vvxm.us-east-2.rds.amazonaws.com',
                                  port='3306', database='mytestdb')
    cursor = cnx.cursor()
    file_path = 'data/' + exchange.id + '.txt'
    add_file = (
        "LOAD DATA LOCAL INFILE '" + file_path + "' INTO TABLE " + exchange.id + " "
        "FIELDS TERMINATED BY ','")
    try:
        cursor.execute(add_file)
    except mysql.connector.Error as err:
        record_sql_exception(exchange, helper, err)

    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    main()
