import pickle
from datetime import datetime
from sys import argv
import ccxt
import mysql.connector

from historical.database.exception_helper import ExceptionHelper


def main():
    helper = ExceptionHelper(argv[0], argv[1], argv[2])
    exchange = init_exchange(helper)
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
                if helper.exchange_malfunction():
                    init_exchange(helper)
                helper.record_exception(e)
        all_tickers[s] = ind_ticker
    write_to_txt_file(exchange, all_tickers, symbols)
    write_to_database(exchange, helper)


def batch_tickers(exchange, symbols, helper):
    all_tickers = None
    while all_tickers is None:
        try:
            all_tickers = exchange.fetch_tickers()
        except Exception as e:
            if helper.exchange_malfunction():
                init_exchange(helper)
            helper.record_exception(e)
    write_to_txt_file(exchange, all_tickers, symbols)
    write_to_database(exchange, helper)


def init_exchange(helper):
    attempt = 0
    while True:
        try:
            exchange = getattr(ccxt, helper.idx)()
            exchange.load_markets()
            break
        except Exception as e:
            attempt += 1
            helper.record_exception(e, attempt)
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
        helper.record_sql_exception(err)

    cnx.commit()
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    main()
