from collections import namedtuple


def read_sql_information():
    connector = {}
    with open('input/sql_information.txt', 'r') as f:
        for line in f:
            if line[0] == '#' or line[0] == ' ' or line[0] == '\n':
                continue
            (key, val) = line.strip().split('=')
            connector[key] = val
    return connector


def read_exchange_keys():
    exchange_keys = {}
    ExchangeKey = namedtuple('ExchangeKey', ['apiKey', 'secret'])
    with open('input/exchange_keys.txt', 'r') as f:
        for line in f:
            if line[0] == '#':
                continue
            (key, apiKey, secret) = line.strip().split(',')
            exchange_keys[key] = ExchangeKey(apiKey, secret)
    return exchange_keys
