#!/bin/sh
python3 trawler.py bittrex 30 &
python3 trawler.py bitfinex 30 &
python3 trawler.py gdax 30 &
python3 trawler.py poloniex 30 &
python3 trawler.py kraken 30 &
python3 trawler.py bitstamp 30 &
python3 trawler.py hitbtc2 30 &
