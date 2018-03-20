#!/bin/sh

python3.5 initializer_wrapper.py &
sleep 5

while read line
do
    python3.5 trawler_wrapper.py "$line" &
done < input/exchanges.txt

sleep 5
python3.5 arbitrager_wrapper.py &