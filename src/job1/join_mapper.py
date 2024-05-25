#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()
    if line.startswith("ticker"):  # Salta l'intestazione
        continue
    fields = line.split(',')

    if len(fields) == 5:
        #historical_stocks.csv
        ticker, exchange, name, sector, industry = fields
        print(f"{ticker}\tstock\t{name}")
    elif len(fields) == 8:
        #historical_stock_prices.csv
        ticker, open_price, close, adj_close, low, high, volume, date = fields
        print(f"{ticker}\tprice\t{date},{close},{low},{high},{volume}")
