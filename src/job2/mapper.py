#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if line.startswith("ticker"):
        continue
    fields = line.split(';')
    if len(fields) == 12:
        ticker, open_, close, adj_close, low, high, volume, date, exchange, name, sector, industry = fields
        year = date[:4]
        if sector and industry:
            key = (sector,industry, year)
            value = (ticker, float(close), int(volume), date)
            print(f"{key}\t{value}")
