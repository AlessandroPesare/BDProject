#!/usr/bin/env python3

import sys

for line in sys.stdin:
    line = line.strip()
    ticker, name, data = line.split('\t', 2)
    date, close, low, high, volume = data.split(',')
    year = date[:4]
    print(f"{ticker}\t{name}\t{year}\t{date},{close},{low},{high},{volume}")
