#!/usr/bin/env python3

import sys
from collections import defaultdict

def calculate_statistics(data):
    data = sorted(data, key=lambda x: x[0])  # Ordina per data
    first_close = data[0][1]
    last_close = data[-1][1]
    min_price = min(entry[2] for entry in data)
    max_price = max(entry[3] for entry in data)
    avg_volume = sum(entry[4] for entry in data) / len(data)
    percent_change = ((last_close - first_close) / first_close) * 100
    
    return round(percent_change, 2), round(min_price, 2), round(max_price, 2), round(avg_volume, 2)

ticker_data = defaultdict(lambda: {"name": None, "years": defaultdict(list)})

for line in sys.stdin:
    line = line.strip()
    ticker, name, year, data = line.split('\t', 3)
    date, close, low, high, volume = data.split(',')
    close, low, high, volume = float(close), float(low), float(high), int(volume)
    
    ticker_data[ticker]["name"] = name
    ticker_data[ticker]["years"][year].append((date, close, low, high, volume))

for ticker, data in ticker_data.items():
    name = data["name"]
    for year, yearly_data in data["years"].items():
        percent_change, min_price, max_price, avg_volume = calculate_statistics(yearly_data)
        print(f"{ticker}\t{name}\t{year}\t{percent_change}\t{min_price}\t{max_price}\t{avg_volume}")
