#!/usr/bin/env python3
import sys
import ast
from collections import defaultdict

def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return ((end - start) / start) * 100


data = defaultdict(list)
for line in sys.stdin:
    key, value = line.strip().split('\t')
    key = ast.literal_eval(key)
    value = ast.literal_eval(value)
    data[key].append(value)

result = []
for (sector, industry, year), values in data.items():
    ticker_close_prices = defaultdict(list)
    volume_by_ticker = defaultdict(int)

    for value in values:
        ticker, close, volume, date = value
        ticker_close_prices[ticker].append((date, close))
        volume_by_ticker[ticker] += volume

    max_increment = -float('inf')
    max_increment_ticker = None
    industry_first_close_sum = 0
    industry_last_close_sum = 0

    for ticker, close_prices in ticker_close_prices.items():
        close_prices.sort(key=lambda x: x[0])  # Sort by date
        first_close = close_prices[0][1]
        last_close = close_prices[-1][1]
        increment = calculate_percentage_change(first_close, last_close)
            
        if increment > max_increment:
            max_increment = increment
            max_increment_ticker = (ticker, increment)
                
        industry_first_close_sum += first_close
        industry_last_close_sum += last_close

    max_volume_ticker, max_volume = max(volume_by_ticker.items(), key=lambda x: x[1])

    industry_change = calculate_percentage_change(industry_first_close_sum, industry_last_close_sum)

    result.append((sector, industry, year, industry_change, max_increment_ticker, max_volume_ticker, max_volume))

for sector, industry, year, industry_change, max_increment_ticker, max_volume_ticker, max_volume in sorted(result, key=lambda x: x[3], reverse=True):
    print(f"{sector}\t{industry}\t{year}\t{industry_change:.2f}\t{max_increment_ticker[0]}\t{max_increment_ticker[1]:.2f}\t{max_volume_ticker}\t{max_volume}")


