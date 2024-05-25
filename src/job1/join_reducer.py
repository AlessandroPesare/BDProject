#!/usr/bin/env python3
import sys
from collections import defaultdict

ticker_data = defaultdict(lambda: {"name": None, "prices": []})

for line in sys.stdin:
    line = line.strip()
    ticker, data_type, data = line.split('\t', 2)
    
    if data_type == "stock":
        name = data
        ticker_data[ticker]["name"] = name
    elif data_type == "price":
        ticker_data[ticker]["prices"].append(data)

for ticker, data in ticker_data.items():
    name = data["name"]
    if name:
        for price_data in data["prices"]:
            print(f"{ticker}\t{name}\t{price_data}")
