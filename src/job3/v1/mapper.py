#!/usr/bin/env python3
import sys
import csv
from collections import defaultdict
import time

def read_input(file):
    for line in file:
        yield line.strip()

def main():
    input = read_input(sys.stdin)
    data = defaultdict(lambda: {'close_prices': [], 'name': None})
    
    # Skip header
    header = True
    for line in input:
        if header:
            header = False
            continue
        # Split the line by semicolon
        parts = line.split(';')
        ticker = parts[0]
        date = parts[7]
        close = float(parts[2])
        name = parts[9]
        
        # Extract year from the date
        year = int(date.split('-')[0])
        # Filter out years before 2000
        if year < 2000:
            continue        
        # Store close price with date
        data[(year, ticker)]['close_prices'].append((date, close))
        data[(year, ticker)]['name'] = name
    
    for key in data:
        # Sort close_prices by date
        close_prices = sorted(data[key]['close_prices'], key=lambda x: x[0])
        if len(close_prices) > 1:
            start_price = close_prices[0][1]
            end_price = close_prices[-1][1]
            percentage_change = ((end_price - start_price) / start_price) * 100
            print(f"{key[1]};{key[0]};{percentage_change:.2f};{data[key]['name']}")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    duration = end_time - start_time
    #print("Tempo di esecuzione:", duration, "secondi")