"""spark application"""

import argparse
from pyspark.sql import SparkSession
from datetime import datetime

def parse_line(line):
    fields = line.split(';')
    
    ticker = fields[0]
    date = fields[7]
    year = extract_year(date)
    close = float(fields[2])
    name = fields[9]

    return (ticker, year), (date, close, name)

# Funzione per estrarre l'anno da una data nel formato 'YYYY-MM-DD'
def extract_year(date):
    return date.split('-')[0]

# Funzione per calcolare la variazione percentuale
def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return ((end - start) / start) * 100

def ticker_stats(values):
    dates, closes, name = zip(*values)
    
    first_close = closes[dates.index(min(dates))]
    last_close = closes[dates.index(max(dates))]

    percentual_variation = calculate_percentage_change(first_close, last_close)
    
    return name[0], int(round(percentual_variation))

def refactor_ticker_stats(key_values):
    (ticker, year), (name, percentual_variation) = key_values
    return ((ticker, name), (year, percentual_variation))

def three_years_trend(ticker_trends):
    (ticker_name, year_trends) = ticker_trends
    (ticker, name) = ticker_name
    years, trends = zip(*year_trends)

    sorted_indices = sorted(range(len(years)), key=lambda i: years[i])

    return [((years[sorted_indices[i]], trends[sorted_indices[i]], 
              years[sorted_indices[i + 1]], trends[sorted_indices[i + 1]], 
              years[sorted_indices[i + 2]], trends[sorted_indices[i + 2]]), 
             (ticker, name)) 
            for i in range(len(sorted_indices) - 2)]

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Three Year Same Trend") \
    .getOrCreate()

# Leggi le righe dal file, rimuovi l'header e suddividi i dati
lines = spark.sparkContext.textFile(dataset_filepath)
header = lines.first()
data = lines.filter(lambda line: line != header).map(parse_line).groupByKey()

# Calcola le statistiche per ogni ticker
stats_per_ticker = data.mapValues(ticker_stats)

# Rifattorizza le statistiche dei ticker
ticker_trends = stats_per_ticker.map(refactor_ticker_stats).groupByKey()

# Calcola i trend di tre anni per ciascun ticker
three_years_trends = ticker_trends.flatMap(three_years_trend).groupByKey()

# Filtra i trend che hanno lo stesso andamento per tre anni consecutivi
same_three_years_trends = three_years_trends.filter(lambda x: len(x[1]) > 1).collect()

# Ordina i risultati
output_sorted = sorted(same_three_years_trends, key=lambda x: (x[0][0], -x[0][1]))

# Crea il DataFrame e scrivi i risultati su file CSV
rows = [(k[0], k[1], k[2], k[3], k[4], k[5], [v[0] for v in vs], [v[1] for v in vs]) for k, vs in output_sorted]
rows_rdd = spark.sparkContext.parallelize(rows)

rows_rdd.saveAsTextFile(output_filepath)
