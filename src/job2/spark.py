from pyspark import SparkContext, SparkConf
import argparse

# Funzione per calcolare la variazione percentuale
def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return ((end - start) / start) * 100

# Funzione per estrarre l'anno da una data nel formato 'YYYY-MM-DD'
def extract_year(date):
    return date.split('-')[0]

# Funzione per elaborare le statistiche per ogni industria e anno
def process_data(key, values):
    sector, industry, year = key

    ticker_close_prices = {}
    volume_by_ticker = {}

    for value in values:
        ticker, date, close, volume = value
        if ticker not in ticker_close_prices:
            ticker_close_prices[ticker] = []
        ticker_close_prices[ticker].append((date, close))
        if ticker not in volume_by_ticker:
            volume_by_ticker[ticker] = 0
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

    return ((sector, industry, year), industry_change, max_increment_ticker, (max_volume_ticker, max_volume))

# Configurazione Spark
conf = SparkConf().setAppName("Stock Analysis").set("spark.driver.maxResultSize", "2g")
sc = SparkContext(conf=conf)

# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Percorso del file CSV di input")
parser.add_argument("--output_path", type=str, required=True, help="Percorso della directory di output su HDFS")
args = parser.parse_args()

# Leggi il dataset e crea un RDD
lines = sc.textFile(args.input_path)
header = lines.first()
data = lines.filter(lambda line: line != header).map(lambda line: line.split(';'))

# Estrai (settore, industria, anno, ticker, data, chiusura, volume) da ciascuna riga
key_values = data.map(lambda x: ((x[10], x[11], extract_year(x[7])), (x[0], x[7], float(x[2]), int(x[6]))))

# Raggruppa per (settore, industria, anno)
grouped_data = key_values.groupByKey()

# Calcola le statistiche per ogni gruppo
statistics = grouped_data.map(lambda x: process_data(x[0], x[1]))

# Raggruppa per settore e ordina per variazione percentuale decrescente
result = statistics.sortBy(lambda x: (x[0][0], x[1]), ascending=False)

# Salva i risultati nella directory di output su HDFS
result.saveAsTextFile(args.output_path)

# Chiusura dello SparkContext
sc.stop()
