from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import year, round, min, max, avg

# Creazione della sessione Spark
spark = SparkSession.builder \
    .appName("Stock Statistics") \
    .getOrCreate()

# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input_paths", type=str, nargs=2, required=True, help="Paths to the input CSV files (prices and info)")
parser.add_argument("--output_path", type=str, required=True, help="Path to the output directory")
args = parser.parse_args()

# Percorsi dei file di input e output
prices_path, info_path = args.input_paths
output_path = args.output_path

# Lettura dei dati
historical_stock_prices = spark.read.csv(prices_path, header=True, inferSchema=True)
historical_stock = spark.read.csv(info_path, header=True, inferSchema=True)

# Join dei dati
merged_df = historical_stock_prices.join(historical_stock, on="ticker", how="inner")

# Calcolo dell'anno
merged_df = merged_df.withColumn("year", year("date"))

# Aggregazione e calcolo delle statistiche per ogni anno e ticker
statistics_df = merged_df.groupBy("ticker", "name", "year").agg(
    round(((max("close") - min("close")) / min("close")) * 100, 2).alias("variazione_percentuale"),
    min("low").alias("prezzo_minimo"),
    max("high").alias("prezzo_massimo"),
    round(avg("volume"), 0).alias("volume_medio")
)

# Ordinamento per ticker e anno
statistics_df = statistics_df.orderBy("ticker", "year")

# Conversione del DataFrame in formato JSON e salvataggio su HDFS
statistics_df.write.format("json").mode("overwrite").save(output_path)

# Stop della sessione Spark
spark.stop()
