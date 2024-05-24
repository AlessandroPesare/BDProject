from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import year, first, last, min, max, avg, col, round, collect_list, struct
from pyspark.sql.window import Window
import time  # Import the time module for timing

# Create parser and set its arguments
parser = argparse.ArgumentParser()
parser.add_argument("--input_paths", type=str, nargs=2, required=True, help="Paths to the input CSV files (prices and info)")
parser.add_argument("--output_path", type=str, required=True, help="Path to the output directory")

# Parse arguments
args = parser.parse_args()

# Ottenere i percorsi dei file di input e il percorso della directory di output
prices_path, info_path = args.input_paths
output_path = args.output_path

spark = SparkSession \
    .builder \
    .appName("Stock Statistics") \
    .config("spark.executor.memory", "1g") \
    .config("spark.master", "yarn") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memoryOverhead", "1g") \
    .getOrCreate()

start_time = time.time()

# Import the csv file as a DataFrame
historical_stock_prices = spark.read.csv(prices_path, header=True, inferSchema=True)
historical_stock = spark.read.csv(info_path, header=True, inferSchema=True)

merged_df = historical_stock_prices.join(historical_stock, on="ticker", how="inner")
min_year_df = merged_df.groupBy("ticker").agg(min(year("date")).alias("IPO_year"))

merged_with_IPO_df = merged_df.join(min_year_df, "ticker")

data_df = merged_with_IPO_df.withColumn("year", year("date"))

window_spec = Window.partitionBy("ticker", "year").orderBy("date")

# Compute the first and last closing prices within each partition
data_with_first_last = data_df.withColumn("first_close", first("close").over(window_spec)) \
                              .withColumn("last_close", last("close").over(window_spec))

# Ensure we include only the necessary columns in the final aggregation
filtered_df = data_with_first_last.select("ticker", "name", "year", "first_close", "last_close", "low", "high", "volume")

# Compute the statistics
statistics_df = filtered_df.groupBy("ticker", "name", "year","first_close","last_close") \
    .agg(
        round(((col("last_close") - col("first_close")) / col("first_close")) * 100, 2).alias("variazione_percentuale"),
        min("low").alias("prezzo_minimo"),
        max("high").alias("prezzo_massimo"),
        round(avg("volume"), 0).alias("volume_medio")
    )

stock_statistics_grouped_df = statistics_df.groupBy("ticker", "name") \
    .agg(collect_list(struct("year", "variazione_percentuale", "prezzo_minimo", "prezzo_massimo", "volume_medio")).alias("andamento"))

# Show the final result
stock_statistics_grouped_df.show(truncate=True)

# Write the DataFrame to HDFS as JSON files
stock_statistics_grouped_df.write.format("json").save(output_path)

end_time = time.time()

# Calculate and print the duration
duration = end_time - start_time
print(f"Time taken: {duration} seconds")

# Stop the SparkSession
spark.stop()
