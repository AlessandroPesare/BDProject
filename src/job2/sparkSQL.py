from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, first, last, sum as _sum, max as _max
from pyspark.sql.window import Window

# Funzione per calcolare la variazione percentuale
def calculate_percentage_change(start, end):
    if start == 0:
        return 0
    return ((end - start) / start) * 100

# Configurazione Spark
spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# Parsing degli argomenti
parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, required=True, help="Percorso del file CSV di input")
parser.add_argument("--output_path", type=str, required=True, help="Percorso della directory di output su HDFS")
args = parser.parse_args()

# Leggi il dataset e crea un DataFrame
data = spark.read.option("header", "true").option("delimiter", ";").csv(args.input_path)
# Seleziona e trasforma i dati necessari
data = data.withColumn("year", data.date.substr(1, 4).cast("int")) \
           .withColumn("close", col("close").cast("float")) \
           .withColumn("volume", col("volume").cast("int"))

# Filtra i dati non nulli per settore e industria
data = data.filter(col("sector").isNotNull() & col("industry").isNotNull())

# Crea una vista temporanea per eseguire le query SQL
data.createOrReplaceTempView("stock_data")

# Query SQL per calcolare le statistiche richieste
statistics_query = """
WITH ticker_close_prices AS (
    SELECT
        sector,
        industry,
        year,
        ticker,
        date,
        close,
        volume,
        ROW_NUMBER() OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) as row_num
    FROM stock_data
),
first_last_prices AS (
    SELECT
        sector,
        industry,
        year,
        ticker,
        FIRST_VALUE(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) as first_close,
        LAST_VALUE(close) OVER (PARTITION BY sector, industry, year, ticker ORDER BY date) as last_close,
        volume
    FROM ticker_close_prices
),
ticker_changes AS (
    SELECT
        sector,
        industry,
        year,
        ticker,
        ((last_close - first_close) / first_close) * 100 as percentage_change,
        SUM(volume) OVER (PARTITION BY sector, industry, year, ticker) as total_volume
    FROM first_last_prices
),
industry_changes AS (
    SELECT
        sector,
        industry,
        year,
        SUM(first_close) as industry_first_close_sum,
        SUM(last_close) as industry_last_close_sum
    FROM first_last_prices
    GROUP BY sector, industry, year
),
industry_statistics AS (
    SELECT
        tc.sector,
        tc.industry,
        tc.year,
        ((ic.industry_last_close_sum - ic.industry_first_close_sum) / ic.industry_first_close_sum) * 100 as industry_change,
        tc.ticker as max_increment_ticker,
        MAX(tc.percentage_change) as max_increment,
        MAX(tc.total_volume) as max_volume,
        tc.ticker as max_volume_ticker
    FROM ticker_changes tc
    JOIN industry_changes ic ON tc.sector = ic.sector AND tc.industry = ic.industry AND tc.year = ic.year
    GROUP BY tc.sector, tc.industry, tc.year, tc.ticker, ic.industry_last_close_sum, ic.industry_first_close_sum
)
SELECT * FROM industry_statistics
ORDER BY industry_change DESC
"""

# Esegui la query
statistics = spark.sql(statistics_query)
# Salva i risultati nella directory di output su HDFS
statistics.write.mode("overwrite").csv(args.output_path)

# Chiusura della sessione Spark
spark.stop()
