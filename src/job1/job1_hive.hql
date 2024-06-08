-- Cancella e crea la tabella historical_stocks
DROP TABLE IF EXISTS historical_stocks;

CREATE TABLE historical_stocks (
    `ticker` STRING,
    `exchange` STRING,
    `name` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

---LOAD DATA INPATH '/user/alessandropesare/input/historical_stocks.csv' INTO TABLE historical_stocks;

LOAD DATA LOCAL INPATH '/home/hadoop/historical_stocks.csv' INTO TABLE historical_stocks;

-- Cancella e crea la tabella historical_stock_prices
DROP TABLE IF EXISTS historical_stock_prices;

CREATE TABLE historical_stock_prices (
    `ticker` STRING,
    `open` FLOAT,
    `close` FLOAT,
    `adj_close` DOUBLE,
    `low` FLOAT,
    `high` FLOAT,
    `volume` INT,
    `day` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties ("skip.header.line.count"="1");

---LOAD DATA INPATH '/user/alessandropesare/input/historical_stock_prices.csv' INTO TABLE historical_stock_prices;

LOAD DATA LOCAL INPATH '/home/hadoop/historical_stock_prices.csv' INTO TABLE historical_stock_prices;

-- Cancella e crea la tabella historical_stock_prices_with_year
DROP TABLE IF EXISTS historical_stock_prices_with_year;

CREATE TABLE historical_stock_prices_with_year AS
SELECT
    ticker,
    open,
    close,
    adj_close,
    low,
    high,
    volume,
    day,
    year(from_unixtime(unix_timestamp(day, 'yyyy-MM-dd'))) as year
FROM historical_stock_prices;

-- Cancella la tabella yearly_stock_analysis se esiste
DROP TABLE IF EXISTS yearly_stock_analysis;

-- Crea la tabella yearly_stock_analysis
CREATE TABLE yearly_stock_analysis (
    ticker STRING,
    name STRING,
    year INT,
    percent_change FLOAT,
    min_price FLOAT,
    max_price FLOAT,
    avg_volume FLOAT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Utilizza una CTE per calcolare first_close e last_close, e inserisce i risultati nella tabella yearly_stock_analysis
WITH yearly_prices AS (
    SELECT
        ticker,
        year,
        FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY day) as first_close,
        LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_close
    FROM
        historical_stock_prices_with_year
)

INSERT INTO yearly_stock_analysis
SELECT
    hsp.ticker,
    hs.name,
    hsp.year,
    ROUND((yp.last_close - yp.first_close) * 100 / yp.first_close, 2) as percent_change,
    MIN(hsp.low) as min_price,
    MAX(hsp.high) as max_price,
    ROUND(AVG(hsp.volume), 2) as avg_volume
FROM
    historical_stock_prices_with_year hsp
JOIN
    historical_stocks hs
ON
    hsp.ticker = hs.ticker
JOIN
    yearly_prices yp
ON
    hsp.ticker = yp.ticker AND hsp.year = yp.year
GROUP BY
    hsp.ticker, hs.name, hsp.year, yp.first_close, yp.last_close
ORDER BY
    hsp.ticker, hsp.year;

