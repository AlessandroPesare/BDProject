import pandas as pd
import numpy as np
# Carica il file CSV
historical_stock_prices = pd.read_csv('historical_stock_prices.csv')
print("Data head:")
# Visualizza le prime righe del dataframe per controllare il caricamento
print(historical_stock_prices.head())
print("Data info:")
# Informazioni sul dataframe
print(historical_stock_prices.info())
print("Statistics:")
# Statistiche descrittive
print(historical_stock_prices.describe())
print("Null values")
# Controlla la presenza di valori mancanti
print(historical_stock_prices.isnull().sum())
print("Duplicated data:")
# Controlla la presenza di duplicati
print(historical_stock_prices.duplicated().sum())

historical_stocks = pd.read_csv('historical_stocks.csv')

print("Data head:")
# Visualizza le prime righe del dataframe per controllare il caricamento
print(historical_stocks.head())
print("Data info:")
# Informazioni sul dataframe
print(historical_stocks.info())
print("Statistics:")
# Statistiche descrittive
print(historical_stocks.describe())
print("Null values")
# Controlla la presenza di valori mancanti
print(historical_stocks.isnull().sum())
print("Duplicated data:")
# Controlla la presenza di duplicati
print(historical_stocks.duplicated().sum())

merged_data = pd.merge(historical_stock_prices, historical_stocks, on='ticker')

merged_data.to_csv('stocks_data.csv', index=False, header=True, sep=';')