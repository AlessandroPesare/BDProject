import pandas as pd

# Carica il file CSV
stock_data = pd.read_csv('historical_stock_prices.csv')
print("Data head:")
# Visualizza le prime righe del dataframe per controllare il caricamento
print(stock_data.head())
print("Data info:")
# Informazioni sul dataframe
print(stock_data.info())
print("Statistics:")
# Statistiche descrittive
print(stock_data.describe())
print("Null values")
# Controlla la presenza di valori mancanti
print(stock_data.isnull().sum())
print("Duplicated data:")
# Controlla la presenza di duplicati
print(stock_data.duplicated().sum())
