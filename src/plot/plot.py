import matplotlib.pyplot as plt

# Dati di esecuzione in minuti
technologies = ['Spark Core','MapReduce']
execution_times = [0.56,0.48 ]  # Tempi di esecuzione in minuti

# Creazione del grafico
plt.figure(figsize=(10, 6))
plt.bar(technologies, execution_times, color=['green', 'red'])

# Aggiungere etichette e titolo
plt.xlabel('Technology')
plt.ylabel('Execution Time (minutes)')
plt.title('Execution Time by Technology')
plt.ylim(0, max(execution_times) + 1)  # Aggiungere un po' di spazio sopra la barra più alta

# Mostrare il valore sopra ogni barra
for i, time in enumerate(execution_times):
    plt.text(i, time + 0.1, f'{time:.2f}', ha='center', va='bottom')

# Visualizzare il grafico
plt.show()
