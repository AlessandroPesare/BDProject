import matplotlib.pyplot as plt

# Dati di esecuzione in minuti
technologies = ['SparkSQL', 'SparkCore', 'MapReduce']
execution_times = [6.08, 3.10, 7.26]  # Tempi di esecuzione in minuti

# Grafico a Linee
plt.figure(figsize=(10, 6))
plt.plot(technologies, execution_times, marker='o', linestyle='-', color='orange', linewidth=2)
plt.xlabel('Technology')
plt.ylabel('Execution Time (minutes)')
plt.title('Execution Time by Technology')
plt.grid(True)

# Mostrare il valore sopra ogni punto
for i, time in enumerate(execution_times):
    plt.text(i, time + 0.1, f'{time:.2f}', ha='center', va='bottom')

plt.show()
