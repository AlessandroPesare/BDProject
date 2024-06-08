import matplotlib.pyplot as plt
import numpy as np

# Dati forniti
datasets = ['Originale', 'Ridotto del 50%', 'Duplicato del 50%', 'Duplicato del 100%']
local_times = {
    'SparkSQL': [2.58, 1.39,  3.15, 3.57],
    'Spark Core': [1.37, 0.44 , 1.57, 3.01],
    'MapReduce': [4.19,  2.58, 8.29, 14.13]
}

cluster_times = {
    'SparkSQL': [1.22, 0.47, 1.56, 2.25],
    'Spark Core': [1.18, 0.40, 2.01, 2.32 ],
    'MapReduce': [3.36, 2.07, 4.11, 5.29]
}

# Creazione del grafico
fig, ax = plt.subplots(3, 1, figsize=(12, 18))

technologies = ['SparkSQL', 'Spark Core', 'MapReduce']
colors = ['b', 'g', 'r']
labels = ['Locale', 'Cluster']

for i, tech in enumerate(technologies):
    ax[i].plot(datasets, local_times[tech], marker='o', color=colors[i], linestyle='-', label='Locale')
    ax[i].plot(datasets, cluster_times[tech], marker='x', color=colors[i], linestyle='--', label='Cluster')
    ax[i].set_xlabel('Dimensione del dataset', fontsize=12)
    ax[i].set_ylabel('Tempo di esecuzione(minuti)', fontsize=12)
    ax[i].legend(loc='lower right')
    ax[i].grid(True)

# Aggiungi una legenda comune
handles = [
    plt.Line2D([0], [0], color='b', lw=2, label='SparkSQL'),
    plt.Line2D([0], [0], color='g', lw=2, label='Spark Core'),
    plt.Line2D([0], [0], color='r', lw=2, label='MapReduce')
]

fig.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, 0.95), ncol=3)

# Aggiusta i margini per evitare sovrapposizioni
plt.subplots_adjust(top=0.9, hspace=0.3)

plt.show()
