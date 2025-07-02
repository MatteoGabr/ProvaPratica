import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# Connessione al database
conn = sqlite3.connect('tutorial.db')

# Carica i dati dalla tabella partecipazione_totale_nazionale in un DataFrame
df = pd.read_sql_query("SELECT * FROM partecipazione_totale_nazionale ORDER BY Anno", conn)

conn.close()

# Controlla i dati caricati
print(df.head())

# Crea il grafico a linea: anno sull'asse x, partecipazione_totale sull'asse y
plt.figure(figsize=(10, 6))
print(df['Anno'].dtype)
plt.plot(df['Anno'], df['Percentuale'], marker='o', linestyle='-', color='b')

# Aggiungi titolo e etichette agli assi
plt.title('Partecipazione Totale Nazionale nel tempo')
plt.xlabel('Anno')
plt.ylabel('Partecipazione Totale (media percentuale)')

# Forza gli anni interi sull'asse x
plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))

# Mostra griglia per maggiore leggibilità
plt.grid(True)

# Mostra il grafico
plt.show()
