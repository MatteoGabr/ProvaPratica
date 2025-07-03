import os  # Per operazioni sul file system (es. creare cartelle, ottenere percorsi)
import requests  # Per effettuare richieste HTTP (es. scaricare file CSV da URL)
import pandas as pd  # Per la manipolazione di dati in DataFrame
from io import StringIO  # Per trattare una stringa come file (utile per CSV in testo)
import sqlite3  # Per interagire con un database SQLite

# ===============================
# URL DEI DATASET
# ===============================

# URL del dataset: spesa in ricerca e sviluppo per regione
csv_spesa_ricerca = "https://raw.githubusercontent.com/MatteoGabr/ProvaPratica/refs/heads/main/Incidenza-spesa-imprese-in-ricerca-e-sviluppo-per-regione.csv"

# URL del dataset: partecipazione al mercato del lavoro per regione
csv_partecipazione_lavoro = "https://raw.githubusercontent.com/MatteoGabr/ProvaPratica/main/Partecipazione-della-popolazione-al-mercato-del-lavoro-per-regione.csv"

# ===============================
# PERCORSI LOCALE
# ===============================

# Directory corrente dello script
curr_dirr = os.getcwd()

# Percorso completo della cartella "csv/" dove salvare i file scaricati
csv_dir = os.path.join(curr_dirr, 'csv')

# ===============================
# FUNZIONI UTILI
# ===============================

"""
Importa i dati da un file CSV remoto e li converte in un DataFrame pandas.
@param url {str} - URL del file CSV da scaricare
@return {DataFrame | None} - Il DataFrame caricato, oppure None in caso di errore
"""
def import_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        csv_content = StringIO(response.text)
        df = pd.read_csv(csv_content, sep=';', na_values=['', 'Null'])
        # Corregge caratteri speciali nei nomi delle colonne
        df.columns = [col.replace('�', 'à') for col in df.columns]
        df.columns = [col.replace('ŕ', 'à') for col in df.columns]
        return df
    else:
        print(f"Errore nell'importazione dei dati da {url}")
        return None

"""
Salva un DataFrame come file CSV in una directory locale.
@param df {DataFrame} - Il DataFrame da salvare
@param filename {str} - Il nome del file CSV
"""
def save_df_local(df, filename):
    os.makedirs(csv_dir, exist_ok=True)  # Crea la cartella se non esiste
    path = os.path.join(csv_dir, filename)
    df.to_csv(path, index=False, encoding='utf-8')
    print(f"DataFrame salvato in {path}")

"""
Interpola i dati mancanti (NaN) per una o più colonne di un DataFrame.
@param df {DataFrame} - Il DataFrame da interpolare
@param columns {list[str]} - Lista delle colonne su cui applicare l'interpolazione
@return {DataFrame} - Il DataFrame con i dati interpolati
"""
def interpolate_missing_data(df, columns):
    for col in columns:
        df[col] = df[col].interpolate(method='linear')
    return df

# ===============================
# TRATTAMENTO DATI - SPESA RICERCA
# ===============================

# Importa dataset della spesa
df_spesa_ricerca = import_data(csv_spesa_ricerca)

# Interpola i dati mancanti nella colonna di interesse
interpolate_missing_data(df_spesa_ricerca, ['Percentuale spesa imprese in ricerca e sviluppo'])

# Salva il file localmente
save_df_local(df_spesa_ricerca, "spesa_ricerca.csv")

# ===============================
# TRATTAMENTO DATI - PARTECIPAZIONE LAVORO
# ===============================

# Importa dataset della partecipazione
df_partecipazione_lavoro = import_data(csv_partecipazione_lavoro)

# Verifica i nomi delle colonne
print(df_partecipazione_lavoro.columns.tolist())

# Interpola i dati mancanti
interpolate_missing_data(df_partecipazione_lavoro, ['Percentuale forze di lavoro in età 15-64 anni'])

# Salva il file localmente
save_df_local(df_partecipazione_lavoro, "partecipazione_lavoro.csv")

# ===============================
# INSERIMENTO DATI NEL DATABASE
# ===============================

# Apre connessione al database SQLite
conn = sqlite3.connect('tutorial.db')
cursor = conn.cursor()

# Inserisce i dati interpolati nella tabella `percentuale_spesa_ricerca_sviluppo`
for _, row in df_spesa_ricerca.iterrows():
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    cursor.execute(
        '''
        INSERT INTO percentuale_spesa_ricerca_sviluppo (Anno, Regione_id, Percentuale)
        VALUES (?, ?, ?)
        ''',
        (row['Anno'], regione_id, row['Percentuale spesa imprese in ricerca e sviluppo'])
    )

# Inserisce i dati interpolati nella tabella `percentuale_partecipazione_lavoro`
for _, row in df_partecipazione_lavoro.iterrows():
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    cursor.execute(
        '''
        INSERT INTO percentuale_partecipazione_lavoro (Anno, Regione_id, Percentuale)
        VALUES (?, ?, ?)
        ''',
        (row['Anno'], regione_id, row['Percentuale forze di lavoro in età 15-64 anni'])
    )

# Salva le modifiche nel database
conn.commit()

# Chiude la connessione
conn.close()
