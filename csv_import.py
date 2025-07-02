import os  # Per operazioni sul file system, come creare cartelle o gestire percorsi
import requests  # Per effettuare richieste HTTP (es. scaricare file da URL)
import pandas as pd  # Per manipolazione, analisi e gestione di dati tabulari con DataFrame
from io import StringIO  # Per trattare una stringa come un oggetto file (utile per leggere CSV)
import sqlite3  # Per connettersi e operare con un database SQLite

# URL dei dataset CSV ospitati su GitHub
csv_spesa_ricerca = "https://raw.githubusercontent.com/MatteoGabr/ProvaPratica/refs/heads/main/Incidenza-spesa-imprese-in-ricerca-e-sviluppo-per-regione.csv"
csv_partecipazione_lavoro = "https://raw.githubusercontent.com/MatteoGabr/ProvaPratica/main/Partecipazione-della-popolazione-al-mercato-del-lavoro-per-regione.csv"

# Ottiene la directory corrente dove si trova lo script Python
curr_dirr = os.getcwd()

# Definisce il percorso completo della cartella `csv/` dove salvare i file localmente
csv_dir = os.path.join(curr_dirr, 'csv')

# Funzione per importare i dati da un URL e convertirli in un DataFrame pandas
def import_data(url):
    response = requests.get(url)  # Effettua richiesta HTTP GET all'URL fornito
    if response.status_code == 200:  # Se la risposta è positiva (200 OK)
        csv_content = StringIO(response.text)  # Converte il contenuto testuale in un oggetto simile a file
        df = pd.read_csv(csv_content, sep=';', na_values=['', 'Null'])  # Legge il CSV usando ';' come separatore
        # Corregge i caratteri speciali errati nei nomi delle colonne
        df.columns = [col.replace('�', 'à') for col in df.columns]
        df.columns = [col.replace('ŕ', 'à') for col in df.columns]
        return df  # Ritorna il DataFrame risultante
    else:
        print(f"Errore nell'importazione dei dati da {url}")  # Messaggio d'errore se il download fallisce
        return None

# Funzione per salvare un DataFrame come file CSV localmente
def save_df_local(df, filename):
    os.makedirs(csv_dir, exist_ok=True)  # Crea la cartella `csv/` se non esiste già
    path = os.path.join(csv_dir, filename)  # Costruisce il percorso completo del file
    df.to_csv(path, index=False, encoding='utf-8')  # Salva il DataFrame nel file CSV senza gli indici
    print(f"DataFrame salvato in {path}")  # Messaggio di conferma

# Funzione per interpolare (stimare) i dati mancanti in colonne specifiche
def interpolate_missing_data(df, columns):
    for col in columns:
        df[col] = df[col].interpolate(method='linear')  # Applica interpolazione lineare ai dati mancanti
    return df  # Ritorna il DataFrame aggiornato

# --- TRATTAMENTO DATI SPESA RICERCA ---

# Importa i dati da GitHub
df_spesa_ricerca = import_data(csv_spesa_ricerca)

# Interpola i valori mancanti nella colonna indicata
interpolate_missing_data(df_spesa_ricerca, ['Percentuale spesa imprese in ricerca e sviluppo'])

# Salva il DataFrame come CSV localmente
save_df_local(df_spesa_ricerca, "spesa_ricerca.csv")

# --- TRATTAMENTO DATI PARTECIPAZIONE LAVORO ---

# Importa i dati da GitHub
df_partecipazione_lavoro = import_data(csv_partecipazione_lavoro)

# Stampa i nomi delle colonne per verifica visiva
print(df_partecipazione_lavoro.columns.tolist())

# Interpola i valori mancanti nella colonna indicata
interpolate_missing_data(df_partecipazione_lavoro, ['Percentuale forze di lavoro in età 15-64 anni'])

# Salva il DataFrame come CSV localmente
save_df_local(df_partecipazione_lavoro, "partecipazione_lavoro.csv")

# --- INSERIMENTO NEL DATABASE ---

# Apre connessione al database SQLite `tutorial.db`
conn = sqlite3.connect('tutorial.db')
cursor = conn.cursor()  # Ottiene un cursore per eseguire query

# Ciclo sulle righe del DataFrame `df_spesa_ricerca`
for _, row in df_spesa_ricerca.iterrows():
    # Ottiene l'ID della regione corrispondente al nome nella riga
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    # Inserisce i valori nella tabella `percentuale_spesa_ricerca_sviluppo`
    cursor.execute(
        'INSERT INTO percentuale_spesa_ricerca_sviluppo (Anno, Regione_id, Percentuale) VALUES (?, ?, ?)',
        (row['Anno'], regione_id, row['Percentuale spesa imprese in ricerca e sviluppo'])
    )

# Ciclo sulle righe del DataFrame `df_partecipazione_lavoro`
for _, row in df_partecipazione_lavoro.iterrows():
    # Ottiene l'ID della regione corrispondente al nome
    regione_id = cursor.execute("SELECT id FROM regioni WHERE nome = ?", (row['Regione'],)).fetchone()[0]
    # Inserisce i valori nella tabella `percentuale_partecipazione_lavoro`
    cursor.execute(
        'INSERT INTO percentuale_partecipazione_lavoro (Anno, Regione_id, Percentuale) VALUES (?, ?, ?)',
        (row['Anno'], regione_id, row['Percentuale forze di lavoro in età 15-64 anni'])
    )

# Salva (committa) tutte le modifiche nel database
conn.commit()

# Chiude la connessione con il database
conn.close()
