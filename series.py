import sqlite3  # Per connettersi e operare su un database SQLite
import pandas as pd  # Per manipolare i dati in DataFrame

# ------------------------------------------------------------------------------
# Funzioni
# ------------------------------------------------------------------------------

"""
    Esegue una query SQL e restituisce il risultato come DataFrame.

    @param query: la query SQL da eseguire
    @param params: parametri opzionali per la query (default: tuple vuota)
    @returns: DataFrame contenente i risultati della query
"""
def query_db(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect('tutorial.db')  # Apre connessione al database
    df = pd.read_sql_query(query, conn, params=params)  # Esegue la query
    conn.close()  # Chiude la connessione
    return df

# ------------------------------------------------------------------------------
# Connessione al database e creazione tabelle
# ------------------------------------------------------------------------------

# Apre una connessione al database per operazioni successive
conn = sqlite3.connect('tutorial.db')
cursor = conn.cursor()

# Crea la tabella per la partecipazione totale nazionale (se non esiste)
cursor.execute('''
CREATE TABLE IF NOT EXISTS partecipazione_totale_nazionale (
    anno INTEGER,
    partecipazione_totale FLOAT,
    PRIMARY KEY (anno)
)
''')

# Crea la tabella per la partecipazione totale per area geografica (se non esiste)
cursor.execute('''
CREATE TABLE IF NOT EXISTS partecipazione_totale_aree (
    anno INTEGER,
    area VARCHAR(50),
    partecipazione_totale FLOAT,
    PRIMARY KEY (anno, area)
)
''')

# ------------------------------------------------------------------------------
# Query e aggregazione dati
# ------------------------------------------------------------------------------

# Query per ottenere dati uniti tra percentuali di partecipazione e regioni
query_join = '''
SELECT p.Anno, p.Percentuale, r.area_geografica
FROM percentuale_partecipazione_lavoro p
JOIN regioni r ON p.Regione_id = r.id
'''

# Esegue la query di join e salva il risultato in un DataFrame
df_partecipazione_joined = query_db(query_join)

# Calcola la media nazionale della partecipazione al lavoro per anno
partecipazione_totale_nazionale = df_partecipazione_joined.groupby(['Anno'])['Percentuale'].mean().reset_index()

# Calcola la media della partecipazione al lavoro per anno e area geografica
partecipazione_totale_aree = df_partecipazione_joined.groupby(['Anno', 'area_geografica'])['Percentuale'].mean().reset_index()

# Nota su reset_index():
# reset_index() trasforma l'indice creato da groupby in una colonna normale del DataFrame,
# facilitando l'accesso e la manipolazione dei dati.

# ------------------------------------------------------------------------------
# Salvataggio dei risultati nel database
# ------------------------------------------------------------------------------

# Salva il DataFrame nazionale nella tabella, sovrascrivendo se già esistente
partecipazione_totale_nazionale.to_sql(
    'partecipazione_totale_nazionale',
    conn,
    if_exists='replace',
    index=False
)

# Salva il DataFrame per aree nella tabella, sovrascrivendo se già esistente
partecipazione_totale_aree.to_sql(
    'partecipazione_totale_aree',
    conn,
    if_exists='replace',
    index=False
)

# Chiude la connessione al database
conn.close()
