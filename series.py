import sqlite3  # Per connettersi e operare su un database SQLite
import pandas as pd  # Per manipolare i dati in DataFrame

# Funzione per eseguire una query SQL e restituire un DataFrame
def query_db(query: str, params: tuple = ()):
    conn = sqlite3.connect('tutorial.db')  # Apre connessione al database
    df = pd.read_sql_query(query, conn, params=params)  # Esegue la query e ottiene un DataFrame
    conn.close()  # Chiude la connessione
    return df  # Ritorna il risultato della query come DataFrame

# Apre una connessione al database per operazioni successive
conn = sqlite3.connect('tutorial.db')
cursor = conn.cursor()

# Crea (se non esiste) la tabella che conterrà la partecipazione totale nazionale
cursor.execute('''
CREATE TABLE IF NOT EXISTS partecipazione_totale_nazionale (
    anno INTEGER,
    partecipazione_totale FLOAT,
    PRIMARY KEY (anno)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS partecipazione_totale_aree (
    anno INTEGER,
    area VARCHAR(50),
    partecipazione_totale FLOAT,
    PRIMARY KEY (anno, area)
)
''')

# Query per ottenere tutti i dati dalla tabella delle percentuali di partecipazione al lavoro con join sulle regioni
query_join = '''
SELECT p.Anno, p.Percentuale, r.area_geografica
FROM percentuale_partecipazione_lavoro p
JOIN regioni r ON p.Regione_id = r.id
'''

# Esegue la query di join e salva il risultato in DataFrame
df_partecipazione_joined = query_db(query_join)

# Raggruppa i dati per anno e calcola la media delle percentuali (media nazionale)
partecipazione_totale_nazionale = df_partecipazione_joined.groupby(['Anno'])['Percentuale'].mean().reset_index()
# Raggruppa i dati per anno e area geografica e calcola la media delle percentuali
partecipazione_totale_aree = df_partecipazione_joined.groupby(['Anno', 'area_geografica'])['Percentuale'].mean().reset_index()
# reset_index() serve a trasformare l'indice creato dal groupby (in questo caso la colonna 'Anno') 
# in una colonna normale del DataFrame. 
# Questo permette di lavorare più facilmente con i dati, perché altrimenti 'Anno' sarebbe l'indice 
# e non una colonna visibile come le altre.

# Salva i DataFrame risultanti nelle rispettive tabelle del database
# Sostituisce la tabella se esiste già
partecipazione_totale_nazionale.to_sql(
    'partecipazione_totale_nazionale',
    conn,
    if_exists='replace',
    index=False
)
partecipazione_totale_aree.to_sql(
    'partecipazione_totale_aree',
    conn,
    if_exists='replace',
    index=False
)

# Chiude la connessione al database
conn.close()
