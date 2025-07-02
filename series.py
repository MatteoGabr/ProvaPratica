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
    PRIMARY KEY (anno)
)
''')

# Query per ottenere tutti i dati dalla tabella delle percentuali di partecipazione al lavoro
query_partecipazione_nazionale = "SELECT * FROM percentuale_partecipazione_lavoro"
query_partecipazione_aree = "SELECT * FROM percentuale_partecipazione_lavoro"

# Query per ottenere tutte le regioni
query_regioni = "SELECT * FROM regioni"

# Esegue le query e salva i risultati in DataFrame
df_partecipazione_nazionale = query_db(query_partecipazione_nazionale)
df_partecipazione_aree = query_db(query_partecipazione_aree)
df_regioni = query_db(query_regioni)

# Effettua un join tra la tabella delle percentuali e quella delle regioni
# Serve a poter leggere i nomi delle regioni se necessario
df_partecipazione_nazionale = pd.merge(
    df_partecipazione_nazionale,  # DataFrame principale
    df_regioni,  # DataFrame da unire
    left_on='Regione_id', right_on='id'  # Corrispondenza tra chiave esterna e primaria
)

df_partecipazione_aree = pd.merge(
    df_partecipazione_aree,  # DataFrame principale
    df_regioni,  # DataFrame da unire
    left_on='Regione_id', right_on='id'  # Corrispondenza tra chiave esterna e primaria
)

# Raggruppa i dati per anno e somma le percentuali su tutte le regioni (somma totale nazionale)
partecipazione_totale_nazionale = df_partecipazione_nazionale.groupby(['Anno'])['Percentuale'].mean().reset_index() 
partecipazione_totale_aree = df_partecipazione_aree.groupby(['Anno', 'area_geografica'])['Percentuale'].mean().reset_index()
# reset_index() serve a trasformare l'indice creato dal groupby (in questo caso la colonna 'Anno') 
# in una colonna normale del DataFrame. 
# Questo permette di lavorare più facilmente con i dati, perché altrimenti 'Anno' sarebbe l'indice 
# e non una colonna visibile come le altre.


# Salva il DataFrame risultante nella tabella `partecipazione_totale_nazionale` del database
# Sostituisce la tabella se esiste già
partecipazione_totale_nazionale.to_sql(
    'partecipazione_totale_nazionale',
    conn,
    if_exists='replace',
    index=False
)
partecipazione_totale_aree.to_sql('partecipazione_totale_aree', conn, if_exists='replace', index=False)

# Chiude la connessione al database
conn.close()
