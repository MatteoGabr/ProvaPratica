from fastapi import FastAPI
from typing import Optional
import sqlite3  # Connessione a database SQLite
import pandas as pd  # Gestione dati in DataFrame

# ------------------------------------------------------------------------------
# Inizializzazione FastAPI
# ------------------------------------------------------------------------------

app = FastAPI(
    title="API",
    description="Questa API fornisce dati italiani",
    version="1.0.0"
)

# ------------------------------------------------------------------------------
# Funzione per interrogare il database
# ------------------------------------------------------------------------------

"""
    Esegue una query SQL su un database SQLite e restituisce i risultati come DataFrame.

    @param query: stringa SQL da eseguire
    @param params: tupla di parametri da passare alla query SQL (serve per evitare SQL injection)
                   Es: ("2020", "2023") per filtrare tra due anni
                   Se la query non richiede parametri, può essere una tupla vuota ()
    @returns: DataFrame contenente i risultati della query
"""
def query_db(query: str, params: tuple = ()) -> pd.DataFrame:
    conn = sqlite3.connect('tutorial.db')  # Connessione al database
    df = pd.read_sql_query(query, conn, params=params)  # Esecuzione query con parametri
    conn.close()
    return df

# ------------------------------------------------------------------------------
# Endpoints API
# ------------------------------------------------------------------------------

"""
    Restituisce i dati di partecipazione al lavoro a livello nazionale.

    @param da_anno: (opzionale) anno iniziale per il filtro
    @param a_anno: (opzionale) anno finale per il filtro
    @returns: Lista di dizionari con i dati di partecipazione
"""
@app.get("/partecipazione_nazionale")
def get_partecipazione_totale_nazionale(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM partecipazione_totale_nazionale"
    params = []  # Lista di parametri da passare alla query (verranno convertiti in tupla)
                 # Usata solo se entrambi gli anni vengono forniti
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"  # ? sono segnaposto per i parametri
        params.extend([da_anno, a_anno])        # Aggiunta dei parametri forniti alla lista
    df = query_db(query, tuple(params))  # Esecuzione della query con i parametri (convertiti in tupla)
    return df.to_dict(orient='records')  # Restituisce i dati come lista di dizionari


"""
    Restituisce i dati di partecipazione al lavoro per area geografica.

    @param da_anno: (opzionale) anno iniziale per il filtro
    @param a_anno: (opzionale) anno finale per il filtro
    @returns: Lista di dizionari con i dati di partecipazione per area
"""
@app.get("/partecipazione_aree")
def get_partecipazione_totale_aree(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM partecipazione_totale_aree"
    params = []  # Lista vuota che verrà riempita se vengono passati gli anni
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"
        params.extend([da_anno, a_anno])
    df = query_db(query, tuple(params))  # Conversione della lista in tupla prima dell'esecuzione
    return df.to_dict(orient='records')


@app.get('/spesa_aree')
def get_spesa_area_geografica(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM spesa_totale_aree"
    params = []  # Lista vuota che verrà riempita se vengono passati gli anni
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"
        params.extend([da_anno, a_anno])
    df = query_db(query, tuple(params))  # Conversione della lista in tupla prima dell'esecuzione
    return df.to_dict(orient='records')