from fastapi import FastAPI
from typing import Optional
import sqlite3
import pandas as pd

app = FastAPI(
    title="API",
    description="Questa API fornisce dati italiani",
    version="1.0.0"
)

def query_db(query: str, params: tuple = ()):
    conn = sqlite3.connect('tutorial.db')
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@app.get("/partecipazione_nazionale")
def get_partecipazione_totale_nazionale(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM partecipazione_totale_nazionale"
    params = []
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"
        params.extend([da_anno, a_anno])
    df = query_db(query, tuple(params))
    return df.to_dict(orient='records')

@app.get("/partecipazione_aree")
def get_partecipazione_totale_aree(da_anno: Optional[int] = None, a_anno: Optional[int] = None):
    query = "SELECT * FROM partecipazione_totale_aree"
    params = []
    if da_anno and a_anno:
        query += " WHERE Anno BETWEEN ? AND ?"
        params.extend([da_anno, a_anno])
    df = query_db(query, tuple(params))
    return df.to_dict(orient='records')