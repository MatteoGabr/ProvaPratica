import sqlite3  # Importa il modulo sqlite3 per la gestione del database SQLite

# Crea una connessione al database SQLite chiamato 'tutorial.db'
# Se il file non esiste, verrà creato automaticamente
conn = sqlite3.connect('tutorial.db')

# Crea la tabella per la percentuale di spesa in ricerca e sviluppo
# Ogni riga rappresenta il valore percentuale per una regione in un certo anno
conn.execute('''
CREATE TABLE IF NOT EXISTS percentuale_spesa_ricerca_sviluppo (
    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,  -- Chiave primaria con incremento automatico
    Anno INT NOT NULL,                              -- Anno del dato (es: 2012)
    Regione_id INT NOT NULL,                        -- ID della regione (collegamento alla tabella regioni)
    Percentuale REAL NOT NULL                       -- Percentuale (meglio usare REAL al posto di BOOLEAN per dati numerici)
)
''')

# Crea la tabella per la percentuale di partecipazione al lavoro
conn.execute('''
CREATE TABLE IF NOT EXISTS percentuale_partecipazione_lavoro (
    ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,  -- Chiave primaria
    Anno INT NOT NULL,                              -- Anno del dato
    Regione_id INT NOT NULL,                        -- ID regione
    Percentuale REAL NOT NULL                       -- Percentuale della forza lavoro (dato numerico)
)
''')

# Crea la tabella delle regioni italiane con la rispettiva area geografica
conn.execute('''
CREATE TABLE IF NOT EXISTS regioni (
    id INTEGER PRIMARY KEY AUTOINCREMENT,   -- ID unico per ogni regione
    nome TEXT NOT NULL,                     -- Nome della regione (es: "Piemonte")
    area_geografica TEXT NOT NULL           -- Area geografica (es: "Nord-ovest")
)
''')

# Definisce una lista di tuple contenenti (nome regione, area geografica)
regioni = [
    ('Valle d\'Aosta', 'Nord-ovest'),
    ('Piemonte', 'Nord-ovest'),
    ('Liguria', 'Nord-ovest'),
    ('Lombardia', 'Nord-ovest'),
    ('Trentino-Alto Adige', 'Nord-est'),
    ('Veneto', 'Nord-est'),
    ('Friuli-Venezia Giulia', 'Nord-est'),
    ('Emilia-Romagna', 'Nord-est'),
    ('Toscana', 'Centro'),
    ('Umbria', 'Centro'),
    ('Marche', 'Centro'),
    ('Lazio', 'Centro'),
    ('Abruzzo', 'Centro'),
    ('Molise', 'Sud'),
    ('Campania', 'Sud'),
    ('Puglia', 'Sud'),
    ('Basilicata', 'Sud'),
    ('Calabria', 'Sud'),
    ('Sicilia', 'Isole'),
    ('Sardegna', 'Isole')
]

# Inserisce tutte le regioni nella tabella 'regioni'
conn.executemany('INSERT INTO regioni (nome, area_geografica) VALUES (?, ?)', regioni)

print("Tabelle create e popolamento regioni completato con successo.")

# Salva (commit) le modifiche nel database
conn.commit()

# Chiude la connessione al database
conn.close()
