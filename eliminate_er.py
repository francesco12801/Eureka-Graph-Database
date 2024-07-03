import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

# Connessione al database PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv('DBNAME', 'dataManagement'),
    user=os.getenv('USER', 'postgres'),
    password=os.getenv('PASSWORD', '2345'),
    host=os.getenv('HOST', '127.0.0.1'),
    port=os.getenv('PORT', '5432')
)
# Crea un cursore
cur = conn.cursor()

# Definizione delle query SQL per eliminare i dati
delete_queries = [
    "DELETE FROM \"Has_Tag\"",
    "DELETE FROM \"Follows\"",
    "DELETE FROM \"Post\"",
    "DELETE FROM \"User\"",
    "DELETE FROM \"Tag\""
]

truncate_queries = [
    "TRUNCATE TABLE \"Has_Tag\" RESTART IDENTITY CASCADE",
    "TRUNCATE TABLE \"Follows\" RESTART IDENTITY CASCADE",
    "TRUNCATE TABLE \"Post\" RESTART IDENTITY CASCADE",
    "TRUNCATE TABLE \"User\" RESTART IDENTITY CASCADE",
    "TRUNCATE TABLE \"Tag\" RESTART IDENTITY CASCADE"
]

try:
    # Esecuzione delle query di DELETE
    for query in delete_queries:
        cur.execute(query)
        print(f"Deleted rows: {cur.rowcount} from {query}")

    # Esecuzione delle query di TRUNCATE
    for query in truncate_queries:
        cur.execute(query)
        print(f"Truncated table: {query}")

    # Commit delle modifiche al database
    conn.commit()
    print("Tutti i dati sono stati eliminati correttamente.")

except psycopg2.Error as e:
    print("Errore durante l'eliminazione dei dati:", e)
    conn.rollback()

finally:
    # Chiusura del cursore e della connessione
    cur.close()
    conn.close()