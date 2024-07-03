import psycopg2
import csv
import sys
from datetime import datetime
import os
from dotenv import load_dotenv
load_dotenv()

# Aumenta il limite massimo dei campi CSV
csv.field_size_limit(sys.maxsize)

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

cur.execute("""
    SELECT COUNT(*)
    FROM "User";
""")
total_users = cur.fetchone()[0]
print(f"Total Users in the database: {total_users}")

# Chiudi la comunicazione con il database PostgreSQL
conn.commit()
cur.close()
conn.close()