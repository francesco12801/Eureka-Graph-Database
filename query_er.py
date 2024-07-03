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
print("Connecting to database..")
DBNAME=os.getenv('DBNAME', 'dataManagement')
USER=os.getenv('USER', 'postgres')
PASSWORD=os.getenv('PASSWORD', '2345')
HOST=os.getenv('HOST', '127.0.0.1')
PORT=os.getenv('PORT', '5432')

conn = psycopg2.connect(
    dbname=DBNAME,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)
print(f"Done! '{USER}' has connected to '{DBNAME}' at {HOST}:{PORT}")
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