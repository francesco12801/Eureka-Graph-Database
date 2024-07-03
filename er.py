import psycopg2
import csv
import sys
from dotenv import load_dotenv
import os

load_dotenv()

maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

rows_limit = None
try:
    rows_limit = int(sys.argv[1])
except (IndexError, ValueError):
    print("Run script by specify an integer argument to limit the csv rows - Default is 'None', no limit")
finally:
    print(f"Consider only {rows_limit} rows.")

print("Connecting to database..")
# Connessione al database PostgreSQL

conn = psycopg2.connect(
    dbname="dataManagement",
    user="postgres",
    password="2345",
    host="127.0.0.1",
    port="5432"
)
cur = conn.cursor()

# Creazione delle tabelle
commands = [
    """
    CREATE TABLE IF NOT EXISTS "User" (
        userId BIGINT PRIMARY KEY,
        screenName VARCHAR(50),
        avatar VARCHAR(255),
        followersCount INT,
        followingCount INT,
        lang VARCHAR(10)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS "Post" (
        postId BIGINT PRIMARY KEY,
        userId BIGINT NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        FOREIGN KEY (userId) REFERENCES "User" (userId)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS "Tag" (
        text VARCHAR(255) PRIMARY KEY
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS "Follows" (
        followerId BIGINT NOT NULL,
        followedId BIGINT NOT NULL,
        PRIMARY KEY (followerId, followedId),
        FOREIGN KEY (followerId) REFERENCES "User" (userId),
        FOREIGN KEY (followedId) REFERENCES "User" (userId)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS "Has_Tag" (
        postId BIGINT NOT NULL,
        tagText VARCHAR(255) NOT NULL,
        PRIMARY KEY (postId, tagText),
        FOREIGN KEY (postId) REFERENCES "Post" (postId),
        FOREIGN KEY (tagText) REFERENCES "Tag" (text)
    )
    """
]

# Esecuzione dei comandi per creare le tabelle
for command in commands:
    cur.execute(command)

# Funzione per inserire i dati nella tabella User
def insert_user(row):
    cur.execute("""
        INSERT INTO "User" (userId, screenName, avatar, followersCount, followingCount, lang)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (userId) DO NOTHING
    """, (int(row['id']), row['screenName'], row['avatar'], int(row['followersCount']), int(row['friendsCount']), row['lang']))

# Funzione per inserire i dati nella tabella Post
def insert_post(row):
    cur.execute("""
        INSERT INTO "Post" (postId, userId, timestamp)
        VALUES (%s, %s, TO_TIMESTAMP(%s / 1000.0))
        ON CONFLICT (postId) DO NOTHING
    """, (int(row['tweetId']), int(row['id']), float(row['lastSeen'])))

# Funzione per inserire i dati nella tabella Tag
def insert_tag(tag):
    cur.execute("""
        INSERT INTO "Tag" (text)
        VALUES (%s)
        ON CONFLICT (text) DO NOTHING
    """, (tag,))

# Funzione per inserire i dati nella tabella Has_Tag
def insert_has_tag(postId, tag):
    cur.execute("""
        INSERT INTO "Has_Tag" (postId, tagText)
        VALUES (%s, %s)
        ON CONFLICT (postId, tagText) DO NOTHING
    """, (postId, tag))

# Funzione per inserire i dati nella tabella Follows
def insert_follows(userId, friendId):
    # Inserisci l'amico come utente se non esiste già
    cur.execute("SELECT 1 FROM \"User\" WHERE userId = %s", (friendId,))
    if cur.fetchone() is None:
        # Ottieni le informazioni dell'amico dai dati disponibili
        friend_data = {
            'id': friendId,
            'screenName': f"Friend_{friendId}",
            'avatar': '',  # Inserisci un valore di default per l'avatar
            'followersCount': 0,  # Inserisci un valore di default per i follower
            'friendsCount': 0,  # Inserisci un valore di default per gli amici
            'lang': 'en'  # Inserisci un valore di default per la lingua
        }
        insert_user(friend_data)

    # Inserisci la relazione di follow
    cur.execute("""
        INSERT INTO "Follows" (followerId, followedId)
        VALUES (%s, %s)
        ON CONFLICT (followerId, followedId) DO NOTHING
    """, (userId, friendId))

# Funzione per inserire tutti gli utenti
def insert_all_users(reader):
    for row in reader:
        insert_user(row)

# Funzione per inserire tutti i post, tag e relazioni di follow
def insert_other_data(reader):
    for row in reader:
        insert_post(row)

        tags = row['tags'].split('|')
        for tag in tags:
            insert_tag(tag)
            insert_has_tag(int(row['tweetId']), tag)

        friends = row['friends'].split('|')
        for friend in friends:
            if friend.strip():  # Controlla che il valore non sia vuoto
                try:
                    insert_follows(int(row['id']), int(friend))
                except ValueError:
                    print(f"Skipping invalid friend ID: {friend}")

# Leggi il file CSV e inserisci i dati nel database
with open('data.csv', 'r') as f:
    reader = csv.DictReader(f)
    rows = list(reader)[:rows_limit]   # Convertiamo il reader in una lista per poterlo rileggere
    insert_all_users(rows)
    insert_other_data(rows)

# Chiudi la comunicazione con il database PostgreSQL
cur.close()
conn.commit()
conn.close()
