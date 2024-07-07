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
    port="5432"
)
print(f"Done! '{USER}' has connected to '{DBNAME}' at {HOST}:{PORT}")
cur = conn.cursor()

# 1. Get Users based on their screenName
def get_users_by_screenname(screen_name):
    cur.execute("""
        SELECT * FROM "User" WHERE "screenname" = %s;
    """, (screen_name,))
    return cur.fetchall()
def get_posts_by_tag(tag):
    # Query per ottenere i post con un tag specifico
        cur.execute("""
            SELECT p.postId, p.userId, p.timestamp
            FROM "Post" p
            JOIN "Has_Tag" ht ON p.postId = ht.postId
            WHERE ht.tagText = %s
        """, (tag,))

        # Recupera tutti i risultati
        return cur.fetchall()

def get_followers(user_id):
    cur.execute("""
        SELECT COUNT(followedId)
        FROM "Follows"
        WHERE followerId = %s;
    """, (user_id,))
    followers = cur.fetchone()
    return followers[0]

# Funzione per contare tutti gli amici di amici
def count_friends_of_friends(user_id):
        cur.execute("""
            SELECT COUNT(DISTINCT f2.followedId)
            FROM "Follows" f1
            JOIN "Follows" f2 ON f1.followedId = f2.followerId
            WHERE f1.followerId = %s AND f2.followedId != %s;
        """, (user_id, user_id))
        result = cur.fetchone()
        return result[0] if result else 0

def get_top_influencing_users():
        cur.execute("""
            SELECT u.userId, u.screenName, COUNT(f.followedId) AS follower_count
            FROM "User" u
            LEFT JOIN "Follows" f ON u.userId = f.followedId
            GROUP BY u.userId, u.screenName
            ORDER BY follower_count DESC
            LIMIT 10;
        """)
        top_users = cur.fetchall()
        return top_users

def get_most_trending_tags():
        cur.execute("""
            SELECT ht.tagText, COUNT(*) AS tag_count
            FROM "Has_Tag" ht
            JOIN "Post" p ON ht.postId = p.postId
            GROUP BY ht.tagText
            ORDER BY tag_count DESC
            LIMIT 10;
        """)
        trending_tags = cur.fetchall()
        for tag in trending_tags:
            print(f"Tag: {tag[0]}, Count: {tag[1]}")
        return trending_tags

def get_trending_tags_among_influencers():
        cur.execute("""
            SELECT ht.tagText, COUNT(*) AS tag_count
            FROM "User" u
            JOIN "Follows" f ON u.userId = f.followerId
            JOIN "Post" p ON f.followedId = p.userId
            JOIN "Has_Tag" ht ON p.postId = ht.postId
            GROUP BY ht.tagText
            ORDER BY tag_count DESC
            LIMIT 10;
        """)
        trending_tags = cur.fetchall()
        for tag in trending_tags:
            print(f"Tag: {tag[0]}, Count: {tag[1]}")
        return trending_tags

def get_first_100_users():
    cur.execute("""
        SELECT userId, screenName
        FROM "User"
        ORDER BY userId 
        LIMIT 100;
    """)
    users = cur.fetchall()
    return users

# Consiglia utenti (tutto il codice successivo serve per consigliare utenti)
# Funzione per ottenere gli utenti seguiti da un determinato utente
def get_followed_users(user_id):
        cur.execute("""
            SELECT followedId
            FROM "Follows"
            WHERE followerId = %s;
        """, (user_id,))
        followed_users = cur.fetchall()
        return [row[0] for row in followed_users]

# Funzione per ottenere gli interessi di un utente basati sui tag dei post che ha interagito
def get_user_interests(user_id):
        cur.execute("""
            SELECT DISTINCT ht.tagText
            FROM "Post" p
            JOIN "Has_Tag" ht ON p.postId = ht.postId
            WHERE p.userId = %s;
        """, (user_id,))
        user_interests = cur.fetchall()
        return [row[0] for row in user_interests]

# Funzione per suggerire altri utenti da seguire in base agli interessi dell'utente

def suggest_users_to_follow(user_id):
    followed_users = get_followed_users(user_id)
    user_interests = get_user_interests(user_id)
    cur.execute("""
        SELECT DISTINCT u.userId, u.screenName
        FROM "User" u
        JOIN "Post" p ON u.userId = p.userId
        JOIN "Has_Tag" ht ON p.postId = ht.postId
        WHERE ht.tagText IN %s
        AND u.userId != %s
        AND u.userId NOT IN %s
        LIMIT 10;
    """, (tuple(user_interests), user_id, tuple(followed_users)))
    suggested_users = cur.fetchall()
    return suggested_users
    
try: 
    print("Followers of user 1393409100:", get_followers("1393409100"))
    print("Users with screenname '_notmichelle':", get_users_by_screenname("_notmichelle"))
    print(get_posts_by_tag("#nationaldogday"))
    print("Friends of friends of user 1393409100:", count_friends_of_friends(461669641))
    print("First 100 User IDs:", get_first_100_users())
    print("Top 10 Influencing Users:", get_top_influencing_users())
    print("Most Trending Tags:", get_most_trending_tags())
    print("Most Trending Tags across Influencer:", get_trending_tags_among_influencers())
    print("Suggest User: ", suggest_users_to_follow(1393409100))

except Exception as e:
            print("Error:", e)


conn.commit()
cur.close()
conn.close()
