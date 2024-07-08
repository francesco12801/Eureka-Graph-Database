import matplotlib.pyplot as plt
import psycopg2
import csv
import sys
from datetime import datetime
import os
from dotenv import load_dotenv
import time
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

# Misurare e stampare il tempo di esecuzione di ogni query
def measure_time(func, *args):
    start_time = time.time()
    result = func(*args)
    elapsed_time = time.time() - start_time
    print(f"Query '{func.__name__}' executed in {elapsed_time:.4f} seconds")
    return elapsed_time, result

# 1. Get Users based on their screenName
def get_users_by_screenname(screen_name):
    cur.execute("""
        SELECT * FROM "User" WHERE "screenname" = %s;
    """, (screen_name,))
    return cur.fetchall()

# 2. Get Posts containing a specific Tag
def get_posts_by_tag(tag):
    cur.execute("""
        SELECT p.postId, p.userId, p.timestamp
        FROM "Post" p
        JOIN "Has_Tag" ht ON p.postId = ht.postId
        WHERE ht.tagText = %s
    """, (tag,))
    return cur.fetchall()

# 3. Get followers of a specific User
def get_followers(user_id):
    cur.execute("""
        SELECT COUNT(followedId)
        FROM "Follows"
        WHERE followerId = %s;
    """, (user_id,))
    followers = cur.fetchone()
    return followers[0]

# 4. Count all friends of friends
def count_friends_of_friends(screenName):
    cur.execute("""
        SELECT u2."User" AS fof
        FROM "User" u1 
        JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
        JOIN "User" u2 ON uf2.followedId = u2.userId
        WHERE u1.screenName = %s AND uf2.followedId != u1.userId;
    """, (screenName,))
    result = cur.fetchone()
    return result[0] if result else 0

# 5. Get top 10 most influencing Users in our social graph based on their follower number
def get_top_influencing_users():
    cur.execute("""
        SELECT u.userId, u.screenName, COUNT(f.followedId) AS follower_count
        FROM "User" u
        LEFT JOIN "Follows" f ON u.userId = f.followedId
        GROUP BY u.userId, u.screenName
        ORDER BY follower_count DESC
        LIMIT 10;
    """)
    return cur.fetchall()

# 6. Get most trending Tags across all Users
def get_most_trending_tags():
    cur.execute("""
        SELECT ht.tagText, COUNT(*) AS tag_count
        FROM "Has_Tag" ht
        JOIN "Post" p ON ht.postId = p.postId
        GROUP BY ht.tagText
        ORDER BY tag_count DESC
        LIMIT 10;
    """)
    return cur.fetchall()

# 7. Get most trending Tags across most influencing Users
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
    return cur.fetchall()

# 8. Get first 100 User IDs and Screennames
def get_first_100_users():
    cur.execute("""
        SELECT userId, screenName
        FROM "User"
        ORDER BY userId 
        LIMIT 100;
    """)
    return cur.fetchall()

# 9. Suggest other users to follow based on interests
def get_followed_users(user_id):
    cur.execute("""
        SELECT followedId
        FROM "Follows"
        WHERE followerId = %s;
    """, (user_id,))
    followed_users = cur.fetchall()
    return [row[0] for row in followed_users]

def get_user_interests(user_id):
    cur.execute("""
        SELECT DISTINCT ht.tagText
        FROM "Post" p
        JOIN "Has_Tag" ht ON p.postId = ht.postId
        WHERE p.userId = %s;
    """, (user_id,))
    user_interests = cur.fetchall()
    return [row[0] for row in user_interests]

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
    return cur.fetchall()

def get_followers_of_followers(user_id, k):
    current_followers = {user_id}
    all_followers = set()
    
    for degree in range(k):
        new_followers = set()
        for follower in current_followers:
            cur.execute("""
                SELECT followerId
                FROM "Follows"
                WHERE followedId = %s;
            """, (follower,))
            followers = cur.fetchall()
            new_followers.update([row[0] for row in followers])
        all_followers.update(new_followers)
        current_followers = new_followers

    return list(all_followers)

def calculate_time():
    queries = [
        (get_followers, "1393409100"),
        (get_users_by_screenname, "_notmichelle"),
        (get_posts_by_tag, "#nationaldogday"),
        (count_friends_of_friends, 461669641),
        (get_followers_of_followers, 461669641, 5),
        (get_first_100_users,),
        (get_top_influencing_users,),
        (get_most_trending_tags,),
        (get_trending_tags_among_influencers,),
        (suggest_users_to_follow, 1393409100)
    ]
    
    execution_times = []
    
    for query in queries:
        func = query[0]
        args = query[1:]
        elapsed_time, _ = measure_time(func, *args)
        execution_times.append((func.__name__, elapsed_time))
    
    return execution_times

def plot_execution_times(execution_times):
    labels, times = zip(*execution_times)
    plt.figure(figsize=(12, 6))
    plt.barh(labels, times, color='skyblue')
    plt.xlabel('Execution Time (seconds)')
    plt.ylabel('Query')
    plt.title('Execution Time of Different Queries')
    plt.grid(True)
    plt.show()

def plot_trending_tags(tags_data):
    tags, counts = zip(*tags_data)
    plt.figure(figsize=(10, 8))
    plt.pie(counts, labels=tags, autopct='%1.1f%%', startangle=140)
    plt.axis('equal')
    plt.title('Most Trending Tags')
    plt.show()

def plot_followers_of_followers_times(user_id, max_k):
    k_values = list(range(1, max_k + 1))
    times = []
    
    for k in k_values:
        elapsed_time, _ = measure_time(get_followers_of_followers, user_id, k)
        times.append(elapsed_time)
    
    plt.figure(figsize=(12, 6))
    plt.plot(k_values, times, marker='o', linestyle='-', color='b')
    plt.xlabel('k (Degree)')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time of get_followers_of_followers for different k values')
    plt.grid(True)
    plt.show()

try: 
    print("Followers of user 1393409100:", get_followers("1393409100"))
    print("Users with screenname '_notmichelle':", get_users_by_screenname("_notmichelle"))
    print(get_posts_by_tag("#nationaldogday"))
    print("Friends of friends of user 1393409100:", count_friends_of_friends(461669641))
    print("First 100 User IDs:", get_first_100_users())
    print("Top 10 Influencing Users:", get_top_influencing_users())
    print("Most Trending Tags:", get_most_trending_tags())
    print("Most Trending Tags across Influencer:", get_trending_tags_among_influencers())
    print("Suggest User:", suggest_users_to_follow(1393409100))

    show_execution_time = input("Do you want to see the execution time? (y/n): ")
    # Check the user's response
    if show_execution_time.lower() == "y":
        execution_times = calculate_time()
        plot_execution_times(execution_times)
        plot_followers_of_followers_times(461669641, 10)  
        plot_trending_tags(get_most_trending_tags())

    else:
        print("Execution time not shown.")

except Exception as e:
    print("Error:", e)

# Chiudi la connessione al database
conn.commit()
cur.close()
conn.close()
