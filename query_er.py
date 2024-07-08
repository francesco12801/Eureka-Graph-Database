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

def FoF(username): 
    cur.execute='''
    SELECT f2.User as FOF
    FROM User u1 JOIN Follows 
'''

# 4. Count all friends of friends
def count_friends_of_friends(user_id):
    cur.execute("""
        SELECT COUNT(DISTINCT f2.followedId)
        FROM "Follows" f1
        JOIN "Follows" f2 ON f1.followedId = f2.followerId
        WHERE f1.followerId = %s AND f2.followedId != %s;
    """, (user_id, user_id))
    result = cur.fetchone()
    return result[0] if result else 0


def suggest_post_itemBased(screenName):
    return

def suggest_post_itemBased(screenName):
    return

def suggest_users_itemBased(screenName):
    cur.execute("""      
        SELECT DISTINCT u.screenName AS originalUser,
                p.postId AS originalPost,
                other_p.postId AS otherPost,
                t.text AS tag,
                other_u.screenName AS otherUser
        FROM "User" u
        JOIN "Post" p ON u.userId = p.userId
        JOIN "Has_Tag" ht1 ON p.postId = ht1.postId
        JOIN "Tag" t ON ht1.tagText = t.text
        JOIN "Has_Tag" ht2 ON t.text = ht2.tagText
        JOIN "Post" other_p ON ht2.postId = other_p.postId
        JOIN "User" other_u ON other_p.userId = other_u.userId
        LEFT JOIN "Follows" f ON u.userId = f.followerId AND other_u.userId = f.followedId
        WHERE u.screenName = %s
        AND other_p.postId <> p.postId
        AND other_u.userId <> u.userId
        AND f.followedId IS NULL
        LIMIT 10;


    """,(screenName,))
    return cur.fetchall() 
    




def suggest_users_userBased(screenName):
    # Amici diretti (distanza 1)
    cur.execute("""
        SELECT uf1.followedId AS userId
        FROM "User" u1
        JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        WHERE u1.screenName = %s;
    """, (screenName,))
    direct_friends = cur.fetchall()
    direct_friend_ids = {friend[0] for friend in direct_friends}

    # Amici degli amici (distanza 2)
    cur.execute("""
        SELECT u2.userId AS userId, 2 AS distance, COUNT(*) AS paths
        FROM "User" u1
        JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
        JOIN "User" u2 ON uf2.followedId = u2.userId
        WHERE u1.screenName = %s AND uf2.followedId != u1.userId
        GROUP BY u2.userId;
    """, (screenName,))
    fof_distance_2 = [row for row in cur.fetchall() if row[0] not in direct_friend_ids]

    # Amici degli amici degli amici (distanza 3)
    cur.execute("""
        SELECT u3.userId AS userId, 3 AS distance, COUNT(*) AS paths
        FROM "User" u1
        JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
        JOIN "Follows" uf3 ON uf2.followedId = uf3.followerId
        JOIN "User" u3 ON uf3.followedId = u3.userId
        WHERE u1.screenName = %s AND uf3.followedId != u1.userId
        GROUP BY u3.userId;
    """, (screenName,))
    fof_distance_3 = [row for row in cur.fetchall() if row[0] not in direct_friend_ids]

    # Combina i risultati
    result = fof_distance_2 + fof_distance_3

    # Ordina per distanza ascendente e per numero di percorsi discendente
    result_sorted = sorted(result, key=lambda x: (x[1], -x[2]))

    # Limita i risultati a 20
    result_limited = result_sorted[:20]

    return result_limited




def get_FOF(screenName):
    cur.execute("""
        SELECT u2.userId AS fof
        FROM "User" u1 
        JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
        JOIN "User" u2 ON uf2.followedId = u2.userId
        WHERE u1.screenName = %s AND uf2.followedId != u1.userId;
    """, (screenName,))
    result = cur.fetchall()
    return result

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
    # print("Followers of user 1393409100:", get_followers("1393409100"))
    # print("Users with screenname '_notmichelle':", get_users_by_screenname("_notmichelle"))
    # print(get_posts_by_tag("#nationaldogday"))
    # print("Friends of friends of user 1393409100:", count_friends_of_friends(461669641))
    # print("First 100 User IDs:", get_first_100_users())
    print("Suggest User Item-Based: ", suggest_users_itemBased("jdfollowhelp"))
    # print("Top 10 Influencing Users:", get_top_influencing_users())
    print("Most Trending Tags:", get_most_trending_tags())
    # print("Most Trending Tags across Influencer:", get_trending_tags_among_influencers())
    # print("Suggest User:", suggest_users_to_follow(1393409100))
    # print("FOF: ", get_FOF('jdfollowhelp'))
    screen_name = 'jdfollowhelp'
    fof_with_distance_and_paths = suggest_users_userBased(screen_name)
    for userId, distance, paths in fof_with_distance_and_paths:
        print(f"UserId: {userId}, Distance: {distance}, Paths: {paths}")
    
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
