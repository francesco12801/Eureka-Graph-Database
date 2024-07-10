import matplotlib.pyplot as plt
import psycopg
import csv
import sys
from datetime import datetime
import os
from dotenv import load_dotenv
import time
import timeit
load_dotenv()

# Aumenta il limite massimo dei campi CSV
maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)

# Connessione al database PostgreSQL
print("Connecting to database..")
DBNAME=os.getenv('DBNAME', 'dataManagement')
USER=os.getenv('USER', 'postgres')
PASSWORD=os.getenv('PASSWORD', '2345')
HOST=os.getenv('HOST', '127.0.0.1')
PORT=os.getenv('PORT', '5432')

conn = psycopg.connect(
    # dbname="dataManagement",
    # user="postgres",
    # password="2345",
    # host="127.0.0.1",
    # port="5432",
    dbname=DBNAME,
    user=USER,
    password=PASSWORD,
    host=HOST,
    port=PORT
)

print(f"Done! '{USER}' has connected to '{DBNAME}' at {HOST}:{PORT}")
cur = conn.cursor()

# 1. Find top 10 most influencing Users in our social graph.
def get_top_influencing_users(limit=10):
    # cur.execute("""
    #     SELECT u.userId, u.screenName, COUNT(f.followedId) AS follower_count
    #     FROM "User" u
    #     LEFT JOIN "Follows" f ON u.userId = f.followedId
    #     GROUP BY u.userId, u.screenName
    #     ORDER BY follower_count DESC
    #     LIMIT 10;
    # """)

    cur.execute("""
        SELECT u.userId, u.screenName, u.followersCount
        FROM "User" u
        ORDER BY u.followersCount DESC
        LIMIT %s;
    """, (limit,))

    return cur.fetchall()

# 2. Get most trending Tags across all Users.
def get_most_trending_tags(limit=10):
    cur.execute("""
        SELECT ht.tagText, COUNT(*) AS tag_count
        FROM "Has_Tag" ht
        JOIN "Post" p ON ht.postId = p.postId
        GROUP BY ht.tagText
        ORDER BY tag_count DESC
        LIMIT %s;
    """, (limit,))
    return cur.fetchall()

# 3. Based on a User, suggest him more Users to follow (that he is not following rn).
#  a. Simple User-based recommendations (Collaborative Filtering)
def suggest_users_itemBased(screenName, limit=10):
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
        LIMIT %s;
    """,(screenName, limit))
    return cur.fetchall() 

#  b. Simple Item-based reccomentations (Content-Based)
def suggest_users_userBased(screenName, limit=10):
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

    # Limita i risultati a limit
    result_limited = result_sorted[:limit]

    return result_limited

# 4. Based on a User, suggest him Posts that he could be interested in.
#  a. Simple User-based recommendations (Collaborative Filtering)
def suggest_post_userBased(screenName, limit=10):
    cur.execute("""
        SELECT 
        p.postId AS post,
        other_u.screenName AS otherUser
        FROM "User" u
        JOIN "Follows" f ON u.userId = f.followerId
        JOIN "User" other_u ON f.followedId = other_u.userId
        JOIN "Post" p ON other_u.userId = p.userId
        WHERE u.screenName = %s
        LIMIT %s;
    """,(screenName, limit))
    return cur.fetchall()

#  b. Simple Item-based reccomentations (Content)
def suggest_post_itemBased(screenName, limit=10):
    cur.execute("""
        SELECT other_p.postId AS otherPost,
        t.text AS tag
        FROM "User" u
        JOIN "Post" p ON u.userId = p.userId
        JOIN "Has_Tag" ht1 ON p.postId = ht1.postId
        JOIN "Tag" t ON ht1.tagText = t.text
        JOIN "Has_Tag" ht2 ON t.text = ht2.tagText
        JOIN "Post" other_p ON ht2.postId = other_p.postId
        WHERE u.screenName = %s
        AND other_p.postId <> p.postId
        LIMIT %s;

    """,(screenName, limit))
    return cur.fetchall()

# ----------------------------------------------------------------------------------------------

# Misurare e stampare il tempo di esecuzione di ogni query
def measure_time(func, *args):
    n = 10
    result = timeit.timeit(lambda: func(*args), setup='pass', globals=globals(), number=n)

    # calculate the execution time
    return result / n


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
    screen_name = 'jdfollowhelp'
    limit = 10

    queries = [
        # (get_followers, "1393409100"),
        # (get_users_by_screenname, "_notmichelle"),
        # (get_posts_by_tag, "#nationaldogday"),
        # (count_friends_of_friends, 461669641),
        # (get_followers_of_followers, 461669641, 5),
        # (get_first_100_users,),
        # (get_top_influencing_users,),
        # (get_most_trending_tags,),
        # (get_trending_tags_among_influencers,),
        # (suggest_users_to_follow, 1393409100)
        (get_top_influencing_users, limit),
        (get_most_trending_tags, limit),
        (suggest_users_userBased, screen_name, limit),
        (suggest_users_itemBased, screen_name, limit),
        (suggest_post_userBased, screen_name, limit),
        (suggest_post_itemBased, screen_name, limit),
    ]
    
    execution_times = []
    
    for query in queries:
        func = query[0]
        args = query[1:]
        elapsed_time = measure_time(func, *args)
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

# ----------------------------------------------------------------------------------------------

try: 
    # print("Followers of user 1393409100:", get_followers("1393409100"))
    # print("Users with screenname '_notmichelle':", get_users_by_screenname("_notmichelle"))
    # print(get_posts_by_tag("#nationaldogday"))
    # print("Friends of friends of user 1393409100:", count_friends_of_friends(461669641))
    # print("First 100 User IDs:", get_first_100_users())
    # print("Most Trending Tags across Influencer:", get_trending_tags_among_influencers())
    # print("Suggest User:", suggest_users_to_follow(1393409100))
    # print("FOF: ", get_FOF('jdfollowhelp'))

    screen_name = 'jdfollowhelp'
    limit = 10

    print("----------------------------------------------------------------------------------------------")

    # 1. Find top 10 most influencing Users in our social graph.
    print("Top Influencing Users:")
    for r in get_top_influencing_users(limit):
        print(r)

    print("----------------------------------------------------------------------------------------------")

    # 2. Get most trending Tags across all Users.
    print("Most Trending Tags:")
    for r in get_most_trending_tags(limit):
        print(r)

    print("----------------------------------------------------------------------------------------------")

    # 3. Based on a User, suggest him more Users to follow (that he is not following rn).
    #  a. Simple User-based recommendations (Collaborative Filtering)ù
    print("Suggest User User-Based: ")
    suggested_users_userBased = suggest_users_userBased(screen_name, limit)
    for userId, distance, frequency in suggested_users_userBased:
        print(f"UserId: {userId}, Distance: {distance}, Frequency: {frequency}")
    #  b. Simple Item-based reccomentations (Content-Based)
    print("\nSuggest User Item-Based: ")
    for r in suggest_users_itemBased(screen_name, limit):
        print(r)

    print("----------------------------------------------------------------------------------------------")

    # 4. Based on a User, suggest him Posts that he could be interested in.
    #  a. Simple User-based recommendations (Collaborative Filtering)
    print("Suggest Post User-Based: ")
    for r in suggest_post_userBased(screen_name, limit):
        print(r)
    #  b. Simple Item-based reccomentations (Content)
    print("\nSuggest Post Item-Based: ")
    for r in suggest_post_itemBased(screen_name, limit):
        print(r)

    print("----------------------------------------------------------------------------------------------")
    
    show_execution_time = input("Do you want to see the execution time? (y/n): ")
    # Check the user's response
    if show_execution_time.lower() == "y":
        execution_times = calculate_time()
        print(execution_times)
        plot_execution_times(execution_times)
        # plot_followers_of_followers_times(461669641, 10)  
        # plot_trending_tags(get_most_trending_tags())

    else:
        print("Execution time not shown.")

except Exception as e:
    print("Error:", e)

# Chiudi la connessione al database
conn.commit()
cur.close()
conn.close()
