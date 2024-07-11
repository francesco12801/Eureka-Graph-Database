from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import timeit
import matplotlib.pyplot as plt
import psycopg

# Load environment variables from .env file
load_dotenv()

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_DBNAME = os.getenv("NEO4J_DBNAME")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
POSTGRE_DBNAME=os.getenv('POSTGRE_DBNAME', 'dataManagement')
POSTGRE_USER=os.getenv('POSTGRE_USER', 'postgres')
POSTGRE_PASSWORD=os.getenv('POSTGRE_PASSWORD', '2345')
POSTGRE_HOST=os.getenv('POSTGRE_HOST', '127.0.0.1')
POSTGRE_PORT=os.getenv('POSTGRE_PORT', '5432')

def measure_time(func, *args):
    n = 1
    result = timeit.timeit(lambda: func(*args), setup='pass', globals=globals(), number=n)

    # calculate the execution time
    return result / n


    start_time = time.time()
    result = func(*args)
    elapsed_time = time.time() - start_time
    print(f"Query '{func.__name__}' executed in {elapsed_time:.4f} seconds")
    return elapsed_time, result

def plot_execution_times_subplots(neo4j_times, postgresql_times):
    # Extract query names and execution times
    queries = [query[0] for query in neo4j_times]
    neo4j_exec_times = [time[1] for time in neo4j_times]
    postgresql_exec_times = [time[1] for time in postgresql_times]

    # Number of subplots
    num_queries = len(queries)

    # Create the subplots
    fig, axes = plt.subplots(num_queries, 1, figsize=(10, 2 * num_queries), constrained_layout=True)

    # Plot each query in a separate subplot
    for i, query in enumerate(queries):
        ax = axes[i]
        ax.bar('Neo4j', neo4j_exec_times[i], width=0.4, label='Neo4j')
        ax.bar('PostgreSQL', postgresql_exec_times[i], width=0.4, label='PostgreSQL')

        ax.set_ylabel('Execution Time (s)')
        ax.set_title(query)
        ax.legend()

    # Set the main title for all subplots
    fig.suptitle('Query Execution Time Comparison: Neo4j vs PostgreSQL', fontsize=16)
    
    # Display the plot
    plt.show()

def plot_execution_times(neo4j_times, postgresql_times):
    # Extract query names and execution times
    queries = [query[0].replace("_", " ").capitalize() for query in neo4j_times]
    neo4j_exec_times = [time[1] for time in neo4j_times]
    postgresql_exec_times = [time[1] for time in postgresql_times]

    # Define the width of the bars
    bar_width = 0.35
    # Define the positions of the bars
    index = range(len(queries))
    
    # Create the bar plot
    fig, ax = plt.subplots()
    fig.set_figwidth(10)
    fig.set_figheight(6)

    # Plot the Neo4j execution times
    neo4j_bars = ax.bar(index, neo4j_exec_times, bar_width, label='Neo4j')

    # Plot the PostgreSQL execution times
    postgresql_bars = ax.bar([i + bar_width for i in index], postgresql_exec_times, bar_width, label='PostgreSQL')

    # Add labels, title, and legend
    ax.set_xlabel('Query')
    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('Query Execution Time Comparison: Neo4j vs PostgreSQL')
    ax.set_xticks([i + bar_width / 2 for i in index])
    ax.set_xticklabels(queries, rotation=45, ha='right')
    ax.legend()
    # ax.grid(True)

    # Display the plot
    plt.tight_layout()
    plt.savefig(f'img/{NEO4J_DBNAME}_queries.png')
    plt.show()

def plot_execution_times_individual(neo4j_times, postgresql_times):
    # Extract query names and execution times
    queries = [query[0] for query in neo4j_times]
    neo4j_exec_times = [time[1] for time in neo4j_times]
    postgresql_exec_times = [time[1] for time in postgresql_times]

    # Create individual plots for each query
    for i, query in enumerate(queries):
        plt.figure(figsize=(6, 4))
        plt.bar(['Neo4j', 'PostgreSQL'], [neo4j_exec_times[i], postgresql_exec_times[i]], width=0.4)
        plt.ylabel('Execution Time (seconds)')
        plt.title(f'Execution Time for {query}')
        plt.show()

def plot_execution_times_old(execution_times):
    labels, times = zip(*execution_times)
    plt.figure(figsize=(12, 6))
    plt.barh(labels, times, color='skyblue')
    plt.xlabel('Execution Time (seconds)')
    plt.ylabel('Query')
    plt.title('Execution Time of Different Queries')
    plt.grid(True)
    plt.show()

def plot_followers_of_followers_times(postgre, neo4j, screenName, max_k):
    k_values = list(range(1, max_k + 1))
    postgre_times1 = []
    postgre_times2 = []
    neo4j_times1 = []
    neo4j_times2 = []
    
    for k in k_values:
        elapsed_time1 = measure_time(postgre.get_followers_of_followers_of_specific_k, screenName, k)
        elapsed_time2 = measure_time(postgre.get_followers_of_followers_up_to_k, screenName, k)
        print(f"POSTGRE time of {k} level: {elapsed_time1} s")
        print(f"POSTGRE time up to {k} level: {elapsed_time2} s")
        
        postgre_times1.append(elapsed_time1)
        postgre_times2.append(elapsed_time2)

    for k in k_values:
        elapsed_time1 = measure_time(neo4j.get_followers_of_followers_of_specific_k, screenName, k, False)
        elapsed_time2 = measure_time(neo4j.get_followers_of_followers_up_to_k, screenName, k, False)
        print(f"NEO4J time of {k} level: {elapsed_time1} s")
        print(f"NEO4J time up to {k} level: {elapsed_time2} s")

        neo4j_times1.append(elapsed_time1)
        neo4j_times2.append(elapsed_time2)

    # print(postgre_times)
    # print(neo4j_times)
        
    # for k in k_values:
    #     fof = postgre.get_followers_of_followers(screenName, k)
    #     print(f"FOF (degree {k}) are: {len(fof)}")
    
    # for k in k_values:
    #     fof = neo4j.get_followers_of_followers(screenName, k)
    #     print(f"FOF (degree {k}) are: {len(fof)}")

    plt.figure(figsize=(10, 6))
    plt.plot(k_values, postgre_times1, marker='o', label='PostgreSQL')
    plt.plot(k_values, neo4j_times1, marker='o', label='Neo4j')
    plt.plot(k_values, postgre_times2, marker='o', label='PostgreSQL_UpTo')
    plt.plot(k_values, neo4j_times2, marker='o', label='Neo4j_UpTo')
    plt.xlabel('Degree of Friends (k)')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time Comparison for Friends of Friends Query')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'img/{NEO4J_DBNAME}_fof_{k}.png')
    plt.show()

class PostgreQueries:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg.connect(
        # dbname="dataManagement",
        # user="postgres",
        # password="2345",
        # host="127.0.0.1",
        # port="5432",
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
        )

        print(f"Done! '{user}' has connected to '{dbname}' at {host}:{port}")
        self.cur = self.conn.cursor()

    def close(self):
        # Chiudi la connessione al database
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    # 1. Find top 10 most influencing Users in our social graph.
    def get_top_influencing_users(self, limit=10):
        # self.cur.execute("""
        #     SELECT u.userId, u.screenName, COUNT(f.followedId) AS follower_count
        #     FROM "User" u
        #     LEFT JOIN "Follows" f ON u.userId = f.followedId
        #     GROUP BY u.userId, u.screenName
        #     ORDER BY follower_count DESC
        #     LIMIT 10;
        # """)

        self.cur.execute("""
            SELECT u.userId, u.screenName, u.followersCount
            FROM "User" u
            ORDER BY u.followersCount DESC
            LIMIT %s;
        """, (limit,))

        return self.cur.fetchall()

    # 2. Get most trending Tags across all Users.
    def get_trending_tags(self, limit=10):
        self.cur.execute("""
            SELECT ht.tagText, COUNT(*) AS tag_count
            FROM "Has_Tag" ht
            JOIN "Post" p ON ht.postId = p.postId
            GROUP BY ht.tagText
            ORDER BY tag_count DESC
            LIMIT %s;
        """, (limit,))
        return self.cur.fetchall()

    # 3. Based on a User, suggest him more Users to follow (that he is not following rn).
    #  a. Simple User-based recommendations (Collaborative Filtering)
    def suggest_users_content_based(self, screenName, limit=10):
        self.cur.execute("""      
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
        return self.cur.fetchall() 

    #  b. Simple Item-based reccomentations (Content-Based)
    def suggest_users_user_based(self, screenName, limit=10):
        # Amici diretti (distanza 1)
        self.cur.execute("""
            SELECT uf1.followedId AS userId
            FROM "User" u1
            JOIN "Follows" uf1 ON u1.userId = uf1.followerId
            WHERE u1.screenName = %s;
        """, (screenName,))
        direct_friends = self.cur.fetchall()
        direct_friend_ids = {friend[0] for friend in direct_friends}

        # Amici degli amici (distanza 2)
        self.cur.execute("""
            SELECT u2.userId AS userId, 2 AS distance, COUNT(*) AS paths
            FROM "User" u1
            JOIN "Follows" uf1 ON u1.userId = uf1.followerId
            JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
            JOIN "User" u2 ON uf2.followedId = u2.userId
            WHERE u1.screenName = %s AND uf2.followedId != u1.userId
            GROUP BY u2.userId;
        """, (screenName,))
        fof_distance_2 = [row for row in self.cur.fetchall() if row[0] not in direct_friend_ids]

        # Amici degli amici degli amici (distanza 3)
        self.cur.execute("""
            SELECT u3.userId AS userId, 3 AS distance, COUNT(*) AS paths
            FROM "User" u1
            JOIN "Follows" uf1 ON u1.userId = uf1.followerId
            JOIN "Follows" uf2 ON uf1.followedId = uf2.followerId
            JOIN "Follows" uf3 ON uf2.followedId = uf3.followerId
            JOIN "User" u3 ON uf3.followedId = u3.userId
            WHERE u1.screenName = %s AND uf3.followedId != u1.userId
            GROUP BY u3.userId;
        """, (screenName,))
        fof_distance_3 = [row for row in self.cur.fetchall() if row[0] not in direct_friend_ids]

        # Combina i risultati
        result = fof_distance_2 + fof_distance_3

        # Ordina per distanza ascendente e per numero di percorsi discendente
        result_sorted = sorted(result, key=lambda x: (x[1], -x[2]))

        # Limita i risultati a limit
        result_limited = result_sorted[:limit]

        return result_limited

    # 4. Based on a User, suggest him Posts that he could be interested in.
    #  a. Simple User-based recommendations (Collaborative Filtering)
    def suggest_posts_user_based(self, screenName, limit=10):
        self.cur.execute("""
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
        return self.cur.fetchall()

    #  b. Simple Item-based reccomentations (Content)
    def suggest_posts_content_based(self, screenName, limit=10):
        self.cur.execute("""
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
        return self.cur.fetchall()

    def get_followers_of_followers_of_specific_k(self, screenName, k):
        # Initialize sets for tracking visited nodes and friends at each distance
        visited = set()
        # current_level = set()
        all_friends = []

        query = f"""
            SELECT uf{k}.followedId
            FROM "User" u1
            JOIN "Follows" uf1 ON u1.userId = uf1.followerId
        """
        for i in range(2, k + 1):
            query += f"""
            JOIN "Follows" uf{i} ON uf{i-1}.followedId = uf{i}.followerId
            """
        query += f"""
            WHERE u1.screenName = %s
        """

        self.cur.execute(query, (screenName,))
        fof = self.cur.fetchall()
        for friend in fof:
            user_id = friend[0]
            if user_id not in visited:
                visited.add(user_id)
                all_friends.append((user_id, k))
        
        return all_friends

    def get_followers_of_followers_up_to_k(self, screenName, k):
        # Initialize sets for tracking visited nodes and friends at each distance
        visited = set()
        # current_level = set()
        all_friends = []

        for distance in range(1, k + 1):
            query = f"""
                SELECT uf{distance}.followedId
                FROM "User" u1
                JOIN "Follows" uf1 ON u1.userId = uf1.followerId
            """
            for i in range(2, distance + 1):
                query += f"""
                JOIN "Follows" uf{i} ON uf{i-1}.followedId = uf{i}.followerId
                """
            query += f"""
                WHERE u1.screenName = %s
            """

            self.cur.execute(query, (screenName,))
            fof = self.cur.fetchall()
            for friend in fof:
                user_id = friend[0]
                if user_id not in visited:
                    visited.add(user_id)
                    all_friends.append((user_id, distance))
        return all_friends

    def run_all_queries(self, screenName, limit=10, show_results=False):
        # 1. Find top 10 most influencing Users in our social graph.
        print("Top Influencing Users:")
        records = self.get_top_influencing_users(limit)
        print(f"Returned {len(records)} records.")
        if show_results:
            for r in records:
                print(r)

        print("----------------------------------------------------------------------------------------------")

        # 2. Get most trending Tags across all Users.
        print("Most Trending Tags:")
        records = self.get_trending_tags(limit)
        print(f"Returned {len(records)} records.")
        if show_results:
            for r in records:
                print(r)

        print("----------------------------------------------------------------------------------------------")

        # 3. Based on a User, suggest him more Users to follow (that he is not following rn).
        #  a. Simple User-based recommendations (Collaborative Filtering)ù
        print("Suggest User User-Based: ")
        suggested_users_userBased = self.suggest_users_user_based(screenName, limit)
        print(f"Returned {len(records)} records.")

        if show_results:
            for userId, distance, frequency in suggested_users_userBased:
                print(f"UserId: {userId}, Distance: {distance}, Frequency: {frequency}")
        #  b. Simple Item-based reccomentations (Content-Based)
        print("\nSuggest User Item-Based: ")
        records = self.suggest_users_content_based(screenName, limit)
        print(f"Returned {len(records)} records.")
        if show_results:
            for r in records:
                print(r)

        print("----------------------------------------------------------------------------------------------")

        # 4. Based on a User, suggest him Posts that he could be interested in.
        #  a. Simple User-based recommendations (Collaborative Filtering)
        print("Suggest Post User-Based: ")
        records = self.suggest_posts_user_based(screenName, limit)
        print(f"Returned {len(records)} records.")
        if show_results:
            for r in records:
                print(r)
        #  b. Simple Item-based reccomentations (Content)
        print("\nSuggest Post Item-Based: ")

        print("Suggest Post User-Based: ")
        records = self.suggest_posts_content_based(screenName, limit)
        print(f"Returned {len(records)} records.")
        if show_results:
            for r in records:
                print(r)

        print("----------------------------------------------------------------------------------------------")
        
        execution_times = self.calculate_time(screenName, limit)
        print(execution_times)
        return execution_times

    def calculate_time(self, screenName, limit=10):
        queries = [
            (self.get_top_influencing_users, limit),
            (self.get_trending_tags, limit),
            (self.suggest_users_user_based, screenName, limit),
            (self.suggest_users_content_based, screenName, limit),
            (self.suggest_posts_user_based, screenName, limit),
            (self.suggest_posts_content_based, screenName, limit),
        ]
        
        execution_times = []
        
        for query in queries:
            func = query[0]
            args = query[1:]
            elapsed_time = measure_time(func, *args)
            execution_times.append((func.__name__, elapsed_time))
        
        return execution_times

class Neo4jQueries:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()
        print(f"Connection established with {uri} as '{user}'.")

    def close(self):
        self.driver.close()

    def run_query(self, query, show_results=False):
            records, summary, _ = self.driver.execute_query(query, database_=NEO4J_DBNAME)
            
            if show_results:
                for record in records:
                    print(record.data())
            
            print(f"The query returned {len(records)} records records in {summary.result_available_after} ms and consumed them after {+summary.result_consumed_after} ms.")
            return records

    def get_top_influencing_users(self, limit, show_results):
        query = f"""
        PROFILE
        MATCH (u:User)
        RETURN
            u.userId AS userId,
            u.screenName AS screenName,
            u.followersCount AS followersCount
        ORDER BY followersCount DESC
        LIMIT {limit}
        """
        return self.run_query(query, show_results)

    def get_trending_tags(self, limit, show_results):
        query = f"""
        PROFILE
        MATCH ()-[r:HAS_TAG]->(t:Tag)
        RETURN
            t.text AS tag,
            COUNT(r) as tag_count
        ORDER BY tag_count DESC
        LIMIT {limit}
        """
        return self.run_query(query, show_results)

    def suggest_users_user_based(self, screenName, limit, show_results):
        query = f"""
        PROFILE
        MATCH p=(u:User{{screenName: "{screenName}"}})-[:FOLLOWS*2..3]->(other_u)
        WHERE NOT (u)-[:FOLLOWS]->(other_u) AND u <> other_u
        RETURN
            other_u.userId AS userId,
            other_u.screenName AS screenName,
            count(other_u) AS frequency,
            length(p) AS hops
        ORDER BY hops ASC, frequency DESC
        LIMIT {limit}
        """
        return self.run_query(query, show_results)

    def suggest_users_content_based(self, screenName, limit, show_results):
        query = f"""
        PROFILE
        MATCH (u:User{{screenName: "{screenName}"}})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)<-[:TWEETED]-(other_u)
        WHERE other_p <> p AND other_u <> u AND NOT (u)-[:FOLLOWS]->(other_u)
        RETURN
            other_u.userId AS userId,
            other_u.screenName AS screenName
        LIMIT {limit}
        """
        return self.run_query(query, show_results)

    def suggest_posts_user_based(self, screenName, limit, show_results):
        query = f"""
        PROFILE
        MATCH (u:User{{screenName: "{screenName}"}})-[:FOLLOWS]->(other_u)-[:TWEETED]->(other_p)-[:HAS_TAG]->(other_t)
        RETURN
            other_p.postId AS post,
            other_t.text AS tag
        LIMIT {limit}
        """
        return self.run_query(query, show_results)

    def suggest_posts_content_based(self, screenName, limit, show_results):
        query = f"""
        PROFILE
        MATCH (u:User{{screenName: "{screenName}"}})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)
        WHERE other_p <> p
        RETURN
            other_p.postId AS post,
            t.text AS tag
        LIMIT {limit}
        """
        return self.run_query(query, show_results)
    
    def get_followers_of_followers_of_specific_k(self, screenName, k, show_results):
        query = f"""
        PROFILE
        MATCH (u:User{{screenName: "{screenName}"}})-[:FOLLOWS*{k}]->(other_u)
        WHERE other_u <> u
        RETURN DISTINCT
            other_u.userId AS userId
        """
        return self.run_query(query, show_results)

    def get_followers_of_followers_up_to_k(self, screenName, k, show_results):
        query = f"""
        PROFILE
        MATCH (u:User{{screenName: "{screenName}"}})-[:FOLLOWS*1..{k}]->(other_u)
        WHERE other_u <> u
        RETURN DISTINCT
            other_u.userId AS userId
        """
        return self.run_query(query, show_results)

    def run_all_queries(self, screenName, limit, show_results=False):
        print(f"1. Top {limit} Influencing Users")
        self.get_top_influencing_users(limit, show_results)

        print("----------------------------------------------------------------------------------------------")
        print(f"2. Top {limit} Trending Tags:")
        self.get_trending_tags(limit, show_results)

        print("----------------------------------------------------------------------------------------------")
        print(f"3a. Top {limit} User-based User Recommendations for {screenName}:")
        self.suggest_users_user_based(screenName, limit, show_results)

        print(f"\n3b. Top {limit} Content-based User Recommendations for {screenName}:")
        self.suggest_users_content_based(screenName, limit, show_results)

        print("----------------------------------------------------------------------------------------------")
        print(f"4a. Top {limit} User-based Post Recommendations for {screenName}:")
        self.suggest_posts_user_based(screenName, limit, show_results)

        print(f"\n4b. Top {limit} Content-based Post Recommendations for {screenName}:")
        self.suggest_posts_content_based(screenName, limit, show_results)

        print("----------------------------------------------------------------------------------------------")
        
        # show_execution_time = input("Do you want to see the execution time? (y/n): ")
        
        execution_times = self.calculate_time(screenName, limit)
        print(execution_times)
        return execution_times
        
        # Check the user's response
        if show_execution_time.lower() == "y":
            plot_execution_times(execution_times)
            # plot_followers_of_followers_times(461669641, 10)  
            # plot_trending_tags(get_trending_tags())

        else:
            print("Execution time not shown.")

    def calculate_time(self, screenName, limit):
        queries = [
            (self.get_top_influencing_users, limit, False),
            (self.get_trending_tags, limit, False),
            (self.suggest_users_user_based, screenName, limit, False),
            (self.suggest_users_content_based, screenName, limit, False),
            (self.suggest_posts_user_based, screenName, limit, False),
            (self.suggest_posts_content_based, screenName, limit, False),
        ]
        
        execution_times = []
        
        for query in queries:
            func = query[0]
            args = query[1:]
            print(func.__name__)
            elapsed_time = measure_time(func, *args)
            execution_times.append((func.__name__, elapsed_time))
        
        return execution_times

if __name__ == "__main__":
    neo4j_queries = Neo4jQueries(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    postgre_queries = PostgreQueries(POSTGRE_DBNAME, POSTGRE_USER, POSTGRE_PASSWORD, POSTGRE_HOST, POSTGRE_PORT)

    screenName = "jdfollowhelp"
    limit = 10
    k_max = 5

    print("=======================================================================")
    print("NEO4J\n\n")
    neo4j_times = neo4j_queries.run_all_queries(screenName, limit, False)
    print("=======================================================================")
    print("POSTGRESQL\n\n")
    postgre_times = postgre_queries.run_all_queries(screenName, limit, False)

    plot_execution_times(neo4j_times, postgre_times)
    plot_followers_of_followers_times(postgre_queries, neo4j_queries, screenName, k_max)

    # plot_execution_times_individual(neo4j_times, postgre_times)
    # plot_followers_of_followers_times(461669641, 10)  
    # plot_trending_tags(get_trending_tags())

    neo4j_queries.close()
    postgre_queries.close()