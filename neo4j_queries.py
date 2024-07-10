from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Neo4j connection details
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

class Neo4jQueries:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def run_query(self, query):
            records, summary, keys = self.driver.execute_query(query)
            # avail = result.summary().result_available_after
            # cons = result.summary().result_consumed_after
            # total_time = avail + cons
            print("The query `{query}` returned {records_count} records in {time} ms.".format(
                query=summary.query, records_count=len(records), time=summary.result_available_after))

            return records

    def get_top_influencing_users(self):
        query = """
        MATCH (u:User)
        RETURN u ORDER BY u.followersCount LIMIT 10
        """
        return self.run_query(query)

    def get_trending_tags(self):
        query = """
        MATCH ()-[r:HAS_TAG]->(t:Tag)
        RETURN t, COUNT(r) as tag_count ORDER BY tag_count DESC
        LIMIT 10
        """
        return self.run_query(query)

    def suggest_users_to_follow_collaborative(self, username):
        query = f"""
        MATCH p=(u:User{{screenName: "{username}"}})-[:FOLLOWS*2..3]->(other_u)
        WHERE NOT (u)-[:FOLLOWS]->(other_u) AND u <> other_u
        RETURN u, other_u, count(other_u) AS frequency, length(p) AS hops ORDER BY hops ASC, frequency DESC LIMIT 10
        """
        return self.run_query(query)

    def suggest_users_to_follow_content(self, username):
        query = f"""
        MATCH (u:User{{screenName: "{username}"}})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)<-[:TWEETED]-(other_u)
        WHERE other_p <> p AND other_u <> u AND NOT (u)-[:FOLLOWS]->(other_u)
        RETURN u, p, other_p, t, other_u LIMIT 10
        """
        return self.run_query(query)

    def suggest_posts_user_based(self, username):
        query = f"""
        MATCH (u:User{{screenName: "{username}"}})-[:FOLLOWS]->(other_u)-[:TWEETED]->(p)
        RETURN u, p, other_u LIMIT 10
        """
        return self.run_query(query)

    def suggest_posts_content_based(self, username):
        query = f"""
        MATCH (u:User{{screenName: "{username}"}})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)
        WHERE other_p <> p
        RETURN u, p, other_p, t LIMIT 10
        """
        return self.run_query(query)
    
    def run_all_queries(self):
        print("Top 10 Influencing Users:")
        for record in self.get_top_influencing_users():
            print("-")

        print("\nTrending Tags:")
        for record in self.get_trending_tags():
            print("-")

        username = "jdfollowhelp"
        print(f"\nUser-based User Recommendations for {username}:")
        for record in self.suggest_users_to_follow_collaborative(username):
            print("-")

        print(f"\nContent-based User Recommendations for {username}:")
        for record in self.suggest_users_to_follow_content(username):
            print("-")

        print(f"\nUser-based Post Recommendations for {username}:")
        for record in neo4j_queries.suggest_posts_user_based(username):
            print("-")

        print(f"\nContent-based Post Recommendations for {username}:")
        for record in neo4j_queries.suggest_posts_content_based(username):
            print("-")


if __name__ == "__main__":
    neo4j_queries = Neo4jQueries(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

    neo4j_queries.run_all_queries()

    neo4j_queries.close()
