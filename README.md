# graph_db
Graph Database for social network analysis through NEO4J knowledge graph. 

Let's start from the scratch. 
 1. I need to understand how can i store all people: basically, i cannot insert in a graph only the value of first user in the row, because all the "friends" must be inserted as "user" ONLY IF THEY ARE NOT INSERTED YET. This is crucial to build a convenient stucture.

   Neo4j lets you import csv identifying nodes and then relation between them.
   In our case we have:
   
   ``` c

   
   // TODO: We may have to create users scanning friends of every user in every row
   // make sure that we don't create duplicate user
   // maybe constraint on User.userId ???
   // Notice that if any of these friend doesn't appear in its own row we have only its id and no other info
   // -> solution: put every other field to NULL
   UNWIND row.friends as friendId
      MERGE (f:User {
         userId: toInteger(friendId),
         screenName: null,
         avatar: null,
         followersCount: null*,
         followingCount: null*,
         lang: null
      })

      // make sure to not create duplicates of the edge
      MERGE (u)-[r1:FOLLOWS]-(f);

   // Create `Post` nodes
   MERGE (p:Post {
      postId: toInteger(row.tweetId),
      timestamp: datetime({epochmillis:row.lastSeen})
   } );

   // Create `Tag` nodes - need to understand how to create a tag from a list and `ABOUT` relationships from Post to Tag
   UNWIND row.tags as tag
      MERGE (t:Tag {
         text: tag,
      } );
      MERGE (p)-[r2:ABOUT]->(t);

   // Create `TWEET` relationships from User to Post
   // MATCH (u:User {userId: row.id})
   // MATCH (p:Post {postId: row.tweetId})
   MERGE (u)-[r3:TWEET]->(p);
   ```

 2. Then i can move on, i need to do some analysis but in which way? Let's breakdown the problem: 
    - Why we choose as data structure a knowledge graph? Is it better? in which terms? 
    - From the computational point of view is convenient? Let's do a comparison between a relation database? 
    - Why knowledge and not graph? we suppose something that could be inferred by our knowledge arch through NPL. 
    - With knowledge is possible to detect cluster. (Since we have tags is possible to infer on data to get al the people interested in some topic). 
    - Anomaly detection on insual transaction data. 
    - Adding metadata to get the context ??? in relational it's impossible. 



