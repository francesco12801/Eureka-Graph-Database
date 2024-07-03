Let's documentate what we're doing and what we have to do.

1. Import csv file into NEO4j transforming a relational database into a graph database. To do so we had to:
    1. Pre-process the csv file (using a python script) in order to match the syntax accepted by neo4j, so using commas as delimiters for columns, and in case of multivalue columns (like tags or friends) the entry is written like this "tag1|tag2|tag3"
    2. Run cypher query for importing nodes and relationship. We decided to have as nodes:
        - User (userId, screenName, avatar, ..)
        - Tag (text)
        - Post (postId, timestamp/date)
    and as relationship:
        - FOLLOWS (from User to User)
        - TWEETED (from User to Post)
        - HAS_TAG/ABOUT (from Post to Tag)
    with constraints:
        - UniqueUser on userId
        - UniqueTag on text
        - UniquePost on postId
    reminding that a constraint create on index on such propriety, in order to speed-up queries that tries to match on that attributes.
    (Later, depending on the queries that we would like to execute is it possible to create index on other attributes)

2. Think about common queries that could benefits from the use of graph database (and maybe discuss time computation between this database and the relational one)
    - QUERY 1: Find friend of friends of .. of a specific User.
    - QUERY 2: Find most influencing Users in our social graph according to proper metrics (https:  -neo4j.com/docs/graph-data-science/current/algorithms/centrality/) (in general, or maybe based on a Tags)
    - QUERY 3: Get Users interested in a specific Tags
    - QUERY 4: Get most trending Tags across all Users
    - QUERY 5: Get most trending Tags across influencer Users
    - QUERY 6: Based on a User, suggest him more people to follow
    - QUERY 7: Based on a User, suggest him posts that he could be interested in
    - QUERY 8: Clusterize Tags into more general Tag - Is necessary ML ?
    - QUERY 9: Find most influencing Users in our social graph based on their follower number (compare with QUERY 2)
We want to run these queries in both relational and graph database comparing the computational time and looking at which model should be used in realation to row entries and presence of indexes.

3. Simplified visualization of graph database thanks to specific tools such as NEO4J BLOOM; allowing non tech people to quickly go through the data in an interactive way. 


// Diagramma ER 
