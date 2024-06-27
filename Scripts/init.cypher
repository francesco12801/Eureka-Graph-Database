// Remove all nodes and relationship
MATCH (n)
DETACH DELETE n;

// Create constraints on id
CREATE CONSTRAINT UniqueUser IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE;
CREATE CONSTRAINT UniquePost IF NOT EXISTS FOR (p:Post) REQUIRE p.postId IS UNIQUE;
CREATE CONSTRAINT UniqueTag IF NOT EXISTS FOR (t:Tag) REQUIRE t.text IS UNIQUE;

// Load dataset with headers
LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

// Create User nodes
MERGE (u:User {userId: toInteger(row.id)})
ON CREATE
    SET
        u.screenName=row.screenName,
        u.avatar=row.avatar,
        u.followersCount=toInteger(row.followersCount),
        u.followingCount=toInteger(row.friendsCount),
        u.lang=row.lang

// Create Post nodes
MERGE (p:Post {postId: toInteger(row.tweetId)})
ON CREATE
    SET
        p.timestamp=datetime({epochmillis:toInteger(row.lastSeen)/1000})

// Create TWEETED relationship
MERGE (u)-[:TWEETED]->(p)

WITH row
MATCH (p:Post {postId: toInteger(row.tweetId)})
UNWIND split(row.tags, "|") as tagText
    // Create Tag nodes
    MERGE (t:Tag {text: tagText})
    // Create HAS_TAG relationship
    MERGE (p)-[:HAS_TAG]->(t)


