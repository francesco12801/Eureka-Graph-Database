MATCH (n)
DETACH DELETE n;

CREATE CONSTRAINT UniqueUser IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE;
CREATE CONSTRAINT UniquePost IF NOT EXISTS FOR (p:Post) REQUIRE p.postId IS UNIQUE;
CREATE CONSTRAINT UniqueTag IF NOT EXISTS FOR (t:Tag) REQUIRE t.text IS UNIQUE;

// Create User nodes and Post nodes
LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

MERGE (u:User {userId: toInteger(row.id)})
ON CREATE
    SET
        u.screenName=row.screenName,
        u.avatar=row.avatar,
        u.followersCount=toInteger(row.followersCount),
        u.followingCount=toInteger(row.friendsCount),
        u.lang=row.lang

MERGE (p:Post {postId: toInteger(row.tweetId)})
ON CREATE
    SET
        p.timestamp=datetime({epochmillis:toInteger(row.lastSeen)/1000})

MERGE (u)-[:TWEETED]->(p)

WITH row
// Create Tag nodes and connect them to Post nodes
MATCH (p:Post {postId: toInteger(row.tweetId)})
UNWIND split(row.tags, "|") as tagText
    MERGE (t:Tag {text: tagText})
    MERGE (p)-[:HAS_TAG]->(t)


