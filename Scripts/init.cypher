// Choose how many rows to import from csv file
:param max_rows => 1000;
:param batch_size => 200;

// ----------------------------------------------------------------------------------------------

// Remove all nodes and relationship
:auto MATCH (n)
CALL {
    WITH n
    DETACH DELETE n
} IN TRANSACTIONS OF $batch_size ROWS
;

// ----------------------------------------------------------------------------------------------

// Create constraints on id
CREATE CONSTRAINT UniqueUser IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE;
CREATE CONSTRAINT UniquePost IF NOT EXISTS FOR (p:Post) REQUIRE p.postId IS UNIQUE;
CREATE CONSTRAINT UniqueTag IF NOT EXISTS FOR (t:Tag) REQUIRE t.text IS UNIQUE;

// Create text indexes on screenName and text
CREATE TEXT INDEX text_index_User_screenName IF NOT EXISTS FOR (u:User) ON (u.screenName);
CREATE TEXT INDEX text_index_Tag_text IF NOT EXISTS FOR (t:Tag) ON (t.text);
// CREATE INDEX range_index_User_followersCount IF NOT EXISTS FOR (u:User) ON (u.followersCount);
// CREATE INDEX range_index_User_followingCount IF NOT EXISTS FOR (u:User) ON (u.followingCount);

// ----------------------------------------------------------------------------------------------

LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row
WITH row LIMIT $max_rows

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
;

// ----------------------------------------------------------------------------------------------

LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row
WITH row LIMIT $max_rows
MATCH (p:Post {postId: toInteger(row.tweetId)})
UNWIND split(row.tags, "|") as tagText
    // Create Tag nodes
    MERGE (t:Tag {text: tagText})
    // Create HAS_TAG relationship
    MERGE (p)-[:HAS_TAG]->(t)
;

// ----------------------------------------------------------------------------------------------

:auto LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row
WITH row LIMIT $max_rows

// Execute in batches of $batch_size rows
CALL {
    WITH row
    MATCH (u:User {userId: toInteger(row.id)})
    // MATCH (old_u: User)
    // RETURN count(old_u) AS old_users
    UNWIND split(row.friends, "|") as friendId
        // For every friend create a User node if it doesn't exist yet
        MERGE (f:User {userId: toInteger(friendId)})
        ON CREATE
            SET
                f.followersCount=0,
                f.followingCount=0

        // Create FOLLOWS relationship
        MERGE (u)-[:FOLLOWS]->(f)
        ON CREATE
            SET
                f.followersCount=f.followersCount+1

} IN TRANSACTIONS OF $batch_size ROWS
;