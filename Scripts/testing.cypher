// Remove all
MATCH (n)
DETACH DELETE n;

// Create constraints
CREATE CONSTRAINT UniqueProduct FOR (u:User) REQUIRE u.userId IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

// Create 'User' nodes based on row.id
MERGE (u:User {userId: toInteger(row.id)})
    SET u.screenName=row.screenName, u.avatar=row.avatar, u.followersCount=toInteger(row.followersCount), u.followingCount=toInteger(row.friendsCount), u.lang=row.lang