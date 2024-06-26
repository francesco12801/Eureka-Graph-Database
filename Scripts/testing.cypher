// Remove all
MATCH (n)
DETACH DELETE n;

// Create constraints
// CREATE CONSTRAINT UniqueProduct FOR (u:User) REQUIRE u.userId IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

// Create 'User' nodes based on row.id
MERGE (u:User {userId: toInteger(row.id)})
ON CREATE
    SET
        u.screenName=row.screenName,
        u.avatar=row.avatar,
        u.followersCount=toInteger(row.followersCount),
        u.followingCount=toInteger(row.friendsCount),
        u.lang=row.lang;

LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

MERGE (u:User {userId: toInteger(row.friends)})

// Create 'User' nodes based on row.friends
UNWIND split(row.friends, ,) AS friendId
//     MERGE (f:User {userId: toInteger(friendId)})
//     // ON CREATE
//     //     SET
//     //         u.screenName=null,
//     //         u.avatar=null,
//     //         u.followersCount=null,
//     //         u.followingCount=null,
//     //         u.lang=null
//     MERGE (u)-[r1:FOLLOWS]-(f);