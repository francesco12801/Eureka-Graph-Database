// 1. Find top 10 most influencing Users in our social graph.
// - based on their follower number.
MATCH (u: User)
RETURN u, u.followersCount ORDER BY u.followersCount DESC LIMIT 10;

// ----------------------------------------------------------------------------------------------

// 2. Get most trending Tags across all Users.
MATCH ()-[r:HAS_TAG]->(t:Tag)
RETURN t, COUNT(r) as tag_count ORDER BY tag_count DESC
LIMIT 10

// ----------------------------------------------------------------------------------------------

// 3. Based on a User, suggest him more Users to follow (that he is not following rn).
//  a. Simple User-based recommendations (Collaborative Filtering)
MATCH p=(u:User{screenName: "jdfollowhelp"})-[:FOLLOWS*2..3]->(other_u)
WHERE NOT (u)-[:FOLLOWS]->(other_u) AND u <> other_u
RETURN u, other_u, count(other_u) AS frequency, length(p) AS hops ORDER BY hops ASC, frequency DESC LIMIT 10;

//  b. Simple Item-based reccomentations (Content-Based)
MATCH (u:User{screenName: "jdfollowhelp"})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)<-[:TWEETED]-(other_u)
WHERE other_p <> p AND other_u <> u AND NOT (u)-[:FOLLOWS]->(other_u)
RETURN u, p, other_p, t, other_u LIMIT 10;

// ----------------------------------------------------------------------------------------------

// 4. Based on a User, suggest him Posts that he could be interested in.
//  - This could help populate its feed (called Timeline in X, For You in TikTok, etc.)
//  a. Simple User-based recommendations (Collaborative Filtering)
MATCH (u:User{screenName: "jdfollowhelp"})-[:FOLLOWS]->(other_u)-[:TWEETED]->(p)
RETURN u, p, other_u LIMIT 10;

//  b. Simple Item-based reccomentations (Content)
MATCH (u:User{screenName: "jdfollowhelp"})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)
WHERE other_p <> p
RETURN u, p, other_p, t LIMIT 10;

// --------------------------------------------------------------------------------------------
// CHECK FOR INCONSISTENCY BETWEEN # OF OUTGOING :FOLLOW EDGES AND FOLLOWINGCOUNT
MATCH (u: User)-[r:FOLLOWS]->(p: User)
WITH u, COUNT(r) AS out
WHERE u.followingCount <> out
RETURN u.screenName, u.followingCount, out
ORDER BY abs(u.followingCount-out) DESC LIMIT 5;

MATCH (u: User)<-[r:FOLLOWS]-(p: User)
WITH u, COUNT(r) AS in
WHERE u.followersCount <> in
RETURN u.screenName, u.followersCount, in
ORDER BY abs(u.followersCount-in) DESC LIMIT 5;