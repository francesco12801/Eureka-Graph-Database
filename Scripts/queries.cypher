userId: 3410763455
screenName: jdfollowhelp

// QUERY 1: Find friend of friends of friend of friends (4) of a specific User.
MATCH (u: User {screenName: "jdfollowhelp"})-[:FOLLOWS*4]->(n: User WHERE n.screenName IS NOT NULL)
RETURN u,n LIMIT 100

// QUERY 2: Find most influencing Users in our social graph according to proper metrics (https://neo4j.com/docs/graph-data-science/current/algorithms/centrality/) (in general, or maybe based on a topic)
// QUERY 3: Get Users interested in a specific topic
// QUERY 4: Get most trending topic across all Users
// QUERY 5: Get most trending topic across influencer Users
// QUERY 6: Based on a User, suggest him more people to follow
// QUERY 7: Based on a User, suggest him posts that he could be interested on


// QUERY 8: Find User searching its screen name
DROP INDEX range_index_screenName
PROFILE
MATCH (u: User {screenName: "_notmichelle"})
RETURN u

CREATE INDEX range_index_screenName IF NOT EXISTS FOR (u:User) ON (u.screenName)
PROFILE
MATCH (u: User {screenName: "_notmichelle"})
RETURN u


// find peoples that follows someone and are followed by someone
MATCH (u: User)
WITH u, COUNT {(u)-[:FOLLOWS]->()} AS out, COUNT {(u)<-[:FOLLOWS]-()} AS in
WHERE out > 0 AND in > 0
RETURN u, out, in