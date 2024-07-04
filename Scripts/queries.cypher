// Set parameters
:param target_screenName => "jdfollowhelp";
:param target_tag => "dog";
:param target_screenName => "jdfollowhelp";
:param k => 4;

// 1. Get Users based on their screenName.

PROFILE
MATCH (u:User {screenName: $target_screenName})
RETURN u
;

// ----------------------------------------------------------------------------------------------

// 2. Get Posts containing a specific Tag.

PROFILE
MATCH (p:Post)-[:HAS_TAG]->(t:Tag)
WHERE t.text CONTAINS $target_tag
RETURN p
;

// ----------------------------------------------------------------------------------------------

// 3. Get followers of followers of ... (up to a certain *k*-degree) of a specific User.

PROFILE
MATCH (u: User {screenName: $target_screenName})-[:FOLLOWS*$k]->(n: User WHERE n.screenName IS NOT NULL)
RETURN n
;

// ----------------------------------------------------------------------------------------------

// 4. Find top 10 most influencing Users in our social graph based on their follower number.
PROFILE
MATCH (u:User)
RETURN u ORDER BY u.followersCount DESC
LIMIT 10
;

// ----------------------------------------------------------------------------------------------

// 5. Find most influencing Users in our social graph according to [centrality algorithms](https://neo4j.com/docs/graph-data-science/current/algorithms/centrality/), in general and based on a specific Tag.
//     - Could be interesting to compare this with query #2 to identify potential differences

// ----------------------------------------------------------------------------------------------

// 6. Get Users interested in a specific Tag.

// ----------------------------------------------------------------------------------------------

// 7. Get most trending Tags across all Users.

// ----------------------------------------------------------------------------------------------

// 8. Get most trending Tags across most influencing Users.

// ----------------------------------------------------------------------------------------------

// 9. Based on a Tag, get similar Tags.
//     - May need to clusterize Tags into more general topics or subjects - Need to understand if is ML involved

// ----------------------------------------------------------------------------------------------

// 10. Based on a User, suggest him more Users to follow.
//     - Still need to understand how this could work, query #3 can help with that
//     - **Idea**: clusterize Users into similar ones?

// ----------------------------------------------------------------------------------------------

// 11. Based on a User, suggest him Post that he could be interested in.
//     - This could help populate its feed (called Timeline in X, For You in TikTok, etc.)

// ----------------------------------------------------------------------------------------------

// BONUS. Find Users that follows someone and are followed by someone
MATCH (u: User)
WITH u, COUNT {(u)-[:FOLLOWS]->()} AS out, COUNT {(u)<-[:FOLLOWS]-()} AS in
WHERE out > 0 AND in > 0
RETURN u, out, in