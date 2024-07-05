// Set parameters
:param target_screenName => "jdfollowhelp";
:param target_tag => "dog";
:param k => 4;

// Remove all projeted graphs
CALL gds.graph.list()
YIELD graphName 
WITH graphName
UNWIND graphName AS gn
    CALL gds.graph.drop(gn)
    YIELD gn, nodeCount, relationshipCount
    RETURN gn, nodeCount, relationshipCount

// ----------------------------------------------------------------------------------------------
// ----------------------------------------------------------------------------------------------


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

// 10. Based on a User, suggest him more Users to follow (that he is not following rn).
//     - Still need to understand how this could work, query #3 can help with that
//     - **Idea**: clusterize Users into similar ones?


// BEST OF THE BEST https://towardsdatascience.com/exploring-practical-recommendation-engines-in-neo4j-ff09fe767782
// https://neo4j.com/docs/getting-started/appendix/tutorials/guide-build-a-recommendation-engine/
// Use Similarity algorithms from gds (https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/)

// Simple User-based recommendations (Collaborative)
PROFILE
MATCH p=(u:User{screenName: "jdfollowhelp"})-[:FOLLOWS*2..5]->(other_u)
WHERE NOT (u)-[:FOLLOWS]->(other_u)
RETURN u, other_u, count(other_u) AS frequency, length(p) AS hops ORDER BY hops ASC, frequency DESC LIMIT 20;

// Simple Item-based recommendations (Content)
PROFILE
MATCH (u:User{screenName: "jdfollowhelp"})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)<-[:TWEETED]-(other_u)
WHERE other_p <> p AND other_u <> u AND NOT (u)-[:FOLLOWS]->(other_u)
RETURN u, p, other_p, t, other_u, LIMIT 10;

// Compute projection (needed for all gds algorithmz)
CALL gds.graph.project(
    'myGraph',      // graph name
    ['User', 'Post'],    // node projection
    ['FOLLOWS', 'TWEETED'] // relationship projection
)
YIELD graphName, nodeCount, relationshipCount;

// Estimate memory requirements
CALL gds.nodeSimilarity.filtered.stream.estimate(
    'myGraph',
    {}
)
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory;

// Run algorithm

// VERSION 1 (should be faster)
// Match first the one you want to compute the similarity on so I can use it later to filter the algorithm
MATCH (u:User {screenName: "jdfollowhelp"})
WITH u
CALL gds.nodeSimilarity.filtered.stream(
    'myGraph',
    {
        topK: 50,
        sourceNodeFilter: u
    }
)
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS n1, gds.util.asNode(node2) AS n2, similarity
WHERE NOT (n1)-[:FOLLOWS]->(n2)
RETURN n1, n2, similarity, exists((n1)-[:FOLLOWS]->(n2)) AS already_following
ORDER BY similarity DESC

// VERSION 2 (should be slower)
// CALL gds.nodeSimilarity.stream(
//     'myGraph',
//     {
//         topK: 50
//     }
// )
// YIELD node1, node2, similarity
// WITH gds.util.asNode(node1) AS n1, gds.util.asNode(node2) AS n2, similarity
// WHERE n1.screenName = "jdfollowhelp" AND NOT (n1)-[:FOLLOWS]->(n2)
// RETURN n1, n2, similarity, exists((n1)-[:FOLLOWS]->(n2)) AS already_following
// ORDER BY similarity DESC

// Remove created projection
CALL gds.graph.drop(
    'myGraph'
)
YIELD graphName, nodeCount, relationshipCount;

// ----------------------------------------------------------------------------------------------

// 11. Based on a User, suggest him Post that he could be interested in.
//     - This could help populate its feed (called Timeline in X, For You in TikTok, etc.)
// Use Similarity algorithms from gds (https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/)

// https://utsavdesai26.medium.com/recommendation-systems-explained-understanding-the-basic-to-advance-43a5fce77c47
// https://stackoverflow.com/questions/16372191/whats-difference-between-item-based-and-content-based-collaborative-filtering
// https://graphaware.com/hume/2021/08/18/whisky-recommendation-graph.html
// https://medium.com/kellton-europe/building-a-recommendation-system-using-neo4j-3baaf349e7

// Simple User-based recommendations (Collaborative)
MATCH (u:User{screenName: "jdfollowhelp"})-[:FOLLOWS]->(other_u)-[:TWEETED]->(p)
RETURN u, p, other_u;

// Simple Item-based recommendations (Content)
MATCH (u:User{screenName: "jdfollowhelp"})-[:TWEETED]->(p)-[:HAS_TAG]->(t)<-[:HAS_TAG]-(other_p)
WHERE other_p <> p
RETURN u, p, other_p, t



// BEST ONE
// https://medium.com/larus-team/how-to-create-recommendation-engine-in-neo4j-7963e635c730

// DIFFERENT WAYS
// Item-based recommendations (or Item-Item Collaborative Filtering/Content-Based Filtering)
//  - get Post that the User has interacted with (in our case just :TWEETED)
//  - compute similar Posts (with filtered nodeSimilariy on a projection)
//  - recommend them to User
// User-based recommendations (or User-User Collaborative Filtering)
//  - compute similar Users (with filtered nodeSimilarity on a projection, cosine or JAQUARD)
//  - get Post that those Users has interacted with (in our case just :TWEETED)
//  - recommend them to User

// youtube algorithm example:
// 1. User engagement
// 2. Similarity
// 3. Popularity
// 4. Freshness
// 5. Diversity



// Compute projection (needed for all gds algorithmz)
CALL gds.graph.project(
    'myGraph',      // graph name
    ['User', 'Post', 'Tag'],    // node projection
    ['FOLLOWS', 'TWEETED', 'HAS_TAG'] // relationship projection
)
YIELD graphName, nodeCount, relationshipCount;

// Run algorithm

// VERSION 1 (should be faster)
// Match first the one you want to compute the similarity on so I can use it later to filter the algorithm
MATCH (u:User {screenName: "jdfollowhelp"})
WITH u
CALL gds.nodeSimilarity.filtered.stream(
    'myGraph',
    {
        topK: 50,
        sourceNodeFilter: u
    }
)
YIELD node1, node2, similarity
WITH gds.util.asNode(node1) AS n1, gds.util.asNode(node2) AS n2, similarity
RETURN n1, n2, similarity, exists((n1)-[:FOLLOWS]->(n2)) AS already_following
ORDER BY similarity DESC

// VERSION 2 (should be slower)
// CALL gds.nodeSimilarity.stream(
//     'myGraph',
//     {
//         topK: 50
//     }
// )
// YIELD node1, node2, similarity
// WITH gds.util.asNode(node1) AS n1, gds.util.asNode(node2) AS n2, similarity
// WHERE n1.screenName = "jdfollowhelp" AND NOT (n1)-[:FOLLOWS]->(n2)
// RETURN n1, n2, similarity, exists((n1)-[:FOLLOWS]->(n2)) AS already_following
// ORDER BY similarity DESC

// Remove created projection
CALL gds.graph.drop(
    'myGraph'
)
YIELD graphName, nodeCount, relationshipCount;



// ----------------------------------------------------------------------------------------------

// BONUS. Find Users that follows someone and are followed by someone
MATCH (u: User)
WITH u, count{(u)-[:FOLLOWS]->()} AS out, count{(u)<-[:FOLLOWS]-()} AS in
WHERE out > 0 AND in > 0
RETURN u, out, in ORDER BY (out+in) DESC LIMIT 20