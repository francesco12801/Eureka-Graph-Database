:auto LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row
WITH row LIMIT 1000

// Execute in batches of 50 rows
CALL {
    WITH row
    MATCH (u:User {userId: toInteger(row.id)})
    UNWIND split(row.friends, "|") as friendId
        // For every friend create a User node if it doesn't exist yet
        MERGE (f:User {userId: toInteger(friendId)})
        // Create FOLLOWS relationship
        MERGE (u)-[:FOLLOWS]->(f)
} IN TRANSACTIONS OF 50 ROWS