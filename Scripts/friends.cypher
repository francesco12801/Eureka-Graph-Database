:auto LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row

CALL{ 
    WITH row   
    MATCH (u:User {userId: toInteger(row.id)})
    UNWIND split(row.friends, "|") as friendId
        MERGE (f:User {userId: toInteger(friendId)})
        MERGE (u)-[:FOLLOWS]->(f)
} IN TRANSACTIONS 