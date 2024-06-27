# graph_db
Graph Database for social network analysis through NEO4J knowledge graph. 

Let's start from the scratch. 
 1. I need to understand how can i store all people: basically, i cannot insert in a graph only the value of first user in the row, because all the "friends" must be inserted as "user" ONLY IF THEY ARE NOT INSERTED YET. This is crucial to build a convenient stucture.

 2. Then i can move on, i need to do some analysis but in which way? Let's breakdown the problem: 
    - Why we choose as data structure a knowledge graph? Is it better? in which terms? 
    - From the computational point of view is convenient? Let's do a comparison between a relation database? 
    - Why knowledge and not graph? we suppose something that could be inferred by our knowledge arch through NPL. 
    - With knowledge is possible to detect cluster. (Since we have tags is possible to infer on data to get al the people interested in some topic). 
    - Anomaly detection on insual transaction data. 
    - Adding metadata to get the context ??? in relational it's impossible. 


## Useful links
### Importing
- https://neo4j.com/docs/getting-started/data-import/

### Indexes
- https://neo4j.com/developer/kb/a-method-to-calculate-index-size/
- https://neo4j.com/docs/cypher-manual/current/indexes/search-performance-indexes/managing-indexes/#create-indexes
- https://neo4j.com/docs/cypher-manual/current/indexes/search-performance-indexes/using-indexes/

### Queries
