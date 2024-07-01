const express = require('express');
const neo4j = require('neo4j-driver');

const app = express();
const port = 3000;

// Neo4j connection details
const uri = 'bolt://localhost:7687';
const user = 'neo4j';
const password = '2345';

// Create a Neo4j driver instance
const driver = neo4j.driver(uri, neo4j.auth.basic(user, password));

app.get('/', (req, res) => {
    res.send('Welcome to the Neo4j mini server!');
});

app.get('/nodes', async (req, res) => {
    const session = driver.session();
    try {
        const query = `
            LOAD CSV WITH HEADERS FROM 'file:///data.csv' AS row
            CREATE (u:User {
                userId: row.id,
                screenName: row.screenName,
                avatar: row.avatar,
                followersCount: row.followersCount,
                followingCount: row.friendsCount,
                lang: row.lang
            });
        `;
        const result = await session.run(query);

        // Fetch summary information about the query execution
        const summary = result.summary;
        res.json({
            nodes_created: summary.counters.nodesCreated(),
            properties_set: summary.counters.propertiesSet()
        });
    } catch (error) {
        console.error('Error executing query', error);
        res.status(500).send('Error executing query');
    } finally {
        await session.close();
    }
});

app.listen(port, () => {
    console.log(`App running on http://localhost:${port}`);
});
