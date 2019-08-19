
const UNIQUE_LABEL = 'GERTSALLAERTS_CYPHER_CLONE_LABEL';
const UNIQUE_ID = 'GERTSALLAERTS_CYPHER_CLONE_ID';

const EMPTY_DB = `
    MATCH (node)
    WITH node LIMIT {limit}
    DETACH DELETE node
    RETURN 1
`;

const QUERY_NODES = `
    MATCH (node)
    WHERE NOT node:${UNIQUE_LABEL}
    WITH node LIMIT ${limit}
    SET node:${UNIQUE_LABEL}
    RETURN node
`;

const QUERY_RELATIONSHIPS = `
    MATCH ()-[rel]->()
    RETURN rel
    ORDER BY id(rel)
    SKIP {skip} LIMIT {limit}
`;

const CREATE_UNIQUE = `
    CREATE CONSTRAINT ON (n:${UNIQUE_LABEL}) ASSERT n.${UNIQUE_ID} IS UNIQUE
`;

const DROP_UNIQUE = `
    DROP CONSTRAINT ON (n:${UNIQUE_LABEL}) ASSERT n.${UNIQUE_ID} IS UNIQUE
`;

const CLEANUP_UNIQUE = `
    MATCH (node:${UNIQUE_LABEL})
    WITH node LIMIT {limit}
    REMOVE node:${UNIQUE_LABEL}
    REMOVE node.${UNIQUE_ID}
    return 1
`;

function getCreateNode(node) {
    const labels = [ UNIQUE_LABEL ].concat(node.labels || []).join(':');
    const query = `CREATE (n:${labels}) SET n = {properties}`;
    const parameters = { properties: {
        ...node.properties,
        [UNIQUE_ID]: node.identity
    } };

    return { query, parameters };
}

function getCreateRelationship(rel) {
    const query = `
        MATCH
            (start:${UNIQUE_LABEL} { ${UNIQUE_ID}: {startId} }),
            (end:${UNIQUE_LABEL} { ${UNIQUE_ID}: {endId} })
        CREATE (start)-[r:${rel.type}]->(end)
        SET r = {properties}
    `;

    const parameters = {
        startId: rel.start,
        endId: rel.end,
        properties: rel.properties,
    };

    return { query, parameters };
}

module.exports = {
    EMPTY_DB,
    QUERY_NODES,
    QUERY_RELATIONSHIPS,
    CREATE_UNIQUE,
    DROP_UNIQUE,
    CLEANUP_UNIQUE,
    getCreateNode,
    getCreateRelationship,
};
