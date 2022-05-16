'use strict';

const url = require('url');
const neo4j = require('neo4j-driver');
const QueryBatcher = require('./query-batcher');

const {
    EMPTY_DB,
    QUERY_NODES,
    QUERY_RELATIONSHIPS,
    CREATE_UNIQUE,
    DROP_UNIQUE,
    CLEANUP_UNIQUE,
    getCreateNode,
    getCreateRelationship,
} = require('./queries');

function getDriver(connectionString) {
    connectionString = url.parse(connectionString);

    const [ user, password ] = (connectionString.auth || '').split(':');
    delete connectionString.auth;

    connectionString = url.format(connectionString);

    if (!user || !password)
        return neo4j.driver(connectionString);

    return neo4j.driver(connectionString, neo4j.auth.basic(user, password));
}

async function dropIndexes(session) {
    const constraints = await session.run('CALL db.constraints()');

    await writeTransaction(session, constraints.records.map(r => ({
        query: `DROP CONSTRAINT ${r.get('name')}`
    })));

    const indexes = await session.run('CALL db.indexes()');

    await writeTransaction(session, indexes.records.map(r => ({
        query: `DROP INDEX ${r.get('name')}`
    })));
}

async function copyIndexes(from, to) {
    const indexes = await from.run('CALL db.indexes()');
    const constraints = await from.run('CALL db.constraints()');
    const queries = [];

    indexes.records.forEach(r => {
        const name = r.get('name');
        const uniqueness = r.get('uniqueness');
        const type = r.get('type');
        const entityType = r.get('entityType');
        const labelsOrTypes = r.get('labelsOrTypes');
        const properties = r.get('properties');

        // Reserved name, auto-created index during migration
        if (name === '__org_neo4j_schema_index_label_scan_store_converted_to_token_index')
            return;

        // It's a constraint;
        if (uniqueness === 'UNIQUE')
            return;

        const query = [ 'CREATE' ];

        if (type !== 'BTREE')
            query.push(type);

        query.push(`INDEX ${name} FOR`);

        if (labelsOrTypes.length > 1)
            throw new Error('Unsupported INDEX in source');

        if (entityType === 'NODE') {
            if (labelsOrTypes.length === 1)
                query.push(`(i:${labelsOrTypes[ 0 ]})`);
            else
                query.push('(i)');
        } else if (entityType === 'RELATIONSHIP') {
            if (labelsOrTypes.length === 1)
                query.push(`()-[r:${labelsOrTypes[ 0 ]}]-()`);
            else
                query.push(`()-[r]-()`);
        } else {
            throw new Error('Unsupported INDEX in source');
        }

        query.push('ON');

        if (labelsOrTypes.length === 1) {
            if (!properties.length)
                throw new Error('Unsupported INDEX in source');

            query.push(`(${properties.map(p => `i.${p}`).join(', ')})`);
        } else {
            query.push('EACH');

            if (entityType === 'NODE')
                query.push('labels(i)');
            else if (entityType === 'RELATIONSHIP')
                query.push('type(i)');
        }

        queries.push({ query: query.join(' ') });
    });


    constraints.records.forEach(r => {
        queries.push({ query: `CREATE ${r.get('description')}` });
    });

    await writeTransaction(to, queries);
}

async function paginateQuery(session, query, limit, onRecord) {
    let skip = 0, gotResults = false;

    function onNext(record) {
        gotResults = true;
        onRecord && onRecord(record);
    }

    do {
        gotResults = false;
        await new Promise((res, rej) => session
            .run(query, {
                skip: neo4j.int(skip),
                limit: neo4j.int(limit)
            })
            .subscribe({ onNext, onCompleted: res, onError: rej })
        );
        skip += limit;
    } while (gotResults);
}


async function writeTransaction(session, queries) {
    await session.writeTransaction(tx => {
        return queries.map(({ query, parameters }) => tx.run(query, parameters));
    })
        .catch(err => console.log(err));
}


async function doSync(fromSession, toSession) {
    const startTime = Date.now();

    await paginateQuery(toSession, EMPTY_DB, 20000);
    await dropIndexes(toSession);
    console.log('Destination database emptied');

    await toSession.run(CREATE_UNIQUE);
    console.log('Prepared UNIQUE constraint for sync performance');

    const writeQueue = new QueryBatcher(batch => writeTransaction(toSession, batch));
    const interval = setInterval(() => {
        console.log('Queue size:', writeQueue.size);
    }, 2000);

    await paginateQuery(fromSession, QUERY_NODES, 1000, rec => {
        writeQueue.add(getCreateNode(rec.get('node')));
    });
    console.log('Queued all node creations');

    await paginateQuery(fromSession, QUERY_RELATIONSHIPS, 1000, rec => {
        writeQueue.add(getCreateRelationship(rec.get('rel')));
    });
    console.log('Queued all relationship creations');

    await writeQueue.finish();
    console.log('Write queue empty');
    clearInterval(interval);

    await paginateQuery(toSession, CLEANUP_UNIQUE, 20000);
    console.log('Removed temporary performance helpers');

    await toSession.run(DROP_UNIQUE);
    console.log('Removed performance helper UNIQUE contraint');

    await copyIndexes(fromSession, toSession);
    console.log('Synchronized indexes');

    const time = Math.round((Date.now() - startTime) / 1000);
    console.log(`Sync completed in ${time} seconds`);
}

module.exports = function sync(from, to) {
    const fromDriver = getDriver(from);
    const toDriver = getDriver(to);
    const fromSession = fromDriver.session();
    const toSession = toDriver.session();

    const promise = doSync(fromSession, toSession);

    promise.finally(() => {
        fromSession.close();
        fromDriver.close();
        toSession.close();
        toDriver.close();
    });

    return promise;
};
