'use strict';

const url = require('url');
const neo4j = require('neo4j-driver').v1;
const QueryBatcher = require('./query-batcher');

const {
    SRC_CHECK,
    EMPTY_DB,
    QUERY_NODES,
    QUERY_RELATIONSHIPS,
    CREATE_UNIQUE,
    DROP_UNIQUE,
    CLEANUP_UNIQUE,
    CLEANUP_SRC_NODES,
    CLEANUP_SRC_RELS,
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

    await Promise.all(constraints.records.map(r => {
        const query = `DROP ${r.get('description')}`;
        return session.run(query);
    }));

    const indexes = await session.run('CALL db.indexes()');

    await Promise.all(indexes.records.map(r => {
        const query = `DROP ${r.get('description')}`;
        return session.run(query);
    }));
}

async function copyIndexes(from, to) {
    const indexes = await from.run('CALL db.indexes()');

    await Promise.all(indexes.records.map(r => {
        const type = r.get('type');

        // It's a constraint;
        if (type.indexOf('unique') > -1)
            return;

        const query = `CREATE ${r.get('description')}`;
        return to.run(query);
    }));

    const constraints = await from.run('CALL db.constraints()');

    await Promise.all(constraints.records.map(r => {
        const query = `CREATE ${r.get('description')}`;
        return to.run(query);
    }));
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
            .run(query, { skip, limit })
            .subscribe({ onNext, onCompleted: res, onError: rej })
        );
        skip += limit;
    } while (gotResults)
}


async function writeTransaction(session, queries) {
    await session.writeTransaction(tx => {
        queries.forEach(({ query, parameters }) => tx.run(query, parameters));
    })
        .catch(err => console.log(err));
}


async function doSync(fromSession, toSession) {
    const writeQueue = new QueryBatcher(batch => writeTransaction(toSession, batch));
    const startTime = Date.now();
    
    const conflicts = await fromSession.run(SRC_CHECK);
    
    if (conflicts)
        throw new Error('Source dataset has nodes or relationships that conflict with this tool.');

    await paginateQuery(toSession, EMPTY_DB, 20000);
    await dropIndexes(toSession);
    console.log('Destination database emptied');

    await toSession.run(CREATE_UNIQUE);
    console.log('Prepared UNIQUE constraint for sync performance');

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
    await paginateQuery(fromSession, CLEANUP_SRC_NODES, 20000);
    await paginateQuery(fromSession, CLEANUP_SRC_RELS, 20000);
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
}
