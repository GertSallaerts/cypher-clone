# cypher-clone

Clone a Neo4j database using Cypher queries.

Not recommended for important tasks like backups because:

- Performance probably sucks
- Could explode if new node are added to source database while syncing
- ...

Usefull if you don't have access to your remote Neo4j instance's backups or are not able to connect using `neo4j-admin`.

## Usage

```
npm install --save @gertt/cypher-clone
```

CLI tool:

```
cypher-clone bolt://source.database:7687 bolt://localhost:7687
```

In code:

```js
const sync = require('@gertt/cypher-clone');

sync(
    'bolt://source.database:7687',
    'bolt://localhost:7687'
).catch(err => console.log(err));
```
