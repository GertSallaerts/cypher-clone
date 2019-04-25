#!/usr/bin/env node

const [ from, to ] = process.argv.slice(2);

if (!from || !to) {
    console.log('Usage: cyper-clone [from] [to]');
    process.exit(1);
}

require('./src/index')(from, to).catch(err => {
    console.log(err);
    process.exit(1);
});
