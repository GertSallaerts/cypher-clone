module.exports = class QueryBatcher {
    constructor(onBatch, batchSize = 500) {
        this.jobs = [];
        this.onBatch = onBatch;
        this.bachSize = batchSize;
    }

    get size() {
        return this.jobs.length;
    }

    add(job) {
        this.jobs.push(job);
        this._work();
    }

    async finish() {
        if (!this._working && !this.jobs.length)
            return;

        return new Promise(resolve => {
            const existingSetReady = this._setReady;
            this._setReady = () => {
                existingSetReady && existingSetReady();
                resolve();
            };
        });
    }

    _work() {
        if (this._working)
            return;

        if (!this.jobs.length) {
            this._setReady && this._setReady();
            return;
        }

        this._working = true;

        const batch = this.jobs.splice(0, this.bachSize);

        Promise.resolve()
            .then(() => this.onBatch(batch))
            .finally(() => {
                this._working = false;
                this._work();
            });
    }
}
