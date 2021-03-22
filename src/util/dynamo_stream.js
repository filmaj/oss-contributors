const {Writable} = require('stream');
const AWS = require('aws-sdk');
const MAX = 12;

class DynamoStream extends Writable {
    constructor (options) {
        super({
            objectMode: true,
            highWaterMark: MAX
        });
        this.client = new AWS.DynamoDB.DocumentClient({region: options.region});
        this.table = options.table;
        this.startdate = options.startdate;
        this.buffer = [];
        this.processed = 0;
    }

    async _write (chunk, _, next) {
        this.buffer.push(this.convert_chunk(chunk));
        await this.maybe_drain(next);
        return next();
    }

    async _writev (records, next) {
        this.buffer = this.buffer.concat(records.map((record) => this.convert_chunk(record.chunk)));
        await this.maybe_drain(next);
        return next();
    }

    async _final (done) {
        while (this.buffer.length) {
            try {
                await this.write_to_dynamo();
            } catch (e) {
                done(e);
            }
        }
        console.log(`\n...complete! Wrote ${this.processed} objects.`);
        done();
    }

    async maybe_drain (next) {
        if (this.buffer.length >= (2 * MAX)) {
            try {
                await this.write_to_dynamo();
            } catch (e) {
                return next(e);
            }
        }
    }

    async write_to_dynamo () {
        let params = {
            RequestItems: {}
        };
        params.RequestItems[this.table] = this.buffer.slice(0, MAX);
        let result = await this.client.batchWrite(params).promise();
        this.buffer = this.buffer.slice(MAX);
        if (result && result.UnprocessedItems && result.UnprocessedItems[this.table] && result.UnprocessedItems[this.table].length) {
            console.warn(result.UnprocessedItems[this.table].length + ' unprocessed items in batch! ' + JSON.stringify(result.UnprocessedItems[this.table]));
        }
        this.processed += params.RequestItems[this.table].length;
        if (this.processed % 1000 === 0) {
            process.stdout.write(`${(this.processed / 1000)}k objects written to DynamoDB...\r`);
        }
    }

    convert_chunk (chunk) {
        let item = {
            PutRequest: {
                Item: {
                    username: chunk.user,
                    startdate: this.startdate,
                    rawcompany: '',
                    polishedcompany: chunk.company
                }
            }
        };
        return item;
    }
}


module.exports = DynamoStream;
