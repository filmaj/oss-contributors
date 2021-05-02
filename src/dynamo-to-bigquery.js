/*
Copyright 2021 Filip Maj. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

const { BigQuery } = require('@google-cloud/bigquery');
const moment = require('moment');
const aws = require('aws-sdk');
const { DocumentClientQueryReadable } = require('dynamodb-read-stream');
const stream = require('stream');
const JSONStream = require('JSONStream');
const prompt = require('async-prompt');
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);

const PROJECT_ID = 'public-github-adobe';
const DATASET_ID = 'github_archive_query_views';
const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: 'bigquery.json'
});

module.exports = async function (argv) {
    const dataset = bigquery.dataset(DATASET_ID);
    let table = dataset.table(argv.destination);
    console.log('Checking BigQuery table...');
    let table_exists = (await table.exists())[0];
    if (table_exists) {
        let destroy = await prompt.confirm(`The BigQuery destination table ${argv.destination} already exists. Do you want to overwrite it?`);
        if (destroy) {
            console.log('Deleting table...');
            await table.delete();
            console.log('... complete.');
        } else {
            console.warn('Then ima bail! l8s');
            process.exit(1337);
        }
    }
    console.log('Creating new table...');
    await dataset.createTable(argv.destination, {
        schema: 'user,company'
    });
    console.log('... complete.');
    table = dataset.table(argv.destination);

    let start_time = moment();
    let end_time = moment();

    const region = argv.region;
    const doc = new aws.DynamoDB.DocumentClient({ region });
    // pipe opening (readable) section: dynamodb query
    const queryReadStream = new DocumentClientQueryReadable(doc, {
        TableName: argv.source,
        IndexName: 'MetaRecordsByDate',
        KeyConditionExpression: 'startdate = :meta AND #updated BETWEEN :start AND :end',
        ExpressionAttributeNames: {
            '#updated': 'updated'
        },
        ExpressionAttributeValues: {
            ':meta': '#META',
            ':start': '2021-04-01',
            ':end': '2021-04-30'
        }
    });
    // pipe next section: transfrom from DDB query results into individual json bits
    let counter = 0;
    const transformOutput = new stream.Transform({
        objectMode: true,
        transform (chunk, encoding, callback) {
            counter += chunk.Items.length;
            chunk.Items.forEach((i) => this.push({
                user: i.username,
                company: i.match || i.raw
            }));
            end_time = moment();
            process.stdout.write(`${counter / 1000}k records processed in ${end_time.from(start_time, true)}                       \r`);
            callback();
        }
    });
    const stringifier = JSONStream.stringify(false);
    let firehose = table.createWriteStream({
        sourceFormat: 'NEWLINE_DELIMITED_JSON'
    });
    firehose.on('error', (e) => {
        console.error('firehose error!', e);
    });
    firehose.on('complete', (job) => {
        end_time = moment();
        console.log(`Firehose into BigQuery emptied ${counter} records in ${end_time.from(start_time, true)}! BigQuery Job details:`, job.metadata.status.state, job.metadata.jobReference.jobId);
        let log_stats = (job) => {
            console.log('BigQuery Job Complete!', job);
        };
        if (job.metadata.status.state === 'DONE') {
            log_stats(job);
            process.exit(0);
        } else {
            console.log('Now we wait for the Job to finish...');
            job.on('complete', log_stats);
            job.on('error', (e) => { console.error('Job error', e); });
        }
    });
    queryReadStream.pipe(transformOutput).pipe(stringifier).pipe(firehose);
};
