/*
Copyright 2020 Filip Maj. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

const {BigQuery} = require('@google-cloud/bigquery');
const transform = require('stream-transform');
const JSONStream = require('JSONStream');
const prompt = require('async-prompt');
const moment = require('moment');
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);

const fs = require('fs');
const csv_reader = require('csv-reader');

// Connects to mysql DB storing user-company associations and streams rows to be written as json
module.exports = async function (argv) {
    const PROJECT_ID = argv.project || 'public-github-adobe';
    const DATASET_ID = argv.dataset || 'github_archive_query_views';
    const bigquery = new BigQuery({
        projectId: PROJECT_ID,
        keyFilename: argv.googlecloud || 'bigquery.json'
    });
    const TABLE_ID = argv.output;
    const FILE = argv.input;
    let inputStream = fs.createReadStream(FILE, 'utf8');
    const dataset = bigquery.dataset(DATASET_ID);
    let table = dataset.table(TABLE_ID);
    console.log('Checking BigQuery table...');
    let table_exists = (await table.exists())[0];
    if (table_exists) {
        let destroy = await prompt.confirm('The BigQuery destination table' + TABLE_ID + ' already exists. Do you want to overwrite it? ');
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
    await dataset.createTable(TABLE_ID, {
        schema: 'user,company'
    });
    console.log('... complete.');
    table = dataset.table(TABLE_ID);
    let start_time = moment();
    let end_time = moment();

    let firehose = table.createWriteStream({
        sourceFormat: 'NEWLINE_DELIMITED_JSON'
    });
    firehose.on('error', (e) => {
        console.error('firehose error!', e);
    });
    firehose.on('complete', (job) => {
        end_time = moment();
        console.log('Firehose into BigQuery emptied in ' + end_time.from(start_time, true) + '! BigQuery Job details:', job.metadata.status.state, job.metadata.jobReference.jobId);
        let log_stats = (job) => {
            console.log('BigQuery Job loaded', job);
        };
        if (job.metadata.status.state === 'DONE') {
            log_stats(job);
        } else {
            console.log('Now we wait for the Job to finish...');
            job.on('complete', log_stats);
            job.on('error', (e) => { console.error('Job error', e); });
        }
    });
    let rowCounter = 0;
    inputStream
        .pipe(new csv_reader({ skipHeader: true, parseNumbers: true, parseBooleans: true, trim: true }))
        .on('data', function (row) {
            rowCounter++;
            console.log(rowCounter, 'row arrived: ', row);
        })
        .on('end', function (data) {
            console.log('No more rows!');
        })
        .pipe(transform((record, callback) => {
            /*
            counter++;
            end_time = moment();
            if (counter % 1000 === 0) {
                process.stdout.write((counter / 1000) + 'k JSONs munged in ' + end_time.from(start_time, true) + '                     \r');
            }
            */
            callback(null, {
                user: String(record[1]),
                company: String(record[0])
            });
        }))
        .pipe(JSONStream.stringify(false))
        .pipe(firehose);
};
