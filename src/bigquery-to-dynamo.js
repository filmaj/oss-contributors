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
const moment = require('moment');
const DynamoStream = require('./util/dynamo_stream');
const util = require('util');
const stream = require('stream');
const pipeline = util.promisify(stream.pipeline);
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);

const PROJECT_ID = 'public-github-adobe';
const DATASET_ID = 'github_archive_query_views';
const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: 'bigquery.json'
});

/*
const startdate = 1577890488000; // this is jan 1 2020; 1546348530000 = jan 1 2019
const arctable = 'ArcOssContributorsStaging-UsersTable-1481B6ZLNL931';
*/

module.exports = async function (argv) {
    const TABLE_ID = argv.source;
    const startdate = argv.startdate;
    const arctable = argv.destination;
    const region = argv.region;
    const dataset = bigquery.dataset(DATASET_ID);
    let table = dataset.table(TABLE_ID);
    let table_exists = (await table.exists())[0];
    if (!table_exists) {
        console.error('Source table does not exist!');
        process.exit(1337);
    }
    let input = table.createQueryStream(`SELECT * FROM ${TABLE_ID}`);
    await pipeline(input, new DynamoStream({
        table: arctable,
        startdate,
        region
    }));
    console.log('... complete!');
};
