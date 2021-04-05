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
const fs = require('fs');
const moment = require('moment');
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);
const AWS = require('aws-sdk');
const companies = require('./util/companies');
let batchWriteParams = {};
let ddb = null;

module.exports = async function (argv) {
    const userSet = new Set();
    ddb = new AWS.DynamoDB.DocumentClient({
        region: argv.region
    });
    let counter = 0;
    let written = 0;
    let deleted = 0;
    let skipped = 0;
    function newBatchParams () {
        return { RequestItems: {[argv.source]: []} };
    }
    batchWriteParams = newBatchParams();
    async function write (records) {
        if (!Array.isArray(records)) records = [records];
        batchWriteParams.RequestItems[argv.source] = batchWriteParams.RequestItems[argv.source].concat(records);
        if (batchWriteParams.RequestItems[argv.source].length > 24) {
            let params = JSON.parse(JSON.stringify(batchWriteParams));
            params.RequestItems[argv.source] = batchWriteParams.RequestItems[argv.source].splice(0, 25);
            try {
                await ddb.batchWrite(params).promise();
                written += params.RequestItems[argv.source].filter(r => r.PutRequest).length;
                deleted += params.RequestItems[argv.source].filter(r => r.DeleteRequest).length;
            } catch (e) {
                console.error('Error during write, will abort!', e);
                process.exit(1337);
            }
        }
    }
    let p = {
        TableName: argv.source
    };
    let op = 'scan';
    if (argv.index) {
        p.IndexName = argv.index;
        p.KeyConditionExpression = 'startdate = :meta';
        p.ExpressionAttributeValues = {':meta': '#META'};
        op = 'query';
    }
    let params = { ...p };
    let affCounter = 0;
    let results = { Items: [], Count: 0 };
    if (fs.existsSync('start.key')) {
        console.log('Reading start key on startup...');
        let startkey = JSON.parse(fs.readFileSync('start.key').toString());
        results.LastEvaluatedKey = startkey;
        console.log(`Set start key to ${JSON.stringify(startkey)}`);
    }
    do {
        console.log(`(${new Date().toISOString()}) Outer loop begins, iterating on Items (${results.Items.length})...`);
        for (let record of results.Items) {
            process.stdout.write(`Scanned ${counter} records, skipped ${skipped}, processed ${userSet.size} users and ${affCounter} affiliations, written ${written} and deleted ${deleted} records in DB)                 \r`);
            let username = record.username;
            if (userSet.has(username)) {
                skipped++;
                continue;
            }
            userSet.add(username);
            let queryParams = { TableName: p.TableName };
            queryParams.KeyConditionExpression = '#username = :username';
            queryParams.ExpressionAttributeNames = { '#username': 'username' };
            queryParams.ExpressionAttributeValues = { ':username': username };
            let queryResults = await ddb.query(queryParams).promise();
            let affiliations = queryResults.Items.filter((r) => r && r.startdate && r.startdate !== '#META');
            // console.log(JSON.stringify(affiliations, null, 2));
            let currMatch = false;
            let currRaw = false;
            let prevMatch = false;
            let prevRaw = false;
            for (let a of affiliations) {
                affCounter++;
                currRaw = a.raw;
                currMatch = a.match;
                // console.log('current', currMatch, currRaw);
                // console.log('prev', prevMatch, prevRaw);
                if (currRaw === currMatch && companies.match(currRaw) !== currMatch) {
                    // update incorrect affiliation record
                    a.match = companies.match(a.raw);
                    currMatch = a.match;
                    await write({ PutRequest: { Item: a }});
                }
                // console.log('post-write current', currMatch, currRaw);
                if (currMatch === prevMatch && currRaw === prevRaw) {
                    // delete this record as it had a false match and now end up
                    // being consecutive identical records
                    await write({ DeleteRequest: { Key: { username: a.username, startdate: a.startdate } }});
                } else {
                    prevRaw = currRaw;
                    prevMatch = currMatch;
                }
            }
            // might need to update meta record!
            if (affiliations.length) {
                let lastAff = affiliations[affiliations.length - 1];
                if (lastAff.raw !== record.raw || lastAff.match !== record.match) {
                    record.raw = lastAff.raw;
                    record.match = lastAff.match;
                    await write({ PutRequest: { Item: record }});
                }
            }
        }
        console.log(`Scanned ${counter} records, skipped ${skipped}, processed ${userSet.size} users and ${affCounter} affiliations, written ${written} and deleted ${deleted} records in DB)`);
        if (results.LastEvaluatedKey) {
            params.ExclusiveStartKey = results.LastEvaluatedKey;
            console.log(`Writing start key ${JSON.stringify(results.LastEvaluatedKey)}`);
            fs.writeFileSync('start.key', JSON.stringify(results.LastEvaluatedKey));
            console.log('... written.');
        }
        counter += results.Count;
        console.log(`Retrieving page of ${op} results (key: ${JSON.stringify(params.ExclusiveStartKey)})...`);
        results = await ddb[op](params).promise();
        console.log(`... retrieved page of ${results.Count} results, loop continues...`);
    } while (results.Count);
};
// mostly for testing
module.exports.flush = async function () {
    await ddb.batchWrite(batchWriteParams).promise();
};
