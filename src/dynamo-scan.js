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

const moment = require('moment');
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);
const AWS = require('aws-sdk');

function clean (str) {
    return str ? str : null;
}
let userSet = new Set();

module.exports = async function (argv) {
    userSet = new Set();
    let ddb = new AWS.DynamoDB.DocumentClient({
        region: argv.region
    });
    let p = {
        TableName: argv.source
    };
    let scanParams = { ...p };
    let counter = 0;
    let written = 0;
    let deleted = 0;
    let scanResults = { Items: [], Count: 0 };
    do {
        for (let record of scanResults.Items) {
            let username = record.username;
            if (userSet.has(username)) continue;
            userSet.add(username);
            let queryParams = { ...p };
            queryParams.KeyConditionExpression = '#username = :username';
            queryParams.ExpressionAttributeNames = { '#username': 'username' };
            queryParams.ExpressionAttributeValues = { ':username': username };
            let queryResults = await ddb.query(queryParams).promise();
            let oldAffiliations = queryResults.Items.filter((r) => r && r.startdate && r.startdate !== '#META');
            let modernAffiliations = queryResults.Items.filter((r) => r && r.startdate && r.startdate !== '#META' && typeof r.match === 'undefined');
            let affiliations = modernAffiliations.sort((a, b) => {
                // sort in chronological order
                return b.startdate > a.startdate ? -1 : 1;
            });
            let meta = queryResults.Items.find((r) => r.startdate === '#META'); // this may be undefined
            let currMatch = false;
            let currRaw = false;
            let prevMatch = false;
            let prevRaw = false;
            let newAffiliations = [];
            let deleteAffiliations = [];
            for (let a of affiliations) {
                currMatch = a.polishedcompany;
                currRaw = a.rawcompany;
                if (clean(currMatch)) {
                    // there is some content in matched field
                    if (clean(currMatch) === clean(prevMatch)) {
                        // matched to previous field, delete it
                        deleteAffiliations.push({ username, startdate: a.startdate });
                        continue;
                    } else {
                        // change in affiliation, make sure we update/write
                        let match = clean(currMatch);
                        let raw = clean(currRaw);
                        // if we have a match, but the raw field is empty, use the
                        // match for the raw
                        if (match && !raw) raw = match;
                        newAffiliations.push({ username, startdate: a.startdate, match, raw });
                    }
                } else if (clean(prevMatch)) {
                    // current match is empty but previous match was not empty,
                    // means matched fields changed - UNLESS that is identical
                    // to the raw field.
                    if (clean(currRaw) === clean(prevMatch)) {
                        // matched to previous field, delete it
                        deleteAffiliations.push({ username, startdate: a.startdate });
                        continue;
                    } else {
                        // change in affiliation, make sure we update/write
                        let match = clean(currMatch);
                        let raw = clean(currRaw);
                        // if we have a match, but the raw field is empty, use the
                        // match for the raw
                        if (match && !raw) raw = match;
                        newAffiliations.push({ username, startdate: a.startdate, match, raw });
                    }
                } else if (clean(currRaw)) {
                    // current match and previous match fields are empty, but current raw field has something!
                    if (clean(currRaw) === clean(prevRaw)) {
                        // matched to previous raw field, delete it
                        deleteAffiliations.push({ username, startdate: a.startdate });
                        continue;
                    } else {
                        // change in affiliation, make sure we update/write
                        let raw = clean(currRaw);
                        let match = raw;
                        newAffiliations.push({ username, startdate: a.startdate, match, raw });
                    }
                } else if (clean(currRaw) === clean(prevRaw)) {
                    // both match fields and current raw field are empty
                    // plus currRaw is empty so previous raw field also empty!
                    // delete it if this isn't the first processing we're doing
                    if (typeof prevRaw === 'boolean') {
                        // if were dealing with boolean then this is the first
                        // record for the user. since fields are empty, then
                        // this user never had a company associated with them.
                        // so we add a record with null entries
                        newAffiliations.push({ username, startdate: a.startdate, match: null, raw: null });
                    } else {
                        // we reserve boolean type on these curr/prev variables
                        // to denote 'not processed yet'
                        deleteAffiliations.push({ username, startdate: a.startdate });
                        continue;
                    }
                } else {
                    // change in raw fields
                    let raw = clean(currRaw);
                    let match = raw;
                    newAffiliations.push({ username, startdate: a.startdate, match, raw });
                }
                prevMatch = currMatch;
                prevRaw = currRaw;
            }
            let writeMeta = false;
            if (oldAffiliations.length === 0 && meta) {
                meta.updated = '2018-01-01T00:00:00.001Z';
                meta.match = null;
                meta.raw = null;
                writeMeta = true;
                newAffiliations.push({ username, startdate: meta.updated, match: null, raw: null });
            }
            if (meta) {
                delete meta.inferredAffiliation;
                delete meta.lastUpdated;
                delete meta.rawCompanyField;
            } else {
                writeMeta = true;
                meta = { username, startdate: '#META' };
            }
            if (newAffiliations.length) {
                writeMeta = true;
                let lastAffiliation = newAffiliations[newAffiliations.length - 1];
                meta.match = lastAffiliation.match;
                meta.raw = lastAffiliation.raw;
                meta.updated = lastAffiliation.startdate;
            }
            let batchWriteParams = { RequestItems: {} };
            // write new affliation records
            batchWriteParams.RequestItems[argv.source] = newAffiliations.map((a) => ({ PutRequest: { Item: a }}));
            if (writeMeta) {
                batchWriteParams.RequestItems[argv.source].push({PutRequest: {Item: meta}});
            }
            // delete old records
            batchWriteParams.RequestItems[argv.source] = batchWriteParams.RequestItems[argv.source].concat(deleteAffiliations.map((a) => ({ DeleteRequest: { Key: { username: a.username, startdate: a.startdate }}})));
            if (batchWriteParams.RequestItems[argv.source].length) {
                try {
                    await ddb.batchWrite(batchWriteParams).promise();
                    written += batchWriteParams.RequestItems[argv.source].filter(i => i.PutRequest).length;
                    deleted += batchWriteParams.RequestItems[argv.source].filter(i => i.DeleteRequest).length;
                } catch (e) {
                    console.error('Error during write!', e);
                }
            }
            process.stdout.write(`\nScanned ${counter} records, processed ${userSet.size} users, written ${written} and deleted ${deleted} records to DB)                                                             \r`);
        }
        if (scanResults.LastEvaluatedKey) scanParams.ExclusiveStartKey = scanResults.LastEvaluatedKey;
        counter += scanResults.Count;
        console.log(`\nprocessed ${counter} records, asking for new page...`);
        scanResults = await ddb.scan(scanParams).promise();
        console.log('retrieved page. iterating.\n');
    } while (scanResults.Count);
};
