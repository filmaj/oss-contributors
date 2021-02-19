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
const split = require('split');
const transform = require('stream-transform');
const JSONStream = require('JSONStream');
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);

module.exports = function (argv) {
    let start_time = moment();
    let end_time = moment();
    let eventMap = {};
    let done = () => {
        console.log(eventMap);
        console.log('total events:', Object.keys(eventMap).reduce((acc, cur) => acc + eventMap[cur], 0));
    };
    let counter = 0;
    fs.createReadStream(argv.input)
        .pipe(split(JSON.parse))
        .pipe(transform((obj) => {
            counter++;
            end_time = moment();
            if (eventMap[obj.type]) eventMap[obj.type]++;
            else eventMap[obj.type] = 1;
            const event = obj.type.split('Event').join('').toLowerCase();
            if (counter % 1000 === 0) process.stdout.write(`${(counter / 1000)}k JSONs munged in ${end_time.from(start_time, true)}                  \r`);
            switch (obj.type) {
            case 'PushEvent':
            case 'WatchEvent':
                return {
                    time: obj.created_at,
                    repo: obj.repo.name,
                    login: obj.actor.login,
                    event
                };
            case 'PullRequestEvent':
            case 'IssuesEvent':
            case 'PullRequestReviewEvent':
            case 'ReleaseEvent':
                return {
                    time: obj.created_at,
                    repo: obj.repo.name,
                    login: obj.actor.login,
                    event,
                    action: obj.payload.action // open, closed, created, published
                };
            case 'ForkEvent':
                return {
                    time: obj.created_at,
                    repo: obj.repo.name,
                    login: obj.actor.login,
                    event,
                    dest: obj.payload.forkee.full_name // where it is being forked to
                };
            case 'CreateEvent':
            case 'IssueCommentEvent':
            case 'CommitCommentEvent':
            case 'DeleteEvent':
            case 'GollumEvent':
            case 'MemberEvent':
            case 'PublicEvent':
            case 'PullRequestReviewCommentEvent':
            default:
                return null;
            }
        }))
        .pipe(JSONStream.stringify(false))
        .pipe(fs.createWriteStream('newdump.json'))
        .on('error', (err) => {
            console.error(err);
            done();
        })
        .on('finish', done);
};
