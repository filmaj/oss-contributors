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
moment.relativeTimeThreshold('m', 55);
moment.relativeTimeThreshold('ss', 5);
moment.relativeTimeThreshold('s', 55);
const PROJECT_ID = 'public-github-adobe';
const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: 'bigquery.json'
});
const csv_writer = require('csv-writer').createArrayCsvWriter;

const ys = ['2018', '2019', '2020'];
const ms = [...Array(12).keys()].map((i) => (i < 9 ? `0${i + 1}` : `${i + 1}`));
const thisMonth = new Date().getMonth() + 1;
const thisYear = new Date().getFullYear();
let months = [];
ys.forEach((y) => {
    let yInt = parseFloat(y, 10);
    ms.forEach((m) => {
        let mInt = parseFloat(m, 10);
        if (yInt !== thisYear || mInt < thisMonth) months.push(`${y}-${m}`);
    });
});
const rawActivityHeader = ['Period', 'Company', 'Contributions', 'Total Contributions', '% Google Contributions', 'Contributors', 'Total Contributors', '% Google Contributors'];

function writeRows (arr, rows) {
    let googleContributions = 0;
    let googleContributors = 0;
    for (let i = 1; i <= rows.length; i++) {
        let row = rows[i - 1];
        arr[i][1] = row.co;
        arr[i][2] = row.contributions;
        arr[i][5] = row.num_contributors;
        if (row.co === 'google') {
            googleContributions = row.contributions;
            googleContributors = row.num_contributors;
        }
    }
    return {googleContributors, googleContributions};
}

module.exports = async function (argv) {
    const activityCsv = csv_writer({
        path: argv.activityFile,
        header: rawActivityHeader,
        alwaysQuote: true
    });
    let butorData = {};
    let butionData = {};
    for (let i = 0; i < months.length; i++) {
        const monthLabel = months[i]; // i.e. 2018-01
        const ghaMonth = monthLabel.replace(/-/, ''); // i.e. 201801
        const year = ghaMonth.substr(0, 4); // i.e. 2018
        let query = `#standardSQL
WITH
period AS (
  SELECT *
  FROM (SELECT * FROM \`githubarchive.month.${ghaMonth}\`) a
),
contributions_to_projects AS (
  SELECT *, CASE
      WHEN (cleaned = 'ampproject' OR CAST(STRPOS(cleaned, 'accelerated-mobile-pages') AS BOOL) OR CAST(STRPOS(cleaned, 'google') AS BOOL)) THEN 'google'
      WHEN cleaned = 'ctripfe' THEN 'ctrip'
      WHEN (cleaned IS NULL OR cleaned = 'self-employed' OR cleaned = 'self employ' OR CAST(STRPOS(cleaned, 'freelance') AS BOOL) or cleaned = 'student') THEN ''
      WHEN CAST(STRPOS(cleaned, 'popsugar') AS BOOL) THEN 'popsugar'
      WHEN (CAST(STRPOS(cleaned, 'condenast') AS BOOL) OR CAST(STRPOS(cleaned, 'condénast') AS BOOL)) THEN 'condé nast'
      WHEN CAST(STRPOS(cleaned, 'buzzfeed') AS BOOL) THEN 'buzzfeed'
      WHEN CAST(STRPOS(cleaned, 'sonobi') AS BOOL) THEN 'sonobi'
      WHEN (CAST(STRPOS(cleaned, 'washington post') AS BOOL) OR CAST(STRPOS(cleaned, 'washingtonpost') AS BOOL)) THEN 'washington post'
      WHEN (CAST(STRPOS(cleaned, 'jungvonmatt') AS BOOL) OR CAST(STRPOS(cleaned, 'jung von matt') AS BOOL)) THEN 'jungvonmatt'
      WHEN (CAST(STRPOS(cleaned, 'parsely') AS BOOL)) THEN 'parsely'
      WHEN (CAST(STRPOS(cleaned, 'digitonic') AS BOOL)) THEN 'digitonic'
      WHEN (CAST(STRPOS(cleaned, 'glomex') AS BOOL)) THEN 'glomex gmbh'
      WHEN (CAST(STRPOS(cleaned, 'vercel') AS BOOL)) THEN 'vercel'
      WHEN (CAST(STRPOS(cleaned, 'freestar') AS BOOL)) THEN 'freestarcapital'
      WHEN (CAST(STRPOS(cleaned, 'chefkoch') AS BOOL)) THEN 'chefkoch-dev'
      WHEN (CAST(STRPOS(cleaned, 'rtcamp') AS BOOL)) THEN 'rtcamp'
      WHEN (CAST(STRPOS(cleaned, 'capitalone') AS BOOL) OR CAST(STRPOS(cleaned, 'capital one') AS BOOL)) THEN 'capitalone'
      ELSE cleaned
    END
    AS co
  FROM
    (SELECT TRIM(LOWER(LTRIM(company, '@'))) as cleaned, *
    FROM (
      SELECT type, actor.login login, count(*) contributions
      FROM period a
      WHERE repo.name = 'ampproject/amphtml' AND (type='PushEvent') AND NOT (ENDS_WITH(actor.login, "bot") OR ENDS_WITH(actor.login, "[bot]"))
      GROUP BY type, login
    ) z
    JOIN \`github_archive_query_views.users_companies_${year}\` y
    ON z.login = y.user)
)
#SELECT * FROM contributions_to_projects
SELECT co, SUM(contributions) contributions, COUNT(DISTINCT login) num_contributors#, ARRAY_AGG(DISTINCT login) contributors
FROM contributions_to_projects
GROUP BY co
ORDER BY num_contributors DESC, contributions DESC`;
        console.log(`Querying activity for ${monthLabel}...`);
        let result = await bigquery.query(query);
        let rows = result[0];
        console.log('...complete.');
        // X rows per month, dependent on how many companies were identified to be active in the month
        let monthly_data = new Array(rows.length + 1);
        for (let i = 0; i < rows.length + 1; i++) {
            monthly_data[i] = new Array(rawActivityHeader.length);
            if (i === 0) {
                // write header for the month
                monthly_data[0][0] = monthLabel;
                for (let j = 1; j < rawActivityHeader.length; j++) {
                    monthly_data[0][j] = rawActivityHeader[j];
                }
            }
        }
        // annotate with sum and percentage annotation data in the first row
        // only
        const totalContributions = rows.reduce((acc, cur) => acc + cur.contributions, 0);
        const totalContributors = rows.reduce((acc, cur) => acc + cur.num_contributors, 0);
        monthly_data[1][3] = totalContributions;
        monthly_data[1][6] = totalContributors;
        // write out bigquery data pre-sorted by contributors DESC
        let {googleContributors, googleContributions} = writeRows(monthly_data, rows);
        monthly_data[1][4] = googleContributions / totalContributions;
        monthly_data[1][7] = googleContributors / totalContributors;
        butorData[monthLabel] = monthly_data;
        // re-sort the data by contributions
        rows.sort((a, b) => b.contributions - a.contributions);
        writeRows(monthly_data, rows);
        butionData[monthLabel] = monthly_data;
    }
    console.log(`... writing intermediary raw project activity to ${argv.activityFile}...`);
    let activityData = [];
    months.forEach((m) => {
        activityData.push(butorData[m]);
    });
    activityData = activityData.flat();
    await activityCsv.writeRecords(activityData);
    let graphHeader = ['Company'].concat(months);
    const contributorCsv = csv_writer({
        path: argv.contributorFile,
        header: graphHeader,
        alwaysQuote: true
    });
    const contributionCsv = csv_writer({
        path: argv.contributionFile,
        header: graphHeader,
        alwaysQuote: true
    });
    // crunch through monthly data and compile into a format that we can import
    // into Google Sheets and easily graph
    let butorRows = [new Array(months.length), new Array(months.length), new Array(months.length)];
    let butionRows = [new Array(months.length), new Array(months.length), new Array(months.length)];
    butorRows[0][0] = 'sum single user companies';
    butorRows[1][0] = 'google';
    butorRows[2][0] = 'N/A';
    const cutoff = 0.02;
    butionRows[0][0] = `sum <${cutoff * 100}% contribution share companies`;
    butionRows[1][0] = 'google';
    butionRows[2][0] = 'N/A';
    console.log('...crunching compiled data into graphable CSV format...');
    for (let i = 0; i < months.length; i++) {
        let month = months[i];
        // FYI: need to ignore first row (headers) and first column (spacer) of rawData
        // look at contributor data first
        let data = butorData[month].slice(1);
        let singleActorCos = data.filter((r) => r[5] === 1);
        let numSingleActors = singleActorCos.length;
        butorRows[0][i + 1] = numSingleActors;
        let standoutCos = data.filter((r) => r[5] > 1);
        standoutCos.forEach((row) => {
            let co = row[1];
            let butors = row[5];
            let index = butorRows.findIndex((r) => r[0] === (co === '' ? 'N/A' : co));
            if (index === -1) {
                // co not in butor row set yet, need to add a new row.
                let newEntry = new Array(months.length);
                newEntry[0] = co;
                newEntry[i + 1] = butors;
                butorRows.push(newEntry);
            } else {
                // co already in butor row set, add an entry there.
                butorRows[index][i + 1] = butors;
            }
        });
        // next lets deal with contribution data as we have to re-sort the data
        data = butionData[month].slice(1);
        let totalButions = data[0][3];
        let threshold = Math.floor(totalButions * cutoff); // start highlighting companies after they cross <cutoff>% of all contributions to the project per month
        let lowShareCos = data.filter((r) => r[2] < threshold);
        let sumButionsLowShareCos = lowShareCos.reduce((acc, cur) => acc + cur[2], 0);
        butionRows[0][i + 1] = sumButionsLowShareCos;
        standoutCos = data.filter((r) => r[2] >= threshold);
        standoutCos.forEach((row) => {
            let co = row[1];
            let butions = row[2];
            let index = butionRows.findIndex((r) => r[0] === (co === '' ? 'N/A' : co));
            if (index === -1) {
                // co not in bution row set yet, need to add a new row.
                let newEntry = new Array(months.length);
                newEntry[0] = co;
                newEntry[i + 1] = butions;
                butionRows.push(newEntry);
            } else {
                // co already in butor row set, add an entry there.
                butionRows[index][i + 1] = butions;
            }
        });
    }
    await contributorCsv.writeRecords(butorRows);
    console.log(`...wrote ${argv.contributorFile}...`);
    await contributionCsv.writeRecords(butionRows);
    console.log(`...wrote ${argv.contributionFile}...`);
    console.log('...complete.');
};
