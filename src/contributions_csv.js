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
const DATASET_ID = 'github_archive_query_views';
const bigquery = new BigQuery({
    projectId: PROJECT_ID,
    keyFilename: 'bigquery.json'
});
const csv_writer = require('csv-writer').createArrayCsvWriter;

const quarters = ['2018_q1', '2018_q2', '2018_q3', '2018_q4', '2019_q1', '2019_q2', '2019_q3', '2019_q4', '2020_q1', '2020_q2'];
const header = ['Period', 'Total Contributions', 'External Contributions', 'Internal Contributions', 'Internal Contribution %', 'Total Contributors', 'External Contributors', 'External Contributor %', 'Internal Contributors', 'Internal Contributor %', 'Repo Rank', 'External Repo Contributed To', 'Repo Contributors', 'Repo Contributions', 'External Contribution %', 'External Contributor %'];

module.exports = async function (argv) {
    const csv = csv_writer({
        path: argv.output,
        header,
        alwaysQuote: true
    });

    for (let q = 0; q < quarters.length; q++) {
        let quarter = quarters[q];
        let q_label = quarter.replace(/_/g, ' ').toUpperCase();
        let query = `CREATE TEMP FUNCTION DistinctCount(arr ANY TYPE) AS (
          (SELECT COUNT(DISTINCT x) FROM UNNEST(arr) AS x)
        );

        WITH

        all_activity AS (SELECT repo, DistinctCount(ARRAY_CONCAT_AGG( contributors)) contributors, SUM(contributions) contributions
        FROM \`github_archive_query_views.company_repo_contributions_${quarter}\`
        WHERE ${argv.company}
        GROUP BY repo
        ORDER BY contributors DESC, contributions DESC
        ),
        internal_repos AS (SELECT * FROM all_activity WHERE ${argv.internal}),
        external_repos AS (SELECT * FROM all_activity WHERE NOT ${argv.internal})
        SELECT "${header[1]}", SUM(contributions) FROM all_activity UNION ALL
        SELECT "${header[3]}", SUM(contributions) FROM internal_repos UNION ALL
        SELECT "${header[5]}", SUM(contributors) FROM all_activity UNION ALL
        SELECT "${header[6]}", SUM(contributors) FROM external_repos UNION ALL
        SELECT "${header[8]}", SUM(contributors) FROM internal_repos`;
        console.log(`Querying contributor counts for ${q_label}...`);
        let result = await bigquery.query(query);
        console.log('...complete.');
        let rows = result[0];
        let quarterly_data = new Array(10); // 10 rows per quarter, for top 10 repos to be added later
        for (let i = 0; i < 10; i++) {
            quarterly_data[i] = new Array(header.length);
        }
        quarterly_data[0][0] = q_label;
        rows.forEach((row) => {
            const column = row.f0_;
            const value = row.f1_;
            quarterly_data[0][header.indexOf(column)] = value;
        });
        quarterly_data[0][2] = quarterly_data[0][1] - quarterly_data[0][3]; // total contributions minus internal contributions equals external contributions
        quarterly_data[0][4] = quarterly_data[0][3] / quarterly_data[0][1]; // internal contribution percentage
        quarterly_data[0][7] = quarterly_data[0][6] / quarterly_data[0][5]; // external contributor percentage
        quarterly_data[0][9] = quarterly_data[0][8] / quarterly_data[0][5]; // external contributor percentage

        // now compile top ten external projects per company
        let repo_query = `CREATE TEMP FUNCTION DistinctCount(arr ANY TYPE) AS (
          (SELECT COUNT(DISTINCT x) FROM UNNEST(arr) AS x)
        );

        WITH
        all_activity AS (SELECT repo, DistinctCount(ARRAY_CONCAT_AGG( contributors)) contributors, SUM(contributions) contributions
        FROM \`github_archive_query_views.company_repo_contributions_${quarter}\`
        WHERE ${argv.company}
        GROUP BY repo
        ORDER BY contributors DESC, contributions DESC
        )
        SELECT * FROM all_activity WHERE NOT ${argv.internal} LIMIT 10`;
        console.log(`Querying top projects for ${q_label}...`);
        result = await bigquery.query(repo_query);
        console.log('...complete.');
        rows = result[0];
        for (let i = 0; i < 10; i++) {
            quarterly_data[i][10] = i + 1; // repo rank
            quarterly_data[i][11] = rows[i].repo;
            quarterly_data[i][12] = rows[i].contributors;
            quarterly_data[i][13] = rows[i].contributions;
            quarterly_data[i][14] = rows[i].contributions / quarterly_data[0][2]; // percentage of external contributions that went to this repo
            quarterly_data[i][15] = rows[i].contributors / quarterly_data[0][6]; // percentage of external contributors active on this repo
        }
        await csv.writeRecords(quarterly_data);
    }
    console.log('Finished writing all data to', argv.output);
};
