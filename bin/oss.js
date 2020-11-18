#!/usr/bin/env node
/*
Copyright 2019 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/
const yargs = require('yargs');
const db_to_bigquery = require('../src/db-to-bigquery.js');
const csv_to_bigquery = require('../src/csv-to-bigquery.js');
const update_db = require('../src/update-db.js');
const rank = require('../src/rank.js');
const contributions_csv = require('../src/contributions_csv.js');
const bigquery_to_dynamo = require('../src/bigquery-to-dynamo.js');

yargs
    .command('company-contributions-csv', 'Compile numbers on the amount of GitHub contributions (issues, PRs and code pushed) from a company to public GitHub repos, differentiating between internal vs. external repository activity. Bucketed by quarter, starting from 2018-Q1. Outputs to a CSV file.', {
        output: {
            alias: 'o',
            default: 'contributions.csv',
            desc: 'Filename to write CSV data to.'
        },
        internal: {
            alias: 'i',
            desc: 'BigQuery-compatible WHERE clause to identify GitHub repositories as internal to the company. Recommended to assemble an expression that matches on the repository slug, that is, both the organization and repository name (e.g. the `repo` variable will have values such as adobe/react-spectrum, octokit/rest.js or microsoft/TypeScript). Include only the portion of the clause _after_ WHERE (the program will prefix your provided clause with NOT to determine repos external to the company). Take care! One can easily SQL inject themselves here! Example for identifying internal Twilio repos:\n"(STARTS_WITH(LOWER(repo), \'twilio\') OR STARTS_WITH(LOWER(repo), \'sendgrid\'))"',
            demandOption: 'You must provide a WHERE clause to identify internal repositories!'
        },
        company: {
            alias: 'c',
            desc: 'BigQuery-compatible WHERE clause to identify users as part of a company based on their GitHub profile\'s Company field. Include only the portion of the clause _after_ WHERE. Take care! One can easily SQL inject themselves here! Example for identifying Twilio users:\n"(company LIKE \'%twilio%\' OR company LIKE \'%sendgrid%\')"',
            demandOption: 'You must provide a WHERE clause to filter users by their company!'
        },
        googlecloud: {
            alias: 'g',
            desc: 'Path to Google Cloud credentials JSON file. For information on how to generate this file, see https://cloud.google.com/docs/authentication/getting-started',
            demandOption: 'You must provide a path to a Google Cloud credentials JSON file!'
        }
    }, contributions_csv)
    .command('bigquery-to-dynamo', 'Move data from BigQuery users-companies style tables to AWS DynamoDB', {
        source: {
            alias: 's',
            demandOption: 'You must provide a BigQuery table name as a source!',
            desc: 'BigQuery table name housing user-company associations'
        },
        startdate: {
            demandOption: 'You must provide a startdate in UNIX timestamp epoch (milliseconds)',
            desc: 'Start date, as a number, will be blased into dynamo'
        },
        destination: {
            alias: 'd',
            demandOption: 'You must provide a Dynamo table name as a destination ',
            'desc': 'DynamoDB table name for destination of data'
        },
        region: {
            alias: 'r',
            demandOption: 'You must provide an AWS region string',
            desc: 'AWS region string, i.e. us-east-2'
        }
    }, bigquery_to_dynamo)
    .command('rank-corporations <source> [limit]', false/* 'show top [limit] companies based on number of active GitHubbers, parsed from the <source> BigQuery table'*/, {
        source: {
            alias: 's',
            demandOption: 'You must provide a BigQuery table name as a GitHub.com activity source!',
            desc: 'BigQuery table name housing GitHub.com activity data'
        },
        limit: {
            alias: 'l',
            desc: 'How many top companies to show?',
            default: null
        }
    }, rank)
    .command('db-to-bigquery <output>', 'ADMINS ONLY! Send user-to-company associations to a BigQuery table', {
        output: {
            alias: 'o',
            default: 'users_companies',
            desc: 'BigQuery table to send data to'
        }
    }, db_to_bigquery)
    .command('update-db <source>', 'ADMINS ONLY! Update user-to-company database based on a BigQuery source table', {
        source: {
            alias: 's',
            demandOption: 'You must provide a BigQuery table name as a GitHub.com activity source!',
            desc: 'BigQuery table name housing GitHub.com activity data'
        },
        drop: {
            alias: 'D',
            default: false,
            desc: 'Drop the temporary usercompany MySQL table'
        }
    }, update_db)
    .command('csv-to-bigquery <input> <output>', 'Send a csv to bigquery', {
        output: {
            alias: 'o',
            demandOption: 'You must provide a BigQuery table name to send data to',
            desc: 'BigQuery table to send data to'
        },
        input: {
            alias: 'i',
            demandOption: 'You must provide a CSV file to use as input',
            desc: 'Input .csv file to send to BigQuery'
        }
    }, csv_to_bigquery)
    .command('update-db <source>', 'ADMINS ONLY! Update user-to-company database based on a BigQuery source table', {
        source: {
            alias: 's',
            demandOption: 'You must provide a BigQuery table name as a GitHub.com activity source!',
            desc: 'BigQuery table name housing GitHub.com activity data'
        },
        drop: {
            alias: 'D',
            default: false,
            desc: 'Drop the temporary usercompany MySQL table'
        }
    }, update_db)
    .env('OSS')
    .option('db-server', {
        alias: 'd',
        default: 'leopardprdd',
        desc: 'Database server name'
    })
    .option('db-user', {
        alias: 'u',
        default: 'GHUSERCO',
        desc: 'Database username'
    })
    .option('db-password', {
        type: 'string'
    })
    .option('db-name', {
        alias: 'n',
        default: 'GHUSERCO',
        desc: 'Database name'
    })
    .option('table-name', {
        alias: 't',
        default: 'usercompany',
        desc: 'Database table name'
    })
    .option('db-port', {
        alias: 'p',
        default: 3323,
        desc: 'Database port'
    })
    .argv;
