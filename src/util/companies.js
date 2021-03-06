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

module.exports = {
  map: {
    '': 'Apple',
    zendesk: 'Zendesk',
    zeit: 'Zeit',
    zalando: 'Zalando DE',
    yelp: 'Yelp',
    yandex: 'Yandex',
    yahoo: 'Yahoo!',
    xamarin: 'Microsoft',
    wso2: 'WSO2',
    workday: 'Workday',
    wix: 'WIX',
    wikimedia: 'WikiMedia Foundation',
    wechat: 'WeChat',
    vmware: 'VMWare',
    unity: 'Unity Technologies',
    uber: 'Uber',
    twitter: 'Twitter',
    twilio: 'Twilio',
    travis: 'Travis CI',
    thoughtworks: 'ThoughtWorks',
    tencent: 'Tencent',
    telerik: 'Telerik',
    suse: 'SUSE',
    stripe: 'Stripe',
    square: 'Square, Inc.',
    spotify: 'Spotify',
    snapchat: 'Snapchat',
    slack: 'Slack',
    shopify: 'Shopify',
    sendgrid: 'Twilio',
    sap: 'SAP SE',
    samsung: 'Samsung',
    salesforce: 'Salesforce',
    redhat: 'Red Hat',
    'red hat': 'Red Hat',
    rackspace: 'Rackspace',
    puppet: 'Puppet Labs',
    pivotal: 'VMWare',
    paypal: 'PayPal',
    palantir: 'Palantir Technologies',
    oracle: 'Oracle',
    'opendistro-for-elasticsearch': 'Amazon',
    odoo: 'Odoo',
    oculus: 'Facebook',
    observable: 'Observable',
    nvidia: 'NVIDIA',
    nuxeo: 'Nuxeo',
    nokia: 'Nokia',
    netflix: 'Netflix',
    netease: 'NetEase',
    mozilla: 'Mozilla',
    mongodb: 'MongoDB',
    microsoft: 'Microsoft',
    mapbox: 'Mapbox',
    magento: 'Adobe',
    lyft: 'Lyft',
    linkedin: 'LinkedIn',
    liferay: 'Liferay',
    kitware: 'Kitware',
    jetbrains: 'JetBrains',
    intel: 'Intel',
    inria: 'INRIA',
    indeed: 'Indeed',
    ibm: 'IBM',
    huawei: 'Huawei',
    guardian: 'The Guardian',
    grafana: 'Grafana Labs',
    google: 'Google',
    gitlab: 'GitLab',
    github: 'Microsoft',
    fotolia: 'Adobe',
    facebook: 'Facebook',
    'european bioinformatics': 'EMBL-EBI',
    'eth z': 'ETH Zurich',
    esri: 'ESRI',
    ericsson: 'Ericsson',
    epfl: 'EPFL',
    epam: 'EPAM Systems',
    embl: 'EMBL-EBI',
    elastic: 'Elastic',
    'duo security': 'Cisco Systems',
    docker: 'Docker',
    datadog: 'DataDog',
    cnrs: 'CNRS',
    cisco: 'Cisco Systems',
    'capital one': 'Capital One',
    canonical: 'Canonical',
    'booking.com': 'Booking.com',
    behance: 'Adobe',
    bbc: 'BBC',
    baidu: 'Baidu',
    azure: 'Microsoft',
    aws: 'Amazon',
    aviary: 'Adobe',
    automattic: 'Automattic',
    atlassian: 'Atlassian',
    apple: 'Apple',
    andela: 'Andela',
    amzn: 'Amazon',
    amazon: 'Amazon',
    alibaba: 'Alibaba',
    airbnb: 'Airbnb',
    adobe: 'Adobe',
    accenture: 'Accenture'
  },
  is_empty: (co) => {
    if (!co || co.length === 0) return true;
    let n = co.toLowerCase();
    return n === 'null' || n === 'na' || n === 'n/a' || n === 'none' || n === '-' || n === 'no' || n === '.' || n === 'freelance' || n === 'freelancer' || n === 'nil';
  },
  is_corporation: (co) => {
    let n = co.toLowerCase();
    return n.indexOf('freelance') === -1 && n.indexOf('student') === -1 &&
        n.indexOf('university') === -1 && n.indexOf('self') === -1 &&
        n.indexOf('personal') === -1 && n !== 'mit' && n !== 'uc berkeley' &&
        n !== 'china' && n !== 'japan' && n.indexOf('college') === -1 &&
        n !== 'ucla' && n.indexOf('independent') === -1 && n !== 'sjtu' &&
        n !== 'virginia tech' && n !== 'myself' && n.indexOf('institute of tech') === -1 &&
        n !== 'georgia tech' && n !== 'uc davis' && n !== 'ucsd' && n !== 'uc san diego' &&
        n.indexOf('individual') === -1;
  },
  ignore: {
    'Unity Technologies': /[a-z\-]unity/i,
    'Square, Inc.':/squared|[a-z\-]square/i,
    'Microsoft': /\.github\.io|github\.com\/[a-z0-9]|MicrosoftSTC|[a-z]Xamarin|Xamarin[ -]*[a-z]/i,
    'Intel': /intel[a-z]/i,
    'Facebook': /[a-z]oculus|oculusdei|oculus info|oculus pc/i,
    'Elastic': /opendistro/i,
    'Cisco Systems': /francisco/i,
    'Amazon': /[a-z\-]aws|aws[a-z\-]|amazonas/i
  },
  catch_all: /opendistro-for-elasticsearch|microsoft|google|amazon|amzn|aws|red hat|redhat|ibm|pivotal|intel|facebook|alibaba|uber|wix|github|tencent|baidu|apple||mozilla|oracle|shopify|mongodb|vmware|netflix|salesforce|linkedin|palantir|yahoo|mapbox|unity|automattic|travis|spotify|zalando|esri|sap|epam|telerik|stripe|kitware|suse|odoo|yandex|adobe|airbnb|guardian|docker|nuxeo|nvidia|elastic|yelp|wso2|inria|puppet|datadog|jetbrains|canonical|rackspace|thoughtworks|andela|liferay|epfl|cnrs|embl|european bioinformatics|accenture|cisco|ericsson|capital one|huawei|booking\.com|netease|bbc|nokia|zendesk|paypal|eth z|samsung|slack|twitter|lyft|snapchat|zeit|atlassian|square|wechat|wikimedia|observable|magento|behance|aviary|fotolia|twilio|workday|grafana|gitlab|oculus|indeed/gi,
  match: function (str) {
    let m = null;
    if (!module.exports.is_empty(str)) {
      let regexMatch = str.match(module.exports.catch_all);
      if (regexMatch) {
        let label = module.exports.map[regexMatch[0].toLowerCase()];
        if (module.exports.ignore[label]) {
          if (!str.match(module.exports.ignore[label])) {
            m = label;
          }
        } else {
          m = label;
        }
      }
    }
    return m;
  }
};
