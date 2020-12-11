#standardSQL
CREATE TEMP FUNCTION IsInternal(company STRING, repo STRING) AS (
  CASE
    WHEN company = 'Amazon' THEN (STARTS_WITH(repo, 'aws/') OR STARTS_WITH(repo, 'amzn/')  OR STARTS_WITH(repo, 'aws-quickstart/') OR STARTS_WITH(repo, 'awsdocs/') OR STARTS_WITH(repo, 'aws-cloudformation/') OR STARTS_WITH(repo, 'aws-robotics/') OR STARTS_WITH(repo, 'aws-amplify/') OR STARTS_WITH(repo, 'aws-actions/') OR STARTS_WITH(repo, 'awslabs/') OR STARTS_WITH(repo, 'aws-samples/') OR STARTS_WITH(repo, 'amazon/') OR STARTS_WITH(repo, 'amazon-connect/') OR STARTS_WITH(repo, 'amazon-research/') OR STARTS_WITH(repo, 'amazonlinux/') OR STARTS_WITH(repo, 'bottlerocket-os/') OR STARTS_WITH(repo, 'alexa/') OR STARTS_WITH(repo, 'boto/') OR STARTS_WITH(repo, 'firecracker-microvm/'))
    WHEN company = 'Microsoft' THEN (STARTS_WITH(repo, 'microsoft/') OR STARTS_WITH(repo, 'microsoftdocs/') OR STARTS_WITH(repo, 'microsoft-thirdparty') OR STARTS_WITH(repo, 'microsoftgraph/') OR STARTS_WITH(repo, 'microsoftlearning/') OR STARTS_WITH(repo, 'azure/') OR STARTS_WITH(repo, 'microsoftedge/') OR STARTS_WITH(repo, 'microsoftdx/') OR STARTS_WITH(repo, 'microsofttranslator/') OR STARTS_WITH(repo, 'ms-iot/') OR STARTS_WITH(repo, 'microsoft-cisl/') OR STARTS_WITH(repo, 'microsoftcontentmoderator/') OR STARTS_WITH(repo, 'microsoftresearch/') OR STARTS_WITH(repo, 'sharepoint/') OR STARTS_WITH(repo, 'microsoft-search/') OR STARTS_WITH(repo, 'powershell/'))
    WHEN company = 'Google' THEN (STARTS_WITH(repo, 'google/') OR STARTS_WITH(repo, 'googlecloudplatform/') OR STARTS_WITH(repo, 'googleapis/') OR STARTS_WITH(repo, 'googlecodelabs/') OR STARTS_WITH(repo, 'googlechromelabs/') OR STARTS_WITH(repo, 'googlefonts/') OR STARTS_WITH(repo, 'google-research/') OR STARTS_WITH(repo, 'googletrends/') OR STARTS_WITH(repo, 'googleads/') OR STARTS_WITH(repo, 'googlesamples/') OR STARTS_WITH(repo, 'googlecreativelab/') OR STARTS_WITH(repo, 'google-research-datasets/') OR STARTS_WITH(repo, 'googlemaps/') OR STARTS_WITH(repo, 'googlechrome/') OR STARTS_WITH(repo, 'googleworkspace/') OR STARTS_WITH(repo, 'googlewebcomponents/') OR STARTS_WITH(repo, 'googleanalytics/') OR STARTS_WITH(repo, 'google-developer-training/') OR STARTS_WITH(repo, 'googlecontainertools/') OR STARTS_WITH(repo, 'googlevr/') OR STARTS_WITH(repo, 'googleglass/') OR STARTS_WITH(repo, 'google-pay/') OR STARTS_WITH(repo, 'actions-on-google/') OR STARTS_WITH(repo, 'google-ar/') OR STARTS_WITH(repo, 'googleclouddataproc/') OR STARTS_WITH(repo, 'googlestadia/') OR STARTS_WITH(repo, 'google-cloudsearch/') OR STARTS_WITH(repo, 'googlecast/'))
    WHEN CAST(STRPOS(company, 'Adobe') AS BOOL) THEN (STARTS_WITH(repo, 'adobe') OR STARTS_WITH(repo, 'magento') OR STARTS_WITH(repo, 'phonegap'))
    WHEN CAST(STRPOS(LOWER(company), 'airbnb') AS BOOL) THEN (STARTS_WITH(repo, 'airbnb'))
    WHEN CAST(STRPOS(LOWER(company), 'autodesk') AS BOOL) THEN (STARTS_WITH(repo, 'autodesk') OR STARTS_WITH(repo, 'dynamods') OR STARTS_WITH(repo, 'developer-autodesk') OR STARTS_WITH(repo, 'shotgunsoftware'))
    WHEN CAST(STRPOS(LOWER(company), 'dropbox') AS BOOL) THEN (STARTS_WITH(repo, 'dropbox'))
    WHEN CAST(STRPOS(LOWER(company), 'facebook') AS BOOL) THEN (STARTS_WITH(repo, 'facebook'))
    WHEN CAST(STRPOS(LOWER(company), 'intuit') AS BOOL) THEN (STARTS_WITH(repo, 'intuit'))
    WHEN CAST(STRPOS(LOWER(company), 'netflix') AS BOOL) THEN (STARTS_WITH(repo, 'netflix'))
    WHEN CAST(STRPOS(LOWER(company), 'pinterest') AS BOOL) THEN (STARTS_WITH(repo, 'pinterest'))
    WHEN CAST(STRPOS(LOWER(company), 'workday') AS BOOL) THEN (STARTS_WITH(repo, 'workday'))
    WHEN CAST(STRPOS(LOWER(company), 'salesforce') AS BOOL) THEN (STARTS_WITH(repo, 'salesforce') OR STARTS_WITH(repo, 'forcedotcom') OR STARTS_WITH(repo, 'forceworkbench') OR STARTS_WITH(repo, 'sfdo-tooling') OR STARTS_WITH(repo, 'heroku') OR STARTS_WITH(repo, 'developerforce') OR STARTS_WITH(repo, 'dreamhouseapp') OR STARTS_WITH(repo, 'trailheadapps'))
    WHEN CAST(STRPOS(LOWER(company), 'sendgrid') AS BOOL) OR CAST(STRPOS(LOWER(company), 'twilio') AS BOOL) THEN (STARTS_WITH(repo, 'twilio') OR STARTS_WITH(repo, 'sendgrid'))
    ELSE FALSE
  END
);
CREATE TEMP FUNCTION MassageCompany(company STRING) AS (
  CASE
    WHEN CAST(STRPOS(LOWER(company), 'adobe') AS BOOL) THEN 'Adobe'
    WHEN CAST(STRPOS(LOWER(company), 'autodesk') AS BOOL) THEN 'Autodesk'
    WHEN CAST(STRPOS(LOWER(company), 'dropbox') AS BOOL) THEN 'Dropbox'
    WHEN CAST(STRPOS(LOWER(company), 'twilio') AS BOOL) OR CAST(STRPOS(LOWER(company), 'sendgrid') AS BOOL) THEN 'Twilio'
    WHEN (NOT CAST(STRPOS(LOWER(company), 'intuitive') AS BOOL)) AND CAST(STRPOS(LOWER(company), 'intuit') AS BOOL) THEN 'Intuit'
    WHEN CAST(STRPOS(LOWER(company), 'pinterest') AS BOOL) THEN 'Pinterest'
    ELSE company
  END
);
WITH
period AS (
  SELECT *
  FROM (SELECT * FROM `githubarchive.month.201801` UNION ALL SELECT * FROM `githubarchive.month.201802` UNION ALL SELECT * FROM `githubarchive.month.201803`# UNION ALL SELECT * FROM `githubarchive.month.202004` UNION ALL SELECT * FROM `githubarchive.month.202005` UNION ALL SELECT * FROM `githubarchive.month.202006` UNION ALL SELECT * FROM `githubarchive.month.202007` UNION ALL SELECT * FROM `githubarchive.month.202008` UNION ALL SELECT * FROM `githubarchive.month.202009` UNION ALL SELECT * FROM `githubarchive.month.202010`  UNION ALL SELECT * FROM `githubarchive.month.202011`
  ) a
),
repo_stars AS (
  SELECT repo.id, COUNT(DISTINCT actor.login) stars, APPROX_TOP_COUNT(repo.name, 1)[OFFSET(0)].value repo_name
  FROM period
  WHERE type='WatchEvent'
  GROUP BY 1
  HAVING stars > 0 # only look at repos that had X new stars over the selected time period
),
user_repo_pushes AS (
  SELECT actor.login, b.repo_name, COUNT(*) as pushes
    FROM period a
    JOIN repo_stars b ON a.repo.id = b.id
    WHERE type='PushEvent'
    GROUP BY login, repo_name
),
company_pushes AS (
  SELECT *, MassageCompany(z.company) as co,
  FROM user_repo_pushes y
  JOIN `public-github-adobe.github_archive_query_views.users_companies_2018` z ON y.login = z.user
),
int_pushes AS (
  SELECT co, SUM(pushes) as pushes
  FROM company_pushes
  WHERE IsInternal(co, repo_name)
  GROUP BY co
),
ext_pushes AS (
  SELECT co, SUM(pushes) as pushes
  FROM company_pushes
  WHERE NOT IsInternal(co, repo_name)
  GROUP BY co
)
SELECT int_pushes.co as company, int_pushes.pushes as int_pushes, ext_pushes.pushes as ext_pushes, (int_pushes.pushes + ext_pushes.pushes) as total_pushes
FROM int_pushes
JOIN ext_pushes ON int_pushes.co = ext_pushes.co
ORDER BY total_pushes DESC
