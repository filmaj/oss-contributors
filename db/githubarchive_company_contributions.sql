#standardSQL
WITH
period AS (
  SELECT *
  FROM (SELECT * FROM `githubarchive.month.201801` UNION ALL SELECT * FROM `githubarchive.month.201802` UNION ALL SELECT * FROM `githubarchive.month.201803`) a
),
repo_stars AS (
  SELECT repo.id repo_id, COUNT(DISTINCT actor.login) stars, APPROX_TOP_COUNT(repo.name, 1)[OFFSET(0)].value repo_name
  FROM period
  WHERE type='WatchEvent'
  GROUP BY 1
  HAVING stars > 0 # only look at repos that had X new stars over the selected time period
),
contributions_to_projects AS (
  SELECT * FROM (
    SELECT repo.name repo, type, actor.login login, count(*) contributions
    FROM period a
    JOIN repo_stars b
    ON a.repo.id = b.repo_id
    WHERE type='PushEvent' OR type='PullRequestEvent' OR type='IssuesEvent' OR type='PullRequestReviewCommentEvent'
    GROUP BY repo, type, login
  ) z
  JOIN `github_archive_query_views.users_companies_2019` y
  ON z.login = y.user
)
SELECT type, company, repo, SUM(contributions) contributions, COUNT(DISTINCT login) num_contributors, ARRAY_AGG(login) contributors FROM contributions_to_projects GROUP BY type, company, repo ORDER BY contributions DESC
