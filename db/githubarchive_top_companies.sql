SELECT company, SUM(githubers) as githubbers
FROM `github_archive_query_views.company_pushes_repos_2020`
WHERE company IS NOT NULL AND LOWER(company) NOT LIKE '%freelance%' AND company <> '' AND LOWER(company) NOT LIKE '%none%' and LOWER(company) NOT LIKE '%student%' and LOWER(company) NOT LIKE '%university%' and LOWER(company) NOT LIKE '%n/a%' AND company <> '-' and LOWER(company) NOT LIKE '%self%' AND LOWER(company) NOT LIKE '%personal%' and LOWER(company) <> 'mit' and LOWER(company) <> 'uc berkeley' and LOWER(company) <> 'china' AND LOWER(company) NOT LIKE '%college%' and LOWER(company) <> 'private' and LOWER(company) <> 'no' and LOWER(company) <> 'ucla' and LOWER(company) NOT LIKE '%independent%' and LOWER(company) <> 'sjtu' and LOWER(company) <> 'virginia tech' and LOWER(company) <> 'myself' and LOWER(company) NOT LIKE '%institute of tech%' and LOWER(company) <> 'georgia tech' and LOWER(company) <> 'uc davis' and LOWER(company) <> 'na' and LOWER(company) <> 'ucsd' and LOWER(company) <> 'uc san diego' and LOWER(company) NOT LIKE '%individual%' and LOWER(company) <> 'japan' and LOWER(company) <> 'null'
GROUP BY company
ORDER BY githubbers DESC
