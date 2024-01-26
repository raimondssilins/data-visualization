use warehouse final_project;

SELECT state, SUM(total_population) AS total_population
FROM demographics
GROUP BY state
ORDER BY total_population DESC;
