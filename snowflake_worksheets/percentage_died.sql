use warehouse final_project;

SELECT COUNTRY_REGION, SUM(deaths) AS total_deaths, MAX(POPULATION) - SUM(deaths) AS remaining_population, ROUND((SUM(deaths) / MAX(POPULATION)) * 100, 4) AS percentage_died FROM ECDC_GLOBAL WHERE DEATHSFINAL_PROJECT > 0 GROUP BY COUNTRY_REGION ORDER BY percentage_died DESC