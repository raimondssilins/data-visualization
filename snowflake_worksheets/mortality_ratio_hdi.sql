SELECT
    A.COUNTRY_REGION,
    B.HDI,
    A.percentage_died,
    ROUND((A.percentage_died / B.HDI) * 100, 3) AS ratio
FROM (
    SELECT
        COUNTRY_REGION,
        SUM(deaths) AS total_deaths,
        MAX(POPULATION) - SUM(deaths) AS remaining_population,
        ROUND((SUM(deaths) / MAX(POPULATION)) * 100, 3) AS percentage_died
    FROM
       COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL
    GROUP BY
        COUNTRY_REGION
    HAVING
        percentage_died > 0
) A
JOIN (
    SELECT DISTINCT
        COUNTRY,
        HDI
    FROM
        FINAL_PROJECT.PUBLIC.ECONOMIC_DATA
    WHERE
        HDI > 0
) B
ON
    A.COUNTRY_REGION = B.COUNTRY
ORDER BY
    ratio DESC;