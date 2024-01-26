SELECT
    COUNTRY_REGION,
    START_DATE,
    END_DATE,
    START_CASES,
    END_CASES
FROM ECDC_GLOBAL_WEEKLY
MATCH_RECOGNIZE (
    PARTITION BY COUNTRY_REGION
    ORDER BY DATE
    MEASURES
        FIRST(A.DATE) AS START_DATE,
        LAST(A.DATE) AS END_DATE,
        A.CASES_WEEKLY AS START_CASES,
        B.CASES_WEEKLY AS END_CASES
    ONE ROW PER MATCH
    PATTERN (A B+)
    DEFINE
        B AS B.CASES_WEEKLY > A.CASES_WEEKLY
);