(
SELECT
    *
FROM
    (
    SELECT
        *
    FROM
        (SELECT 1) AS main
    WHERE
        submission_date = '2023-01-01'
    )
)