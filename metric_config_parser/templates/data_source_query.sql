{% include 'data_source_macros.j2' %}

(
SELECT
    *
FROM
    {{ data_source_query(data_source) }}
)
