(
    SELECT
        *
    FROM
        ({{ data_source.from_expression }})
    {% if where -%}
    WHERE
        {{ where }}
    {% endif -%}
)
