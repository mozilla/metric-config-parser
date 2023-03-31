(
{% for data_source_slug, data_source_info in metrics_per_data_source.items() -%}
{{ "WITH" if loop.first else "" }} {{ data_source_slug }} AS (
    SELECT
        {{ data_source_info["data_source"].client_id_column }} AS client_id,
        {{ data_source_info["data_source"].submission_date_column }} AS submission_date,
        {% for dimension, dimension_sql in group_by.items() -%}
        {{ dimension_sql }} AS {{ dimension }},
        {% endfor -%}
        {% for metric in data_source_info["metrics"] -%}
        {{ metric.select_expression }} AS {{ metric.name }},
        {% endfor %}
    FROM
        {{ data_source_info["data_source"].from_expression }}
    {% if where -%}
    WHERE
        {{ where }}
    {% endif -%}
    GROUP BY    
        {% for dimension, dimension_sql in group_by.items() -%}
        {{ dimension }},
        {% endfor -%}
        client_id,
        submission_date
){{ "," if not loop.last else "" }}
{% endfor -%}

{% for data_source_slug, data_source_info in metrics_per_data_source.items() -%}
{% if loop.first -%}
SELECT
    {{ metrics_per_data_source.keys() | first }}.client_id,
    {{ metrics_per_data_source.keys() | first }}.submission_date,
    {% for dimension, dimension_sql in group_by.items() -%}
    {{ metrics_per_data_source.keys() | first }}.{{ dimension_sql }} AS {{ dimension }},
    {% endfor -%}
    {% for d, data_source_info in metrics_per_data_source.items() -%}
    {% for metric in data_source_info["metrics"] -%}
    {{ metric.name }},
    {% endfor -%}
    {% endfor %}
FROM
    {{ metrics_per_data_source.keys() | first }}
{% else -%}
    FULL OUTER JOIN {{ data_source_slug }}
    ON
        {{ data_source_slug }}.submission_date = {{ metrics_per_data_source.keys() | first }}.submission_date AND
        {{ data_source_slug }}.client_id = {{ metrics_per_data_source.keys() | first }}.client_id 
        {% for dimension, dimension_sql in group_by.items() -%}
        AND {{ data_source_slug }}.{{ dimension }} = {{ metrics_per_data_source.keys() | first }}.{{ dimension }} 
        {% endfor -%}
{% endif -%}
{% endfor -%}
)