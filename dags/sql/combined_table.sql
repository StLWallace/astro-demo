SELECT ft.*
{%- for table in params.tables %}
,(SELECT AS STRUCT dim_{{ loop.index }}.*) AS dim_{{ loop.index }}
{%- endfor %}
FROM {{ params.fact_table }} ft
{%- for table in params.tables %}

JOIN {{ table }} dim_{{ loop.index }}
ON ft.id_{{ loop.index }} = dim_{{ loop.index }}.id
AND dim_{{ loop.index }}._PARTITIONTIME = PARSE_TIMESTAMP("%Y%m%d%H", '{{ dag_run.logical_date.strftime("%Y%m%d%H") }}')
{%- endfor %}

WHERE ft._PARTITIONTIME = PARSE_TIMESTAMP("%Y%m%d%H", '{{ dag_run.logical_date.strftime("%Y%m%d%H") }}')

