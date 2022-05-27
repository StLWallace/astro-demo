SELECT dim_2.code1 as dim_2_code1
{%- for i in range(1, 6) %}
,SUM(metric_{{ i }}) AS total_metric_{{ i }}
{%- endfor %}

FROM {{ params.wide_table }}
GROUP BY 1