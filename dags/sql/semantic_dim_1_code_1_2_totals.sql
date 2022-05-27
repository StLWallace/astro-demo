SELECT dim_1.code1 as dim_1_code1
,dim_1.code2 as dim_1_code2
{%- for i in range(1, 6) %}
,SUM(metric_{{ i }}) AS total_metric_{{ i }}
{%- endfor %}

FROM {{ params.wide_table }}
GROUP BY 1, 2