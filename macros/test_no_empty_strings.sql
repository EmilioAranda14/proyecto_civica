{% test no_empty_strings(model, column_name) %}

select *
from {{ model }}
where trim(coalesce({{ column_name }}, '')) = ''

{% endtest %}


