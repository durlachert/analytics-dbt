{% test non_negative(model, column_name) %}

with validation as (
  select
    {{ column_name }} as value
  from {{ model }}
  where {{ column_name }} < 0
)

select * from validation

{% endtest %}
