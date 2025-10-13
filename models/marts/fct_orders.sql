{{ config(materialized='table') }}

select
  CUSTOMER_ID,
  count(*)               as order_count,
  sum(TOTAL_AMOUNT)      as total_spend,
  min(ORDER_DATE)        as first_order_date,
  max(ORDER_DATE)        as last_order_date
from {{ ref('stg_orders') }}
group by 1
