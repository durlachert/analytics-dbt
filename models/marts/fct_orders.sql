{{ config(materialized='table') }}


select
CUSTOMER_ID,
count(*) as order_count,
sum(TOTAL_AMOUNT) as total_spend
from {{ ref('stg_orders') }}
group by 1
