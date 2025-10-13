{{ config(materialized='view') }}

select
  o.ORDER_ID,
  o.CUSTOMER_ID,
  o.ORDER_DATE,
  o.ORDER_STATUS,
  o.TOTAL_AMOUNT
from {{ source('raw', 'orders') }} as o
