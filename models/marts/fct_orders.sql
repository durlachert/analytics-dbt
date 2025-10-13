{{ config(
    materialized='incremental',
    unique_key='CUSTOMER_ID',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    cluster_by=['CUSTOMER_ID']
) }}

with
all_orders as (
  select
    CUSTOMER_ID,
    ORDER_ID,
    ORDER_DATE,
    ORDER_STATUS,
    TOTAL_AMOUNT
  from {{ ref('stg_orders') }}
),

incremental_new_orders as (
  select *
  from {{ ref('stg_orders') }}
  {% if is_incremental() %}
    where ORDER_DATE >= (
      select coalesce(max(LAST_ORDER_DATE), to_date('1900-01-01'))
      from {{ this }}
    )
  {% endif %}
),

affected_customers as (
  {% if is_incremental() %}
    select distinct CUSTOMER_ID from incremental_new_orders
  {% else %}
    select distinct CUSTOMER_ID from all_orders
  {% endif %}
),

recalc as (
  select
    o.CUSTOMER_ID,
    count(*)            as ORDER_COUNT,
    sum(o.TOTAL_AMOUNT) as TOTAL_SPEND,
    min(o.ORDER_DATE)   as FIRST_ORDER_DATE,
    max(o.ORDER_DATE)   as LAST_ORDER_DATE
  from all_orders o
  where o.CUSTOMER_ID in (select CUSTOMER_ID from affected_customers)
  group by 1
)

select * from recalc
