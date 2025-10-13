#!/usr/bin/env python3
import os
import random
from faker import Faker
import snowflake.connector

ENV = os.getenv("ENV", "dev").lower()  # dev|stg|prod
DB = f"{ENV.upper()}_DB"
RAW = f"{DB}.RAW"

# Account identifier:
# 1) If SNOWFLAKE_ACCOUNT_IDENTIFIER is set (e.g., xy12345.eu-central-1), use it.
# 2) Else build <ORG>-<ACCOUNT> from org/account names.
ACCOUNT_IDENTIFIER = os.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER") or \
    f"{os.environ['SNOWFLAKE_ORGANIZATION_NAME']}-{os.environ['SNOWFLAKE_ACCOUNT_NAME']}"

USER = os.environ["SNOWFLAKE_USER"]
PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
ROLE = os.getenv("SNOWFLAKE_ROLE", f"{ENV.upper()}_ROLE")
WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", f"{ENV.upper()}_WH")

fake = Faker()

def get_conn():
    return snowflake.connector.connect(
        account=ACCOUNT_IDENTIFIER,
        user=USER,
        password=PASSWORD,
        role=ROLE,
        warehouse=WAREHOUSE,
        database=DB,
        schema="RAW",
    )

def bootstrap(cur):
    # DB + SCHEMAS were created by Terraform, but keep this idempotent.
    cur.execute(f'create database if not exists {DB}')
    cur.execute(f'create schema if not exists {RAW}')
    cur.execute(f"""
        create table if not exists {RAW}.CUSTOMERS (
            CUSTOMER_ID number,
            FIRST_NAME  string,
            LAST_NAME   string,
            EMAIL       string,
            SIGNUP_DATE date
        )
    """)
    cur.execute(f"""
        create table if not exists {RAW}.ORDERS (
            ORDER_ID     number,
            CUSTOMER_ID  number,
            ORDER_DATE   date,
            ORDER_STATUS string,
            TOTAL_AMOUNT number(10,2)
        )
    """)

def generate_customers(n=500):
    rows = []
    for cid in range(1, n + 1):
        rows.append((
            cid,
            fake.first_name(),
            fake.last_name(),
            fake.email(),
            fake.date_between(start_date='-2y', end_date='today')
        ))
    return rows

def generate_orders(customers, avg_orders=5):
    rows, oid = [], 1
    statuses = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
    for c in customers:
        cid = c[0]
        k = max(0, int(random.gauss(avg_orders, 2)))
        for _ in range(k):
            rows.append((
                oid,
                cid,
                fake.date_between(start_date='-2y', end_date='today'),
                random.choice(statuses),
                round(random.uniform(5, 500), 2)
            ))
            oid += 1
    return rows

def load(cur, table, rows):
    if not rows:
        return
    chunksize = 1000
    for i in range(0, len(rows), chunksize):
        chunk = rows[i:i+chunksize]
        if len(chunk[0]) == 5:
            cols = "(CUSTOMER_ID,FIRST_NAME,LAST_NAME,EMAIL,SIGNUP_DATE)"
            placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(chunk))
        else:
            cols = "(ORDER_ID,CUSTOMER_ID,ORDER_DATE,ORDER_STATUS,TOTAL_AMOUNT)"
            placeholders = ",".join(["(%s,%s,%s,%s,%s)"] * len(chunk))
        flat = [v for row in chunk for v in row]
        sql = f"insert into {table} {cols} values {placeholders}"
        cur.execute(sql, flat)

if __name__ == "__main__":
    n_customers = int(os.getenv("N_CUSTOMERS", "500"))
    avg_orders = int(os.getenv("AVG_ORDERS", "5"))

    with get_conn() as conn:
        with conn.cursor() as cur:
            bootstrap(cur)
            customers = generate_customers(n_customers)
            orders = generate_orders(customers, avg_orders)

            # Truncate + load (idempotent for dev/stg)
            cur.execute(f"truncate table {RAW}.CUSTOMERS")
            cur.execute(f"truncate table {RAW}.ORDERS")
            load(cur, f"{RAW}.CUSTOMERS", customers)
            load(cur, f"{RAW}.ORDERS", orders)

            print(f"Loaded {len(customers)} customers and {len(orders)} orders into {RAW}.")
