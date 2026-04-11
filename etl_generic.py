"""
Generic ETL: Server 28 -> Snowflake V2RETAIL.GOLD
Usage: python etl_generic.py TABLE_NAME [WHERE_CLAUSE]
"""
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import time
import sys

API_URL = "https://sap-api.v2retail.net/api/datav2/query"
API_KEY = "v2-datav2-analyst-2026"
BATCH_SIZE = 50000

SF_ACCOUNT = "iafphkw-hh80816"
SF_USER = "akashv2kart"
SF_PASSWORD = "SVXqEe5pDdamMb9"
SF_DATABASE = "V2RETAIL"
SF_SCHEMA = "GOLD"
SF_WAREHOUSE = "V2_WH"


def query_server28(sql):
    r = requests.post(API_URL, json={"sql": sql}, headers={"x-api-key": API_KEY}, timeout=180)
    r.raise_for_status()
    return r.json()


def run_etl(table_name, where_clause=""):
    start = time.time()
    sf_table = table_name
    where_sql = f" WHERE {where_clause}" if where_clause else ""

    print(f"\n{'='*50}")
    print(f"ETL: {table_name} -> V2RETAIL.GOLD.{sf_table}")
    if where_clause:
        print(f"Filter: {where_clause}")

    result = query_server28(f"SELECT COUNT(*) AS CNT FROM {table_name}{where_sql}")
    total = result["data"][0]["CNT"]
    print(f"Total rows: {total:,}")

    if total == 0:
        print("No data — skipping.")
        return

    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DATABASE, schema=SF_SCHEMA, warehouse=SF_WAREHOUSE, login_timeout=30,
    )

    # Check if table has usable ID column for pagination
    sample = query_server28(f"SELECT TOP 1 * FROM {table_name}{where_sql}")
    columns = sample["columns"]
    id_col = None
    for candidate in ["ID", "PRODUCT_ID", "STORE_ID"]:
        if candidate in columns:
            id_col = candidate
            break
    print(f"Columns: {len(columns)}, Pagination: {id_col or 'OFFSET'}")

    total_inserted = 0
    batch_num = 0
    last_id = 0
    table_created = False

    while True:
        batch_num += 1
        t0 = time.time()

        if id_col:
            id_filter = f"{id_col} > {last_id}"
            full_where = f" WHERE {where_clause} AND {id_filter}" if where_clause else f" WHERE {id_filter}"
            sql = f"SELECT TOP {BATCH_SIZE} * FROM {table_name}{full_where} ORDER BY {id_col}"
        else:
            sql = f"SELECT TOP {BATCH_SIZE} * FROM {table_name}{where_sql} ORDER BY (SELECT NULL) OFFSET {total_inserted} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY"

        print(f"  Batch {batch_num} ...", end=" ", flush=True)
        result = query_server28(sql)
        if "data" not in result:
            print(f"\nAPI error: {result.get('error', result)}")
            break
        rows = result["data"]
        print(f"fetched {len(rows):,} in {time.time()-t0:.1f}s", end=" ", flush=True)

        if not rows:
            print()
            break

        df = pd.DataFrame(rows)
        t1 = time.time()

        if not table_created:
            conn.cursor().execute(f"DROP TABLE IF EXISTS {sf_table}")
            write_pandas(conn, df, sf_table, auto_create_table=True, overwrite=True, quote_identifiers=True)
            table_created = True
        else:
            write_pandas(conn, df, sf_table, quote_identifiers=True)

        total_inserted += len(rows)
        print(f"-> loaded in {time.time()-t1:.1f}s (total: {total_inserted:,}/{total:,})")

        if id_col:
            last_id = rows[-1][id_col]
        if len(rows) < BATCH_SIZE:
            break

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {sf_table}")
    sf_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    elapsed = time.time() - start
    print(f"Done! {sf_table}: Server28={total:,} Snowflake={sf_count:,} Time={elapsed:.1f}s")
    return sf_count


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl_generic.py TABLE_NAME [WHERE_CLAUSE]")
        sys.exit(1)
    table = sys.argv[1]
    where = sys.argv[2] if len(sys.argv) > 2 else ""
    run_etl(table, where)
