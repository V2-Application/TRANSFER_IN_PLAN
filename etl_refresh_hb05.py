"""
Refresh all 5 Snowflake tables: TRUNCATE then INSERT from Server 28
Only store HB05 for MRST and SALE_BUDGET
"""
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import time

API_URL = "https://sap-api.v2retail.net/api/datav2/query"
API_KEY = "v2-datav2-analyst-2026"
BATCH_SIZE = 50000

SF_CONF = dict(
    account='iafphkw-hh80816', user='akashv2kart', password='SVXqEe5pDdamMb9',
    database='V2RETAIL', schema='GOLD', warehouse='V2_WH', login_timeout=30
)

TABLES = [
    {"sf": "MRST_REPORT_TABLE_DUMP", "src": "MRST_REPORT_TABLE_DUMP", "where": "STORE_CODE='hb05'", "id_col": "ID"},
    {"sf": "MRDC_REPORT_TABLE_DUMP", "src": "MRDC_REPORT_TABLE_DUMP", "where": "", "id_col": "ID"},
    {"sf": "DIM_PRODUCT",            "src": "DIM_PRODUCT",            "where": "", "id_col": "PRODUCT_ID"},
    {"sf": "STORE_PLANT_MASTER",     "src": "STORE_PLANT_MASTER",     "where": "", "id_col": "ID"},
    {"sf": "SALE_BUDGET",            "src": "SALE_BUDGET",            "where": "ST_CD='hb05'", "id_col": "ID"},
]


def query_s28(sql):
    r = requests.post(API_URL, json={"sql": sql}, headers={"x-api-key": API_KEY}, timeout=180)
    r.raise_for_status()
    return r.json()


def etl_table(conn, tbl):
    sf_table = tbl["sf"]
    src = tbl["src"]
    where = tbl["where"]
    id_col = tbl["id_col"]
    where_sql = f" WHERE {where}" if where else ""

    print(f"\n{'='*60}")
    print(f"  {sf_table}")
    if where:
        print(f"  Filter: {where}")

    # Count
    result = query_s28(f"SELECT COUNT(*) AS CNT FROM {src}{where_sql}")
    total = result["data"][0]["CNT"]
    print(f"  Source rows: {total:,}")

    if total == 0:
        print("  No data - skipping")
        return

    # Truncate Snowflake table
    try:
        conn.cursor().execute(f'TRUNCATE TABLE IF EXISTS {sf_table}')
        print(f"  Truncated {sf_table}")
    except Exception:
        print(f"  Table {sf_table} does not exist yet - will create")

    # Batch load
    total_inserted = 0
    batch_num = 0
    last_id = 0
    table_exists = False

    # Check if table exists
    try:
        conn.cursor().execute(f'SELECT 1 FROM {sf_table} LIMIT 0')
        table_exists = True
    except Exception:
        table_exists = False

    while True:
        batch_num += 1
        t0 = time.time()

        id_filter = f"{id_col} > {last_id}"
        if where:
            full_where = f" WHERE {where} AND {id_filter}"
        else:
            full_where = f" WHERE {id_filter}"
        sql = f"SELECT TOP {BATCH_SIZE} * FROM {src}{full_where} ORDER BY {id_col}"

        print(f"  Batch {batch_num} ...", end=" ", flush=True)
        result = query_s28(sql)
        rows = result["data"]
        print(f"fetched {len(rows):,} in {time.time()-t0:.1f}s", end=" ", flush=True)

        if not rows:
            print()
            break

        df = pd.DataFrame(rows)
        t1 = time.time()

        if not table_exists:
            write_pandas(conn, df, sf_table, auto_create_table=True, overwrite=True, quote_identifiers=True)
            table_exists = True
        else:
            write_pandas(conn, df, sf_table, quote_identifiers=True)

        total_inserted += len(rows)
        print(f"-> loaded in {time.time()-t1:.1f}s (total: {total_inserted:,}/{total:,})")

        last_id = rows[-1][id_col]
        if len(rows) < BATCH_SIZE:
            break

    # Verify
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {sf_table}')
    sf_count = cur.fetchone()[0]
    cur.close()
    match = "OK" if sf_count == total else "MISMATCH!"
    print(f"  Done: Server28={total:,} Snowflake={sf_count:,} [{match}]")


def main():
    start = time.time()
    conn = snowflake.connector.connect(**SF_CONF)
    print("Connected to Snowflake V2RETAIL.GOLD")

    for tbl in TABLES:
        etl_table(conn, tbl)

    conn.close()
    print(f"\n{'='*60}")
    print(f"All tables refreshed in {time.time()-start:.1f}s")


if __name__ == "__main__":
    main()
