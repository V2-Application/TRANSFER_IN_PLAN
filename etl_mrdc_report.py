"""
ETL: MRDC_REPORT_TABLE_DUMP from Server 28 → Snowflake V2RETAIL.GOLD
643K rows, 177 columns, batched at 50K rows per API call
Uses write_pandas (PUT + COPY INTO) for fast bulk loading
"""
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import time

# --- Config ---
API_URL = "https://sap-api.v2retail.net/api/datav2/query"
API_KEY = "v2-datav2-analyst-2026"
BATCH_SIZE = 50000

SF_ACCOUNT = "iafphkw-hh80816"
SF_USER = "akashv2kart"
SF_PASSWORD = "SVXqEe5pDdamMb9"
SF_DATABASE = "V2RETAIL"
SF_SCHEMA = "GOLD"
SF_WAREHOUSE = "V2_WH"
SF_TABLE = "MRDC_REPORT_TABLE_DUMP"


def query_server28(sql):
    r = requests.post(
        API_URL,
        json={"sql": sql},
        headers={"x-api-key": API_KEY},
        timeout=180,
    )
    r.raise_for_status()
    return r.json()


def main():
    start = time.time()

    # Step 1: Get total count
    print("Getting row count...")
    result = query_server28("SELECT COUNT(*) AS CNT FROM MRDC_REPORT_TABLE_DUMP")
    total = result["data"][0]["CNT"]
    print(f"Total rows: {total:,}")

    # Step 2: Connect to Snowflake
    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DATABASE, schema=SF_SCHEMA, warehouse=SF_WAREHOUSE,
        login_timeout=30,
    )

    # Step 3: Fetch and load in batches
    total_inserted = 0
    batch_num = 0
    last_id = 0
    table_created = False

    while True:
        batch_num += 1
        print(f"\nFetching batch {batch_num} (ID > {last_id})...")
        t0 = time.time()

        result = query_server28(
            f"SELECT TOP {BATCH_SIZE} * FROM MRDC_REPORT_TABLE_DUMP WHERE ID > {last_id} ORDER BY ID"
        )
        rows = result["data"]
        fetch_time = time.time() - t0
        print(f"  Fetched {len(rows):,} rows in {fetch_time:.1f}s")

        if not rows:
            break

        # Convert to DataFrame
        df = pd.DataFrame(rows)

        # First batch: create table
        if not table_created:
            print(f"  Creating table with {len(df.columns)} columns...")
            # Drop existing table
            conn.cursor().execute(f"DROP TABLE IF EXISTS {SF_TABLE}")
            # write_pandas with auto_create_table creates the table
            t1 = time.time()
            success, nchunks, nrows, _ = write_pandas(
                conn, df, SF_TABLE, auto_create_table=True, overwrite=True,
                quote_identifiers=True
            )
            load_time = time.time() - t1
            table_created = True
        else:
            t1 = time.time()
            success, nchunks, nrows, _ = write_pandas(
                conn, df, SF_TABLE, quote_identifiers=True
            )
            load_time = time.time() - t1

        total_inserted += len(rows)
        print(f"  Loaded {len(rows):,} rows in {load_time:.1f}s (total: {total_inserted:,}/{total:,})")

        last_id = rows[-1]["ID"]

        if len(rows) < BATCH_SIZE:
            break

    # Step 4: Verify
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {SF_TABLE}")
    sf_count = cur.fetchone()[0]
    cur.close()
    conn.close()

    elapsed = time.time() - start
    print(f"\n{'='*50}")
    print(f"Done! {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
    print(f"  Server 28: {total:,} rows")
    print(f"  Snowflake: {sf_count:,} rows")
    print(f"  Time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
