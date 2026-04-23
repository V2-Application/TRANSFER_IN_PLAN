"""
Push ET_SALES_DATA (2 years) from datav2 (Server 28) -> Snowflake V2RETAIL.GOLD
Approach: BCP export (fast native bulk) -> Snowflake PUT + COPY INTO
Resumes from where it left off (checks Snowflake for already-loaded months).
"""

import time, subprocess, os, glob
import snowflake.connector

# -- Config --
SQL_SERVER  = "192.168.151.28"
SQL_DB      = "datav2"
SQL_USER    = "nikhil"
SQL_PASS    = "Vrl@12345"

SF_ACCOUNT  = "iafphkw-hh80816"
SF_USER     = "akashv2kart"
SF_PASSWORD = "SVXqEe5pDdamMb9"
SF_DB       = "V2RETAIL"
SF_SCHEMA   = "GOLD"
SF_WH       = "V2_WH"
SF_TABLE    = "ET_SALES_DATA"

TEMP_DIR    = os.path.join(os.environ.get("TEMP", "."), "et_sales_bcp")
os.makedirs(TEMP_DIR, exist_ok=True)

SF_DDL = f"""
CREATE TABLE IF NOT EXISTS {SF_TABLE} (
    VBELN        VARCHAR,
    POSNR        VARCHAR,
    FKDAT        VARCHAR,
    WERKS        VARCHAR(100),
    LGORT        VARCHAR(100),
    MATNR        VARCHAR(100),
    FKIMG        NUMBER(18,4),
    VRKME        VARCHAR,
    WAERK        VARCHAR,
    NETWR        NUMBER(18,4),
    KZWI1        NUMBER(18,4),
    KZWI2        NUMBER(18,4),
    MWSBP        NUMBER(18,4),
    WAVWR        NUMBER(18,4),
    NET_VAL      NUMBER(18,4),
    VKP0         NUMBER(18,4),
    KWERT_VPRS   NUMBER(18,4),
    KBETR_VPRS   NUMBER(18,4),
    SALES_DATE   TIMESTAMP_NTZ,
    ID           NUMBER(19,0)
);
"""

COLS = "VBELN,POSNR,FKDAT,WERKS,LGORT,MATNR,FKIMG,VRKME,WAERK,NETWR,KZWI1,KZWI2,MWSBP,WAVWR,NET_VAL,VKP0,KWERT_VPRS,KBETR_VPRS,SALES_DATE,ID"

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def bcp_export(year, month, out_file):
    """Export one month via BCP queryout - fast native bulk."""
    query = (
        f"SELECT VBELN, POSNR, FKDAT, WERKS, LGORT, MATNR, "
        f"FKIMG, VRKME, WAERK, NETWR, KZWI1, KZWI2, "
        f"MWSBP, WAVWR, NET_VAL, VKP0, KWERT_VPRS, KBETR_VPRS, "
        f"CONVERT(VARCHAR(23), Sales_Date, 121), ID "
        f"FROM {SQL_DB}.dbo.ET_SALES_DATA WITH (NOLOCK) "
        f"WHERE YEAR(Sales_Date) = {year} AND MONTH(Sales_Date) = {month}"
    )
    cmd = [
        "bcp", query, "queryout", out_file,
        "-S", SQL_SERVER,
        "-U", SQL_USER, "-P", SQL_PASS,
        "-c",                    # character mode (text)
        "-t", "|",               # pipe delimiter (safe for this data)
        "-r", "\n",              # row terminator
        "-b", "100000",          # batch size
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        raise RuntimeError(f"BCP failed: {result.stderr}")
    # Parse row count from BCP output
    for line in result.stdout.splitlines():
        if "rows copied" in line.lower():
            return int(line.split()[0])
    return 0

def main():
    t0 = time.time()

    # -- 1. Connect Snowflake, create table --
    log("Connecting to Snowflake...")
    sf = snowflake.connector.connect(
        account=SF_ACCOUNT, user=SF_USER, password=SF_PASSWORD,
        database=SF_DB, schema=SF_SCHEMA, warehouse=SF_WH,
        login_timeout=30
    )
    cur = sf.cursor()
    cur.execute(SF_DDL)
    log(f"Table {SF_TABLE} ensured.")

    # Create internal stage for file upload
    stage = "STG_ET_SALES_LOAD"
    cur.execute(f"CREATE OR REPLACE STAGE {stage} FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 FIELD_OPTIONALLY_ENCLOSED_BY = NONE NULL_IF = (''))")
    log(f"Stage {stage} ready.")

    # -- 2. Check which months are already loaded (resume support) --
    cur.execute(f"SELECT DISTINCT TO_CHAR(SALES_DATE, 'YYYY-MM') AS M FROM {SF_TABLE}")
    loaded = {row[0] for row in cur.fetchall()}
    if loaded:
        log(f"Already loaded months: {sorted(loaded)}")

    # -- 3. Build month list (2 years) --
    from datetime import datetime, timedelta
    now = datetime.now()
    two_years_ago = now.replace(year=now.year - 2)
    months = []
    d = two_years_ago.replace(day=1)
    while d <= now:
        m_key = d.strftime("%Y-%m")
        months.append((d.year, d.month, m_key))
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)

    log(f"{len(months)} months total, {len(months) - len(loaded)} remaining.")

    # -- 4. Transfer month by month: BCP -> PUT -> COPY --
    total_rows = 0
    skipped = 0

    for i, (year, mo, m_key) in enumerate(months, 1):
        if m_key in loaded:
            log(f"[{i}/{len(months)}] {m_key} -- already in Snowflake, skipping.")
            skipped += 1
            continue

        csv_file = os.path.join(TEMP_DIR, f"et_sales_{year}_{mo:02d}.csv")

        # BCP export
        log(f"[{i}/{len(months)}] {m_key} -- BCP exporting...")
        t1 = time.time()
        row_count = bcp_export(year, mo, csv_file)
        bcp_time = time.time() - t1

        if row_count == 0:
            log(f"  -> 0 rows, skipping.")
            continue

        file_mb = os.path.getsize(csv_file) / (1024 * 1024)
        log(f"  -> {row_count:,} rows exported ({file_mb:.0f} MB) in {bcp_time:.0f}s")

        # PUT to Snowflake stage
        log(f"  -> PUT to Snowflake stage...")
        t2 = time.time()
        # Use forward slashes and escape for Snowflake PUT
        sf_path = csv_file.replace("\\", "/")
        cur.execute(f"PUT 'file://{sf_path}' @{stage} AUTO_COMPRESS=TRUE PARALLEL=4 OVERWRITE=TRUE")
        put_time = time.time() - t2
        log(f"  -> PUT done in {put_time:.0f}s")

        # COPY INTO from stage
        log(f"  -> COPY INTO {SF_TABLE}...")
        t3 = time.time()
        file_name = os.path.basename(csv_file)
        cur.execute(f"""
            COPY INTO {SF_TABLE} ({COLS})
            FROM @{stage}/{file_name}.gz
            FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0 NULL_IF = (''))
            ON_ERROR = 'CONTINUE'
            PURGE = TRUE
        """)
        copy_result = cur.fetchall()
        copy_time = time.time() - t3
        sf_loaded = copy_result[0][3] if copy_result else 0  # rows_loaded
        log(f"  -> COPY done: {sf_loaded:,} rows loaded in {copy_time:.0f}s")

        total_rows += row_count
        elapsed = time.time() - t0
        rate = total_rows / elapsed if elapsed > 0 else 0
        log(f"  -> Cumulative: {total_rows:,} rows | {elapsed:.0f}s | {rate:,.0f} rows/s")

        # Clean up CSV
        try:
            os.remove(csv_file)
        except:
            pass

    # -- 5. Summary --
    elapsed = time.time() - t0
    log("=" * 60)
    log(f"COMPLETE: {total_rows:,} new rows transferred in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    log(f"Months skipped (already loaded): {skipped}")
    if total_rows > 0:
        log(f"Average rate: {total_rows/elapsed:,.0f} rows/sec")

    # Verify total
    cur.execute(f"SELECT COUNT(*) FROM {SF_TABLE}")
    sf_count = cur.fetchone()[0]
    log(f"Snowflake total row count: {sf_count:,}")
    log(f"Target: {SF_DB}.{SF_SCHEMA}.{SF_TABLE}")

    cur.close()
    sf.close()

if __name__ == "__main__":
    main()
