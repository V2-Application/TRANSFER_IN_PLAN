import snowflake.connector

conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='SVXqEe5pDdamMb9',
    database='V2RETAIL', schema='GOLD', warehouse='V2_WH', login_timeout=30
)
cur = conn.cursor()

with open(r'C:\Users\V2\Documents\TRANSFER_IN_PLAN\sf_create_view.sql', 'r') as f:
    view_sql = f.read()

cur.execute(view_sql)
print('View created: V2RETAIL.GOLD.V_MRST_REPORT_ENRICHED')

cur.execute('SELECT COUNT(*) FROM V_MRST_REPORT_ENRICHED')
print(f'Total rows: {cur.fetchone()[0]:,}')

cur.execute("""
    SELECT "STORE_CODE", "MATNR", "MAJ_CAT", REPORT_MONTH,
           ROUND("AVG_DENSITY",2) AS AVG_DENSITY,
           ROUND("AVG_AREA_PER_FIX",2) AS AVG_AREA_PER_FIX,
           ROUND(PPA, 4) AS PPA,
           ROUND(FINAL_AREA, 2) AS FINAL_AREA,
           ROUND(TD_PSF, 2) AS TD_PSF,
           ROUND(GP_SALE, 2) AS GP_SALE,
           ROUND(BGT_VAL_MTH, 2) AS BGT_VAL_MTH,
           ROUND(BGT_QTY_MTH, 2) AS BGT_QTY_MTH,
           ROUND(BGT_AREA, 2) AS BGT_AREA,
           ROUND(BGT_PSF, 2) AS BGT_PSF
    FROM V_MRST_REPORT_ENRICHED
    WHERE TD_PSF IS NOT NULL AND BGT_PSF IS NOT NULL
    LIMIT 5
""")
cols = [d[0] for d in cur.description]
print(f'\nSample:')
print(' | '.join(cols))
for row in cur.fetchall():
    print(' | '.join([str(v) for v in row]))

cur.execute("""
    SELECT
        COUNT(*) AS TOTAL,
        SUM(CASE WHEN PPA IS NOT NULL THEN 1 ELSE 0 END) AS HAS_PPA,
        SUM(CASE WHEN FINAL_AREA > 0 THEN 1 ELSE 0 END) AS HAS_AREA,
        SUM(CASE WHEN TD_PSF IS NOT NULL THEN 1 ELSE 0 END) AS HAS_TD_PSF,
        SUM(CASE WHEN GP_SALE IS NOT NULL THEN 1 ELSE 0 END) AS HAS_GP_SALE,
        SUM(CASE WHEN BGT_VAL_MTH IS NOT NULL THEN 1 ELSE 0 END) AS HAS_BGT_VAL,
        SUM(CASE WHEN BGT_AREA IS NOT NULL THEN 1 ELSE 0 END) AS HAS_BGT_AREA,
        SUM(CASE WHEN BGT_PSF IS NOT NULL THEN 1 ELSE 0 END) AS HAS_BGT_PSF
    FROM V_MRST_REPORT_ENRICHED
""")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
print('\nColumn coverage:')
for c, v in zip(cols, row):
    print(f'  {c}: {v:,}')

cur.close()
conn.close()
