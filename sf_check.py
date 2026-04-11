import snowflake.connector
conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='SVXqEe5pDdamMb9',
    database='V2RETAIL', schema='GOLD', warehouse='V2_WH', login_timeout=30
)
cur = conn.cursor()

cur.execute("SELECT DATE_TRUNC('MONTH', TO_DATE(\"DATE\")) AS MTH, COUNT(*) AS CNT, COUNT(DISTINCT \"MAJ_CAT\") AS CATS FROM SALE_BUDGET GROUP BY 1 ORDER BY 1")
print('SALE_BUDGET months:')
for row in cur.fetchall():
    print(row)

cur.execute('SELECT DISTINCT "LOGDATE" FROM MRST_REPORT_TABLE_DUMP LIMIT 5')
print('\nMRST LOGDATE:')
for row in cur.fetchall():
    print(row)

cur.execute("SELECT DATE_TRUNC('MONTH', TO_DATE(\"DATE\")) AS MTH, COUNT(*) AS CNT FROM BGT_DISP_GANDOLA GROUP BY 1 ORDER BY 1")
print('\nBGT_DISP_GANDOLA months:')
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
