/*==============================================================
  TEST: Run TRF_IN + PURCHASE PLAN for 1 store only
  Purpose: Quick validation before full run
==============================================================*/

USE [planning];
GO

-- Step 1: Find one ACTIVE store to test with
SELECT TOP 5 [ST CD], [ST NM], [RDC_CD], [STATUS]
FROM dbo.MASTER_ST_MASTER
WHERE [STATUS] = 'NEW'
ORDER BY [ST CD];
GO

-- Step 2: Pick the first active store (change this if needed)
DECLARE @TestStore VARCHAR(20);
SELECT TOP 1 @TestStore = [ST CD]
FROM dbo.MASTER_ST_MASTER
WHERE [STATUS] = 'NEW'
ORDER BY [ST CD];

PRINT '>> Testing with store: ' + @TestStore;
PRINT '';

-- Step 3: Run TRF_IN for this 1 store
PRINT '========================================';
PRINT 'RUNNING SP_GENERATE_TRF_IN_PLAN';
PRINT '========================================';

EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = 1,
    @EndWeekID = 52,
    @StoreCode = @TestStore,
    @Debug = 1;
GO

-- Step 4: Check TRF_IN results
PRINT '';
PRINT '>> TRF_IN_PLAN sample results:';

SELECT TOP 10
    ST_CD, MAJ_CAT, WEEK_ID, SSN,
    BGT_TTL_CF_OP_STK_Q AS OP_STK,
    NT_ACT_Q,
    NET_BGT_CF_STK_Q AS NET_CF,
    CM_BGT_SALE_Q AS SALE,
    TRF_IN_STK_Q AS TRF_IN,
    BGT_TTL_CF_CL_STK_Q AS CL_STK,
    BGT_ST_CL_MBQ AS MBQ
FROM dbo.TRF_IN_PLAN
WHERE ST_CD = (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD])
ORDER BY MAJ_CAT, WEEK_ID;

-- Step 5: Check week chaining is correct (CL_STK of week N = OP_STK of week N+1)
PRINT '';
PRINT '>> Week chaining verification (should show 0 mismatches):';

SELECT COUNT(*) AS ChainMismatches
FROM dbo.TRF_IN_PLAN t1
INNER JOIN dbo.TRF_IN_PLAN t2
    ON t2.ST_CD = t1.ST_CD
    AND t2.MAJ_CAT = t1.MAJ_CAT
    AND t2.WEEK_ID = t1.WEEK_ID + 1
WHERE t1.ST_CD = (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD])
  AND t1.WEEK_ID BETWEEN 1 AND 51
  AND ABS(t1.BGT_TTL_CF_CL_STK_Q - t2.BGT_TTL_CF_OP_STK_Q) > 0.01;

-- Step 6: Row count summary
SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT MAJ_CAT) AS Categories,
    COUNT(DISTINCT WEEK_ID) AS Weeks,
    MIN(WEEK_ID) AS MinWeek,
    MAX(WEEK_ID) AS MaxWeek
FROM dbo.TRF_IN_PLAN
WHERE ST_CD = (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD]);
GO

-- Step 7: Now run PURCHASE PLAN (uses the TRF_IN data we just generated)
DECLARE @TestRdc VARCHAR(20);
SELECT TOP 1 @TestRdc = [RDC_CD]
FROM dbo.MASTER_ST_MASTER
WHERE [STATUS] = 'NEW'
ORDER BY [ST CD];

PRINT '';
PRINT '========================================';
PRINT 'RUNNING SP_GENERATE_PURCHASE_PLAN';
PRINT 'RDC: ' + @TestRdc;
PRINT '========================================';

EXEC dbo.SP_GENERATE_PURCHASE_PLAN
    @StartWeekID = 1,
    @EndWeekID = 52,
    @RdcCode = @TestRdc,
    @Debug = 1;
GO

-- Step 8: Check PURCHASE_PLAN results
PRINT '';
PRINT '>> PURCHASE_PLAN sample results:';

SELECT TOP 10
    RDC_CD, MAJ_CAT, WEEK_ID, SSN,
    BGT_DC_OP_STK_Q AS DC_OP,
    BGT_CF_STK_Q AS CF_STK,
    GRT_CONS_Q,
    TTL_TRF_OUT_Q AS TRF_OUT,
    BGT_PUR_Q_INIT AS PUR_QTY,
    BGT_DC_CL_STK_Q AS DC_CL,
    BGT_DC_CL_MBQ AS DC_MBQ
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = (SELECT TOP 1 [RDC_CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD])
ORDER BY MAJ_CAT, WEEK_ID;

-- Step 9: Purchase Plan chaining verification
PRINT '';
PRINT '>> PP week chaining verification (should show 0 mismatches):';

SELECT COUNT(*) AS PPChainMismatches
FROM dbo.PURCHASE_PLAN p1
INNER JOIN dbo.PURCHASE_PLAN p2
    ON p2.RDC_CD = p1.RDC_CD
    AND p2.MAJ_CAT = p1.MAJ_CAT
    AND p2.WEEK_ID = p1.WEEK_ID + 1
WHERE p1.RDC_CD = (SELECT TOP 1 [RDC_CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD])
  AND p1.WEEK_ID BETWEEN 1 AND 51
  AND ABS(p1.BGT_DC_CL_STK_Q - p2.BGT_DC_OP_STK_Q) > 0.01;

SELECT
    COUNT(*) AS TotalRows,
    COUNT(DISTINCT MAJ_CAT) AS Categories,
    COUNT(DISTINCT WEEK_ID) AS Weeks
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = (SELECT TOP 1 [RDC_CD] FROM dbo.MASTER_ST_MASTER WHERE [STATUS] = 'NEW' ORDER BY [ST CD]);
GO

PRINT '';
PRINT '>> TEST COMPLETE - Check results above';
GO
