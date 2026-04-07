-- =====================================================
-- TEST SCRIPT: Execute both SPs with sample parameters
-- Run this in SSMS on 192.168.151.28 [planning] database
-- =====================================================

USE [planning];
GO

-- =====================================================
-- STEP 0: Check what WEEK_IDs are available
-- =====================================================
PRINT '=== AVAILABLE WEEK RANGE ===';
SELECT
    MIN(WEEK_ID) AS [Min_WEEK_ID],
    MAX(WEEK_ID) AS [Max_WEEK_ID],
    COUNT(*) AS [Total_Weeks]
FROM dbo.WEEK_CALENDAR;

PRINT '';
PRINT '=== SAMPLE WEEKS (first 5) ===';
SELECT TOP 5 WEEK_ID, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT
FROM dbo.WEEK_CALENDAR
ORDER BY WEEK_ID;

PRINT '';
PRINT '=== AVAILABLE STORES ===';
SELECT DISTINCT [ST CD] AS ST_CD, [ST NM] AS ST_NM, RDC_CD, RDC_NM
FROM dbo.MASTER_ST_MASTER
ORDER BY RDC_CD, [ST CD];

PRINT '';
PRINT '=== AVAILABLE CATEGORIES ===';
SELECT DISTINCT [MAJ-CAT] AS MAJ_CAT FROM dbo.MASTER_BIN_CAPACITY ORDER BY [MAJ-CAT];

GO

-- =====================================================
-- STEP 1: TEST SP_GENERATE_TRF_IN_PLAN (Transfer In)
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  TEST 1: SP_GENERATE_TRF_IN_PLAN';
PRINT '============================================';
PRINT '';

-- Run for 3 weeks, all stores, all categories, debug ON
-- WEEK_IDs are 1-52 (not YYYYWW format)
EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = 1,
    @EndWeekID   = 3,
    @StoreCode   = NULL,
    @MajCat      = NULL,
    @CoverDaysCM1 = 14,
    @CoverDaysCM2 = 0,
    @Debug       = 1;

GO

-- Verify Transfer In output
PRINT '';
PRINT '=== TRF_IN_PLAN OUTPUT (sample rows) ===';
SELECT TOP 10
    ST_CD, ST_NM, RDC_CD, MAJ_CAT, WEEK_ID, SSN,
    S_GRT_STK_Q, BGT_DISP_CL_Q, CM_BGT_SALE_Q,
    TRF_IN_STK_Q, BGT_TTL_CF_CL_STK_Q,
    ST_CL_EXCESS_Q, ST_CL_SHORT_Q
FROM dbo.TRF_IN_PLAN
WHERE WEEK_ID BETWEEN 1 AND 3
ORDER BY ST_CD, MAJ_CAT, WEEK_ID;

PRINT '';
PRINT '=== TRF_IN_PLAN TOTAL ROWS ===';
SELECT COUNT(*) AS [TRF_IN_PLAN_Rows] FROM dbo.TRF_IN_PLAN
WHERE WEEK_ID BETWEEN 1 AND 3;

GO

-- =====================================================
-- STEP 2: TEST SP_GENERATE_PURCHASE_PLAN (Purchase Plan)
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  TEST 2: SP_GENERATE_PURCHASE_PLAN';
PRINT '============================================';
PRINT '';

-- Run for same 3 weeks, all RDCs, all categories, debug ON
-- WEEK_IDs are 1-52 (not YYYYWW format)
EXEC dbo.SP_GENERATE_PURCHASE_PLAN
    @StartWeekID = 1,
    @EndWeekID   = 3,
    @RdcCode     = NULL,
    @MajCat      = NULL,
    @Debug       = 1;

GO

-- Verify Purchase Plan output
PRINT '';
PRINT '=== PURCHASE_PLAN OUTPUT (sample rows) ===';
SELECT TOP 10
    RDC_CD, RDC_NM, MAJ_CAT, WEEK_ID, SSN,
    DC_STK_Q, GRT_STK_Q, TTL_STK,
    CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q,
    BGT_DC_OP_STK_Q, BGT_DC_CL_STK_Q,
    BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED,
    DC_STK_EXCESS_Q, DC_STK_SHORT_Q,
    CO_STK_EXCESS_Q, CO_STK_SHORT_Q
FROM dbo.PURCHASE_PLAN
WHERE WEEK_ID BETWEEN 1 AND 3
ORDER BY RDC_CD, MAJ_CAT, WEEK_ID;

PRINT '';
PRINT '=== PURCHASE_PLAN TOTAL ROWS ===';
SELECT COUNT(*) AS [PURCHASE_PLAN_Rows] FROM dbo.PURCHASE_PLAN
WHERE WEEK_ID BETWEEN 1 AND 3;

GO

-- =====================================================
-- STEP 3: CROSS-CHECK Ã¢Â€Â” Purchase Plan aggregation matches Transfer In
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  TEST 3: CROSS-CHECK AGGREGATION';
PRINT '============================================';
PRINT '';

-- Verify that Purchase Plan sale qty = SUM of Transfer In sale qty by RDC
SELECT
    t.RDC_CD,
    t.MAJ_CAT,
    t.WEEK_ID,
    SUM(t.CM_BGT_SALE_Q) AS [TRF_IN_SUM_CW_SALE],
    p.CW_BGT_SALE_Q AS [PURCHASE_PLAN_CW_SALE],
    CASE WHEN ABS(SUM(t.CM_BGT_SALE_Q) - p.CW_BGT_SALE_Q) < 0.01 THEN 'MATCH' ELSE 'MISMATCH' END AS [Status]
FROM dbo.TRF_IN_PLAN t
INNER JOIN dbo.PURCHASE_PLAN p ON p.RDC_CD = t.RDC_CD
    AND p.MAJ_CAT = t.MAJ_CAT
    AND p.WEEK_ID = t.WEEK_ID
WHERE t.WEEK_ID BETWEEN 1 AND 3
GROUP BY t.RDC_CD, t.MAJ_CAT, t.WEEK_ID, p.CW_BGT_SALE_Q
ORDER BY t.RDC_CD, t.MAJ_CAT, t.WEEK_ID;

GO

-- =====================================================
-- STEP 4: TEST VIEWS
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  TEST 4: VIEWS';
PRINT '============================================';
PRINT '';

PRINT '--- Transfer In Views ---';
SELECT TOP 3 * FROM dbo.VW_TRF_IN_STORE_SUMMARY;
SELECT TOP 3 * FROM dbo.VW_TRF_IN_RDC_SUMMARY;
SELECT TOP 3 * FROM dbo.VW_TRF_IN_CATEGORY_SUMMARY;
SELECT TOP 3 * FROM dbo.VW_TRF_IN_ALERTS;
SELECT TOP 3 * FROM dbo.VW_TRF_IN_DETAIL ORDER BY [ST-CD], [MAJ-CAT], [WEEK_ID];

PRINT '--- Purchase Plan Views ---';
SELECT TOP 3 * FROM dbo.VW_PURCHASE_PLAN_DETAIL;
SELECT TOP 3 * FROM dbo.VW_PURCHASE_PLAN_SUMMARY;
SELECT TOP 3 * FROM dbo.VW_PURCHASE_PLAN_ALERTS;
SELECT TOP 3 * FROM dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY;
SELECT TOP 3 * FROM dbo.VW_WEEK_REFERENCE ORDER BY [WEEK_ID];

GO

-- =====================================================
-- FINAL SUMMARY
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  FINAL SUMMARY';
PRINT '============================================';

DECLARE @trfRows INT, @ppRows INT;
SELECT @trfRows = COUNT(*) FROM dbo.TRF_IN_PLAN;
SELECT @ppRows = COUNT(*) FROM dbo.PURCHASE_PLAN;

PRINT '  TRF_IN_PLAN total rows:    ' + CAST(@trfRows AS VARCHAR);
PRINT '  PURCHASE_PLAN total rows:  ' + CAST(@ppRows AS VARCHAR);
PRINT '';

IF @trfRows > 0 AND @ppRows > 0
    PRINT '  RESULT: BOTH SPs EXECUTED SUCCESSFULLY!';
ELSE IF @trfRows > 0 AND @ppRows = 0
    PRINT '  RESULT: Transfer In OK, Purchase Plan FAILED Ã¢Â€Â” check errors above';
ELSE IF @trfRows = 0 AND @ppRows > 0
    PRINT '  RESULT: Transfer In FAILED, Purchase Plan OK Ã¢Â€Â” check errors above';
ELSE
    PRINT '  RESULT: BOTH SPs FAILED Ã¢Â€Â” check errors above';

PRINT '============================================';
GO
