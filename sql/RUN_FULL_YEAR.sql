-- =====================================================
-- FULL YEAR EXECUTION: Transfer In Plan + Purchase Plan
-- Real data Ã¢Â€Â” WEEK_ID 1 to 52
-- Run on 192.168.151.28 [planning]
-- =====================================================

USE [planning];
GO

-- =====================================================
-- PRE-CHECK: Verify reference data exists
-- =====================================================
PRINT '============================================';
PRINT '  PRE-CHECK: Reference Data';
PRINT '============================================';

SELECT 'WEEK_CALENDAR' AS [Table], COUNT(*) AS [Rows] FROM dbo.WEEK_CALENDAR
UNION ALL SELECT 'MASTER_ST_MASTER', COUNT(*) FROM dbo.MASTER_ST_MASTER
UNION ALL SELECT 'MASTER_BIN_CAPACITY', COUNT(*) FROM dbo.MASTER_BIN_CAPACITY
UNION ALL SELECT 'MASTER_GRT_CONS_percentage', COUNT(*) FROM dbo.MASTER_GRT_CONS_percentage
UNION ALL SELECT 'QTY_SALE_QTY', COUNT(*) FROM dbo.QTY_SALE_QTY
UNION ALL SELECT 'QTY_DISP_QTY', COUNT(*) FROM dbo.QTY_DISP_QTY
UNION ALL SELECT 'QTY_ST_STK_Q', COUNT(*) FROM dbo.QTY_ST_STK_Q
UNION ALL SELECT 'QTY_MSA_AND_GRT', COUNT(*) FROM dbo.QTY_MSA_AND_GRT
UNION ALL SELECT 'QTY_DEL_PENDING', COUNT(*) FROM dbo.QTY_DEL_PENDING;

PRINT '';
PRINT '=== WEEK RANGE ===';
SELECT MIN(WEEK_ID) AS [Min_WEEK_ID], MAX(WEEK_ID) AS [Max_WEEK_ID], COUNT(*) AS [Total_Weeks] FROM dbo.WEEK_CALENDAR;

PRINT '';
PRINT '=== STORES & RDCs ===';
SELECT RDC_CD, RDC_NM, COUNT(*) AS [Store_Count]
FROM dbo.MASTER_ST_MASTER
WHERE [STATUS] = 'NEW'
GROUP BY RDC_CD, RDC_NM
ORDER BY RDC_CD;

PRINT '';
PRINT '=== CATEGORIES ===';
SELECT [MAJ-CAT] AS MAJ_CAT FROM dbo.MASTER_BIN_CAPACITY ORDER BY [MAJ-CAT];

GO

-- =====================================================
-- STEP 1: GENERATE TRANSFER IN PLAN (Full Year)
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  STEP 1: SP_GENERATE_TRF_IN_PLAN';
PRINT '  Range: WEEK_ID 1 to 52 (Full Year)';
PRINT '  Started: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================';
PRINT '';

EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID  = 1,
    @EndWeekID    = 52,
    @StoreCode    = NULL,
    @MajCat       = NULL,
    @CoverDaysCM1 = 14,
    @CoverDaysCM2 = 0,
    @Debug        = 1;

GO

PRINT '';
PRINT '=== TRF_IN_PLAN RESULTS ===';
SELECT
    COUNT(*) AS [Total_Rows],
    COUNT(DISTINCT ST_CD) AS [Stores],
    COUNT(DISTINCT RDC_CD) AS [RDCs],
    COUNT(DISTINCT MAJ_CAT) AS [Categories],
    COUNT(DISTINCT WEEK_ID) AS [Weeks],
    MIN(WEEK_ID) AS [Min_Week],
    MAX(WEEK_ID) AS [Max_Week]
FROM dbo.TRF_IN_PLAN;

GO

-- =====================================================
-- STEP 2: GENERATE PURCHASE PLAN (Full Year)
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  STEP 2: SP_GENERATE_PURCHASE_PLAN';
PRINT '  Range: WEEK_ID 1 to 52 (Full Year)';
PRINT '  Started: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================';
PRINT '';

EXEC dbo.SP_GENERATE_PURCHASE_PLAN
    @StartWeekID = 1,
    @EndWeekID   = 52,
    @RdcCode     = NULL,
    @MajCat      = NULL,
    @Debug       = 1;

GO

PRINT '';
PRINT '=== PURCHASE_PLAN RESULTS ===';
SELECT
    COUNT(*) AS [Total_Rows],
    COUNT(DISTINCT RDC_CD) AS [RDCs],
    COUNT(DISTINCT MAJ_CAT) AS [Categories],
    COUNT(DISTINCT WEEK_ID) AS [Weeks],
    MIN(WEEK_ID) AS [Min_Week],
    MAX(WEEK_ID) AS [Max_Week]
FROM dbo.PURCHASE_PLAN;

GO

-- =====================================================
-- STEP 3: SUMMARY
-- =====================================================
PRINT '';
PRINT '============================================';
PRINT '  FINAL SUMMARY';
PRINT '============================================';

DECLARE @trfRows INT, @ppRows INT;
SELECT @trfRows = COUNT(*) FROM dbo.TRF_IN_PLAN;
SELECT @ppRows = COUNT(*) FROM dbo.PURCHASE_PLAN;

PRINT '  TRF_IN_PLAN rows:    ' + CAST(@trfRows AS VARCHAR);
PRINT '  PURCHASE_PLAN rows:  ' + CAST(@ppRows AS VARCHAR);
PRINT '  Completed: ' + CONVERT(VARCHAR, GETDATE(), 120);

IF @trfRows > 0 AND @ppRows > 0
    PRINT '  STATUS: BOTH PLANS GENERATED SUCCESSFULLY!';
ELSE
    PRINT '  STATUS: CHECK ERRORS ABOVE';

PRINT '============================================';
GO
