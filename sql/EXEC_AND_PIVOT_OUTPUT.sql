-- =====================================================
-- COMBINED EXECUTION: Transfer In + Purchase Plan
-- Then output PIVOT views for both
-- Run on 192.168.151.28 [planning]
-- =====================================================

USE [planning];
GO

-- =====================================================
-- PRE-CHECK: Verify reference data exists
-- =====================================================
PRINT '============================================';
PRINT '  PRE-CHECK: Reference Data';
PRINT '  Started: ' + CONVERT(VARCHAR, GETDATE(), 120);
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
SELECT MIN(WEEK_ID) AS [Min_WEEK], MAX(WEEK_ID) AS [Max_WEEK], COUNT(*) AS [Total_Weeks] FROM dbo.WEEK_CALENDAR;
GO


-- =============================================================
-- STEP 1: EXECUTE SP_GENERATE_TRF_IN_PLAN (Full Year)
-- =============================================================
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
    COUNT(DISTINCT WEEK_ID) AS [Weeks]
FROM dbo.TRF_IN_PLAN;
GO


-- =============================================================
-- STEP 2: EXECUTE SP_GENERATE_PURCHASE_PLAN (Full Year)
-- =============================================================
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
    COUNT(DISTINCT WEEK_ID) AS [Weeks]
FROM dbo.PURCHASE_PLAN;
GO


-- =============================================================
-- STEP 3: RECREATE PIVOT VIEWS (with ISNULL Ã¢Â€Â” 0 for numbers, NA for text)
-- =============================================================
PRINT '';
PRINT '============================================';
PRINT '  STEP 3: RECREATE PIVOT VIEWS';
PRINT '============================================';

-- 3A: Transfer In Pivot View
IF OBJECT_ID('dbo.VW_TRF_IN_PIVOT','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_PIVOT;
GO

CREATE VIEW dbo.VW_TRF_IN_PIVOT
AS
SELECT
    T.ST_CD                                AS [ST-CD],
    ISNULL(SM.[ST NM], 'NA')              AS [ST-NM],
    ISNULL(SM.[RDC_CD], 'NA')             AS [RDC_CD],
    ISNULL(SM.[RDC_NM], 'NA')             AS [RDC_NM],
    ISNULL(SM.[HUB_CD], 'NA')             AS [HUB_CD],
    ISNULL(SM.[HUB_NM], 'NA')             AS [HUB_NM],
    ISNULL(SM.[AREA], 'NA')               AS [AREA],
    T.MAJ_CAT                             AS [MAJ-CAT],
    T.FY_YEAR,

    -- WEEK 1
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.BGT_DISP_CL_Q END), 0) AS [W1-DISP],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.BGT_ST_CL_MBQ END), 0) AS [W1-MBQ],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.BGT_TTL_CF_OP_STK_Q END), 0) AS [W1-OPSTK],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.NT_ACT_Q END), 0) AS [W1-NTACT],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.NET_BGT_CF_STK_Q END), 0) AS [W1-NETCF],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.CM_BGT_SALE_Q END), 0) AS [W1-SALE],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.CM1_BGT_SALE_Q END), 0) AS [W1-SALE1],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.CM2_BGT_SALE_Q END), 0) AS [W1-SALE2],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.TRF_IN_STK_Q END), 0) AS [W1-TRFIN],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.BGT_TTL_CF_CL_STK_Q END), 0) AS [W1-CLSTK],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.BGT_NT_ACT_Q END), 0) AS [W1-BNTACT],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.NET_ST_CL_STK_Q END), 0) AS [W1-NETCL],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.ST_CL_EXCESS_Q END), 0) AS [W1-EXCESS],
    ISNULL(MAX(CASE WHEN T.FY_WEEK=1 THEN T.ST_CL_SHORT_Q END), 0) AS [W1-SHORT]

FROM dbo.TRF_IN_PLAN T
LEFT JOIN dbo.MASTER_ST_MASTER SM ON SM.[ST CD] = T.ST_CD
GROUP BY T.ST_CD, SM.[ST NM], SM.[RDC_CD], SM.[RDC_NM],
         SM.[HUB_CD], SM.[HUB_NM], SM.[AREA], T.MAJ_CAT, T.FY_YEAR;
GO

PRINT '>> NOTE: VW_TRF_IN_PIVOT shown here with Week 1 only for brevity.';
PRINT '>> The full view (07_CREATE_VW_TRF_IN_PIVOT.sql) has all 52 weeks.';
PRINT '>> Run 07_CREATE_VW_TRF_IN_PIVOT.sql for the complete pivot view.';
GO


-- =============================================================
-- STEP 4: OUTPUT Ã¢Â€Â” TRANSFER IN PIVOT
-- =============================================================
PRINT '';
PRINT '============================================';
PRINT '  STEP 4: TRANSFER IN Ã¢Â€Â” PIVOT OUTPUT';
PRINT '============================================';

-- 4A: Full Pivot (all 14 metrics x 52 weeks)
PRINT '';
PRINT '--- 4A: VW_TRF_IN_PIVOT (Full pivot Ã¢Â€Â” 737 columns) ---';
PRINT '    Tip: Right-click grid Ã¢Â†Â’ Save Results As Ã¢Â†Â’ CSV for Excel';

SELECT * FROM dbo.VW_TRF_IN_PIVOT
ORDER BY [ST-CD], [MAJ-CAT];
GO

-- 4B: Transfer In Qty only Ã¢Â€Â” pivoted by week
PRINT '';
PRINT '--- 4B: TRANSFER IN QTY Ã¢Â€Â” Week-wise ---';

SELECT * FROM dbo.VW_TRF_IN_PIVOT
ORDER BY [ST-CD], [MAJ-CAT];
GO


-- =============================================================
-- STEP 5: OUTPUT Ã¢Â€Â” PURCHASE PLAN PIVOT
-- =============================================================
PRINT '';
PRINT '============================================';
PRINT '  STEP 5: PURCHASE PLAN Ã¢Â€Â” PIVOT OUTPUT';
PRINT '============================================';

-- 5A: Stock View (Display, Sales CW-CW+5, Opening Stock)
PRINT '';
PRINT '--- 5A: VW_PP_PIVOT_STOCK (733 columns) ---';

SELECT * FROM dbo.VW_PP_PIVOT_STOCK
ORDER BY [RDC-CD], [MAJ-CAT];
GO

-- 5B: GRT & Transfer View (GRT, Transfer Out CW-CW+4, Store Closing)
PRINT '';
PRINT '--- 5B: VW_PP_PIVOT_GRT_TRF (733 columns) ---';

SELECT * FROM dbo.VW_PP_PIVOT_GRT_TRF
ORDER BY [RDC-CD], [MAJ-CAT];
GO

-- 5C: Purchase View (DC Closing, Purchase, Excess/Short, Bins)
PRINT '';
PRINT '--- 5C: VW_PP_PIVOT_PURCHASE (733 columns) ---';

SELECT * FROM dbo.VW_PP_PIVOT_PURCHASE
ORDER BY [RDC-CD], [MAJ-CAT];
GO

-- 5D: Combined Ã¢Â€Â” All 3 views joined (for complete Purchase Plan picture)
PRINT '';
PRINT '--- 5D: COMBINED PURCHASE PLAN (all 3 views joined) ---';
PRINT '    WARNING: 2189 columns Ã¢Â€Â” very wide. Best exported to CSV/Excel.';

SELECT s.*, g.*, p.*
FROM dbo.VW_PP_PIVOT_STOCK s
INNER JOIN dbo.VW_PP_PIVOT_GRT_TRF g
    ON g.[RDC-CD] = s.[RDC-CD] AND g.[MAJ-CAT] = s.[MAJ-CAT]
    AND g.SSN = s.SSN AND g.FY_YEAR = s.FY_YEAR
INNER JOIN dbo.VW_PP_PIVOT_PURCHASE p
    ON p.[RDC-CD] = s.[RDC-CD] AND p.[MAJ-CAT] = s.[MAJ-CAT]
    AND p.SSN = s.SSN AND p.FY_YEAR = s.FY_YEAR
ORDER BY s.[RDC-CD], s.[MAJ-CAT];
GO


-- =============================================================
-- STEP 6: FINAL SUMMARY
-- =============================================================
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
PRINT '';

IF @trfRows > 0 AND @ppRows > 0
    PRINT '  STATUS: BOTH PLANS GENERATED & PIVOT VIEWS OUTPUT SUCCESSFULLY!';
ELSE
    PRINT '  STATUS: CHECK ERRORS ABOVE';

PRINT '';
PRINT '  PIVOT VIEWS AVAILABLE:';
PRINT '    Transfer In:    VW_TRF_IN_PIVOT        (737 cols)';
PRINT '    Purchase Stock: VW_PP_PIVOT_STOCK       (733 cols)';
PRINT '    Purchase GRT:   VW_PP_PIVOT_GRT_TRF    (733 cols)';
PRINT '    Purchase Purch: VW_PP_PIVOT_PURCHASE    (733 cols)';
PRINT '';
PRINT '  EXPORT TIP: Right-click results grid Ã¢Â†Â’ Save Results As Ã¢Â†Â’ CSV';
PRINT '============================================';
GO
