/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 5 of 5: VIEWS, SUMMARY QUERIES & EXECUTION
==============================================================*/

USE [planning];
GO

--------------------------------------------------------------
-- VIEW 1: Store Transfer In Summary (by store x week)
--------------------------------------------------------------
IF OBJECT_ID('dbo.VW_TRF_IN_STORE_SUMMARY','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_STORE_SUMMARY;
GO

CREATE VIEW dbo.VW_TRF_IN_STORE_SUMMARY
AS
SELECT
    T.ST_CD,
    T.ST_NM,
    T.RDC_CD,
    T.RDC_NM,
    T.AREA,
    T.WEEK_ID,
    T.WK_ST_DT,
    T.FY_YEAR,
    T.FY_WEEK,
    T.SSN,
    COUNT(DISTINCT T.MAJ_CAT)      AS CAT_COUNT,
    SUM(T.TRF_IN_STK_Q)            AS TOTAL_TRF_IN_QTY,
    SUM(T.CM_BGT_SALE_Q)           AS TOTAL_CM_SALE,
    SUM(T.BGT_TTL_CF_OP_STK_Q)     AS TOTAL_OP_STK,
    SUM(T.BGT_TTL_CF_CL_STK_Q)     AS TOTAL_CL_STK,
    SUM(T.ST_CL_EXCESS_Q)          AS TOTAL_EXCESS,
    SUM(T.ST_CL_SHORT_Q)           AS TOTAL_SHORT,
    SUM(T.DC_MBQ)                   AS TOTAL_DC_MBQ
FROM dbo.TRF_IN_PLAN T
GROUP BY
    T.ST_CD, T.ST_NM, T.RDC_CD, T.RDC_NM, T.AREA,
    T.WEEK_ID, T.WK_ST_DT, T.FY_YEAR, T.FY_WEEK, T.SSN;
GO

PRINT '>> VW_TRF_IN_STORE_SUMMARY created.';
GO

--------------------------------------------------------------
-- VIEW 2: RDC Transfer In Summary (aggregated by RDC)
--------------------------------------------------------------
IF OBJECT_ID('dbo.VW_TRF_IN_RDC_SUMMARY','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_RDC_SUMMARY;
GO

CREATE VIEW dbo.VW_TRF_IN_RDC_SUMMARY
AS
SELECT
    T.RDC_CD,
    T.RDC_NM,
    T.MAJ_CAT,
    T.WEEK_ID,
    T.WK_ST_DT,
    T.FY_YEAR,
    T.SSN,
    COUNT(DISTINCT T.ST_CD)         AS STORE_COUNT,
    SUM(T.TRF_IN_STK_Q)            AS TOTAL_TRF_IN_QTY,
    SUM(T.TRF_IN_OPT_CNT)          AS TOTAL_TRF_IN_OPT,
    SUM(T.DC_MBQ)                   AS TOTAL_DC_MBQ,
    SUM(T.BGT_ST_CL_MBQ)           AS TOTAL_MBQ_REQUIREMENT,
    SUM(T.ST_CL_SHORT_Q)           AS TOTAL_SHORT_QTY,
    SUM(T.ST_CL_EXCESS_Q)          AS TOTAL_EXCESS_QTY
FROM dbo.TRF_IN_PLAN T
GROUP BY
    T.RDC_CD, T.RDC_NM, T.MAJ_CAT,
    T.WEEK_ID, T.WK_ST_DT, T.FY_YEAR, T.SSN;
GO

PRINT '>> VW_TRF_IN_RDC_SUMMARY created.';
GO

--------------------------------------------------------------
-- VIEW 3: Category-wise Transfer In Summary
--------------------------------------------------------------
IF OBJECT_ID('dbo.VW_TRF_IN_CATEGORY_SUMMARY','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_CATEGORY_SUMMARY;
GO

CREATE VIEW dbo.VW_TRF_IN_CATEGORY_SUMMARY
AS
SELECT
    T.MAJ_CAT,
    T.FY_YEAR,
    T.SSN,
    COUNT(DISTINCT T.ST_CD)         AS STORE_COUNT,
    COUNT(DISTINCT T.WEEK_ID)       AS WEEK_COUNT,
    SUM(T.TRF_IN_STK_Q)            AS TOTAL_TRF_IN_QTY,
    SUM(T.CM_BGT_SALE_Q)           AS TOTAL_SALE_QTY,
    SUM(T.BGT_DISP_CL_Q)          AS TOTAL_DISPLAY_QTY,
    SUM(T.ST_CL_EXCESS_Q)          AS TOTAL_EXCESS,
    SUM(T.ST_CL_SHORT_Q)           AS TOTAL_SHORT,
    CASE WHEN SUM(T.CM_BGT_SALE_Q) > 0
        THEN ROUND(SUM(T.TRF_IN_STK_Q) * 100.0 / SUM(T.CM_BGT_SALE_Q), 1)
        ELSE 0 END                  AS TRF_IN_TO_SALE_PCT
FROM dbo.TRF_IN_PLAN T
GROUP BY T.MAJ_CAT, T.FY_YEAR, T.SSN;
GO

PRINT '>> VW_TRF_IN_CATEGORY_SUMMARY created.';
GO

--------------------------------------------------------------
-- VIEW 4: Excess & Short Alert View
--------------------------------------------------------------
IF OBJECT_ID('dbo.VW_TRF_IN_ALERTS','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_ALERTS;
GO

CREATE VIEW dbo.VW_TRF_IN_ALERTS
AS
SELECT
    T.ST_CD,
    T.ST_NM,
    T.MAJ_CAT,
    T.WEEK_ID,
    T.WK_ST_DT,
    T.SSN,
    T.BGT_ST_CL_MBQ,
    T.NET_ST_CL_STK_Q,
    T.ST_CL_EXCESS_Q,
    T.ST_CL_SHORT_Q,
    CASE
        WHEN T.ST_CL_SHORT_Q > T.BGT_ST_CL_MBQ * 0.3 THEN 'CRITICAL SHORT'
        WHEN T.ST_CL_SHORT_Q > 0                       THEN 'SHORT'
        WHEN T.ST_CL_EXCESS_Q > T.BGT_ST_CL_MBQ * 0.5 THEN 'HIGH EXCESS'
        WHEN T.ST_CL_EXCESS_Q > 0                      THEN 'EXCESS'
        ELSE 'OK'
    END AS ALERT_STATUS,
    T.TRF_IN_STK_Q
FROM dbo.TRF_IN_PLAN T
WHERE T.ST_CL_EXCESS_Q > 0 OR T.ST_CL_SHORT_Q > 0;
GO

PRINT '>> VW_TRF_IN_ALERTS created.';
GO

--------------------------------------------------------------
-- EXECUTION: Generate Transfer In Plan for all 52 weeks
--------------------------------------------------------------

-- ⚠️ SCALE WARNING: Do NOT call SP_GENERATE_TRF_IN_PLAN with NULL StoreCode
-- at 1,000-store × 1,000-category scale. The internal #Chain temp table would
-- reach ~52M rows (~4 GB) in a single execution and will cause OOM / timeout.
--
-- USE SP_RUN_ALL_PLANS INSTEAD — it processes one store at a time via a WHILE loop.
--
-- COMMENTED OUT (unsafe at production scale):
-- EXEC dbo.SP_GENERATE_TRF_IN_PLAN
--     @StartWeekID  = 1,
--     @EndWeekID    = 52,
--     @StoreCode    = NULL,   -- ← NULL processes ALL stores at once — DO NOT USE
--     @MajCat       = NULL,
--     @CoverDaysCM1 = 14,
--     @CoverDaysCM2 = 0,
--     @Debug        = 1;

-- CORRECT production execution — runs per store, 52 weeks:
EXEC dbo.SP_RUN_ALL_PLANS
    @StartWeekID = 1,
    @EndWeekID   = 52;
GO

--------------------------------------------------------------
-- VERIFICATION QUERIES
--------------------------------------------------------------

-- 1. Total row count
SELECT COUNT(*) AS TotalPlanRows FROM dbo.TRF_IN_PLAN;

-- 2. Sample output for ST001 / APPAREL / first 4 weeks
SELECT TOP 4
    ST_CD, MAJ_CAT, WEEK_ID, SSN,
    BGT_TTL_CF_OP_STK_Q AS OP_STK,
    CM_BGT_SALE_Q AS CM_SALE,
    BGT_ST_CL_MBQ AS MBQ,
    TRF_IN_STK_Q AS TRF_IN,
    BGT_TTL_CF_CL_STK_Q AS CL_STK,
    ST_CL_EXCESS_Q AS EXCESS,
    ST_CL_SHORT_Q AS SHORT
FROM dbo.TRF_IN_PLAN
WHERE ST_CD = 'ST001' AND MAJ_CAT = 'APPAREL'
ORDER BY WEEK_ID;

-- 3. RDC-level summary
SELECT * FROM dbo.VW_TRF_IN_RDC_SUMMARY
WHERE WEEK_ID = 1
ORDER BY RDC_CD, MAJ_CAT;

-- 4. Alerts check
SELECT TOP 20 * FROM dbo.VW_TRF_IN_ALERTS
ORDER BY
    CASE ALERT_STATUS
        WHEN 'CRITICAL SHORT' THEN 1
        WHEN 'SHORT' THEN 2
        WHEN 'HIGH EXCESS' THEN 3
        ELSE 4 END,
    WEEK_ID;

PRINT '>> SETUP COMPLETE. Transfer In Plan generated and verified.';
GO
