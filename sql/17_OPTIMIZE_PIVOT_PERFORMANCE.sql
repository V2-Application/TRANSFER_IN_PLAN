-- =====================================================
-- FILE: 17_OPTIMIZE_PIVOT_PERFORMANCE.sql
-- PURPOSE: Speed up pivot view queries
--
-- APPROACH:
--   1. Add covering indexes for pivot GROUP BY pattern
--   2. Create physical pivot tables (pre-computed)
--   3. SP populates pivot tables after plan generation
--   4. SELECT from physical table = instant (vs view = minutes)
-- =====================================================

USE [planning];
GO

-- =====================================================
-- STEP 1: COVERING INDEXES for pivot patterns
-- =====================================================
PRINT '--- STEP 1: Creating covering indexes ---';

-- TRF_IN_PLAN: Index for pivot GROUP BY (ST_CD, MAJ_CAT, FY_YEAR, FY_WEEK)
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_TRF_PIVOT_COVER' AND object_id = OBJECT_ID('TRF_IN_PLAN'))
    DROP INDEX IX_TRF_PIVOT_COVER ON dbo.TRF_IN_PLAN;

CREATE NONCLUSTERED INDEX IX_TRF_PIVOT_COVER
    ON dbo.TRF_IN_PLAN ([ST_CD], [MAJ_CAT], [FY_YEAR], [FY_WEEK])
    INCLUDE ([SEG], [DIV], [SUB_DIV], [MAJ_CAT_NM], [SSN],
             [BGT_DISP_CL_Q], [BGT_ST_CL_MBQ], [BGT_TTL_CF_OP_STK_Q],
             [NT_ACT_Q], [NET_BGT_CF_STK_Q], [CM_BGT_SALE_Q],
             [CM1_BGT_SALE_Q], [CM2_BGT_SALE_Q], [TRF_IN_STK_Q],
             [BGT_TTL_CF_CL_STK_Q], [BGT_NT_ACT_Q], [NET_ST_CL_STK_Q],
             [ST_CL_EXCESS_Q], [ST_CL_SHORT_Q]);

PRINT '  IX_TRF_PIVOT_COVER created.';

-- PURCHASE_PLAN: Index for pivot GROUP BY (RDC_CD, MAJ_CAT, FY_YEAR, FY_WEEK)
IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_PP_PIVOT_COVER' AND object_id = OBJECT_ID('PURCHASE_PLAN'))
    DROP INDEX IX_PP_PIVOT_COVER ON dbo.PURCHASE_PLAN;

CREATE NONCLUSTERED INDEX IX_PP_PIVOT_COVER
    ON dbo.PURCHASE_PLAN ([RDC_CD], [MAJ_CAT], [FY_YEAR], [FY_WEEK])
    INCLUDE ([SEG], [DIV], [SUB_DIV], [MAJ_CAT_NM], [SSN], [RDC_NM],
             [BGT_DISP_CL_Q], [CW_BGT_SALE_Q], [CW1_BGT_SALE_Q],
             [CW2_BGT_SALE_Q], [CW3_BGT_SALE_Q], [CW4_BGT_SALE_Q],
             [CW5_BGT_SALE_Q], [BGT_ST_OP_MBQ], [NET_ST_OP_STK_Q],
             [BGT_DC_OP_STK_Q], [PP_NT_ACT_Q], [BGT_CF_STK_Q],
             [TTL_STK], [OP_STK],
             [NT_ACT_STK], [GRT_CONS_PCT], [GRT_CONS_Q],
             [DEL_PEND_Q], [PP_NET_BGT_CF_STK_Q],
             [TTL_TRF_OUT_Q], [CW_TRF_OUT_Q], [CW1_TRF_OUT_Q],
             [BGT_ST_CL_MBQ], [NET_BGT_ST_CL_STK_Q],
             [BGT_DC_CL_MBQ], [BGT_DC_CL_STK_Q],
             [BGT_PUR_Q_INIT], [POS_PO_RAISED], [NEG_PO_RAISED],
             [DC_STK_EXCESS_Q], [DC_STK_SHORT_Q],
             [CO_STK_EXCESS_Q], [CO_STK_SHORT_Q],
             [FRESH_BIN_REQ], [GRT_BIN_REQ]);

PRINT '  IX_PP_PIVOT_COVER created.';
GO


-- =====================================================
-- STEP 2: CREATE PHYSICAL PIVOT TABLES
-- These are pre-computed Ã¢Â€Â” SELECT is instant
-- =====================================================
PRINT '';
PRINT '--- STEP 2: Creating physical pivot tables ---';

-- 2A: Transfer In Pivot Table
IF OBJECT_ID('dbo.TRF_IN_PIVOT_DATA', 'U') IS NOT NULL
    DROP TABLE dbo.TRF_IN_PIVOT_DATA;

SELECT TOP 0 * INTO dbo.TRF_IN_PIVOT_DATA
FROM dbo.VW_TRF_IN_PIVOT;

CREATE CLUSTERED INDEX CX ON dbo.TRF_IN_PIVOT_DATA ([ST-CD], [MAJ-CAT]);
PRINT '  TRF_IN_PIVOT_DATA table created.';

-- 2B: Purchase Plan Ã¢Â€Â” 3 separate physical tables (avoids duplicate column names)
IF OBJECT_ID('dbo.PP_PIVOT_STOCK_DATA', 'U') IS NOT NULL
    DROP TABLE dbo.PP_PIVOT_STOCK_DATA;
IF OBJECT_ID('dbo.PP_PIVOT_GRT_TRF_DATA', 'U') IS NOT NULL
    DROP TABLE dbo.PP_PIVOT_GRT_TRF_DATA;
IF OBJECT_ID('dbo.PP_PIVOT_PURCHASE_DATA', 'U') IS NOT NULL
    DROP TABLE dbo.PP_PIVOT_PURCHASE_DATA;

SELECT TOP 0 * INTO dbo.PP_PIVOT_STOCK_DATA FROM dbo.VW_PP_PIVOT_STOCK;
CREATE CLUSTERED INDEX CX ON dbo.PP_PIVOT_STOCK_DATA ([RDC-CD], [MAJ-CAT]);
PRINT '  PP_PIVOT_STOCK_DATA table created.';

SELECT TOP 0 * INTO dbo.PP_PIVOT_GRT_TRF_DATA FROM dbo.VW_PP_PIVOT_GRT_TRF;
CREATE CLUSTERED INDEX CX ON dbo.PP_PIVOT_GRT_TRF_DATA ([RDC-CD], [MAJ-CAT]);
PRINT '  PP_PIVOT_GRT_TRF_DATA table created.';

SELECT TOP 0 * INTO dbo.PP_PIVOT_PURCHASE_DATA FROM dbo.VW_PP_PIVOT_PURCHASE;
CREATE CLUSTERED INDEX CX ON dbo.PP_PIVOT_PURCHASE_DATA ([RDC-CD], [MAJ-CAT]);
PRINT '  PP_PIVOT_PURCHASE_DATA table created.';
GO


-- =====================================================
-- STEP 3: CREATE SP TO REFRESH PIVOT TABLES
-- Run this after generating plans
-- =====================================================
PRINT '';
PRINT '--- STEP 3: Creating SP_REFRESH_PIVOT_DATA ---';

IF OBJECT_ID('dbo.SP_REFRESH_PIVOT_DATA', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_REFRESH_PIVOT_DATA;
GO

CREATE PROCEDURE dbo.SP_REFRESH_PIVOT_DATA
    @Debug BIT = 0
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @StepTime DATETIME;

    -- 1: Refresh Transfer In Pivot
    SET @StepTime = GETDATE();
    IF @Debug = 1 PRINT 'Refreshing TRF_IN_PIVOT_DATA...';

    TRUNCATE TABLE dbo.TRF_IN_PIVOT_DATA;

    INSERT INTO dbo.TRF_IN_PIVOT_DATA WITH (TABLOCK)
    SELECT * FROM dbo.VW_TRF_IN_PIVOT;

    IF @Debug = 1
        PRINT '  TRF_IN_PIVOT_DATA: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows, '
            + CAST(DATEDIFF(SECOND, @StepTime, GETDATE()) AS VARCHAR) + 's';

    -- 2: Refresh Purchase Plan Pivot (3 tables)
    SET @StepTime = GETDATE();
    IF @Debug = 1 PRINT 'Refreshing PP_PIVOT_STOCK_DATA...';

    TRUNCATE TABLE dbo.PP_PIVOT_STOCK_DATA;
    INSERT INTO dbo.PP_PIVOT_STOCK_DATA WITH (TABLOCK)
    SELECT * FROM dbo.VW_PP_PIVOT_STOCK;

    IF @Debug = 1
        PRINT '  PP_PIVOT_STOCK_DATA: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows, '
            + CAST(DATEDIFF(SECOND, @StepTime, GETDATE()) AS VARCHAR) + 's';

    SET @StepTime = GETDATE();
    IF @Debug = 1 PRINT 'Refreshing PP_PIVOT_GRT_TRF_DATA...';

    TRUNCATE TABLE dbo.PP_PIVOT_GRT_TRF_DATA;
    INSERT INTO dbo.PP_PIVOT_GRT_TRF_DATA WITH (TABLOCK)
    SELECT * FROM dbo.VW_PP_PIVOT_GRT_TRF;

    IF @Debug = 1
        PRINT '  PP_PIVOT_GRT_TRF_DATA: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows, '
            + CAST(DATEDIFF(SECOND, @StepTime, GETDATE()) AS VARCHAR) + 's';

    SET @StepTime = GETDATE();
    IF @Debug = 1 PRINT 'Refreshing PP_PIVOT_PURCHASE_DATA...';

    TRUNCATE TABLE dbo.PP_PIVOT_PURCHASE_DATA;
    INSERT INTO dbo.PP_PIVOT_PURCHASE_DATA WITH (TABLOCK)
    SELECT * FROM dbo.VW_PP_PIVOT_PURCHASE;

    IF @Debug = 1
        PRINT '  PP_PIVOT_PURCHASE_DATA: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows, '
            + CAST(DATEDIFF(SECOND, @StepTime, GETDATE()) AS VARCHAR) + 's';

    -- Summary
    DECLARE @trfPivot INT, @ppStock INT, @ppGrt INT, @ppPur INT;
    SELECT @trfPivot = COUNT(*) FROM dbo.TRF_IN_PIVOT_DATA;
    SELECT @ppStock = COUNT(*) FROM dbo.PP_PIVOT_STOCK_DATA;
    SELECT @ppGrt = COUNT(*) FROM dbo.PP_PIVOT_GRT_TRF_DATA;
    SELECT @ppPur = COUNT(*) FROM dbo.PP_PIVOT_PURCHASE_DATA;

    PRINT '';
    PRINT '========================================';
    PRINT 'PIVOT DATA REFRESH COMPLETE';
    PRINT '  TRF_IN_PIVOT_DATA:      ' + CAST(@trfPivot AS VARCHAR) + ' rows';
    PRINT '  PP_PIVOT_STOCK_DATA:     ' + CAST(@ppStock AS VARCHAR) + ' rows';
    PRINT '  PP_PIVOT_GRT_TRF_DATA:   ' + CAST(@ppGrt AS VARCHAR) + ' rows';
    PRINT '  PP_PIVOT_PURCHASE_DATA:   ' + CAST(@ppPur AS VARCHAR) + ' rows';
    PRINT '  Total time: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds';
    PRINT '';
    PRINT 'FAST QUERIES (use these instead of views):';
    PRINT '  SELECT * FROM dbo.TRF_IN_PIVOT_DATA (NOLOCK) ORDER BY [ST-CD], [MAJ-CAT];';
    PRINT '  SELECT * FROM dbo.PP_PIVOT_STOCK_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
    PRINT '  SELECT * FROM dbo.PP_PIVOT_GRT_TRF_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
    PRINT '  SELECT * FROM dbo.PP_PIVOT_PURCHASE_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
    PRINT '========================================';

    SELECT @trfPivot AS TRF_Rows, @ppStock AS PP_Stock_Rows,
           @ppGrt AS PP_GRT_Rows, @ppPur AS PP_Purchase_Rows,
           DATEDIFF(SECOND, @StartTime, GETDATE()) AS TotalSeconds;
END;
GO

PRINT '  SP_REFRESH_PIVOT_DATA created.';
GO


-- =====================================================
-- STEP 4: INITIAL LOAD Ã¢Â€Â” populate pivot tables now
-- =====================================================
PRINT '';
PRINT '--- STEP 4: Initial pivot data load ---';
EXEC dbo.SP_REFRESH_PIVOT_DATA @Debug = 1;
GO

PRINT '';
PRINT '========================================';
PRINT 'DONE! Use these fast queries:';
PRINT '';
PRINT '  -- Transfer In (instant)';
PRINT '  SELECT * FROM dbo.TRF_IN_PIVOT_DATA (NOLOCK) ORDER BY [ST-CD], [MAJ-CAT];';
PRINT '';
PRINT '  -- Purchase Plan Stock (instant)';
PRINT '  SELECT * FROM dbo.PP_PIVOT_STOCK_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
PRINT '';
PRINT '  -- Purchase Plan GRT & Transfer (instant)';
PRINT '  SELECT * FROM dbo.PP_PIVOT_GRT_TRF_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
PRINT '';
PRINT '  -- Purchase Plan Purchase (instant)';
PRINT '  SELECT * FROM dbo.PP_PIVOT_PURCHASE_DATA (NOLOCK) ORDER BY [RDC-CD], [MAJ-CAT];';
PRINT '';
PRINT '  -- After re-running SPs, refresh with:';
PRINT '  EXEC dbo.SP_REFRESH_PIVOT_DATA @Debug = 1;';
PRINT '========================================';
GO
