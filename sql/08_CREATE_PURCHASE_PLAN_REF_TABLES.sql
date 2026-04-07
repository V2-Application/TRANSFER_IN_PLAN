-- =====================================================
-- FILE: 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql
-- PURPOSE: Create reference tables for Purchase Plan system
-- DATABASE: [planning]
-- =====================================================

USE [planning];
GO

-- =====================================================
-- TABLE: QTY_DEL_PENDING
-- DESCRIPTION: Tracks quantity of deliveries pending by RDC and category
-- GRAIN: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— DATE (latest snapshot)
-- =====================================================

IF OBJECT_ID('dbo.QTY_DEL_PENDING', 'U') IS NOT NULL
    DROP TABLE dbo.QTY_DEL_PENDING;
GO

CREATE TABLE dbo.QTY_DEL_PENDING (
    [ID] INT IDENTITY(1,1) NOT NULL,
    [RDC_CD] VARCHAR(20) NOT NULL,
    [MAJ_CAT] VARCHAR(50) NOT NULL,
    [DEL_PEND_Q] DECIMAL(18,4) NULL DEFAULT 0,
    [DATE] DATE NOT NULL,

    CONSTRAINT PK_QTY_DEL_PENDING PRIMARY KEY CLUSTERED ([ID])
);

GO

-- =====================================================
-- INDEXES: QTY_DEL_PENDING
-- =====================================================

-- Index for quick lookup by RDC_CD and MAJ_CAT (with DATE for latest)
CREATE NONCLUSTERED INDEX IDX_QTY_DEL_PENDING_RDC_CAT_DT
    ON dbo.QTY_DEL_PENDING ([RDC_CD] ASC, [MAJ_CAT] ASC, [DATE] DESC)
    INCLUDE ([DEL_PEND_Q]);

GO

-- Index for date-based queries (for refresh/archival processes)
CREATE NONCLUSTERED INDEX IDX_QTY_DEL_PENDING_DATE
    ON dbo.QTY_DEL_PENDING ([DATE] ASC)
    INCLUDE ([RDC_CD], [MAJ_CAT], [DEL_PEND_Q]);

GO

-- =====================================================
-- NOTES ON EXISTING REFERENCE TABLES
-- =====================================================
-- The following tables are assumed to already exist in [planning]:
--
-- WEEK_CALENDAR:
--   Columns: WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT
--
-- MASTER_ST_MASTER:
--   Columns: [ST CD], [ST NM], RDC_CD, RDC_NM, HUB_CD, HUB_NM, STATUS, AREA, etc.
--
-- MASTER_BIN_CAPACITY:
--   Columns: [MAJ-CAT], [BIN CAP DC TEAM], [BIN CAP]
--   Note: Column names use hyphens; in queries, reference as [MAJ-CAT], [BIN CAP DC TEAM], [BIN CAP]
--
-- MASTER_GRT_CONS_percentage:
--   Columns: SSN, [WK-1] through [WK-48] (48 weekly columns, each is decimal percentage)
--
-- QTY_MSA_AND_GRT:
--   Columns: RDC_CD, RDC, [MAJ-CAT], [DC-STK-Q], [GRT-STK-Q], [W-GRT-STK-Q], DATE
--   Note: These columns contain hyphenated names; reference with square brackets in queries
--
-- TRF_IN_PLAN:
--   Columns: ST_CD, ST_NM, RDC_CD, RDC_NM, MAJ_CAT, SSN, WEEK_ID, FY_WEEK, FY_YEAR,
--            WK_ST_DT, WK_END_DT, S_GRT_STK_Q, W_GRT_STK_Q, BGT_DISP_CL_Q, BGT_ST_CL_MBQ,
--            BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q, CM_BGT_SALE_Q, CM1_BGT_SALE_Q,
--            CM2_BGT_SALE_Q, TRF_IN_STK_Q, DC_MBQ, BGT_TTL_CF_CL_STK_Q, NET_ST_CL_STK_Q,
--            ST_CL_EXCESS_Q, ST_CL_SHORT_Q

PRINT 'Reference tables created successfully.';
GO
