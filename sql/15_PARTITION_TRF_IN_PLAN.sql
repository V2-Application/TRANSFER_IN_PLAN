/*==============================================================
  TABLE PARTITIONING FOR TRF_IN_PLAN
  Run this when data volume grows large (millions+ rows).

  Partitions by WEEK_ID for:
  - Fast DELETE of old weeks (partition switch, instant)
  - Parallel query execution across weeks
  - Efficient bulk loading per week partition

  NOTE: Table partitioning requires SQL Server Enterprise
  or Developer edition. For Standard edition, consider
  partitioned views instead.
==============================================================*/

USE [planning];
GO

-- 1. Drop scheme first (it depends on the function — must go before the function)
IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = 'ps_WeekID')
    DROP PARTITION SCHEME ps_WeekID;
GO

-- 2. Drop function only after the scheme is gone
IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = 'pf_WeekID')
    DROP PARTITION FUNCTION pf_WeekID;
GO

-- 3. Create partition function (52 weeks)
CREATE PARTITION FUNCTION pf_WeekID (INT)
AS RANGE RIGHT FOR VALUES (
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,52
);
GO

-- 4. Create partition scheme (all on PRIMARY - change for
--    multiple filegroups in production)

CREATE PARTITION SCHEME ps_WeekID
AS PARTITION pf_WeekID
ALL TO ([PRIMARY]);
GO

-- 3. To apply to existing table, you need to:
--    a) Create new partitioned table
--    b) Copy data
--    c) Swap names
--
-- EXAMPLE (uncomment when ready):
/*
-- Create partitioned version
CREATE TABLE dbo.TRF_IN_PLAN_PART (
    [ID]                    BIGINT          IDENTITY(1,1) NOT NULL,
    [ST_CD]                 VARCHAR(20)     NOT NULL,
    [ST_NM]                 NVARCHAR(100)   NULL,
    [RDC_CD]                VARCHAR(20)     NULL,
    [RDC_NM]                NVARCHAR(100)   NULL,
    [HUB_CD]                VARCHAR(20)     NULL,
    [HUB_NM]                NVARCHAR(100)   NULL,
    [AREA]                  VARCHAR(50)     NULL,
    [MAJ_CAT]               VARCHAR(50)     NOT NULL,
    [SSN]                   VARCHAR(10)     NULL,
    [WEEK_ID]               INT             NOT NULL,
    [WK_ST_DT]              DATE            NULL,
    [WK_END_DT]             DATE            NULL,
    [FY_YEAR]               INT             NULL,
    [FY_WEEK]               INT             NULL,
    [S_GRT_STK_Q]           DECIMAL(18,4)   NULL DEFAULT 0,
    [W_GRT_STK_Q]           DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_DISP_CL_Q]         DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_DISP_CL_OPT]       DECIMAL(18,4)   NULL DEFAULT 0,
    [CM1_SALE_COVER_DAY]     DECIMAL(18,4)   NULL DEFAULT 0,
    [CM2_SALE_COVER_DAY]     DECIMAL(18,4)   NULL DEFAULT 0,
    [COVER_SALE_QTY]         DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_ST_CL_MBQ]         DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_DISP_CL_OPT_MBQ]   DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_TTL_CF_OP_STK_Q]   DECIMAL(18,4)   NULL DEFAULT 0,
    [NT_ACT_Q]              DECIMAL(18,4)   NULL DEFAULT 0,
    [NET_BGT_CF_STK_Q]      DECIMAL(18,4)   NULL DEFAULT 0,
    [CM_BGT_SALE_Q]         DECIMAL(18,4)   NULL DEFAULT 0,
    [CM1_BGT_SALE_Q]        DECIMAL(18,4)   NULL DEFAULT 0,
    [CM2_BGT_SALE_Q]        DECIMAL(18,4)   NULL DEFAULT 0,
    [TRF_IN_STK_Q]          DECIMAL(18,4)   NULL DEFAULT 0,
    [TRF_IN_OPT_CNT]        DECIMAL(18,4)   NULL DEFAULT 0,
    [TRF_IN_OPT_MBQ]        DECIMAL(18,4)   NULL DEFAULT 0,
    [DC_MBQ]                DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_TTL_CF_CL_STK_Q]   DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_NT_ACT_Q]          DECIMAL(18,4)   NULL DEFAULT 0,
    [NET_ST_CL_STK_Q]       DECIMAL(18,4)   NULL DEFAULT 0,
    [ST_CL_EXCESS_Q]        DECIMAL(18,4)   NULL DEFAULT 0,
    [ST_CL_SHORT_Q]         DECIMAL(18,4)   NULL DEFAULT 0,
    [SEG]                   VARCHAR(100)    NULL DEFAULT 'NA',
    [DIV]                   VARCHAR(100)    NULL DEFAULT 'NA',
    [SUB_DIV]               VARCHAR(100)    NULL DEFAULT 'NA',
    [MAJ_CAT_NM]            VARCHAR(100)    NULL DEFAULT 'NA',
    [CREATED_DT]            DATETIME        NOT NULL DEFAULT GETDATE(),
    [CREATED_BY]            VARCHAR(50)     NULL DEFAULT SYSTEM_USER,
    CONSTRAINT PK_TRF_IN_PLAN_PART PRIMARY KEY CLUSTERED ([ID], [WEEK_ID])
) ON ps_WeekID ([WEEK_ID]);

-- Note: ID changed to BIGINT for trillion-scale.
-- PK includes WEEK_ID (required for partition alignment).

-- Aligned indexes
CREATE NONCLUSTERED INDEX IX_PART_STCD_MAJ
    ON dbo.TRF_IN_PLAN_PART ([ST_CD], [MAJ_CAT], [WEEK_ID])
    ON ps_WeekID ([WEEK_ID]);

CREATE NONCLUSTERED INDEX IX_PART_RDC
    ON dbo.TRF_IN_PLAN_PART ([RDC_CD], [MAJ_CAT])
    ON ps_WeekID ([WEEK_ID]);

-- Copy data
INSERT INTO dbo.TRF_IN_PLAN_PART (...) SELECT ... FROM dbo.TRF_IN_PLAN;

-- Swap
EXEC sp_rename 'TRF_IN_PLAN', 'TRF_IN_PLAN_OLD';
EXEC sp_rename 'TRF_IN_PLAN_PART', 'TRF_IN_PLAN';
*/

PRINT '>> Partition function and scheme created.';
PRINT '>> Uncomment the table creation section when ready to apply.';
GO
