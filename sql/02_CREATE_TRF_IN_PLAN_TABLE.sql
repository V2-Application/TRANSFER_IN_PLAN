/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 2 of 5: CREATE OUTPUT TABLE - TRF_IN_PLAN
  This table stores the calculated Transfer In Plan output
  at ST-CD x MAJ-CAT x WEEK granularity.
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.TRF_IN_PLAN','U') IS NOT NULL
    DROP TABLE dbo.TRF_IN_PLAN;
GO

CREATE TABLE dbo.TRF_IN_PLAN (
    -- Identifiers
    [ID]                    INT             IDENTITY(1,1) NOT NULL,
    [ST_CD]                 VARCHAR(20)     NOT NULL,
    [ST_NM]                 NVARCHAR(100)   NULL DEFAULT 'NA',
    [RDC_CD]                VARCHAR(20)     NULL DEFAULT 'NA',
    [RDC_NM]                NVARCHAR(100)   NULL DEFAULT 'NA',
    [HUB_CD]                VARCHAR(20)     NULL DEFAULT 'NA',
    [HUB_NM]                NVARCHAR(100)   NULL DEFAULT 'NA',
    [AREA]                  VARCHAR(50)     NULL DEFAULT 'NA',
    [MAJ_CAT]               VARCHAR(50)     NOT NULL,
    [SSN]                   VARCHAR(10)     NULL DEFAULT 'NA',
    [WEEK_ID]               INT             NOT NULL,
    [WK_ST_DT]              DATE            NULL,
    [WK_END_DT]             DATE            NULL,
    [FY_YEAR]               INT             NULL,
    [FY_WEEK]               INT             NULL,

    -- GRT Stock
    [S_GRT_STK_Q]           DECIMAL(18,4)   NULL DEFAULT 0,
    [W_GRT_STK_Q]           DECIMAL(18,4)   NULL DEFAULT 0,

    -- Display
    [BGT_DISP_CL_Q]         DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_DISP_CL_OPT]       DECIMAL(18,4)   NULL DEFAULT 0,

    -- Cover Days & Cover Sale
    [CM1_SALE_COVER_DAY]     DECIMAL(18,4)   NULL DEFAULT 0,
    [CM2_SALE_COVER_DAY]     DECIMAL(18,4)   NULL DEFAULT 0,
    [COVER_SALE_QTY]         DECIMAL(18,4)   NULL DEFAULT 0,

    -- MBQ Targets
    [BGT_ST_CL_MBQ]         DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_DISP_CL_OPT_MBQ]   DECIMAL(18,4)   NULL DEFAULT 0,

    -- Opening & Net Stock
    [BGT_TTL_CF_OP_STK_Q]   DECIMAL(18,4)   NULL DEFAULT 0,
    [NT_ACT_Q]              DECIMAL(18,4)   NULL DEFAULT 0,
    [NET_BGT_CF_STK_Q]      DECIMAL(18,4)   NULL DEFAULT 0,

    -- Budget Sale Qty
    [CM_BGT_SALE_Q]         DECIMAL(18,4)   NULL DEFAULT 0,
    [CM1_BGT_SALE_Q]        DECIMAL(18,4)   NULL DEFAULT 0,
    [CM2_BGT_SALE_Q]        DECIMAL(18,4)   NULL DEFAULT 0,

    -- Transfer In
    [TRF_IN_STK_Q]          DECIMAL(18,4)   NULL DEFAULT 0,
    [TRF_IN_OPT_CNT]        DECIMAL(18,4)   NULL DEFAULT 0,
    [TRF_IN_OPT_MBQ]        DECIMAL(18,4)   NULL DEFAULT 0,

    -- DC MBQ
    [DC_MBQ]                DECIMAL(18,4)   NULL DEFAULT 0,

    -- Closing Stock
    [BGT_TTL_CF_CL_STK_Q]   DECIMAL(18,4)   NULL DEFAULT 0,
    [BGT_NT_ACT_Q]          DECIMAL(18,4)   NULL DEFAULT 0,
    [NET_ST_CL_STK_Q]       DECIMAL(18,4)   NULL DEFAULT 0,

    -- Excess / Short
    [ST_CL_EXCESS_Q]        DECIMAL(18,4)   NULL DEFAULT 0,
    [ST_CL_SHORT_Q]         DECIMAL(18,4)   NULL DEFAULT 0,

    -- Category Hierarchy (from Broader_Menu)
    [SEG]                   VARCHAR(100)    NULL DEFAULT 'NA',
    [DIV]                   VARCHAR(100)    NULL DEFAULT 'NA',
    [SUB_DIV]               VARCHAR(100)    NULL DEFAULT 'NA',
    [MAJ_CAT_NM]            VARCHAR(100)    NULL DEFAULT 'NA',

    -- Metadata
    [CREATED_DT]            DATETIME        NOT NULL DEFAULT GETDATE(),
    [CREATED_BY]            VARCHAR(50)     NULL DEFAULT SYSTEM_USER,

    CONSTRAINT PK_TRF_IN_PLAN PRIMARY KEY CLUSTERED ([ID])
);
GO

-- Performance indexes
CREATE NONCLUSTERED INDEX IX_TRF_PLAN_STCD_MAJ
    ON dbo.TRF_IN_PLAN ([ST_CD], [MAJ_CAT], [WEEK_ID]);

CREATE NONCLUSTERED INDEX IX_TRF_PLAN_WEEK
    ON dbo.TRF_IN_PLAN ([WEEK_ID], [FY_YEAR]);

CREATE NONCLUSTERED INDEX IX_TRF_PLAN_RDC
    ON dbo.TRF_IN_PLAN ([RDC_CD], [MAJ_CAT]);

CREATE NONCLUSTERED INDEX IX_TRF_PLAN_DATES
    ON dbo.TRF_IN_PLAN ([WK_ST_DT], [WK_END_DT]);
GO

PRINT '>> TRF_IN_PLAN output table created successfully.';
GO
