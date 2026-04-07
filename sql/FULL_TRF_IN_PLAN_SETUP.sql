/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 1 of 5: CREATE REFERENCE TABLES
  Database: [planning]
  Run this script first to create all reference/master tables.
==============================================================*/

USE [planning];
GO

--------------------------------------------------------------
-- 1. WEEK_CALENDAR
--------------------------------------------------------------
IF OBJECT_ID('dbo.WEEK_CALENDAR','U') IS NOT NULL
    DROP TABLE dbo.WEEK_CALENDAR;
GO

CREATE TABLE dbo.WEEK_CALENDAR (
    WEEK_ID         INT             NOT NULL,
    WEEK_SEQ        INT             NOT NULL,
    FY_WEEK         INT             NOT NULL,
    FY_YEAR         INT             NOT NULL,
    CAL_YEAR        INT             NOT NULL,
    YEAR_WEEK       VARCHAR(10)     NOT NULL,
    WK_ST_DT        DATE            NOT NULL,
    WK_END_DT       DATE            NOT NULL,
    CONSTRAINT PK_WEEK_CALENDAR PRIMARY KEY (WEEK_ID)
);
GO

CREATE INDEX IX_WEEK_CALENDAR_DATES ON dbo.WEEK_CALENDAR (WK_ST_DT, WK_END_DT);
CREATE INDEX IX_WEEK_CALENDAR_FY    ON dbo.WEEK_CALENDAR (FY_YEAR, FY_WEEK);
GO

--------------------------------------------------------------
-- 2. MASTER_ST_MASTER (Store Master)
--------------------------------------------------------------
IF OBJECT_ID('dbo.MASTER_ST_MASTER','U') IS NOT NULL
    DROP TABLE dbo.MASTER_ST_MASTER;
GO

CREATE TABLE dbo.MASTER_ST_MASTER (
    [ST CD]         VARCHAR(20)     NOT NULL,
    [ST NM]         NVARCHAR(100)   NOT NULL,
    [RDC_CD]        VARCHAR(20)     NULL,
    [RDC_NM]        NVARCHAR(100)   NULL,
    [HUB_CD]        VARCHAR(20)     NULL,
    [HUB_NM]        NVARCHAR(100)   NULL,
    [STATUS]        VARCHAR(20)     NULL,
    [GRID_ST_STS]   VARCHAR(20)     NULL,
    [OP-DATE]       DATE            NULL,
    [AREA]          VARCHAR(50)     NULL,
    [STATE]         VARCHAR(50)     NULL,
    [REF STATE]     VARCHAR(50)     NULL,
    [SALE GRP]      VARCHAR(50)     NULL,
    [REF_ST CD]     VARCHAR(20)     NULL,
    [REF_ST NM]     NVARCHAR(100)   NULL,
    [REF-GRP-NEW]   VARCHAR(50)     NULL,
    [REF-GRP-OLD]   VARCHAR(50)     NULL,
    [Date]          DATE            NULL,
    [ID]            INT             IDENTITY(1,1) NOT NULL,
    CONSTRAINT PK_MASTER_ST_MASTER PRIMARY KEY ([ID])
);
GO

CREATE INDEX IX_ST_MASTER_STCD  ON dbo.MASTER_ST_MASTER ([ST CD]);
CREATE INDEX IX_ST_MASTER_RDC   ON dbo.MASTER_ST_MASTER ([RDC_CD]);
CREATE INDEX IX_ST_MASTER_HUB   ON dbo.MASTER_ST_MASTER ([HUB_CD]);
GO

--------------------------------------------------------------
-- 3. MASTER_BIN_CAPACITY
--------------------------------------------------------------
IF OBJECT_ID('dbo.MASTER_BIN_CAPACITY','U') IS NOT NULL
    DROP TABLE dbo.MASTER_BIN_CAPACITY;
GO

CREATE TABLE dbo.MASTER_BIN_CAPACITY (
    [MAJ-CAT]           VARCHAR(50)     NOT NULL,
    [BIN CAP DC TEAM]   DECIMAL(18,4)   NULL,
    [BIN CAP]           DECIMAL(18,4)   NOT NULL,
    [ID]                INT             IDENTITY(1,1) NOT NULL,
    CONSTRAINT PK_MASTER_BIN_CAPACITY PRIMARY KEY ([ID])
);
GO

CREATE INDEX IX_BIN_CAP_MAJCAT ON dbo.MASTER_BIN_CAPACITY ([MAJ-CAT]);
GO

--------------------------------------------------------------
-- 4. MASTER_GRT_CONS_PERCENTAGE
--------------------------------------------------------------
IF OBJECT_ID('dbo.MASTER_GRT_CONS_percentage','U') IS NOT NULL
    DROP TABLE dbo.MASTER_GRT_CONS_percentage;
GO

CREATE TABLE dbo.MASTER_GRT_CONS_percentage (
    [SSN]       VARCHAR(10)     NOT NULL,
    [WK-1]      DECIMAL(18,4)  NULL, [WK-2]  DECIMAL(18,4) NULL, [WK-3]  DECIMAL(18,4) NULL,
    [WK-4]      DECIMAL(18,4)  NULL, [WK-5]  DECIMAL(18,4) NULL, [WK-6]  DECIMAL(18,4) NULL,
    [WK-7]      DECIMAL(18,4)  NULL, [WK-8]  DECIMAL(18,4) NULL, [WK-9]  DECIMAL(18,4) NULL,
    [WK-10]     DECIMAL(18,4)  NULL, [WK-11] DECIMAL(18,4) NULL, [WK-12] DECIMAL(18,4) NULL,
    [WK-13]     DECIMAL(18,4)  NULL, [WK-14] DECIMAL(18,4) NULL, [WK-15] DECIMAL(18,4) NULL,
    [WK-16]     DECIMAL(18,4)  NULL, [WK-17] DECIMAL(18,4) NULL, [WK-18] DECIMAL(18,4) NULL,
    [WK-19]     DECIMAL(18,4)  NULL, [WK-20] DECIMAL(18,4) NULL, [WK-21] DECIMAL(18,4) NULL,
    [WK-22]     DECIMAL(18,4)  NULL, [WK-23] DECIMAL(18,4) NULL, [WK-24] DECIMAL(18,4) NULL,
    [WK-25]     DECIMAL(18,4)  NULL, [WK-26] DECIMAL(18,4) NULL, [WK-27] DECIMAL(18,4) NULL,
    [WK-28]     DECIMAL(18,4)  NULL, [WK-29] DECIMAL(18,4) NULL, [WK-30] DECIMAL(18,4) NULL,
    [WK-31]     DECIMAL(18,4)  NULL, [WK-32] DECIMAL(18,4) NULL, [WK-33] DECIMAL(18,4) NULL,
    [WK-34]     DECIMAL(18,4)  NULL, [WK-35] DECIMAL(18,4) NULL, [WK-36] DECIMAL(18,4) NULL,
    [WK-37]     DECIMAL(18,4)  NULL, [WK-38] DECIMAL(18,4) NULL, [WK-39] DECIMAL(18,4) NULL,
    [WK-40]     DECIMAL(18,4)  NULL, [WK-41] DECIMAL(18,4) NULL, [WK-42] DECIMAL(18,4) NULL,
    [WK-43]     DECIMAL(18,4)  NULL, [WK-44] DECIMAL(18,4) NULL, [WK-45] DECIMAL(18,4) NULL,
    [WK-46]     DECIMAL(18,4)  NULL, [WK-47] DECIMAL(18,4) NULL, [WK-48] DECIMAL(18,4) NULL,
    [2]         DECIMAL(18,4)  NULL
);
GO

CREATE INDEX IX_GRT_CONS_SSN ON dbo.MASTER_GRT_CONS_percentage ([SSN]);
GO

--------------------------------------------------------------
-- 5. QTY_SALE_QTY (Store x MajCat weekly sale plan)
--------------------------------------------------------------
IF OBJECT_ID('dbo.QTY_SALE_QTY','U') IS NOT NULL
    DROP TABLE dbo.QTY_SALE_QTY;
GO

CREATE TABLE dbo.QTY_SALE_QTY (
    [ST-CD]     VARCHAR(20)     NOT NULL,
    [MAJ-CAT]   VARCHAR(50)     NOT NULL,
    [WK-1]      DECIMAL(18,4)  NULL, [WK-2]  DECIMAL(18,4) NULL, [WK-3]  DECIMAL(18,4) NULL,
    [WK-4]      DECIMAL(18,4)  NULL, [WK-5]  DECIMAL(18,4) NULL, [WK-6]  DECIMAL(18,4) NULL,
    [WK-7]      DECIMAL(18,4)  NULL, [WK-8]  DECIMAL(18,4) NULL, [WK-9]  DECIMAL(18,4) NULL,
    [WK-10]     DECIMAL(18,4)  NULL, [WK-11] DECIMAL(18,4) NULL, [WK-12] DECIMAL(18,4) NULL,
    [WK-13]     DECIMAL(18,4)  NULL, [WK-14] DECIMAL(18,4) NULL, [WK-15] DECIMAL(18,4) NULL,
    [WK-16]     DECIMAL(18,4)  NULL, [WK-17] DECIMAL(18,4) NULL, [WK-18] DECIMAL(18,4) NULL,
    [WK-19]     DECIMAL(18,4)  NULL, [WK-20] DECIMAL(18,4) NULL, [WK-21] DECIMAL(18,4) NULL,
    [WK-22]     DECIMAL(18,4)  NULL, [WK-23] DECIMAL(18,4) NULL, [WK-24] DECIMAL(18,4) NULL,
    [WK-25]     DECIMAL(18,4)  NULL, [WK-26] DECIMAL(18,4) NULL, [WK-27] DECIMAL(18,4) NULL,
    [WK-28]     DECIMAL(18,4)  NULL, [WK-29] DECIMAL(18,4) NULL, [WK-30] DECIMAL(18,4) NULL,
    [WK-31]     DECIMAL(18,4)  NULL, [WK-32] DECIMAL(18,4) NULL, [WK-33] DECIMAL(18,4) NULL,
    [WK-34]     DECIMAL(18,4)  NULL, [WK-35] DECIMAL(18,4) NULL, [WK-36] DECIMAL(18,4) NULL,
    [WK-37]     DECIMAL(18,4)  NULL, [WK-38] DECIMAL(18,4) NULL, [WK-39] DECIMAL(18,4) NULL,
    [WK-40]     DECIMAL(18,4)  NULL, [WK-41] DECIMAL(18,4) NULL, [WK-42] DECIMAL(18,4) NULL,
    [WK-43]     DECIMAL(18,4)  NULL, [WK-44] DECIMAL(18,4) NULL, [WK-45] DECIMAL(18,4) NULL,
    [WK-46]     DECIMAL(18,4)  NULL, [WK-47] DECIMAL(18,4) NULL, [WK-48] DECIMAL(18,4) NULL,
    [2]         DECIMAL(18,4)  NULL
);
GO

CREATE INDEX IX_SALE_QTY_STCD ON dbo.QTY_SALE_QTY ([ST-CD], [MAJ-CAT]);
GO

--------------------------------------------------------------
-- 6. QTY_DISP_QTY (Store x MajCat weekly display qty plan)
--------------------------------------------------------------
IF OBJECT_ID('dbo.QTY_DISP_QTY','U') IS NOT NULL
    DROP TABLE dbo.QTY_DISP_QTY;
GO

CREATE TABLE dbo.QTY_DISP_QTY (
    [ST-CD]     VARCHAR(20)     NOT NULL,
    [MAJ-CAT]   VARCHAR(50)     NOT NULL,
    [WK-1]      DECIMAL(18,4)  NULL, [WK-2]  DECIMAL(18,4) NULL, [WK-3]  DECIMAL(18,4) NULL,
    [WK-4]      DECIMAL(18,4)  NULL, [WK-5]  DECIMAL(18,4) NULL, [WK-6]  DECIMAL(18,4) NULL,
    [WK-7]      DECIMAL(18,4)  NULL, [WK-8]  DECIMAL(18,4) NULL, [WK-9]  DECIMAL(18,4) NULL,
    [WK-10]     DECIMAL(18,4)  NULL, [WK-11] DECIMAL(18,4) NULL, [WK-12] DECIMAL(18,4) NULL,
    [WK-13]     DECIMAL(18,4)  NULL, [WK-14] DECIMAL(18,4) NULL, [WK-15] DECIMAL(18,4) NULL,
    [WK-16]     DECIMAL(18,4)  NULL, [WK-17] DECIMAL(18,4) NULL, [WK-18] DECIMAL(18,4) NULL,
    [WK-19]     DECIMAL(18,4)  NULL, [WK-20] DECIMAL(18,4) NULL, [WK-21] DECIMAL(18,4) NULL,
    [WK-22]     DECIMAL(18,4)  NULL, [WK-23] DECIMAL(18,4) NULL, [WK-24] DECIMAL(18,4) NULL,
    [WK-25]     DECIMAL(18,4)  NULL, [WK-26] DECIMAL(18,4) NULL, [WK-27] DECIMAL(18,4) NULL,
    [WK-28]     DECIMAL(18,4)  NULL, [WK-29] DECIMAL(18,4) NULL, [WK-30] DECIMAL(18,4) NULL,
    [WK-31]     DECIMAL(18,4)  NULL, [WK-32] DECIMAL(18,4) NULL, [WK-33] DECIMAL(18,4) NULL,
    [WK-34]     DECIMAL(18,4)  NULL, [WK-35] DECIMAL(18,4) NULL, [WK-36] DECIMAL(18,4) NULL,
    [WK-37]     DECIMAL(18,4)  NULL, [WK-38] DECIMAL(18,4) NULL, [WK-39] DECIMAL(18,4) NULL,
    [WK-40]     DECIMAL(18,4)  NULL, [WK-41] DECIMAL(18,4) NULL, [WK-42] DECIMAL(18,4) NULL,
    [WK-43]     DECIMAL(18,4)  NULL, [WK-44] DECIMAL(18,4) NULL, [WK-45] DECIMAL(18,4) NULL,
    [WK-46]     DECIMAL(18,4)  NULL, [WK-47] DECIMAL(18,4) NULL, [WK-48] DECIMAL(18,4) NULL,
    [2]         DECIMAL(18,4)  NULL
);
GO

CREATE INDEX IX_DISP_QTY_STCD ON dbo.QTY_DISP_QTY ([ST-CD], [MAJ-CAT]);
GO

--------------------------------------------------------------
-- 7. QTY_ST_STK_Q (Store stock quantity snapshot)
--------------------------------------------------------------
IF OBJECT_ID('dbo.QTY_ST_STK_Q','U') IS NOT NULL
    DROP TABLE dbo.QTY_ST_STK_Q;
GO

CREATE TABLE dbo.QTY_ST_STK_Q (
    [ST_CD]     VARCHAR(20)     NOT NULL,
    [MAJ_CAT]   VARCHAR(50)     NOT NULL,
    [STK_QTY]   DECIMAL(18,4)   NOT NULL DEFAULT 0,
    [DATE]      DATE            NOT NULL,
    [ID]        INT             IDENTITY(1,1) NOT NULL,
    CONSTRAINT PK_QTY_ST_STK_Q PRIMARY KEY ([ID])
);
GO

CREATE INDEX IX_ST_STK_STCD ON dbo.QTY_ST_STK_Q ([ST_CD], [MAJ_CAT], [DATE]);
GO

--------------------------------------------------------------
-- 8. QTY_MSA_AND_GRT (DC/GRT stock quantity)
--------------------------------------------------------------
IF OBJECT_ID('dbo.QTY_MSA_AND_GRT','U') IS NOT NULL
    DROP TABLE dbo.QTY_MSA_AND_GRT;
GO

CREATE TABLE dbo.QTY_MSA_AND_GRT (
    [RDC_CD]        VARCHAR(20)     NOT NULL,
    [RDC]           NVARCHAR(100)   NULL,
    [MAJ-CAT]       VARCHAR(50)     NOT NULL,
    [DC-STK-Q]      DECIMAL(18,4)   NULL DEFAULT 0,
    [GRT-STK-Q]     DECIMAL(18,4)   NULL DEFAULT 0,
    [W-GRT-STK-Q]   DECIMAL(18,4)   NULL DEFAULT 0,
    [DATE]          DATE            NOT NULL,
    [ID]            INT             IDENTITY(1,1) NOT NULL,
    CONSTRAINT PK_QTY_MSA_AND_GRT PRIMARY KEY ([ID])
);
GO

CREATE INDEX IX_MSA_GRT_RDC ON dbo.QTY_MSA_AND_GRT ([RDC_CD], [MAJ-CAT], [DATE]);
GO

PRINT '>> All 8 reference tables created successfully.';
GO
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
/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 3 of 5: STORED PROCEDURE - SP_GENERATE_TRF_IN_PLAN

  This SP implements the full Transfer In algorithm:
  1. Resolves week calendar, season, store master, bin capacity
  2. Pulls sale qty, display qty, stock from reference tables
  3. Calculates all 24 plan columns per your algorithm doc
  4. Inserts results into TRF_IN_PLAN table

  Parameters:
    @StartWeekID  - Starting week (from WEEK_CALENDAR)
    @EndWeekID    - Ending week
    @StoreCode    - Optional: single store filter (NULL = all)
    @MajCat       - Optional: single major category (NULL = all)
    @CoverDaysCM1 - Cover days for CM+1 (default 14)
    @CoverDaysCM2 - Cover days for CM+2 (default 0)
    @Debug        - 1 = print diagnostics, 0 = silent
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.SP_GENERATE_TRF_IN_PLAN','P') IS NOT NULL
    DROP PROCEDURE dbo.SP_GENERATE_TRF_IN_PLAN;
GO

CREATE PROCEDURE dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID    INT,
    @EndWeekID      INT,
    @StoreCode      VARCHAR(20)  = NULL,
    @MajCat         VARCHAR(50)  = NULL,
    @CoverDaysCM1   DECIMAL(18,4) = 14,
    @CoverDaysCM2   DECIMAL(18,4) = 0,
    @Debug          BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowCount INT = 0;
    DECLARE @StartTime DATETIME = GETDATE();

    IF @Debug = 1
        PRINT '>> SP_GENERATE_TRF_IN_PLAN started at ' + CONVERT(VARCHAR, @StartTime, 120);

    -----------------------------------------------------------
    -- STEP 0: Validate inputs
    -----------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM dbo.WEEK_CALENDAR WHERE WEEK_ID = @StartWeekID)
    BEGIN
        RAISERROR('Invalid @StartWeekID: %d not found in WEEK_CALENDAR', 16, 1, @StartWeekID);
        RETURN;
    END

    IF NOT EXISTS (SELECT 1 FROM dbo.WEEK_CALENDAR WHERE WEEK_ID = @EndWeekID)
    BEGIN
        RAISERROR('Invalid @EndWeekID: %d not found in WEEK_CALENDAR', 16, 1, @EndWeekID);
        RETURN;
    END

    -----------------------------------------------------------
    -- STEP 1: Build working set of weeks
    -----------------------------------------------------------
    IF OBJECT_ID('tempdb..#Weeks') IS NOT NULL DROP TABLE #Weeks;

    SELECT
        W.WEEK_ID,
        W.WEEK_SEQ,
        W.FY_WEEK,
        W.FY_YEAR,
        W.WK_ST_DT,
        W.WK_END_DT,
        -- Derive season from FY_WEEK position
        -- WK 1-12 = S (Summer), 13-24 = PW (Pre-Winter), 25-36 = W (Winter), 37-48 = PS (Pre-Summer)
        CASE
            WHEN W.FY_WEEK BETWEEN 1  AND 12 THEN 'S'
            WHEN W.FY_WEEK BETWEEN 13 AND 24 THEN 'PW'
            WHEN W.FY_WEEK BETWEEN 25 AND 36 THEN 'W'
            WHEN W.FY_WEEK BETWEEN 37 AND 48 THEN 'PS'
            ELSE 'S'
        END AS SSN,
        -- Week column name for dynamic pivot (WK-1 through WK-48)
        'WK-' + CAST(W.FY_WEEK AS VARCHAR) AS WK_COL_NAME
    INTO #Weeks
    FROM dbo.WEEK_CALENDAR W
    WHERE W.WEEK_ID BETWEEN @StartWeekID AND @EndWeekID;

    IF @Debug = 1
        SELECT COUNT(*) AS WeekCount FROM #Weeks;

    -----------------------------------------------------------
    -- STEP 2: Build store x majcat combinations
    -----------------------------------------------------------
    IF OBJECT_ID('tempdb..#StoreCat') IS NOT NULL DROP TABLE #StoreCat;

    SELECT
        SM.[ST CD]      AS ST_CD,
        SM.[ST NM]      AS ST_NM,
        SM.[RDC_CD],
        SM.[RDC_NM],
        SM.[HUB_CD],
        SM.[HUB_NM],
        SM.[AREA],
        BC.[MAJ-CAT]    AS MAJ_CAT,
        BC.[BIN CAP]    AS BIN_CAP
    INTO #StoreCat
    FROM dbo.MASTER_ST_MASTER SM
    CROSS JOIN dbo.MASTER_BIN_CAPACITY BC
    WHERE SM.[STATUS] = 'NEW'
      AND (@StoreCode IS NULL OR SM.[ST CD] = @StoreCode)
      AND (@MajCat    IS NULL OR BC.[MAJ-CAT] = @MajCat);

    IF @Debug = 1
        SELECT COUNT(*) AS StoreCatCombinations FROM #StoreCat;

    -----------------------------------------------------------
    -- STEP 3: Unpivot QTY_SALE_QTY into normalized form
    -----------------------------------------------------------
    IF OBJECT_ID('tempdb..#SaleQty') IS NOT NULL DROP TABLE #SaleQty;

    SELECT [ST-CD] AS ST_CD, [MAJ-CAT] AS MAJ_CAT, WK_NAME, SALE_QTY
    INTO #SaleQty
    FROM dbo.QTY_SALE_QTY
    UNPIVOT (
        SALE_QTY FOR WK_NAME IN (
            [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],
            [WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],
            [WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
            [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],
            [WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],
            [WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]
        )
    ) AS unpvt;

    -----------------------------------------------------------
    -- STEP 4: Unpivot QTY_DISP_QTY into normalized form
    -----------------------------------------------------------
    IF OBJECT_ID('tempdb..#DispQty') IS NOT NULL DROP TABLE #DispQty;

    SELECT [ST-CD] AS ST_CD, [MAJ-CAT] AS MAJ_CAT, WK_NAME, DISP_QTY
    INTO #DispQty
    FROM dbo.QTY_DISP_QTY
    UNPIVOT (
        DISP_QTY FOR WK_NAME IN (
            [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],
            [WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],
            [WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
            [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],
            [WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],
            [WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]
        )
    ) AS unpvt;

    -----------------------------------------------------------
    -- STEP 5: Get latest store stock (opening stock)
    -----------------------------------------------------------
    IF OBJECT_ID('tempdb..#LatestStock') IS NOT NULL DROP TABLE #LatestStock;

    SELECT ST_CD, MAJ_CAT, STK_QTY
    INTO #LatestStock
    FROM (
        SELECT
            ST_CD, MAJ_CAT, STK_QTY,
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY [DATE] DESC) AS RN
        FROM dbo.QTY_ST_STK_Q
    ) ranked
    WHERE RN = 1;

    -----------------------------------------------------------
    -- STEP 6: Delete existing data for the run range
    -----------------------------------------------------------
    DELETE FROM dbo.TRF_IN_PLAN
    WHERE WEEK_ID BETWEEN @StartWeekID AND @EndWeekID
      AND (@StoreCode IS NULL OR ST_CD = @StoreCode)
      AND (@MajCat    IS NULL OR MAJ_CAT = @MajCat);

    IF @Debug = 1
        PRINT '>> Cleared existing plan data for week range.';

    -----------------------------------------------------------
    -- STEP 7: MAIN CALCULATION - Insert Transfer In Plan
    --
    -- Algorithm per column (from Transferin.xlsx):
    --   S_GRT_STK_Q      = 0 (always)
    --   W_GRT_STK_Q      = IF SSN IN ('W','PW') THEN prev_week closing stock ELSE 0
    --   BGT_DISP_CL_Q    = Display Qty from QTY_DISP_QTY for next month
    --   BGT_DISP_CL_OPT  = ROUND(DISP * 1000 / BIN_CAP, 0)
    --   CM1_SALE_COVER_DAY = @CoverDaysCM1 parameter
    --   CM2_SALE_COVER_DAY = @CoverDaysCM2 parameter (default 0)
    --   COVER_SALE_QTY    = CM1_COVER_DAY * CM1_SALE + CM2_COVER_DAY * CM2_SALE
    --   BGT_ST_CL_MBQ    = COVER_SALE_QTY + BGT_DISP_CL_Q
    --   BGT_DISP_CL_OPT_MBQ = ROUND(BGT_ST_CL_MBQ * 1000 / NULLIF(BGT_DISP_CL_OPT,0), 0)
    --   BGT_TTL_CF_OP_STK_Q = Opening stock from QTY_ST_STK_Q
    --   NT_ACT_Q          = C_ARTICLE_STOCK * IF SSN IN ('S','PS') THEN 100% ELSE 50%
    --   NET_BGT_CF_STK_Q  = MAX(OP_STK - NT_ACT, 0)
    --   CM_BGT_SALE_Q     = Current month sale qty
    --   CM1_BGT_SALE_Q    = Next month sale qty
    --   CM2_BGT_SALE_Q    = Month+2 sale qty
    --   TRF_IN_STK_Q      = IF(MBQ=0 AND CM_SALE=0, 0, MAX(MBQ + CM_SALE - NET_CF_STK, 0))
    --   TRF_IN_OPT_CNT    = ROUND(TRF_IN / NULLIF(BGT_DISP_CL_OPT_MBQ,0) * 1000, 0)
    --   TRF_IN_OPT_MBQ    = TRF_IN * 1000 / NULLIF(TRF_IN_OPT_CNT, 0)
    --   DC_MBQ            = MAX(CM1_SALE / 30 * 15, 0)
    --   BGT_TTL_CF_CL_STK_Q = MAX(OP_STK + TRF_IN - CM_SALE, 0)
    --   BGT_NT_ACT_Q      = 0 (NT_ACT * 0 per algo)
    --   NET_ST_CL_STK_Q   = MAX(CL_STK - BGT_NT_ACT, 0)
    --   ST_CL_EXCESS_Q    = MAX(NET_CL_STK - MBQ, 0)
    --   ST_CL_SHORT_Q     = MAX(MBQ - NET_CL_STK, 0)
    -----------------------------------------------------------

    ;WITH BaseData AS (
        SELECT
            SC.ST_CD,
            SC.ST_NM,
            SC.RDC_CD,
            SC.RDC_NM,
            SC.HUB_CD,
            SC.HUB_NM,
            SC.AREA,
            SC.MAJ_CAT,
            SC.BIN_CAP,
            W.WEEK_ID,
            W.WEEK_SEQ,
            W.FY_WEEK,
            W.FY_YEAR,
            W.WK_ST_DT,
            W.WK_END_DT,
            W.SSN,
            W.WK_COL_NAME,

            -- Sale quantities: CM, CM+1, CM+2
            ISNULL(SQ_CM.SALE_QTY, 0)   AS CM_SALE_QTY,
            ISNULL(SQ_CM1.SALE_QTY, 0)  AS CM1_SALE_QTY,
            ISNULL(SQ_CM2.SALE_QTY, 0)  AS CM2_SALE_QTY,

            -- Display qty (next month plan)
            ISNULL(DQ.DISP_QTY, 0)      AS DISP_QTY,

            -- Opening stock
            ISNULL(LS.STK_QTY, 0)       AS OP_STK_QTY,

            -- Non-active article percentage (estimated at 8% of stock)
            ROUND(ISNULL(LS.STK_QTY, 0) * 0.08, 0) AS C_ARTICLE_STK

        FROM #StoreCat SC
        CROSS JOIN #Weeks W

        -- Current week sale
        LEFT JOIN #SaleQty SQ_CM
            ON SQ_CM.ST_CD = SC.ST_CD
            AND SQ_CM.MAJ_CAT = SC.MAJ_CAT
            AND SQ_CM.WK_NAME = W.WK_COL_NAME

        -- Next week sale (CM+1) - offset by +4 weeks (approx 1 month)
        LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 4
        LEFT JOIN #SaleQty SQ_CM1
            ON SQ_CM1.ST_CD = SC.ST_CD
            AND SQ_CM1.MAJ_CAT = SC.MAJ_CAT
            AND SQ_CM1.WK_NAME = W1.WK_COL_NAME

        -- CM+2 sale - offset by +8 weeks
        LEFT JOIN #Weeks W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8
        LEFT JOIN #SaleQty SQ_CM2
            ON SQ_CM2.ST_CD = SC.ST_CD
            AND SQ_CM2.MAJ_CAT = SC.MAJ_CAT
            AND SQ_CM2.WK_NAME = W2.WK_COL_NAME

        -- Display qty (next month)
        LEFT JOIN #DispQty DQ
            ON DQ.ST_CD = SC.ST_CD
            AND DQ.MAJ_CAT = SC.MAJ_CAT
            AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME)

        -- Latest stock
        LEFT JOIN #LatestStock LS
            ON LS.ST_CD = SC.ST_CD
            AND LS.MAJ_CAT = SC.MAJ_CAT
    ),
    Calculated AS (
        SELECT
            BD.*,

            -- S-GRT-STK-Q = always 0
            CAST(0 AS DECIMAL(18,4)) AS S_GRT_STK_Q,

            -- W-GRT-STK-Q = IF(SSN IN W/PW, use prev closing, else 0)
            -- For first week we use 0; for subsequent weeks this will be updated in a second pass
            CASE WHEN BD.SSN IN ('W','PW') THEN BD.OP_STK_QTY ELSE 0 END AS W_GRT_STK_Q,

            -- BGT-DISP-CL-Q
            BD.DISP_QTY AS BGT_DISP_CL_Q,

            -- BGT-DISP-CL-OPT = ROUND(DISP * 1000 / BIN_CAP)
            CASE WHEN BD.BIN_CAP > 0
                THEN ROUND(BD.DISP_QTY * 1000.0 / BD.BIN_CAP, 0)
                ELSE 0
            END AS BGT_DISP_CL_OPT,

            -- Cover days (from parameters)
            @CoverDaysCM1 AS CM1_SALE_COVER_DAY,
            @CoverDaysCM2 AS CM2_SALE_COVER_DAY,

            -- COVER SALE QTY
            (@CoverDaysCM1 * BD.CM1_SALE_QTY) + (@CoverDaysCM2 * BD.CM2_SALE_QTY) AS COVER_SALE_QTY,

            -- BGT-ST-CL-MBQ = COVER_SALE + DISP
            ((@CoverDaysCM1 * BD.CM1_SALE_QTY) + (@CoverDaysCM2 * BD.CM2_SALE_QTY)) + BD.DISP_QTY AS BGT_ST_CL_MBQ,

            -- BGT-DISP-CL-OPT-MBQ
            CASE WHEN BD.BIN_CAP > 0 AND (ROUND(BD.DISP_QTY * 1000.0 / BD.BIN_CAP, 0)) > 0
                THEN ROUND(
                    (((@CoverDaysCM1 * BD.CM1_SALE_QTY) + (@CoverDaysCM2 * BD.CM2_SALE_QTY)) + BD.DISP_QTY)
                    * 1000.0
                    / NULLIF(ROUND(BD.DISP_QTY * 1000.0 / BD.BIN_CAP, 0), 0),
                    0)
                ELSE 0
            END AS BGT_DISP_CL_OPT_MBQ,

            -- Opening stock
            BD.OP_STK_QTY AS BGT_TTL_CF_OP_STK_Q,

            -- NT-ACT-Q = C_ARTICLE * season multiplier
            ROUND(BD.C_ARTICLE_STK * CASE WHEN BD.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) AS NT_ACT_Q,

            -- NET-BGT-CF-STK-Q = MAX(OP - NT_ACT, 0)
            CASE
                WHEN BD.OP_STK_QTY - ROUND(BD.C_ARTICLE_STK * CASE WHEN BD.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
                THEN BD.OP_STK_QTY - ROUND(BD.C_ARTICLE_STK * CASE WHEN BD.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
                ELSE 0
            END AS NET_BGT_CF_STK_Q,

            -- Sale quantities
            BD.CM_SALE_QTY  AS CM_BGT_SALE_Q,
            BD.CM1_SALE_QTY AS CM1_BGT_SALE_Q,
            BD.CM2_SALE_QTY AS CM2_BGT_SALE_Q

        FROM BaseData BD
    ),
    Final AS (
        SELECT
            C.*,

            -- TRF-IN-STK-Q
            CASE
                WHEN C.BGT_ST_CL_MBQ = 0 AND C.CM_BGT_SALE_Q = 0 THEN 0
                WHEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q) > 0
                    THEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q)
                ELSE 0
            END AS TRF_IN_STK_Q,

            -- TRF-IN OPT CNT
            CASE
                WHEN C.BGT_DISP_CL_OPT_MBQ > 0
                    AND (CASE WHEN C.BGT_ST_CL_MBQ = 0 AND C.CM_BGT_SALE_Q = 0 THEN 0
                              WHEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q) > 0
                              THEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q)
                              ELSE 0 END) > 0
                THEN ROUND(
                    (CASE WHEN C.BGT_ST_CL_MBQ = 0 AND C.CM_BGT_SALE_Q = 0 THEN 0
                          WHEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q) > 0
                          THEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q)
                          ELSE 0 END) * 1000.0 / NULLIF(C.BGT_DISP_CL_OPT_MBQ, 0),
                    0)
                ELSE 0
            END AS TRF_IN_OPT_CNT,

            -- DC-MBQ = MAX(CM1_SALE / 30 * 15, 0)
            CASE
                WHEN ROUND(C.CM1_BGT_SALE_Q / 30.0 * 15, 0) > 0
                THEN ROUND(C.CM1_BGT_SALE_Q / 30.0 * 15, 0)
                ELSE 0
            END AS DC_MBQ,

            -- BGT-TTL-CF-CL-STK-Q = MAX(OP + TRF_IN - CM_SALE, 0)
            CASE
                WHEN C.BGT_TTL_CF_OP_STK_Q
                    + (CASE WHEN C.BGT_ST_CL_MBQ = 0 AND C.CM_BGT_SALE_Q = 0 THEN 0
                            WHEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q) > 0
                            THEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q)
                            ELSE 0 END)
                    - C.CM_BGT_SALE_Q > 0
                THEN C.BGT_TTL_CF_OP_STK_Q
                    + (CASE WHEN C.BGT_ST_CL_MBQ = 0 AND C.CM_BGT_SALE_Q = 0 THEN 0
                            WHEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q) > 0
                            THEN (C.BGT_ST_CL_MBQ + C.CM_BGT_SALE_Q - C.NET_BGT_CF_STK_Q)
                            ELSE 0 END)
                    - C.CM_BGT_SALE_Q
                ELSE 0
            END AS BGT_TTL_CF_CL_STK_Q

        FROM Calculated C
    )

    INSERT INTO dbo.TRF_IN_PLAN (
        ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA,
        MAJ_CAT, SSN, WEEK_ID, WK_ST_DT, WK_END_DT, FY_YEAR, FY_WEEK,
        S_GRT_STK_Q, W_GRT_STK_Q,
        BGT_DISP_CL_Q, BGT_DISP_CL_OPT,
        CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY,
        BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ,
        BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q,
        CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q,
        TRF_IN_STK_Q, TRF_IN_OPT_CNT,
        TRF_IN_OPT_MBQ,
        DC_MBQ,
        BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q,
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q
    )
    SELECT
        F.ST_CD, F.ST_NM, F.RDC_CD, F.RDC_NM, F.HUB_CD, F.HUB_NM, F.AREA,
        F.MAJ_CAT, F.SSN, F.WEEK_ID, F.WK_ST_DT, F.WK_END_DT, F.FY_YEAR, F.FY_WEEK,

        F.S_GRT_STK_Q,
        F.W_GRT_STK_Q,
        F.BGT_DISP_CL_Q,
        F.BGT_DISP_CL_OPT,
        F.CM1_SALE_COVER_DAY,
        F.CM2_SALE_COVER_DAY,
        F.COVER_SALE_QTY,
        F.BGT_ST_CL_MBQ,
        F.BGT_DISP_CL_OPT_MBQ,
        F.BGT_TTL_CF_OP_STK_Q,
        F.NT_ACT_Q,
        F.NET_BGT_CF_STK_Q,
        F.CM_BGT_SALE_Q,
        F.CM1_BGT_SALE_Q,
        F.CM2_BGT_SALE_Q,

        -- TRF_IN_STK_Q
        F.TRF_IN_STK_Q,

        -- TRF_IN_OPT_CNT
        F.TRF_IN_OPT_CNT,

        -- TRF_IN_OPT_MBQ = TRF_IN * 1000 / NULLIF(TRF_IN_OPT_CNT, 0)
        ISNULL(F.TRF_IN_STK_Q * 1000.0 / NULLIF(F.TRF_IN_OPT_CNT, 0), 0),

        -- DC_MBQ
        F.DC_MBQ,

        -- BGT_TTL_CF_CL_STK_Q
        F.BGT_TTL_CF_CL_STK_Q,

        -- BGT_NT_ACT_Q = 0 (per algo: NT_ACT * 0)
        0,

        -- NET_ST_CL_STK_Q = MAX(CL_STK - 0, 0) = CL_STK
        F.BGT_TTL_CF_CL_STK_Q,

        -- ST_CL_EXCESS_Q = MAX(NET_CL - MBQ, 0)
        CASE WHEN F.BGT_TTL_CF_CL_STK_Q - F.BGT_ST_CL_MBQ > 0
             THEN F.BGT_TTL_CF_CL_STK_Q - F.BGT_ST_CL_MBQ ELSE 0 END,

        -- ST_CL_SHORT_Q = MAX(MBQ - NET_CL, 0)
        CASE WHEN F.BGT_ST_CL_MBQ - F.BGT_TTL_CF_CL_STK_Q > 0
             THEN F.BGT_ST_CL_MBQ - F.BGT_TTL_CF_CL_STK_Q ELSE 0 END

    FROM Final F;

    SET @RowCount = @@ROWCOUNT;

    -----------------------------------------------------------
    -- STEP 8: Update W-GRT-STK-Q using previous week's closing
    -- (second pass: chain closing stock forward)
    -----------------------------------------------------------
    UPDATE T
    SET T.W_GRT_STK_Q = CASE
            WHEN T.SSN IN ('W','PW') THEN ISNULL(PREV.BGT_TTL_CF_CL_STK_Q, 0)
            ELSE 0
        END
    FROM dbo.TRF_IN_PLAN T
    OUTER APPLY (
        SELECT TOP 1 P2.BGT_TTL_CF_CL_STK_Q
        FROM dbo.TRF_IN_PLAN P2
        WHERE P2.ST_CD = T.ST_CD
          AND P2.MAJ_CAT = T.MAJ_CAT
          AND P2.WEEK_ID < T.WEEK_ID
        ORDER BY P2.WEEK_ID DESC
    ) PREV
    WHERE T.WEEK_ID BETWEEN @StartWeekID AND @EndWeekID
      AND (@StoreCode IS NULL OR T.ST_CD = @StoreCode)
      AND (@MajCat    IS NULL OR T.MAJ_CAT = @MajCat);

    -----------------------------------------------------------
    -- DONE
    -----------------------------------------------------------
    IF @Debug = 1
    BEGIN
        PRINT '>> Inserted ' + CAST(@RowCount AS VARCHAR) + ' rows into TRF_IN_PLAN.';
        PRINT '>> Completed in ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds.';
    END

    -- Cleanup temp tables
    DROP TABLE IF EXISTS #Weeks;
    DROP TABLE IF EXISTS #StoreCat;
    DROP TABLE IF EXISTS #SaleQty;
    DROP TABLE IF EXISTS #DispQty;
    DROP TABLE IF EXISTS #LatestStock;

    -- Return summary
    SELECT
        @RowCount AS RowsInserted,
        @StartWeekID AS StartWeek,
        @EndWeekID AS EndWeek,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionSeconds;
END;
GO

PRINT '>> SP_GENERATE_TRF_IN_PLAN created successfully.';
GO
/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 4 of 5: INSERT SAMPLE DATA
  Populates all reference tables with realistic sample data
  for 8 stores, 5 major categories, 52 weeks.
==============================================================*/

USE [planning];
GO

--------------------------------------------------------------
-- 1. WEEK_CALENDAR - 52 weeks starting 2026-04-03
--------------------------------------------------------------
TRUNCATE TABLE dbo.WEEK_CALENDAR;

DECLARE @i INT = 1;
DECLARE @BaseDate DATE = '2026-04-03';

WHILE @i <= 52
BEGIN
    INSERT INTO dbo.WEEK_CALENDAR (WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT)
    VALUES (
        @i,                                         -- WEEK_ID
        @i,                                         -- WEEK_SEQ
        @i,                                         -- FY_WEEK
        2026,                                       -- FY_YEAR
        YEAR(DATEADD(WEEK, @i-1, @BaseDate)),       -- CAL_YEAR
        CAST(YEAR(DATEADD(WEEK, @i-1, @BaseDate)) AS VARCHAR)
            + '-W' + RIGHT('0' + CAST(@i AS VARCHAR), 2),  -- YEAR_WEEK
        DATEADD(WEEK, @i-1, @BaseDate),             -- WK_ST_DT
        DATEADD(DAY, 6, DATEADD(WEEK, @i-1, @BaseDate))  -- WK_END_DT
    );
    SET @i = @i + 1;
END;
GO

PRINT '>> WEEK_CALENDAR: 52 weeks inserted.';
GO

--------------------------------------------------------------
-- 2. MASTER_ST_MASTER - 8 stores
--------------------------------------------------------------
DELETE FROM dbo.MASTER_ST_MASTER;

INSERT INTO dbo.MASTER_ST_MASTER
    ([ST CD],[ST NM],[RDC_CD],[RDC_NM],[HUB_CD],[HUB_NM],[STATUS],[GRID_ST_STS],[OP-DATE],[AREA],[STATE],[REF STATE],[SALE GRP],[REF_ST CD],[REF_ST NM],[REF-GRP-NEW],[REF-GRP-OLD],[Date])
VALUES
    ('ST001','DELHI STORE',      'RDC01','RDC NORTH','HUB01','HUB DELHI',    'NEW','A','2020-01-15','NORTH','DELHI',     'DELHI',  'GRP-N1','ST001','DELHI',      'NGRP-1','NGRP-OLD1','2026-04-01'),
    ('ST002','MUMBAI STORE',     'RDC02','RDC WEST', 'HUB02','HUB MUMBAI',   'NEW','A','2019-06-01','WEST', 'MAHARASHTRA','MUMBAI', 'GRP-W1','ST002','MUMBAI',     'WGRP-1','WGRP-OLD1','2026-04-01'),
    ('ST003','BANGALORE STORE',  'RDC03','RDC SOUTH','HUB03','HUB BANGALORE','NEW','A','2020-03-10','SOUTH','KARNATAKA', 'BANGALORE','GRP-S1','ST003','BANGALORE','SGRP-1','SGRP-OLD1','2026-04-01'),
    ('ST004','CHENNAI STORE',    'RDC03','RDC SOUTH','HUB04','HUB CHENNAI',  'NEW','A','2021-01-20','SOUTH','TAMILNADU', 'CHENNAI', 'GRP-S2','ST004','CHENNAI',   'SGRP-2','SGRP-OLD2','2026-04-01'),
    ('ST005','KOLKATA STORE',    'RDC04','RDC EAST', 'HUB05','HUB KOLKATA',  'NEW','A','2020-08-15','EAST', 'WESTBENGAL','KOLKATA', 'GRP-E1','ST005','KOLKATA',   'EGRP-1','EGRP-OLD1','2026-04-01'),
    ('ST006','HYDERABAD STORE',  'RDC03','RDC SOUTH','HUB06','HUB HYDERABAD','NEW','A','2021-05-01','SOUTH','TELANGANA', 'HYDERABAD','GRP-S3','ST006','HYDERABAD','SGRP-3','SGRP-OLD3','2026-04-01'),
    ('ST007','PUNE STORE',       'RDC02','RDC WEST', 'HUB07','HUB PUNE',     'NEW','A','2022-02-14','WEST', 'MAHARASHTRA','PUNE',   'GRP-W2','ST007','PUNE',      'WGRP-2','WGRP-OLD2','2026-04-01'),
    ('ST008','JAIPUR STORE',     'RDC01','RDC NORTH','HUB08','HUB JAIPUR',   'NEW','A','2021-11-01','NORTH','RAJASTHAN', 'JAIPUR', 'GRP-N2','ST008','JAIPUR',    'NGRP-2','NGRP-OLD2','2026-04-01');
GO

PRINT '>> MASTER_ST_MASTER: 8 stores inserted.';
GO

--------------------------------------------------------------
-- 3. MASTER_BIN_CAPACITY - 5 major categories
--------------------------------------------------------------
DELETE FROM dbo.MASTER_BIN_CAPACITY;

INSERT INTO dbo.MASTER_BIN_CAPACITY ([MAJ-CAT],[BIN CAP DC TEAM],[BIN CAP])
VALUES
    ('APPAREL',     150.00, 120.00),
    ('FOOTWEAR',    100.00,  80.00),
    ('ACCESSORIES', 250.00, 200.00),
    ('ELECTRONICS',  80.00,  60.00),
    ('HOME',        130.00, 100.00);
GO

PRINT '>> MASTER_BIN_CAPACITY: 5 categories inserted.';
GO

--------------------------------------------------------------
-- 4. MASTER_GRT_CONS_percentage - Season consumption %
--------------------------------------------------------------
DELETE FROM dbo.MASTER_GRT_CONS_percentage;

INSERT INTO dbo.MASTER_GRT_CONS_percentage ([SSN],
    [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],[WK-9],[WK-10],[WK-11],[WK-12],
    [WK-13],[WK-14],[WK-15],[WK-16],[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
    [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],[WK-33],[WK-34],[WK-35],[WK-36],
    [WK-37],[WK-38],[WK-39],[WK-40],[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48],[2])
VALUES
    ('S',
     0.030,0.030,0.025,0.025,0.020,0.020,0.020,0.018,0.018,0.018,0.015,0.015,
     0.015,0.012,0.012,0.012,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,
     0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,
     0.010,0.010,0.012,0.012,0.015,0.015,0.018,0.018,0.020,0.020,0.025,0.025, 0),
    ('W',
     0.008,0.008,0.008,0.008,0.010,0.010,0.012,0.012,0.015,0.015,0.018,0.018,
     0.020,0.020,0.025,0.025,0.030,0.030,0.030,0.030,0.028,0.028,0.025,0.025,
     0.030,0.030,0.028,0.028,0.025,0.025,0.020,0.020,0.018,0.018,0.015,0.015,
     0.012,0.012,0.010,0.010,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008, 0),
    ('PW',
     0.010,0.010,0.010,0.010,0.012,0.012,0.015,0.015,0.020,0.020,0.025,0.025,
     0.030,0.030,0.030,0.028,0.028,0.025,0.025,0.020,0.020,0.018,0.018,0.015,
     0.015,0.012,0.012,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,
     0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.010,0.010, 0),
    ('PS',
     0.020,0.020,0.018,0.018,0.015,0.015,0.012,0.012,0.010,0.010,0.010,0.010,
     0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.012,0.012,0.015,0.015,0.018,
     0.018,0.020,0.020,0.025,0.025,0.025,0.025,0.028,0.028,0.030,0.030,0.030,
     0.028,0.025,0.025,0.020,0.020,0.018,0.015,0.015,0.012,0.010,0.010,0.010, 0);
GO

PRINT '>> MASTER_GRT_CONS_percentage: 4 seasons inserted.';
GO

--------------------------------------------------------------
-- 5. QTY_SALE_QTY - Weekly sale plan per store x majcat
--    (generated via loop with realistic seasonal patterns)
--------------------------------------------------------------
DELETE FROM dbo.QTY_SALE_QTY;

DECLARE @stores TABLE (ST_CD VARCHAR(20), BaseSale INT);
INSERT INTO @stores VALUES
    ('ST001',180),('ST002',220),('ST003',160),('ST004',140),
    ('ST005',130),('ST006',150),('ST007',170),('ST008',120);

DECLARE @cats TABLE (MAJ_CAT VARCHAR(50), CatMult DECIMAL(5,2));
INSERT INTO @cats VALUES
    ('APPAREL',1.0),('FOOTWEAR',0.6),('ACCESSORIES',0.8),('ELECTRONICS',0.4),('HOME',0.5);

DECLARE @sql NVARCHAR(MAX);
DECLARE @st VARCHAR(20), @mc VARCHAR(50), @base INT;
DECLARE @cm DECIMAL(5,2);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.ST_CD, s.BaseSale, c.MAJ_CAT, c.CatMult FROM @stores s CROSS JOIN @cats c;
OPEN cur;
FETCH NEXT FROM cur INTO @st, @base, @mc, @cm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'INSERT INTO dbo.QTY_SALE_QTY ([ST-CD],[MAJ-CAT],'
        + '[WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],'
        + '[WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],'
        + '[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],'
        + '[WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],'
        + '[WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],'
        + '[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]) '
        + 'VALUES (''' + @st + ''',''' + @mc + ''',';

    DECLARE @w INT = 1;
    DECLARE @vals NVARCHAR(MAX) = '';
    WHILE @w <= 48
    BEGIN
        -- Seasonal multiplier: summer peaks WK 1-12, winter peaks WK 25-36
        DECLARE @seasonal DECIMAL(5,2) = CASE
            WHEN @w BETWEEN 1  AND 12 THEN 1.2
            WHEN @w BETWEEN 13 AND 24 THEN 0.9
            WHEN @w BETWEEN 25 AND 36 THEN 1.3
            ELSE 1.0 END;

        DECLARE @val INT = ROUND(@base * @cm * @seasonal + (ABS(CHECKSUM(NEWID())) % 40 - 20), 0);
        IF @val < 10 SET @val = 10;

        SET @vals = @vals + CAST(@val AS VARCHAR);
        IF @w < 48 SET @vals = @vals + ',';
        SET @w = @w + 1;
    END

    SET @sql = @sql + @vals + ')';
    EXEC sp_executesql @sql;

    FETCH NEXT FROM cur INTO @st, @base, @mc, @cm;
END

CLOSE cur;
DEALLOCATE cur;
GO

PRINT '>> QTY_SALE_QTY: 40 rows (8 stores x 5 cats) inserted.';
GO

--------------------------------------------------------------
-- 6. QTY_DISP_QTY - Weekly display plan (similar pattern)
--------------------------------------------------------------
DELETE FROM dbo.QTY_DISP_QTY;

DECLARE @stores2 TABLE (ST_CD VARCHAR(20), BaseDisp INT);
INSERT INTO @stores2 VALUES
    ('ST001',60),('ST002',75),('ST003',50),('ST004',45),
    ('ST005',40),('ST006',55),('ST007',65),('ST008',35);

DECLARE @cats2 TABLE (MAJ_CAT VARCHAR(50), CatMult DECIMAL(5,2));
INSERT INTO @cats2 VALUES
    ('APPAREL',1.0),('FOOTWEAR',0.7),('ACCESSORIES',0.9),('ELECTRONICS',0.5),('HOME',0.6);

DECLARE @sql2 NVARCHAR(MAX);
DECLARE @st2 VARCHAR(20), @mc2 VARCHAR(50), @base2 INT;
DECLARE @cm2x DECIMAL(5,2);

DECLARE cur2 CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.ST_CD, s.BaseDisp, c.MAJ_CAT, c.CatMult FROM @stores2 s CROSS JOIN @cats2 c;
OPEN cur2;
FETCH NEXT FROM cur2 INTO @st2, @base2, @mc2, @cm2x;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql2 = 'INSERT INTO dbo.QTY_DISP_QTY ([ST-CD],[MAJ-CAT],'
        + '[WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],'
        + '[WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],'
        + '[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],'
        + '[WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],'
        + '[WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],'
        + '[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]) '
        + 'VALUES (''' + @st2 + ''',''' + @mc2 + ''',';

    DECLARE @w2 INT = 1;
    DECLARE @vals2 NVARCHAR(MAX) = '';
    WHILE @w2 <= 48
    BEGIN
        DECLARE @dval INT = ROUND(@base2 * @cm2x + (ABS(CHECKSUM(NEWID())) % 20 - 10), 0);
        IF @dval < 5 SET @dval = 5;
        SET @vals2 = @vals2 + CAST(@dval AS VARCHAR);
        IF @w2 < 48 SET @vals2 = @vals2 + ',';
        SET @w2 = @w2 + 1;
    END

    SET @sql2 = @sql2 + @vals2 + ')';
    EXEC sp_executesql @sql2;

    FETCH NEXT FROM cur2 INTO @st2, @base2, @mc2, @cm2x;
END

CLOSE cur2;
DEALLOCATE cur2;
GO

PRINT '>> QTY_DISP_QTY: 40 rows inserted.';
GO

--------------------------------------------------------------
-- 7. QTY_ST_STK_Q - Current store stock snapshot
--------------------------------------------------------------
DELETE FROM dbo.QTY_ST_STK_Q;

INSERT INTO dbo.QTY_ST_STK_Q ([ST_CD],[MAJ_CAT],[STK_QTY],[DATE])
VALUES
    ('ST001','APPAREL',520,'2026-04-01'),('ST001','FOOTWEAR',310,'2026-04-01'),
    ('ST001','ACCESSORIES',420,'2026-04-01'),('ST001','ELECTRONICS',180,'2026-04-01'),
    ('ST001','HOME',350,'2026-04-01'),
    ('ST002','APPAREL',680,'2026-04-01'),('ST002','FOOTWEAR',400,'2026-04-01'),
    ('ST002','ACCESSORIES',550,'2026-04-01'),('ST002','ELECTRONICS',220,'2026-04-01'),
    ('ST002','HOME',430,'2026-04-01'),
    ('ST003','APPAREL',450,'2026-04-01'),('ST003','FOOTWEAR',270,'2026-04-01'),
    ('ST003','ACCESSORIES',380,'2026-04-01'),('ST003','ELECTRONICS',150,'2026-04-01'),
    ('ST003','HOME',300,'2026-04-01'),
    ('ST004','APPAREL',380,'2026-04-01'),('ST004','FOOTWEAR',230,'2026-04-01'),
    ('ST004','ACCESSORIES',320,'2026-04-01'),('ST004','ELECTRONICS',120,'2026-04-01'),
    ('ST004','HOME',260,'2026-04-01'),
    ('ST005','APPAREL',340,'2026-04-01'),('ST005','FOOTWEAR',200,'2026-04-01'),
    ('ST005','ACCESSORIES',280,'2026-04-01'),('ST005','ELECTRONICS',110,'2026-04-01'),
    ('ST005','HOME',230,'2026-04-01'),
    ('ST006','APPAREL',410,'2026-04-01'),('ST006','FOOTWEAR',250,'2026-04-01'),
    ('ST006','ACCESSORIES',350,'2026-04-01'),('ST006','ELECTRONICS',140,'2026-04-01'),
    ('ST006','HOME',290,'2026-04-01'),
    ('ST007','APPAREL',490,'2026-04-01'),('ST007','FOOTWEAR',290,'2026-04-01'),
    ('ST007','ACCESSORIES',400,'2026-04-01'),('ST007','ELECTRONICS',170,'2026-04-01'),
    ('ST007','HOME',330,'2026-04-01'),
    ('ST008','APPAREL',300,'2026-04-01'),('ST008','FOOTWEAR',180,'2026-04-01'),
    ('ST008','ACCESSORIES',250,'2026-04-01'),('ST008','ELECTRONICS',100,'2026-04-01'),
    ('ST008','HOME',200,'2026-04-01');
GO

PRINT '>> QTY_ST_STK_Q: 40 stock snapshots inserted.';
GO

--------------------------------------------------------------
-- 8. QTY_MSA_AND_GRT - DC/GRT stock
--------------------------------------------------------------
DELETE FROM dbo.QTY_MSA_AND_GRT;

INSERT INTO dbo.QTY_MSA_AND_GRT ([RDC_CD],[RDC],[MAJ-CAT],[DC-STK-Q],[GRT-STK-Q],[W-GRT-STK-Q],[DATE])
VALUES
    ('RDC01','RDC NORTH','APPAREL',5000,2000,800,'2026-04-01'),
    ('RDC01','RDC NORTH','FOOTWEAR',3000,1200,500,'2026-04-01'),
    ('RDC01','RDC NORTH','ACCESSORIES',4000,1600,600,'2026-04-01'),
    ('RDC01','RDC NORTH','ELECTRONICS',2000,800,300,'2026-04-01'),
    ('RDC01','RDC NORTH','HOME',3500,1400,500,'2026-04-01'),
    ('RDC02','RDC WEST','APPAREL',6000,2500,1000,'2026-04-01'),
    ('RDC02','RDC WEST','FOOTWEAR',3500,1400,600,'2026-04-01'),
    ('RDC02','RDC WEST','ACCESSORIES',4500,1800,700,'2026-04-01'),
    ('RDC02','RDC WEST','ELECTRONICS',2500,1000,400,'2026-04-01'),
    ('RDC02','RDC WEST','HOME',4000,1600,600,'2026-04-01'),
    ('RDC03','RDC SOUTH','APPAREL',7000,2800,1100,'2026-04-01'),
    ('RDC03','RDC SOUTH','FOOTWEAR',4000,1600,650,'2026-04-01'),
    ('RDC03','RDC SOUTH','ACCESSORIES',5500,2200,900,'2026-04-01'),
    ('RDC03','RDC SOUTH','ELECTRONICS',3000,1200,500,'2026-04-01'),
    ('RDC03','RDC SOUTH','HOME',5000,2000,800,'2026-04-01'),
    ('RDC04','RDC EAST','APPAREL',3500,1400,550,'2026-04-01'),
    ('RDC04','RDC EAST','FOOTWEAR',2000,800,300,'2026-04-01'),
    ('RDC04','RDC EAST','ACCESSORIES',2800,1100,450,'2026-04-01'),
    ('RDC04','RDC EAST','ELECTRONICS',1500,600,250,'2026-04-01'),
    ('RDC04','RDC EAST','HOME',2500,1000,400,'2026-04-01');
GO

PRINT '>> QTY_MSA_AND_GRT: 20 DC stock rows inserted.';
PRINT '>> ALL SAMPLE DATA LOADED SUCCESSFULLY.';
GO
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

-- Run the stored procedure for weeks 1 through 52:
EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = 1,
    @EndWeekID   = 52,
    @StoreCode   = NULL,   -- All stores
    @MajCat      = NULL,   -- All categories
    @CoverDaysCM1 = 14,    -- 14 days cover for CM+1
    @CoverDaysCM2 = 0,     -- 0 days cover for CM+2 (per algo)
    @Debug       = 1;      -- Show diagnostics
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
