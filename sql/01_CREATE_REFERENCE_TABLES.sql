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
    [RDC_CD]        VARCHAR(20)     NULL DEFAULT 'NA',
    [RDC_NM]        NVARCHAR(100)   NULL DEFAULT 'NA',
    [HUB_CD]        VARCHAR(20)     NULL DEFAULT 'NA',
    [HUB_NM]        NVARCHAR(100)   NULL DEFAULT 'NA',
    [STATUS]        VARCHAR(20)     NULL DEFAULT 'NA',
    [GRID_ST_STS]   VARCHAR(20)     NULL DEFAULT 'NA',
    [OP-DATE]       DATE            NULL,
    [AREA]          VARCHAR(50)     NULL DEFAULT 'NA',
    [STATE]         VARCHAR(50)     NULL DEFAULT 'NA',
    [REF STATE]     VARCHAR(50)     NULL DEFAULT 'NA',
    [SALE GRP]      VARCHAR(50)     NULL DEFAULT 'NA',
    [REF_ST CD]     VARCHAR(20)     NULL DEFAULT 'NA',
    [REF_ST NM]     NVARCHAR(100)   NULL DEFAULT 'NA',
    [REF-GRP-NEW]   VARCHAR(50)     NULL DEFAULT 'NA',
    [REF-GRP-OLD]   VARCHAR(50)     NULL DEFAULT 'NA',
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
    [BIN CAP DC TEAM]   DECIMAL(18,4)   NULL DEFAULT 0,
    [BIN CAP]           DECIMAL(18,4)   NOT NULL DEFAULT 0,
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
