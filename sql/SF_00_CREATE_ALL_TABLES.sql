-- ============================================================
-- SNOWFLAKE DDL: ALL PLANNING TABLES
-- Generated from SQL Server schemas + EF Core models
-- Column names normalized: hyphens/spaces -> underscores
-- Types mapped: INT->INTEGER, DECIMAL->NUMBER, VARCHAR(MAX)->VARCHAR,
--               DATETIME/DATETIME2->TIMESTAMP_NTZ, DATE->DATE, BIT->BOOLEAN,
--               NVARCHAR->VARCHAR
-- Total: 38 tables (33 planning + 5 staging)
-- ============================================================

-- ============================================================
-- SCHEMA: Use a dedicated schema (optional, default PUBLIC)
-- ============================================================
-- CREATE SCHEMA IF NOT EXISTS PLANNING;
-- USE SCHEMA PLANNING;


-- ────────────────────────────────────────────────────────────
-- 1. WEEK_CALENDAR (8 cols)
-- Source: sql/01, Models/WeekCalendar.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS WEEK_CALENDAR (
    WEEK_ID         INTEGER         NOT NULL,
    WEEK_SEQ        INTEGER         NOT NULL,
    FY_WEEK         INTEGER         NOT NULL,
    FY_YEAR         INTEGER         NOT NULL,
    CAL_YEAR        INTEGER         NOT NULL,
    YEAR_WEEK       VARCHAR(50)     NULL,
    WK_ST_DT        DATE            NULL,
    WK_END_DT       DATE            NULL,
    CONSTRAINT PK_WEEK_CALENDAR PRIMARY KEY (WEEK_ID)
);


-- ────────────────────────────────────────────────────────────
-- 2. MASTER_ST_MASTER (18 cols + ID)
-- Source: sql/01, Models/StoreMaster.cs
-- SQL Server cols: [ST CD], [ST NM], [RDC_CD], [RDC_NM], [HUB_CD],
--   [HUB_NM], [STATUS], [GRID_ST_STS], [OP-DATE], [AREA], [STATE],
--   [REF STATE], [SALE GRP], [REF_ST CD], [REF_ST NM], [REF-GRP-NEW],
--   [REF-GRP-OLD], [Date], [ID]
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS MASTER_ST_MASTER (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NULL,
    ST_NM           VARCHAR(255)    NULL,
    RDC_CD          VARCHAR(50)     NULL DEFAULT 'NA',
    RDC_NM          VARCHAR(255)    NULL DEFAULT 'NA',
    HUB_CD          VARCHAR(50)     NULL DEFAULT 'NA',
    HUB_NM          VARCHAR(255)    NULL DEFAULT 'NA',
    STATUS          VARCHAR(50)     NULL DEFAULT 'NA',
    GRID_ST_STS     VARCHAR(50)     NULL DEFAULT 'NA',
    OP_DATE         DATE            NULL,
    AREA            VARCHAR(100)    NULL DEFAULT 'NA',
    STATE           VARCHAR(100)    NULL DEFAULT 'NA',
    REF_STATE       VARCHAR(100)    NULL DEFAULT 'NA',
    SALE_GRP        VARCHAR(100)    NULL DEFAULT 'NA',
    REF_ST_CD       VARCHAR(50)     NULL DEFAULT 'NA',
    REF_ST_NM       VARCHAR(255)    NULL DEFAULT 'NA',
    REF_GRP_NEW     VARCHAR(100)    NULL DEFAULT 'NA',
    REF_GRP_OLD     VARCHAR(100)    NULL DEFAULT 'NA',
    DATE            DATE            NULL,
    CONSTRAINT PK_MASTER_ST_MASTER PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 3. MASTER_BIN_CAPACITY (3 cols + ID)
-- Source: sql/01, Models/BinCapacity.cs
-- SQL Server cols: [MAJ-CAT], [BIN CAP DC TEAM], [BIN CAP], [ID]
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS MASTER_BIN_CAPACITY (
    ID              INTEGER         AUTOINCREMENT,
    MAJ_CAT         VARCHAR(100)    NULL,
    BIN_CAP_DC_TEAM NUMBER(18,4)    NULL DEFAULT 0,
    BIN_CAP         NUMBER(18,4)    NULL DEFAULT 0,
    CONSTRAINT PK_MASTER_BIN_CAPACITY PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 4. QTY_SALE_QTY (2 + 48 week cols + trailing col)
-- Source: sql/01, Models/SaleQty.cs
-- SQL Server cols: [ST-CD], [MAJ-CAT], [WK-1]..[WK-48], [2]
-- Note: Composite key (ST_CD, MAJ_CAT) in EF Core
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QTY_SALE_QTY (
    ST_CD           VARCHAR(50)     NOT NULL,
    MAJ_CAT         VARCHAR(100)    NOT NULL,
    WK_1            NUMBER(18,4)    NULL, WK_2  NUMBER(18,4) NULL, WK_3  NUMBER(18,4) NULL,
    WK_4            NUMBER(18,4)    NULL, WK_5  NUMBER(18,4) NULL, WK_6  NUMBER(18,4) NULL,
    WK_7            NUMBER(18,4)    NULL, WK_8  NUMBER(18,4) NULL, WK_9  NUMBER(18,4) NULL,
    WK_10           NUMBER(18,4)    NULL, WK_11 NUMBER(18,4) NULL, WK_12 NUMBER(18,4) NULL,
    WK_13           NUMBER(18,4)    NULL, WK_14 NUMBER(18,4) NULL, WK_15 NUMBER(18,4) NULL,
    WK_16           NUMBER(18,4)    NULL, WK_17 NUMBER(18,4) NULL, WK_18 NUMBER(18,4) NULL,
    WK_19           NUMBER(18,4)    NULL, WK_20 NUMBER(18,4) NULL, WK_21 NUMBER(18,4) NULL,
    WK_22           NUMBER(18,4)    NULL, WK_23 NUMBER(18,4) NULL, WK_24 NUMBER(18,4) NULL,
    WK_25           NUMBER(18,4)    NULL, WK_26 NUMBER(18,4) NULL, WK_27 NUMBER(18,4) NULL,
    WK_28           NUMBER(18,4)    NULL, WK_29 NUMBER(18,4) NULL, WK_30 NUMBER(18,4) NULL,
    WK_31           NUMBER(18,4)    NULL, WK_32 NUMBER(18,4) NULL, WK_33 NUMBER(18,4) NULL,
    WK_34           NUMBER(18,4)    NULL, WK_35 NUMBER(18,4) NULL, WK_36 NUMBER(18,4) NULL,
    WK_37           NUMBER(18,4)    NULL, WK_38 NUMBER(18,4) NULL, WK_39 NUMBER(18,4) NULL,
    WK_40           NUMBER(18,4)    NULL, WK_41 NUMBER(18,4) NULL, WK_42 NUMBER(18,4) NULL,
    WK_43           NUMBER(18,4)    NULL, WK_44 NUMBER(18,4) NULL, WK_45 NUMBER(18,4) NULL,
    WK_46           NUMBER(18,4)    NULL, WK_47 NUMBER(18,4) NULL, WK_48 NUMBER(18,4) NULL,
    COL_2           NUMBER(18,4)    NULL,
    CONSTRAINT PK_QTY_SALE_QTY PRIMARY KEY (ST_CD, MAJ_CAT)
);


-- ────────────────────────────────────────────────────────────
-- 5. QTY_DISP_QTY (2 + 48 week cols + trailing col)
-- Source: sql/01, Models/DispQty.cs
-- SQL Server cols: [ST-CD], [MAJ-CAT], [WK-1]..[WK-48], [2]
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QTY_DISP_QTY (
    ST_CD           VARCHAR(50)     NOT NULL,
    MAJ_CAT         VARCHAR(100)    NOT NULL,
    WK_1            NUMBER(18,4)    NULL, WK_2  NUMBER(18,4) NULL, WK_3  NUMBER(18,4) NULL,
    WK_4            NUMBER(18,4)    NULL, WK_5  NUMBER(18,4) NULL, WK_6  NUMBER(18,4) NULL,
    WK_7            NUMBER(18,4)    NULL, WK_8  NUMBER(18,4) NULL, WK_9  NUMBER(18,4) NULL,
    WK_10           NUMBER(18,4)    NULL, WK_11 NUMBER(18,4) NULL, WK_12 NUMBER(18,4) NULL,
    WK_13           NUMBER(18,4)    NULL, WK_14 NUMBER(18,4) NULL, WK_15 NUMBER(18,4) NULL,
    WK_16           NUMBER(18,4)    NULL, WK_17 NUMBER(18,4) NULL, WK_18 NUMBER(18,4) NULL,
    WK_19           NUMBER(18,4)    NULL, WK_20 NUMBER(18,4) NULL, WK_21 NUMBER(18,4) NULL,
    WK_22           NUMBER(18,4)    NULL, WK_23 NUMBER(18,4) NULL, WK_24 NUMBER(18,4) NULL,
    WK_25           NUMBER(18,4)    NULL, WK_26 NUMBER(18,4) NULL, WK_27 NUMBER(18,4) NULL,
    WK_28           NUMBER(18,4)    NULL, WK_29 NUMBER(18,4) NULL, WK_30 NUMBER(18,4) NULL,
    WK_31           NUMBER(18,4)    NULL, WK_32 NUMBER(18,4) NULL, WK_33 NUMBER(18,4) NULL,
    WK_34           NUMBER(18,4)    NULL, WK_35 NUMBER(18,4) NULL, WK_36 NUMBER(18,4) NULL,
    WK_37           NUMBER(18,4)    NULL, WK_38 NUMBER(18,4) NULL, WK_39 NUMBER(18,4) NULL,
    WK_40           NUMBER(18,4)    NULL, WK_41 NUMBER(18,4) NULL, WK_42 NUMBER(18,4) NULL,
    WK_43           NUMBER(18,4)    NULL, WK_44 NUMBER(18,4) NULL, WK_45 NUMBER(18,4) NULL,
    WK_46           NUMBER(18,4)    NULL, WK_47 NUMBER(18,4) NULL, WK_48 NUMBER(18,4) NULL,
    COL_2           NUMBER(18,4)    NULL,
    CONSTRAINT PK_QTY_DISP_QTY PRIMARY KEY (ST_CD, MAJ_CAT)
);


-- ────────────────────────────────────────────────────────────
-- 6. QTY_ST_STK_Q (4 cols + ID)
-- Source: sql/01, Models/StoreStock.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QTY_ST_STK_Q (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NULL,
    MAJ_CAT         VARCHAR(100)    NULL,
    STK_QTY         NUMBER(18,4)    NULL DEFAULT 0,
    DATE            DATE            NULL,
    CONSTRAINT PK_QTY_ST_STK_Q PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 7. QTY_MSA_AND_GRT (7 cols + ID)
-- Source: sql/01, Models/DcStock.cs
-- SQL Server cols: [RDC_CD], [RDC], [MAJ-CAT], [DC-STK-Q],
--   [GRT-STK-Q], [W-GRT-STK-Q], [DATE], [ID]
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QTY_MSA_AND_GRT (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(20)     NOT NULL,
    RDC             VARCHAR(100)    NULL,
    MAJ_CAT         VARCHAR(50)     NOT NULL,
    DC_STK_Q        NUMBER(18,4)    NULL DEFAULT 0,
    GRT_STK_Q       NUMBER(18,4)    NULL DEFAULT 0,
    W_GRT_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    DATE            DATE            NOT NULL,
    CONSTRAINT PK_QTY_MSA_AND_GRT PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 8. QTY_DEL_PENDING (4 cols + ID)
-- Source: sql/08, Models/DelPending.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS QTY_DEL_PENDING (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(50)     NOT NULL,
    MAJ_CAT         VARCHAR(100)    NOT NULL,
    DEL_PEND_Q      NUMBER(18,4)    NULL DEFAULT 0,
    DATE            DATE            NOT NULL,
    CONSTRAINT PK_QTY_DEL_PENDING PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 9. MASTER_GRT_CONTRIBUTION (1 + 48 week cols)
-- Source: sql/01 (MASTER_GRT_CONS_percentage), Models/GrtContribution.cs
-- Note: SQL Server table name is MASTER_GRT_CONS_percentage but
--       EF maps to MASTER_GRT_CONTRIBUTION. Using EF name.
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS MASTER_GRT_CONTRIBUTION (
    SSN             VARCHAR(100)    NOT NULL,
    WK_1            NUMBER(18,4)    NULL DEFAULT 0, WK_2  NUMBER(18,4) NULL DEFAULT 0, WK_3  NUMBER(18,4) NULL DEFAULT 0,
    WK_4            NUMBER(18,4)    NULL DEFAULT 0, WK_5  NUMBER(18,4) NULL DEFAULT 0, WK_6  NUMBER(18,4) NULL DEFAULT 0,
    WK_7            NUMBER(18,4)    NULL DEFAULT 0, WK_8  NUMBER(18,4) NULL DEFAULT 0, WK_9  NUMBER(18,4) NULL DEFAULT 0,
    WK_10           NUMBER(18,4)    NULL DEFAULT 0, WK_11 NUMBER(18,4) NULL DEFAULT 0, WK_12 NUMBER(18,4) NULL DEFAULT 0,
    WK_13           NUMBER(18,4)    NULL DEFAULT 0, WK_14 NUMBER(18,4) NULL DEFAULT 0, WK_15 NUMBER(18,4) NULL DEFAULT 0,
    WK_16           NUMBER(18,4)    NULL DEFAULT 0, WK_17 NUMBER(18,4) NULL DEFAULT 0, WK_18 NUMBER(18,4) NULL DEFAULT 0,
    WK_19           NUMBER(18,4)    NULL DEFAULT 0, WK_20 NUMBER(18,4) NULL DEFAULT 0, WK_21 NUMBER(18,4) NULL DEFAULT 0,
    WK_22           NUMBER(18,4)    NULL DEFAULT 0, WK_23 NUMBER(18,4) NULL DEFAULT 0, WK_24 NUMBER(18,4) NULL DEFAULT 0,
    WK_25           NUMBER(18,4)    NULL DEFAULT 0, WK_26 NUMBER(18,4) NULL DEFAULT 0, WK_27 NUMBER(18,4) NULL DEFAULT 0,
    WK_28           NUMBER(18,4)    NULL DEFAULT 0, WK_29 NUMBER(18,4) NULL DEFAULT 0, WK_30 NUMBER(18,4) NULL DEFAULT 0,
    WK_31           NUMBER(18,4)    NULL DEFAULT 0, WK_32 NUMBER(18,4) NULL DEFAULT 0, WK_33 NUMBER(18,4) NULL DEFAULT 0,
    WK_34           NUMBER(18,4)    NULL DEFAULT 0, WK_35 NUMBER(18,4) NULL DEFAULT 0, WK_36 NUMBER(18,4) NULL DEFAULT 0,
    WK_37           NUMBER(18,4)    NULL DEFAULT 0, WK_38 NUMBER(18,4) NULL DEFAULT 0, WK_39 NUMBER(18,4) NULL DEFAULT 0,
    WK_40           NUMBER(18,4)    NULL DEFAULT 0, WK_41 NUMBER(18,4) NULL DEFAULT 0, WK_42 NUMBER(18,4) NULL DEFAULT 0,
    WK_43           NUMBER(18,4)    NULL DEFAULT 0, WK_44 NUMBER(18,4) NULL DEFAULT 0, WK_45 NUMBER(18,4) NULL DEFAULT 0,
    WK_46           NUMBER(18,4)    NULL DEFAULT 0, WK_47 NUMBER(18,4) NULL DEFAULT 0, WK_48 NUMBER(18,4) NULL DEFAULT 0,
    CONSTRAINT PK_MASTER_GRT_CONTRIBUTION PRIMARY KEY (SSN)
);


-- ────────────────────────────────────────────────────────────
-- 10. MASTER_PRODUCT_HIERARCHY (5 cols + ID)
-- Source: Models/ProductHierarchy.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS MASTER_PRODUCT_HIERARCHY (
    ID              INTEGER         AUTOINCREMENT,
    SEG             VARCHAR(100)    NULL DEFAULT 'NA',
    DIV             VARCHAR(100)    NULL DEFAULT 'NA',
    SUB_DIV         VARCHAR(100)    NULL DEFAULT 'NA',
    MAJ_CAT_NM      VARCHAR(200)    NULL DEFAULT 'NA',
    SSN             VARCHAR(100)    NULL DEFAULT 'NA',
    CONSTRAINT PK_MASTER_PRODUCT_HIERARCHY PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 11. ST_MAJ_CAT_MACRO_MVGR_PLAN (4 cols + ID)
-- Source: sql/21, Models/ContMacroMvgr.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ST_MAJ_CAT_MACRO_MVGR_PLAN (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT_CD      VARCHAR(100)    NOT NULL DEFAULT 'NA',
    DISP_MVGR_MATRIX VARCHAR(200)   NOT NULL DEFAULT 'NA',
    CONT_PCT        NUMBER(18,4)    NOT NULL DEFAULT 0,
    CONSTRAINT PK_ST_MAJ_CAT_MACRO_MVGR_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 12. ST_MAJ_CAT_SZ_PLAN (4 cols + ID)
-- Source: sql/21, Models/ContSz.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ST_MAJ_CAT_SZ_PLAN (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT_CD      VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SZ              VARCHAR(100)    NOT NULL DEFAULT 'NA',
    CONT_PCT        NUMBER(18,4)    NOT NULL DEFAULT 0,
    CONSTRAINT PK_ST_MAJ_CAT_SZ_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 13. ST_MAJ_CAT_SEG_PLAN (4 cols + ID)
-- Source: sql/21, Models/ContSeg.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ST_MAJ_CAT_SEG_PLAN (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT_CD      VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SEG             VARCHAR(100)    NOT NULL DEFAULT 'NA',
    CONT_PCT        NUMBER(18,4)    NOT NULL DEFAULT 0,
    CONSTRAINT PK_ST_MAJ_CAT_SEG_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 14. ST_MAJ_CAT_VND_PLAN (4 cols + ID)
-- Source: sql/21, Models/ContVnd.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ST_MAJ_CAT_VND_PLAN (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT_CD      VARCHAR(100)    NOT NULL DEFAULT 'NA',
    M_VND_CD        VARCHAR(200)    NOT NULL DEFAULT 'NA',
    CONT_PCT        NUMBER(18,4)    NOT NULL DEFAULT 0,
    CONSTRAINT PK_ST_MAJ_CAT_VND_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 15. SUB_ST_STK_MVGR (5 cols + ID)
-- Source: sql/23, Models/SubStockModels.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_ST_STK_MVGR (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    STK_QTY         NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_ST_STK_MVGR PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 16. SUB_ST_STK_SZ (5 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_ST_STK_SZ (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    STK_QTY         NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_ST_STK_SZ PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 17. SUB_ST_STK_SEG (5 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_ST_STK_SEG (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    STK_QTY         NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_ST_STK_SEG PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 18. SUB_ST_STK_VND (5 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_ST_STK_VND (
    ID              INTEGER         AUTOINCREMENT,
    ST_CD           VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    STK_QTY         NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_ST_STK_VND PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 19. SUB_DC_STK_MVGR (7 cols + ID)
-- Source: sql/23, Models/SubStockModels.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_DC_STK_MVGR (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    DC_STK_Q        NUMBER(18,4)    NOT NULL DEFAULT 0,
    GRT_STK_Q       NUMBER(18,4)    NOT NULL DEFAULT 0,
    W_GRT_STK_Q     NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_DC_STK_MVGR PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 20. SUB_DC_STK_SZ (7 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_DC_STK_SZ (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    DC_STK_Q        NUMBER(18,4)    NOT NULL DEFAULT 0,
    GRT_STK_Q       NUMBER(18,4)    NOT NULL DEFAULT 0,
    W_GRT_STK_Q     NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_DC_STK_SZ PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 21. SUB_DC_STK_SEG (7 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_DC_STK_SEG (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    DC_STK_Q        NUMBER(18,4)    NOT NULL DEFAULT 0,
    GRT_STK_Q       NUMBER(18,4)    NOT NULL DEFAULT 0,
    W_GRT_STK_Q     NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_DC_STK_SEG PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 22. SUB_DC_STK_VND (7 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_DC_STK_VND (
    ID              INTEGER         AUTOINCREMENT,
    RDC_CD          VARCHAR(50)     NOT NULL DEFAULT 'NA',
    MAJ_CAT         VARCHAR(100)    NOT NULL DEFAULT 'NA',
    SUB_VALUE       VARCHAR(200)    NOT NULL DEFAULT 'NA',
    DC_STK_Q        NUMBER(18,4)    NOT NULL DEFAULT 0,
    GRT_STK_Q       NUMBER(18,4)    NOT NULL DEFAULT 0,
    W_GRT_STK_Q     NUMBER(18,4)    NOT NULL DEFAULT 0,
    DATE            TIMESTAMP_NTZ   NULL,
    CONSTRAINT PK_SUB_DC_STK_VND PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 23. TRF_IN_PLAN (output table, ~40 cols)
-- Source: sql/02, Models/TrfInPlan.cs
-- Full column list from SQL Server CREATE TABLE + EF model
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS TRF_IN_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    ST_CD                   VARCHAR(50)     NOT NULL,
    ST_NM                   VARCHAR(255)    NULL DEFAULT 'NA',
    RDC_CD                  VARCHAR(50)     NULL DEFAULT 'NA',
    RDC_NM                  VARCHAR(255)    NULL DEFAULT 'NA',
    HUB_CD                  VARCHAR(50)     NULL DEFAULT 'NA',
    HUB_NM                  VARCHAR(255)    NULL DEFAULT 'NA',
    AREA                    VARCHAR(100)    NULL DEFAULT 'NA',
    MAJ_CAT                 VARCHAR(100)    NOT NULL,
    SSN                     VARCHAR(10)     NULL DEFAULT 'NA',
    WEEK_ID                 INTEGER         NOT NULL,
    WK_ST_DT                DATE            NULL,
    WK_END_DT               DATE            NULL,
    FY_YEAR                 INTEGER         NULL,
    FY_WEEK                 INTEGER         NULL,
    -- GRT Stock
    S_GRT_STK_Q             NUMBER(18,4)    NULL DEFAULT 0,
    W_GRT_STK_Q             NUMBER(18,4)    NULL DEFAULT 0,
    -- Display
    BGT_DISP_CL_Q           NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DISP_CL_OPT         NUMBER(18,4)    NULL DEFAULT 0,
    -- Cover Days & Cover Sale
    CM1_SALE_COVER_DAY       NUMBER(18,4)    NULL DEFAULT 0,
    CM2_SALE_COVER_DAY       NUMBER(18,4)    NULL DEFAULT 0,
    COVER_SALE_QTY           NUMBER(18,4)    NULL DEFAULT 0,
    -- MBQ Targets
    BGT_ST_CL_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DISP_CL_OPT_MBQ     NUMBER(18,4)    NULL DEFAULT 0,
    -- Opening & Net Stock
    BGT_TTL_CF_OP_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    NT_ACT_Q                NUMBER(18,4)    NULL DEFAULT 0,
    NET_BGT_CF_STK_Q        NUMBER(18,4)    NULL DEFAULT 0,
    -- Budget Sale Qty
    CM_BGT_SALE_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CM1_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CM2_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    -- Transfer In
    TRF_IN_STK_Q            NUMBER(18,4)    NULL DEFAULT 0,
    TRF_IN_OPT_CNT          NUMBER(18,4)    NULL DEFAULT 0,
    TRF_IN_OPT_MBQ          NUMBER(18,4)    NULL DEFAULT 0,
    -- DC MBQ
    DC_MBQ                  NUMBER(18,4)    NULL DEFAULT 0,
    -- Closing Stock
    BGT_TTL_CF_CL_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    BGT_NT_ACT_Q            NUMBER(18,4)    NULL DEFAULT 0,
    NET_ST_CL_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    -- Excess / Short
    ST_CL_EXCESS_Q          NUMBER(18,4)    NULL DEFAULT 0,
    ST_CL_SHORT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    -- Category Hierarchy
    SEG                     VARCHAR(100)    NULL DEFAULT 'NA',
    DIV                     VARCHAR(100)    NULL DEFAULT 'NA',
    SUB_DIV                 VARCHAR(100)    NULL DEFAULT 'NA',
    MAJ_CAT_NM              VARCHAR(100)    NULL DEFAULT 'NA',
    -- Metadata
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR(100)    NULL,
    CONSTRAINT PK_TRF_IN_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 24. PURCHASE_PLAN (output table, ~60 cols)
-- Source: sql/09 + sql/13 patch, Models/PurchasePlan.cs
-- Full column list from SQL + EF model merged
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS PURCHASE_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    RDC_CD                  VARCHAR(50)     NOT NULL,
    RDC_NM                  VARCHAR(255)    NULL DEFAULT 'NA',
    MAJ_CAT                 VARCHAR(100)    NOT NULL,
    SSN                     VARCHAR(10)     NULL DEFAULT 'NA',
    -- Week References
    WEEK_ID                 INTEGER         NOT NULL,
    FY_WEEK                 INTEGER         NULL,
    FY_YEAR                 INTEGER         NULL,
    WK_ST_DT                DATE            NULL,
    WK_END_DT               DATE            NULL,
    -- Stock Reference Data
    DC_STK_Q                NUMBER(18,4)    NULL DEFAULT 0,
    GRT_STK_Q               NUMBER(18,4)    NULL DEFAULT 0,
    S_GRT_STK_Q             NUMBER(18,4)    NULL DEFAULT 0,
    W_GRT_STK_Q             NUMBER(18,4)    NULL DEFAULT 0,
    -- Bin Capacity
    BIN_CAP_DC_TEAM         NUMBER(18,4)    NULL DEFAULT 0,
    BIN_CAP                 NUMBER(18,4)    NULL DEFAULT 0,
    -- Store-Level Planning
    BGT_DISP_CL_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW_BGT_SALE_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW1_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW2_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW3_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW4_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW5_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    -- Opening Stock
    BGT_ST_OP_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    NET_ST_OP_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    -- DC Opening Stock
    BGT_DC_OP_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    PP_NT_ACT_Q             NUMBER(18,4)    NULL DEFAULT 0,
    BGT_CF_STK_Q            NUMBER(18,4)    NULL DEFAULT 0,
    -- Total Stock Calculation
    TTL_STK                 NUMBER(18,4)    NULL DEFAULT 0,
    OP_STK                  NUMBER(18,4)    NULL DEFAULT 0,
    NT_ACT_STK              NUMBER(18,4)    NULL DEFAULT 0,
    -- GRT Consumption
    GRT_CONS_PCT            NUMBER(18,4)    NULL DEFAULT 0,
    GRT_CONS_Q              NUMBER(18,4)    NULL DEFAULT 0,
    -- Delivery Pending
    DEL_PEND_Q              NUMBER(18,4)    NULL DEFAULT 0,
    -- DC Confirm Stock
    PP_NET_BGT_CF_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    -- Transfer Out
    CW_TRF_OUT_Q            NUMBER(18,4)    NULL DEFAULT 0,
    CW1_TRF_OUT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW2_TRF_OUT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW3_TRF_OUT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW4_TRF_OUT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    TTL_TRF_OUT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    -- Store Closing Stock
    BGT_ST_CL_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    NET_BGT_ST_CL_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    NET_SSNL_CL_STK_Q       NUMBER(18,4)    NULL DEFAULT 0,
    -- DC Closing Stock & Sales
    BGT_DC_MBQ_SALE         NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DC_CL_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DC_CL_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    -- Purchase Quantities
    BGT_PUR_Q_INIT          NUMBER(18,4)    NULL DEFAULT 0,
    POS_PO_RAISED           NUMBER(18,4)    NULL DEFAULT 0,
    NEG_PO_RAISED           NUMBER(18,4)    NULL DEFAULT 0,
    -- Company Closing Stock
    BGT_CO_CL_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    -- Excess/Shortage
    DC_STK_EXCESS_Q         NUMBER(18,4)    NULL DEFAULT 0,
    DC_STK_SHORT_Q          NUMBER(18,4)    NULL DEFAULT 0,
    ST_STK_EXCESS_Q         NUMBER(18,4)    NULL DEFAULT 0,
    ST_STK_SHORT_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CO_STK_EXCESS_Q         NUMBER(18,4)    NULL DEFAULT 0,
    CO_STK_SHORT_Q          NUMBER(18,4)    NULL DEFAULT 0,
    -- Bin Requirements
    FRESH_BIN_REQ           NUMBER(18,4)    NULL DEFAULT 0,
    GRT_BIN_REQ             NUMBER(18,4)    NULL DEFAULT 0,
    -- Category Hierarchy
    SEG                     VARCHAR(100)    NULL DEFAULT 'NA',
    DIV                     VARCHAR(100)    NULL DEFAULT 'NA',
    SUB_DIV                 VARCHAR(100)    NULL DEFAULT 'NA',
    MAJ_CAT_NM              VARCHAR(100)    NULL DEFAULT 'NA',
    -- Audit
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY              VARCHAR(100)    NULL,
    CONSTRAINT PK_PURCHASE_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 25. SUB_LEVEL_TRF_PLAN (18 cols + ID)
-- Source: sql/22, Models referenced in SubLevelController
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_LEVEL_TRF_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    LEVEL                   VARCHAR(10)     NOT NULL,
    SUB_VALUE               VARCHAR(200)    NOT NULL DEFAULT 'NA',
    ST_CD                   VARCHAR(50)     NOT NULL,
    MAJ_CAT                 VARCHAR(100)    NOT NULL,
    CONT_PCT                NUMBER(18,4)    NOT NULL DEFAULT 0,
    FY_YEAR                 INTEGER         NULL,
    FY_WEEK                 INTEGER         NULL,
    BGT_DISP_CL_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CM_BGT_SALE_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CM1_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CM2_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    COVER_SALE_QTY          NUMBER(18,4)    NULL DEFAULT 0,
    TRF_IN_STK_Q            NUMBER(18,4)    NULL DEFAULT 0,
    DC_MBQ                  NUMBER(18,4)    NULL DEFAULT 0,
    BGT_TTL_CF_OP_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    BGT_TTL_CF_CL_STK_Q     NUMBER(18,4)    NULL DEFAULT 0,
    BGT_ST_CL_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    ST_CL_EXCESS_Q          NUMBER(18,4)    NULL DEFAULT 0,
    ST_CL_SHORT_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SUB_LEVEL_TRF_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 26. SUB_LEVEL_PP_PLAN (19 cols + ID)
-- Source: sql/22
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SUB_LEVEL_PP_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    LEVEL                   VARCHAR(10)     NOT NULL,
    SUB_VALUE               VARCHAR(200)    NOT NULL DEFAULT 'NA',
    RDC_CD                  VARCHAR(50)     NOT NULL,
    MAJ_CAT                 VARCHAR(100)    NOT NULL,
    CONT_PCT                NUMBER(18,4)    NOT NULL DEFAULT 0,
    FY_YEAR                 INTEGER         NULL,
    FY_WEEK                 INTEGER         NULL,
    BGT_DISP_CL_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW_BGT_SALE_Q           NUMBER(18,4)    NULL DEFAULT 0,
    CW1_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW2_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW3_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CW4_BGT_SALE_Q          NUMBER(18,4)    NULL DEFAULT 0,
    BGT_PUR_Q_INIT          NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DC_CL_STK_Q         NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DC_CL_MBQ           NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DC_MBQ_SALE         NUMBER(18,4)    NULL DEFAULT 0,
    DC_STK_EXCESS_Q         NUMBER(18,4)    NULL DEFAULT 0,
    DC_STK_SHORT_Q          NUMBER(18,4)    NULL DEFAULT 0,
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SUB_LEVEL_PP_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 27. SALE_BUDGET_PLAN (33 cols + ID)
-- Source: sql/26, Models/SaleBudgetPlan.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SALE_BUDGET_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    RUN_ID                  VARCHAR(50)     NOT NULL,
    -- Store dimensions
    STORE_CODE              VARCHAR(20)     NOT NULL,
    STORE_NAME              VARCHAR(200)    NULL,
    STATE                   VARCHAR(50)     NULL,
    ZONE                    VARCHAR(50)     NULL,
    REGION                  VARCHAR(50)     NULL,
    SIZE_CATEGORY           VARCHAR(20)     NULL,
    OLD_NEW                 VARCHAR(10)     NULL,
    -- Category dimensions
    MAJOR_CATEGORY          VARCHAR(100)    NOT NULL,
    DIVISION                VARCHAR(100)    NULL,
    SUBDIVISION             VARCHAR(100)    NULL,
    SEGMENT                 VARCHAR(100)    NULL,
    -- Time
    PLAN_MONTH              DATE            NOT NULL,
    -- LYSP
    LYSP_SALE_QTY           NUMBER(18,4)    NULL DEFAULT 0,
    LYSP_SALE_VAL           NUMBER(18,4)    NULL DEFAULT 0,
    LYSP_GM_VAL             NUMBER(18,4)    NULL DEFAULT 0,
    -- Growth rates
    GROWTH_RATE_ST_CAT      NUMBER(18,6)    NULL DEFAULT 0,
    GROWTH_RATE_CATEGORY    NUMBER(18,6)    NULL DEFAULT 0,
    GROWTH_RATE_STORE       NUMBER(18,6)    NULL DEFAULT 0,
    GROWTH_RATE_COMBINED    NUMBER(18,6)    NULL DEFAULT 0,
    -- Adjustments
    FILL_RATE_ADJ           NUMBER(18,6)    NULL DEFAULT 1.0,
    FESTIVAL_ADJ            NUMBER(18,6)    NULL DEFAULT 0,
    -- ML forecast
    ML_FORECAST_QTY         NUMBER(18,4)    NULL DEFAULT 0,
    ML_FORECAST_LOW         NUMBER(18,4)    NULL DEFAULT 0,
    ML_FORECAST_HIGH        NUMBER(18,4)    NULL DEFAULT 0,
    ML_FORECAST_MAPE        NUMBER(18,4)    NULL,
    ML_BEST_METHOD          VARCHAR(50)     NULL,
    -- Final budget
    BGT_SALE_QTY            NUMBER(18,4)    NULL DEFAULT 0,
    BGT_SALE_VAL            NUMBER(18,4)    NULL DEFAULT 0,
    BGT_GM_VAL              NUMBER(18,4)    NULL DEFAULT 0,
    AVG_SELLING_PRICE       NUMBER(18,4)    NULL DEFAULT 0,
    -- Tracking
    ALGO_METHOD             VARCHAR(20)     NULL,
    STORE_CONT_PCT          NUMBER(18,8)    NULL DEFAULT 0,
    -- Metadata
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SALE_BUDGET_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 28. SALE_BUDGET_CONFIG (4 cols + ID)
-- Source: sql/26, Models/SaleBudgetConfig.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SALE_BUDGET_CONFIG (
    ID              INTEGER         AUTOINCREMENT,
    CONFIG_KEY      VARCHAR(100)    NOT NULL,
    CONFIG_VALUE    VARCHAR(500)    NOT NULL,
    DESCRIPTION     VARCHAR(500)    NULL,
    UPDATED_DT      TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_SALE_BUDGET_CONFIG PRIMARY KEY (ID),
    CONSTRAINT UQ_SALE_BUDGET_CONFIG_KEY UNIQUE (CONFIG_KEY)
);


-- ────────────────────────────────────────────────────────────
-- 29. FIXTURE_DENSITY_PLAN (26 cols + ID)
-- Source: sql/27, Models/FixtureDensityPlan.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FIXTURE_DENSITY_PLAN (
    ID                      INTEGER         AUTOINCREMENT,
    RUN_ID                  VARCHAR(50)     NOT NULL,
    -- Store
    STORE_CODE              VARCHAR(20)     NOT NULL,
    STORE_NAME              VARCHAR(200)    NULL,
    STATE                   VARCHAR(50)     NULL,
    ZONE                    VARCHAR(50)     NULL,
    REGION                  VARCHAR(50)     NULL,
    STORE_SIZE_SQFT         NUMBER(12,2)    NULL,
    SIZE_CATEGORY           VARCHAR(20)     NULL,
    -- Category
    MAJOR_CATEGORY          VARCHAR(100)    NOT NULL,
    DIVISION                VARCHAR(100)    NULL,
    SUBDIVISION             VARCHAR(100)    NULL,
    SEGMENT                 VARCHAR(100)    NULL,
    -- Time
    PLAN_MONTH              DATE            NOT NULL,
    -- Fixture & Density outputs
    BGT_DISP_QTY            NUMBER(18,4)    NULL DEFAULT 0,
    BGT_DISP_VAL            NUMBER(18,4)    NULL DEFAULT 0,
    ACC_DENSITY             NUMBER(18,4)    NULL DEFAULT 0,
    FIX_COUNT               NUMBER(18,4)    NULL DEFAULT 0,
    AREA_SQFT               NUMBER(12,2)    NULL DEFAULT 0,
    -- Inputs used
    SALE_BGT_VAL            NUMBER(18,4)    NULL DEFAULT 0,
    CL_STK_QTY              NUMBER(18,4)    NULL DEFAULT 0,
    CL_STK_VAL              NUMBER(18,4)    NULL DEFAULT 0,
    AVG_MRP                 NUMBER(18,4)    NULL DEFAULT 0,
    -- ROI metrics
    GP_PSF                  NUMBER(18,4)    NULL DEFAULT 0,
    SALES_PSF               NUMBER(18,4)    NULL DEFAULT 0,
    STR_PCT                 NUMBER(18,6)    NULL DEFAULT 0,
    -- Algo
    ALGO_METHOD             VARCHAR(30)     NULL,
    CREATED_DT              TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_FIXTURE_DENSITY_PLAN PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 30. WEEKLY_DISAGG_LOG (8 cols + ID)
-- Source: sql/28, Models/WeeklyDisaggLog.cs
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS WEEKLY_DISAGG_LOG (
    ID                  INTEGER         AUTOINCREMENT,
    RUN_ID              VARCHAR(50)     NOT NULL,
    SOURCE_TABLE        VARCHAR(50)     NOT NULL,
    TARGET_TABLE        VARCHAR(50)     NOT NULL,
    ROWS_WRITTEN        INTEGER         NOT NULL DEFAULT 0,
    MONTHS_PROCESSED    INTEGER         NOT NULL DEFAULT 0,
    WEEKS_PER_MONTH     INTEGER         NOT NULL DEFAULT 0,
    METHOD              VARCHAR(30)     NULL,
    CREATED_DT          TIMESTAMP_NTZ   NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_WEEKLY_DISAGG_LOG PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 31. FESTIVAL_CALENDAR (5 cols + ID)
-- Source: sql/29
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FESTIVAL_CALENDAR (
    ID              INTEGER         AUTOINCREMENT,
    FESTIVAL        VARCHAR(50)     NOT NULL,
    STATE           VARCHAR(50)     NOT NULL,
    MONTH_NUM       INTEGER         NOT NULL,
    IMPACT_PCT      NUMBER(8,4)     NOT NULL,
    DURATION_DAYS   INTEGER         NULL DEFAULT 7,
    CONSTRAINT PK_FESTIVAL_CALENDAR PRIMARY KEY (ID)
);


-- ============================================================
-- STAGING TABLES (from Snowflake feeds into Sale Budget)
-- Source: sql/26
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- 32. STG_SF_SALE_ACTUAL (13 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS STG_SF_SALE_ACTUAL (
    ID                  INTEGER         AUTOINCREMENT,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SUBDIVISION         VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    SALE_MONTH          DATE            NOT NULL,
    SALE_QTY            NUMBER(18,4)    NULL DEFAULT 0,
    SALE_VAL            NUMBER(18,4)    NULL DEFAULT 0,
    GM_VAL              NUMBER(18,4)    NULL DEFAULT 0,
    LYSP_SALE_QTY       NUMBER(18,4)    NULL DEFAULT 0,
    LYSP_SALE_VAL       NUMBER(18,4)    NULL DEFAULT 0,
    LYSP_GM_VAL         NUMBER(18,4)    NULL DEFAULT 0,
    FETCHED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_SF_SALE_ACTUAL PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 33. STG_SF_DEMAND_FORECAST (13 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS STG_SF_DEMAND_FORECAST (
    ID                  INTEGER         AUTOINCREMENT,
    TARGET_MONTH        DATE            NOT NULL,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    STORE_NAME          VARCHAR(200)    NULL,
    ENSEMBLE_FORECAST   NUMBER(18,4)    NULL DEFAULT 0,
    STORE_CONT_PCT      NUMBER(18,8)    NULL DEFAULT 0,
    STORE_FORECAST      NUMBER(18,4)    NULL DEFAULT 0,
    FORECAST_LOW        NUMBER(18,4)    NULL DEFAULT 0,
    FORECAST_HIGH       NUMBER(18,4)    NULL DEFAULT 0,
    BEST_METHOD         VARCHAR(50)     NULL,
    WEIGHTED_MAPE       NUMBER(18,4)    NULL DEFAULT 0,
    DATA_MONTHS_USED    INTEGER         NULL DEFAULT 0,
    FETCHED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_SF_DEMAND_FORECAST PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 34. STG_SF_CONT_PCT (17 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS STG_SF_CONT_PCT (
    ID                  INTEGER         AUTOINCREMENT,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    STATE               VARCHAR(50)     NULL,
    ZONE                VARCHAR(50)     NULL,
    REGION              VARCHAR(50)     NULL,
    L7D_SALE_CONT_PCT   NUMBER(18,8)    NULL DEFAULT 0,
    MTD_SALE_CONT_PCT   NUMBER(18,8)    NULL DEFAULT 0,
    LM_SALE_CONT_PCT    NUMBER(18,8)    NULL DEFAULT 0,
    L3M_SALE_CONT_PCT   NUMBER(18,8)    NULL DEFAULT 0,
    YTD_SALE_CONT_PCT   NUMBER(18,8)    NULL DEFAULT 0,
    CL_STK_Q            NUMBER(18,4)    NULL DEFAULT 0,
    CL_STK_V            NUMBER(18,4)    NULL DEFAULT 0,
    YTD_GM_PCT          NUMBER(18,4)    NULL DEFAULT 0,
    YTD_SALES_PSF       NUMBER(18,4)    NULL DEFAULT 0,
    FETCHED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_SF_CONT_PCT PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 35. STG_SF_DIM_STORE (11 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS STG_SF_DIM_STORE (
    ID                  INTEGER         AUTOINCREMENT,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    STORE_NAME          VARCHAR(200)    NULL,
    STATE               VARCHAR(50)     NULL,
    STORE_SIZE_SQFT     NUMBER(12,2)    NULL,
    SIZE_CATEGORY       VARCHAR(20)     NULL,
    COHORT              VARCHAR(50)     NULL,
    OLD_NEW             VARCHAR(10)     NULL,
    ZONE                VARCHAR(50)     NULL,
    REGION              VARCHAR(50)     NULL,
    IS_ACTIVE           BOOLEAN         NULL DEFAULT TRUE,
    FETCHED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_SF_DIM_STORE PRIMARY KEY (ID)
);


-- ────────────────────────────────────────────────────────────
-- 36. STG_SF_DIM_ARTICLE (8 cols + ID)
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS STG_SF_DIM_ARTICLE (
    ID                  INTEGER         AUTOINCREMENT,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SUBDIVISION         VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    RNG_SEG             VARCHAR(50)     NULL,
    AVG_MRP             NUMBER(18,4)    NULL DEFAULT 0,
    ARTICLE_COUNT       INTEGER         NULL DEFAULT 0,
    FETCHED_AT          TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_STG_SF_DIM_ARTICLE PRIMARY KEY (ID)
);


-- ============================================================
-- COLUMN NAME MAPPING REFERENCE (SQL Server -> Snowflake)
-- ============================================================
-- [ST CD]          -> ST_CD
-- [ST NM]          -> ST_NM
-- [MAJ-CAT]        -> MAJ_CAT
-- [BIN CAP]        -> BIN_CAP
-- [BIN CAP DC TEAM]-> BIN_CAP_DC_TEAM
-- [ST-CD]          -> ST_CD
-- [OP-DATE]        -> OP_DATE
-- [REF STATE]      -> REF_STATE
-- [SALE GRP]       -> SALE_GRP
-- [REF_ST CD]      -> REF_ST_CD
-- [REF_ST NM]      -> REF_ST_NM
-- [REF-GRP-NEW]    -> REF_GRP_NEW
-- [REF-GRP-OLD]    -> REF_GRP_OLD
-- [WK-1]..[WK-48]  -> WK_1..WK_48
-- [DC-STK-Q]       -> DC_STK_Q
-- [GRT-STK-Q]      -> GRT_STK_Q
-- [W-GRT-STK-Q]    -> W_GRT_STK_Q
-- [2]              -> COL_2
-- GETDATE()        -> CURRENT_TIMESTAMP()
-- SYSTEM_USER      -> CURRENT_USER()
-- INT IDENTITY     -> INTEGER AUTOINCREMENT
-- DECIMAL(p,s)     -> NUMBER(p,s)
-- VARCHAR(MAX)     -> VARCHAR
-- NVARCHAR(n)      -> VARCHAR(n)
-- DATETIME/DATETIME2 -> TIMESTAMP_NTZ
-- BIT              -> BOOLEAN


-- ============================================================
-- DONE: 36 tables created
-- 10 reference/master tables
--  4 contribution tables
--  8 sub-level stock tables (4 store + 4 DC)
--  2 sub-level output tables
--  2 main output tables (TRF_IN_PLAN, PURCHASE_PLAN)
--  4 sale budget module tables
--  1 fixture density table
--  1 weekly disaggregation log
--  1 festival calendar
--  5 staging tables (STG_SF_*)
-- ============================================================
