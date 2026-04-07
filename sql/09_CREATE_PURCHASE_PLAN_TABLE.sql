-- =====================================================
-- FILE: 09_CREATE_PURCHASE_PLAN_TABLE.sql
-- PURPOSE: Create PURCHASE_PLAN output table
-- GRAIN: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID
-- DATABASE: [planning]
-- =====================================================

USE [planning];
GO

-- =====================================================
-- TABLE: PURCHASE_PLAN
-- DESCRIPTION: Aggregated purchase plan metrics at RDC ÃƒÂ— Category ÃƒÂ— Week level
-- =====================================================

IF OBJECT_ID('dbo.PURCHASE_PLAN', 'U') IS NOT NULL
    DROP TABLE dbo.PURCHASE_PLAN;
GO

CREATE TABLE dbo.PURCHASE_PLAN (
    -- ===== IDENTITY & KEYS =====
    [ID] INT IDENTITY(1,1) NOT NULL,
    [RDC_CD] VARCHAR(20) NOT NULL,
    [RDC_NM] VARCHAR(100) NULL DEFAULT 'NA',
    [MAJ_CAT] VARCHAR(50) NOT NULL,
    [SSN] VARCHAR(10) NULL DEFAULT 'NA',  -- Season derived from FY_WEEK

    -- ===== WEEK REFERENCES =====
    [WEEK_ID] INT NOT NULL,
    [FY_WEEK] INT NULL,
    [FY_YEAR] INT NULL,
    [WK_ST_DT] DATE NULL,
    [WK_END_DT] DATE NULL,

    -- ===== STOCK REFERENCE DATA =====
    [DC_STK_Q] DECIMAL(18,4) DEFAULT 0,      -- From QTY_MSA_AND_GRT [DC-STK-Q]
    [GRT_STK_Q] DECIMAL(18,4) DEFAULT 0,     -- From QTY_MSA_AND_GRT [GRT-STK-Q]
    [S_GRT_STK_Q] DECIMAL(18,4) DEFAULT 0,   -- Store-level GRT stock
    [W_GRT_STK_Q] DECIMAL(18,4) DEFAULT 0,   -- Weekly GRT stock

    -- ===== BIN CAPACITY =====
    [BIN_CAP_DC_TEAM] DECIMAL(18,4) DEFAULT 0,
    [BIN_CAP] DECIMAL(18,4) DEFAULT 0,

    -- ===== STORE-LEVEL PLANNING (from TRF_IN_PLAN aggregation) =====
    [BGT_DISP_CL_Q] DECIMAL(18,4) DEFAULT 0,        -- Budget display closed qty
    [CW_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,        -- Current week budget sales (CM_BGT_SALE_Q)
    [CW1_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,       -- CW+1 budget sales (CM1_BGT_SALE_Q)
    [CW2_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,       -- CW+2 budget sales (CM2_BGT_SALE_Q)
    [CW3_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,       -- CW+3 budget sales (joined from future weeks)
    [CW4_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,       -- CW+4 budget sales
    [CW5_BGT_SALE_Q] DECIMAL(18,4) DEFAULT 0,       -- CW+5 budget sales

    -- ===== OPENING STOCK (Prior Week Closing) =====
    [BGT_ST_OP_MBQ] DECIMAL(18,4) DEFAULT 0,        -- Store opening MD/bulk qty (from prior week BGT_ST_CL_MBQ)
    [NET_ST_OP_STK_Q] DECIMAL(18,4) DEFAULT 0,      -- Net store opening stock (from BGT_TTL_CF_OP_STK_Q)

    -- ===== DC-LEVEL OPENING STOCK =====
    [BGT_DC_OP_STK_Q] DECIMAL(18,4) DEFAULT 0,      -- DC opening stock (=MAX(DC_STK_Q,0))
    [PP_NT_ACT_Q] DECIMAL(18,4) DEFAULT 0,          -- No-Touch Active qty at DC
    [BGT_CF_STK_Q] DECIMAL(18,4) DEFAULT 0,         -- Budget confirm stock qty at DC

    -- ===== TOTAL STOCK CALCULATION =====
    [TTL_STK] DECIMAL(18,4) DEFAULT 0,              -- Total stock (=GRT_STK_Q)
    [OP_STK] DECIMAL(18,4) DEFAULT 0,               -- Operating stock (=TTL_STK)
    [NT_ACT_STK] DECIMAL(18,4) DEFAULT 0,           -- No-touch/seasonal active stock

    -- ===== GRT CONSUMPTION =====
    [GRT_CONS_PCT] DECIMAL(18,4) DEFAULT 0,         -- GRT consumption percentage (from MASTER_GRT_CONS_percentage)
    [GRT_CONS_Q] DECIMAL(18,4) DEFAULT 0,           -- Calculated GRT consumption qty

    -- ===== DELIVERY PENDING =====
    [DEL_PEND_Q] DECIMAL(18,4) DEFAULT 0,           -- From QTY_DEL_PENDING

    -- ===== DC-LEVEL CONFIRM STOCK (Purchase Plan Logic) =====
    [PP_NET_BGT_CF_STK_Q] DECIMAL(18,4) DEFAULT 0,  -- Purchase plan net budget confirm stock

    -- ===== TRANSFER OUT (from TRF_IN_PLAN aggregation) =====
    [CW_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,         -- Current week transfer out (TRF_IN_STK_Q)
    [CW1_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,        -- Next week transfer out
    [TTL_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,        -- Total transfer out

    -- ===== STORE CLOSING STOCK =====
    [BGT_ST_CL_MBQ] DECIMAL(18,4) DEFAULT 0,        -- Store closing MD/bulk qty
    [NET_BGT_ST_CL_STK_Q] DECIMAL(18,4) DEFAULT 0,  -- Net budget store closing stock
    [NET_SSNL_CL_STK_Q] DECIMAL(18,4) DEFAULT 0,    -- Net seasonal closing stock

    -- ===== DC CLOSING STOCK & SALES =====
    [BGT_DC_MBQ_SALE] DECIMAL(18,4) DEFAULT 0,      -- DC MD/bulk qty (from DC_MBQ)
    [BGT_DC_CL_MBQ] DECIMAL(18,4) DEFAULT 0,        -- DC closing MD/bulk qty (=MIN(CW1_TRF_OUT_Q, BGT_DC_MBQ_SALE))
    [BGT_DC_CL_STK_Q] DECIMAL(18,4) DEFAULT 0,      -- DC closing stock qty (calculated)

    -- ===== PURCHASE QUANTITIES =====
    [BGT_PUR_Q_INIT] DECIMAL(18,4) DEFAULT 0,       -- Initial budget purchase qty
    [POS_PO_RAISED] DECIMAL(18,4) DEFAULT 0,        -- Positive PO raised
    [NEG_PO_RAISED] DECIMAL(18,4) DEFAULT 0,        -- Negative PO raised (returns)

    -- ===== COMPANY-LEVEL CLOSING STOCK =====
    [BGT_CO_CL_STK_Q] DECIMAL(18,4) DEFAULT 0,      -- Company (store + DC) closing stock

    -- ===== STOCK EXCESS/SHORTAGE =====
    [DC_STK_EXCESS_Q] DECIMAL(18,4) DEFAULT 0,      -- DC stock excess
    [DC_STK_SHORT_Q] DECIMAL(18,4) DEFAULT 0,       -- DC stock shortage
    [ST_STK_EXCESS_Q] DECIMAL(18,4) DEFAULT 0,      -- Store stock excess
    [ST_STK_SHORT_Q] DECIMAL(18,4) DEFAULT 0,       -- Store stock shortage
    [CO_STK_EXCESS_Q] DECIMAL(18,4) DEFAULT 0,      -- Company stock excess
    [CO_STK_SHORT_Q] DECIMAL(18,4) DEFAULT 0,       -- Company stock shortage

    -- ===== BIN REQUIREMENTS =====
    [FRESH_BIN_REQ] DECIMAL(18,4) DEFAULT 0,        -- Fresh bin requirement (BGT_DC_CL_STK_Q / BIN_CAP)
    [GRT_BIN_REQ] DECIMAL(18,4) DEFAULT 0,          -- GRT bin requirement (OP_STK / BIN_CAP)

    -- ===== ADDITIONAL TRANSFER OUT WEEKS =====
    [CW2_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,        -- CW+2 transfer out
    [CW3_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,        -- CW+3 transfer out
    [CW4_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0,        -- CW+4 transfer out

    -- ===== CATEGORY HIERARCHY (from Broader_Menu) =====
    [SEG]           VARCHAR(100) DEFAULT 'NA',
    [DIV]           VARCHAR(100) DEFAULT 'NA',
    [SUB_DIV]       VARCHAR(100) DEFAULT 'NA',
    [MAJ_CAT_NM]    VARCHAR(100) DEFAULT 'NA',

    -- ===== AUDIT COLUMNS =====
    [CREATED_DT] DATETIME2 DEFAULT GETDATE(),
    [CREATED_BY] VARCHAR(100) DEFAULT SYSTEM_USER,
    [MODIFIED_DT] DATETIME2 DEFAULT GETDATE(),
    [MODIFIED_BY] VARCHAR(100) DEFAULT SYSTEM_USER,

    CONSTRAINT PK_PURCHASE_PLAN PRIMARY KEY CLUSTERED ([ID])
);

GO

-- =====================================================
-- INDEXES: PURCHASE_PLAN
-- =====================================================

-- Primary search index: RDC ÃƒÂ— Category ÃƒÂ— Week
CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_RDC_CAT_WK
    ON dbo.PURCHASE_PLAN ([RDC_CD] ASC, [MAJ_CAT] ASC, [WEEK_ID] ASC)
    INCLUDE ([TTL_STK], [GRT_CONS_Q], [BGT_PUR_Q_INIT], [DC_STK_SHORT_Q], [CO_STK_SHORT_Q]);

GO

-- Secondary index: Week ÃƒÂ— Fiscal Year (for temporal queries)
CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_WEEK_FY
    ON dbo.PURCHASE_PLAN ([WEEK_ID] ASC, [FY_YEAR] ASC)
    INCLUDE ([RDC_CD], [MAJ_CAT], [BGT_PUR_Q_INIT], [POS_PO_RAISED]);

GO

-- Tertiary index: RDC ÃƒÂ— Category (for dimensional queries)
CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_RDC_CAT
    ON dbo.PURCHASE_PLAN ([RDC_CD] ASC, [MAJ_CAT] ASC)
    INCLUDE ([WEEK_ID], [TTL_STK], [BGT_PUR_Q_INIT]);

GO

-- Alert queries index: Short quantities (filtered indexes cannot use OR, so use separate indexes)
CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_DC_SHORT
    ON dbo.PURCHASE_PLAN ([RDC_CD] ASC, [WEEK_ID] ASC)
    INCLUDE ([MAJ_CAT], [DC_STK_SHORT_Q])
    WHERE [DC_STK_SHORT_Q] > 0;

GO

CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_CO_SHORT
    ON dbo.PURCHASE_PLAN ([RDC_CD] ASC, [WEEK_ID] ASC)
    INCLUDE ([MAJ_CAT], [CO_STK_SHORT_Q])
    WHERE [CO_STK_SHORT_Q] > 0;

GO

CREATE NONCLUSTERED INDEX IDX_PURCHASE_PLAN_PO_RAISED
    ON dbo.PURCHASE_PLAN ([RDC_CD] ASC, [WEEK_ID] ASC)
    INCLUDE ([MAJ_CAT], [POS_PO_RAISED])
    WHERE [POS_PO_RAISED] > 0;

GO

PRINT 'PURCHASE_PLAN table created successfully with all indexes.';
GO
