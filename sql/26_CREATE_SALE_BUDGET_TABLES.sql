-- ============================================================
-- SALE BUDGET PLAN MODULE — Tables, Staging, Config, Indexes
-- Run this script against the 'planning' database
-- ============================================================

USE planning;
GO

-- ────────────────────────────────────────────────────────────
-- 1. STAGING: Actual sales from Snowflake (Store x MajCat x Month)
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.STG_SF_SALE_ACTUAL', 'U') IS NULL
CREATE TABLE dbo.STG_SF_SALE_ACTUAL (
    ID                  INT IDENTITY(1,1) NOT NULL,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SUBDIVISION         VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    SALE_MONTH          DATE            NOT NULL,
    SALE_QTY            DECIMAL(18,4)   NULL DEFAULT 0,
    SALE_VAL            DECIMAL(18,4)   NULL DEFAULT 0,
    GM_VAL              DECIMAL(18,4)   NULL DEFAULT 0,
    LYSP_SALE_QTY       DECIMAL(18,4)   NULL DEFAULT 0,
    LYSP_SALE_VAL       DECIMAL(18,4)   NULL DEFAULT 0,
    LYSP_GM_VAL         DECIMAL(18,4)   NULL DEFAULT 0,
    FETCHED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_STG_SF_SALE PRIMARY KEY (ID)
);
GO

-- ────────────────────────────────────────────────────────────
-- 2. STAGING: ML demand forecasts from Snowflake
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.STG_SF_DEMAND_FORECAST', 'U') IS NULL
CREATE TABLE dbo.STG_SF_DEMAND_FORECAST (
    ID                  INT IDENTITY(1,1) NOT NULL,
    TARGET_MONTH        DATE            NOT NULL,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    STORE_NAME          NVARCHAR(200)   NULL,
    ENSEMBLE_FORECAST   DECIMAL(18,4)   NULL DEFAULT 0,
    STORE_CONT_PCT      DECIMAL(18,8)   NULL DEFAULT 0,
    STORE_FORECAST      DECIMAL(18,4)   NULL DEFAULT 0,
    FORECAST_LOW        DECIMAL(18,4)   NULL DEFAULT 0,
    FORECAST_HIGH       DECIMAL(18,4)   NULL DEFAULT 0,
    BEST_METHOD         VARCHAR(50)     NULL,
    WEIGHTED_MAPE       DECIMAL(18,4)   NULL DEFAULT 0,
    DATA_MONTHS_USED    INT             NULL DEFAULT 0,
    FETCHED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_STG_SF_FORECAST PRIMARY KEY (ID)
);
GO

-- ────────────────────────────────────────────────────────────
-- 3. STAGING: Store contribution percentages from Snowflake
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.STG_SF_CONT_PCT', 'U') IS NULL
CREATE TABLE dbo.STG_SF_CONT_PCT (
    ID                  INT IDENTITY(1,1) NOT NULL,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    STATE               VARCHAR(50)     NULL,
    ZONE                VARCHAR(50)     NULL,
    REGION              VARCHAR(50)     NULL,
    L7D_SALE_CONT_PCT   DECIMAL(18,8)  NULL DEFAULT 0,
    MTD_SALE_CONT_PCT   DECIMAL(18,8)  NULL DEFAULT 0,
    LM_SALE_CONT_PCT    DECIMAL(18,8)  NULL DEFAULT 0,
    L3M_SALE_CONT_PCT   DECIMAL(18,8)  NULL DEFAULT 0,
    YTD_SALE_CONT_PCT   DECIMAL(18,8)  NULL DEFAULT 0,
    CL_STK_Q            DECIMAL(18,4)  NULL DEFAULT 0,
    CL_STK_V            DECIMAL(18,4)  NULL DEFAULT 0,
    YTD_GM_PCT          DECIMAL(18,4)  NULL DEFAULT 0,
    YTD_SALES_PSF       DECIMAL(18,4)  NULL DEFAULT 0,
    FETCHED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_STG_SF_CONT PRIMARY KEY (ID)
);
GO

-- ────────────────────────────────────────────────────────────
-- 4. STAGING: Store dimension from Snowflake
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.STG_SF_DIM_STORE', 'U') IS NULL
CREATE TABLE dbo.STG_SF_DIM_STORE (
    ID                  INT IDENTITY(1,1) NOT NULL,
    STORE_CODE          VARCHAR(20)     NOT NULL,
    STORE_NAME          NVARCHAR(200)   NULL,
    STATE               VARCHAR(50)     NULL,
    STORE_SIZE_SQFT     DECIMAL(12,2)   NULL,
    SIZE_CATEGORY       VARCHAR(20)     NULL,
    COHORT              VARCHAR(50)     NULL,
    OLD_NEW             VARCHAR(10)     NULL,
    ZONE                VARCHAR(50)     NULL,
    REGION              VARCHAR(50)     NULL,
    IS_ACTIVE           BIT             NULL DEFAULT 1,
    FETCHED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_STG_SF_STORE PRIMARY KEY (ID)
);
GO

-- ────────────────────────────────────────────────────────────
-- 5. STAGING: Article hierarchy (MajCat-level) from Snowflake
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.STG_SF_DIM_ARTICLE', 'U') IS NULL
CREATE TABLE dbo.STG_SF_DIM_ARTICLE (
    ID                  INT IDENTITY(1,1) NOT NULL,
    MAJOR_CATEGORY      VARCHAR(100)    NOT NULL,
    DIVISION            VARCHAR(100)    NULL,
    SUBDIVISION         VARCHAR(100)    NULL,
    SEGMENT             VARCHAR(100)    NULL,
    RNG_SEG             VARCHAR(50)     NULL,
    AVG_MRP             DECIMAL(18,4)   NULL DEFAULT 0,
    ARTICLE_COUNT       INT             NULL DEFAULT 0,
    FETCHED_AT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_STG_SF_ARTICLE PRIMARY KEY (ID)
);
GO

-- ────────────────────────────────────────────────────────────
-- 6. OUTPUT: Sale Budget Plan
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.SALE_BUDGET_PLAN', 'U') IS NULL
CREATE TABLE dbo.SALE_BUDGET_PLAN (
    ID                      INT IDENTITY(1,1) NOT NULL,
    RUN_ID                  VARCHAR(50)     NOT NULL,

    -- Store dimensions
    STORE_CODE              VARCHAR(20)     NOT NULL,
    STORE_NAME              NVARCHAR(200)   NULL,
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

    -- LYSP (Last Year Same Period)
    LYSP_SALE_QTY           DECIMAL(18,4)   NULL DEFAULT 0,
    LYSP_SALE_VAL           DECIMAL(18,4)   NULL DEFAULT 0,
    LYSP_GM_VAL             DECIMAL(18,4)   NULL DEFAULT 0,

    -- Growth rates
    GROWTH_RATE_ST_CAT      DECIMAL(18,6)   NULL DEFAULT 0,
    GROWTH_RATE_CATEGORY    DECIMAL(18,6)   NULL DEFAULT 0,
    GROWTH_RATE_STORE       DECIMAL(18,6)   NULL DEFAULT 0,
    GROWTH_RATE_COMBINED    DECIMAL(18,6)   NULL DEFAULT 0,

    -- Adjustments
    FILL_RATE_ADJ           DECIMAL(18,6)   NULL DEFAULT 1.0,
    FESTIVAL_ADJ            DECIMAL(18,6)   NULL DEFAULT 0,

    -- ML forecast
    ML_FORECAST_QTY         DECIMAL(18,4)   NULL DEFAULT 0,
    ML_FORECAST_LOW         DECIMAL(18,4)   NULL DEFAULT 0,
    ML_FORECAST_HIGH        DECIMAL(18,4)   NULL DEFAULT 0,
    ML_FORECAST_MAPE        DECIMAL(18,4)   NULL,
    ML_BEST_METHOD          VARCHAR(50)     NULL,

    -- Final budget output
    BGT_SALE_QTY            DECIMAL(18,4)   NULL DEFAULT 0,
    BGT_SALE_VAL            DECIMAL(18,4)   NULL DEFAULT 0,
    BGT_GM_VAL              DECIMAL(18,4)   NULL DEFAULT 0,
    AVG_SELLING_PRICE       DECIMAL(18,4)   NULL DEFAULT 0,

    -- Tracking
    ALGO_METHOD             VARCHAR(20)     NULL,
    STORE_CONT_PCT          DECIMAL(18,8)   NULL DEFAULT 0,

    -- Metadata
    CREATED_DT              DATETIME        NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_SALE_BUDGET_PLAN PRIMARY KEY CLUSTERED (ID)
);
GO

-- Indexes
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SBP_RUN')
    CREATE NONCLUSTERED INDEX IX_SBP_RUN ON dbo.SALE_BUDGET_PLAN (RUN_ID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SBP_STORE_CAT')
    CREATE NONCLUSTERED INDEX IX_SBP_STORE_CAT ON dbo.SALE_BUDGET_PLAN (STORE_CODE, MAJOR_CATEGORY, PLAN_MONTH);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SBP_MONTH_DIV')
    CREATE NONCLUSTERED INDEX IX_SBP_MONTH_DIV ON dbo.SALE_BUDGET_PLAN (PLAN_MONTH, DIVISION);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SBP_CATEGORY')
    CREATE NONCLUSTERED INDEX IX_SBP_CATEGORY ON dbo.SALE_BUDGET_PLAN (MAJOR_CATEGORY, PLAN_MONTH);
GO

-- ────────────────────────────────────────────────────────────
-- 7. CONFIG: Algorithm parameters
-- ────────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.SALE_BUDGET_CONFIG', 'U') IS NULL
CREATE TABLE dbo.SALE_BUDGET_CONFIG (
    ID                  INT IDENTITY(1,1) NOT NULL,
    CONFIG_KEY          VARCHAR(100)    NOT NULL,
    CONFIG_VALUE        VARCHAR(500)    NOT NULL,
    DESCRIPTION         NVARCHAR(500)   NULL,
    UPDATED_DT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_SBP_CONFIG PRIMARY KEY (ID),
    CONSTRAINT UQ_SBP_CONFIG_KEY UNIQUE (CONFIG_KEY)
);
GO

-- Seed config
IF NOT EXISTS (SELECT 1 FROM dbo.SALE_BUDGET_CONFIG WHERE CONFIG_KEY = 'DEFAULT_GROWTH_RATE')
BEGIN
    INSERT INTO dbo.SALE_BUDGET_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION) VALUES
    ('DEFAULT_GROWTH_RATE',       '0.10',  'Default 10% YoY growth when no historical data'),
    ('FILL_RATE_LOW_THRESHOLD',   '0.70',  'Below this fill rate, increase budget'),
    ('FILL_RATE_HIGH_THRESHOLD',  '1.30',  'Above this fill rate, decrease budget'),
    ('FILL_RATE_ADJ_FACTOR',      '0.50',  'Dampening factor for fill rate adjustment'),
    ('MIN_COVER_DAYS_APP',        '45',    'Default cover days for Apparel when no data'),
    ('MIN_COVER_DAYS_GM',         '60',    'Default cover days for GM when no data'),
    ('ML_HYBRID_WEIGHT',          '0.60',  'ML weight in hybrid mode (0.6 = 60% ML, 40% LYSP)');
END
GO

PRINT 'Sale Budget Plan tables created successfully.';
GO
