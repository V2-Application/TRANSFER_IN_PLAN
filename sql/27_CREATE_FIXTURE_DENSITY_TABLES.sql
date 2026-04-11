-- ============================================================
-- FIXTURE & DENSITY PLAN MODULE — Tables
-- Run against 'planning' database on SQL Server
-- ============================================================
USE planning;
GO

IF OBJECT_ID('dbo.FIXTURE_DENSITY_PLAN', 'U') IS NULL
CREATE TABLE dbo.FIXTURE_DENSITY_PLAN (
    ID                      INT IDENTITY(1,1) NOT NULL,
    RUN_ID                  VARCHAR(50)     NOT NULL,

    -- Store
    STORE_CODE              VARCHAR(20)     NOT NULL,
    STORE_NAME              NVARCHAR(200)   NULL,
    STATE                   VARCHAR(50)     NULL,
    ZONE                    VARCHAR(50)     NULL,
    REGION                  VARCHAR(50)     NULL,
    STORE_SIZE_SQFT         DECIMAL(12,2)   NULL,
    SIZE_CATEGORY           VARCHAR(20)     NULL,

    -- Category
    MAJOR_CATEGORY          VARCHAR(100)    NOT NULL,
    DIVISION                VARCHAR(100)    NULL,
    SUBDIVISION             VARCHAR(100)    NULL,
    SEGMENT                 VARCHAR(100)    NULL,

    -- Time
    PLAN_MONTH              DATE            NOT NULL,

    -- Fixture & Density outputs
    BGT_DISP_QTY            DECIMAL(18,4)   NULL DEFAULT 0,
    BGT_DISP_VAL            DECIMAL(18,4)   NULL DEFAULT 0,
    ACC_DENSITY             DECIMAL(18,4)   NULL DEFAULT 0,
    FIX_COUNT               DECIMAL(18,4)   NULL DEFAULT 0,
    AREA_SQFT               DECIMAL(12,2)   NULL DEFAULT 0,

    -- Inputs used
    SALE_BGT_VAL            DECIMAL(18,4)   NULL DEFAULT 0,
    CL_STK_QTY              DECIMAL(18,4)   NULL DEFAULT 0,
    CL_STK_VAL              DECIMAL(18,4)   NULL DEFAULT 0,
    AVG_MRP                 DECIMAL(18,4)   NULL DEFAULT 0,

    -- ROI metrics
    GP_PSF                  DECIMAL(18,4)   NULL DEFAULT 0,
    SALES_PSF               DECIMAL(18,4)   NULL DEFAULT 0,
    STR_PCT                 DECIMAL(18,6)   NULL DEFAULT 0,

    -- Algo
    ALGO_METHOD             VARCHAR(30)     NULL,
    CREATED_DT              DATETIME        NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_FIXTURE_DENSITY_PLAN PRIMARY KEY CLUSTERED (ID)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FDP_RUN')
    CREATE NONCLUSTERED INDEX IX_FDP_RUN ON dbo.FIXTURE_DENSITY_PLAN (RUN_ID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FDP_STORE_CAT')
    CREATE NONCLUSTERED INDEX IX_FDP_STORE_CAT ON dbo.FIXTURE_DENSITY_PLAN (STORE_CODE, MAJOR_CATEGORY, PLAN_MONTH);
GO

PRINT 'Fixture & Density Plan tables created.';
GO
