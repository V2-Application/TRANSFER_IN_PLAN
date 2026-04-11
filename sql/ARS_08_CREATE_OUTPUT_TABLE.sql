-- ============================================================
-- ARS ALLOCATION MODULE — Output + Staging Tables
-- Database: datav2 (Server 192.168.151.28)
-- ============================================================
USE datav2;
GO

-- ────────────────────────────────────────────────────────
-- 1. OUTPUT: Final Allocation Results
-- ────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.ARS_ALLOCATION_OUTPUT', 'U') IS NULL
CREATE TABLE dbo.ARS_ALLOCATION_OUTPUT (
    ID                      INT IDENTITY(1,1) NOT NULL,
    RUN_ID                  VARCHAR(50)       NOT NULL,

    -- Store
    ST                      VARCHAR(20)       NOT NULL,
    ST_NM                   NVARCHAR(200)     NULL,
    TAGGED_RDC              VARCHAR(20)       NULL,
    HUB_CD                  VARCHAR(20)       NULL,

    -- Article
    MJ                      VARCHAR(100)      NOT NULL,
    [GEN-ART]               VARCHAR(50)       NOT NULL,
    CLR                     VARCHAR(50)       NOT NULL,
    SZ                      VARCHAR(20)       NULL,

    -- Classification
    ART_CLASS               VARCHAR(10)       NULL,   -- L-ART, MIX-ART, OLD-ART, NEW-L

    -- Stock Position
    TTL_ST_STK_Q            DECIMAL(18,4)     NULL DEFAULT 0,
    STK_0001                DECIMAL(18,4)     NULL DEFAULT 0,
    STK_0002                DECIMAL(18,4)     NULL DEFAULT 0,
    STK_0004                DECIMAL(18,4)     NULL DEFAULT 0,
    STK_0006                DECIMAL(18,4)     NULL DEFAULT 0,
    STK_HUB_INTRA           DECIMAL(18,4)     NULL DEFAULT 0,
    STK_HUB_PRD             DECIMAL(18,4)     NULL DEFAULT 0,
    STK_INTRA               DECIMAL(18,4)     NULL DEFAULT 0,
    STK_PRD                 DECIMAL(18,4)     NULL DEFAULT 0,
    MSA_QTY                 DECIMAL(18,4)     NULL DEFAULT 0,

    -- Days & Sale
    TTL_ALC_DAYS            DECIMAL(18,4)     NULL DEFAULT 0,
    HOLD_DAYS               DECIMAL(18,4)     NULL DEFAULT 0,
    CM_PD_SALE_Q            DECIMAL(18,4)     NULL DEFAULT 0,
    NM_PD_SALE_Q            DECIMAL(18,4)     NULL DEFAULT 0,
    ART_AUTO_SALE_PD        DECIMAL(18,4)     NULL DEFAULT 0,

    -- Display & MBQ
    ACC_DENSITY             DECIMAL(18,4)     NULL DEFAULT 0,
    ST_MJ_DISP_Q            DECIMAL(18,4)     NULL DEFAULT 0,
    ST_MJ_MBQ               DECIMAL(18,4)     NULL DEFAULT 0,
    ART_MBQ                 DECIMAL(18,4)     NULL DEFAULT 0,
    ART_HOLD_MBQ            DECIMAL(18,4)     NULL DEFAULT 0,
    VAR_ART_MBQ             DECIMAL(18,4)     NULL DEFAULT 0,
    VAR_ART_HOLD_MBQ        DECIMAL(18,4)     NULL DEFAULT 0,

    -- Requirement
    ST_MJ_REQ               DECIMAL(18,4)     NULL DEFAULT 0,
    ST_ART_REQ              DECIMAL(18,4)     NULL DEFAULT 0,
    ST_ART_HOLD_REQ         DECIMAL(18,4)     NULL DEFAULT 0,
    ST_VAR_ART_REQ          DECIMAL(18,4)     NULL DEFAULT 0,
    ST_VAR_ART_HOLD_REQ     DECIMAL(18,4)     NULL DEFAULT 0,

    -- ALLOCATION OUTPUT
    ART_ALC_Q               DECIMAL(18,4)     NULL DEFAULT 0,
    ART_HOLD_Q              DECIMAL(18,4)     NULL DEFAULT 0,
    REM_MSA                 DECIMAL(18,4)     NULL DEFAULT 0,

    -- Metadata
    CREATED_DT              DATETIME          NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_ARS_ALC_OUTPUT PRIMARY KEY CLUSTERED (ID)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ALC_RUN')
    CREATE INDEX IX_ARS_ALC_RUN ON dbo.ARS_ALLOCATION_OUTPUT (RUN_ID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ALC_ST_MJ')
    CREATE INDEX IX_ARS_ALC_ST_MJ ON dbo.ARS_ALLOCATION_OUTPUT (ST, MJ, [GEN-ART], CLR);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ALC_CLASS')
    CREATE INDEX IX_ARS_ALC_CLASS ON dbo.ARS_ALLOCATION_OUTPUT (ART_CLASS);
GO

-- ────────────────────────────────────────────────────────
-- 2. RUN LOG: Track each allocation run
-- ────────────────────────────────────────────────────────
IF OBJECT_ID('dbo.ARS_RUN_LOG', 'U') IS NULL
CREATE TABLE dbo.ARS_RUN_LOG (
    ID                  INT IDENTITY(1,1) NOT NULL,
    RUN_ID              VARCHAR(50)       NOT NULL,
    STARTED_DT          DATETIME          NOT NULL DEFAULT GETDATE(),
    COMPLETED_DT        DATETIME          NULL,
    STATUS              VARCHAR(20)       NULL DEFAULT 'RUNNING',
    TOTAL_STORES        INT               NULL DEFAULT 0,
    TOTAL_ARTICLES      INT               NULL DEFAULT 0,
    TOTAL_ALLOCATED     INT               NULL DEFAULT 0,
    TOTAL_HELD          INT               NULL DEFAULT 0,
    L_ART_COUNT         INT               NULL DEFAULT 0,
    MIX_ART_COUNT       INT               NULL DEFAULT 0,
    OLD_ART_COUNT       INT               NULL DEFAULT 0,
    NEW_L_COUNT         INT               NULL DEFAULT 0,
    ERROR_MSG           NVARCHAR(MAX)     NULL,
    CONSTRAINT PK_ARS_RUN_LOG PRIMARY KEY (ID)
);
GO

PRINT '>> ARS_ALLOCATION_OUTPUT + ARS_RUN_LOG created on datav2.';
GO
