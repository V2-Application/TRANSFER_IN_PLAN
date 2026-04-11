-- ============================================================
-- ARS ALLOCATION MODULE — Store Master Table + Computed View
-- Database: datav2 (Server 192.168.151.28)
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.ARS_ST_MASTER', 'U') IS NULL
CREATE TABLE dbo.ARS_ST_MASTER (
    ID                      INT IDENTITY(1,1) NOT NULL,
    ST_CD                   VARCHAR(20)       NOT NULL,
    ST_NM                   NVARCHAR(200)     NULL,
    HUB_CD                  VARCHAR(20)       NULL,
    HUB_NM                  NVARCHAR(200)     NULL,
    DIRECT_HUB              VARCHAR(20)       NULL,
    TAGGED_RDC              VARCHAR(20)       NULL,
    DH24_DC_TO_HUB_INTRA    DECIMAL(18,4)     NULL DEFAULT 0,
    DH24_HUB_TO_ST_INTRA    DECIMAL(18,4)     NULL DEFAULT 0,
    DW01_DC_TO_HUB_INTRA    DECIMAL(18,4)     NULL DEFAULT 0,
    DW01_HUB_TO_ST_INTRA    DECIMAL(18,4)     NULL DEFAULT 0,
    ST_OP_DT                DATE              NULL,
    ST_STAT                 VARCHAR(20)       NULL,
    SALE_COVER_DAYS          DECIMAL(18,4)     NULL DEFAULT 0,
    PRD_DAYS                DECIMAL(18,4)     NULL DEFAULT 0,
    CONSTRAINT PK_ARS_ST_MASTER PRIMARY KEY (ID),
    CONSTRAINT UQ_ARS_ST_MASTER_CD UNIQUE (ST_CD)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ST_MASTER_CD')
    CREATE INDEX IX_ARS_ST_MASTER_CD ON dbo.ARS_ST_MASTER (ST_CD);
GO

-- ════════════════════════════════════════════════════════════
-- VIEW: Computed INTRA_DAYS + TTL_ALC_DAYS
-- ════════════════════════════════════════════════════════════
IF OBJECT_ID('dbo.VW_ARS_ST_MASTER', 'V') IS NOT NULL
    DROP VIEW dbo.VW_ARS_ST_MASTER;
GO

CREATE VIEW dbo.VW_ARS_ST_MASTER
AS
SELECT
    ID, ST_CD, ST_NM, HUB_CD, HUB_NM, DIRECT_HUB, TAGGED_RDC,
    DH24_DC_TO_HUB_INTRA, DH24_HUB_TO_ST_INTRA,
    DW01_DC_TO_HUB_INTRA, DW01_HUB_TO_ST_INTRA,
    ST_OP_DT, ST_STAT, SALE_COVER_DAYS, PRD_DAYS,

    -- INTRA_DAYS = IF TAGGED_RDC='DW01' THEN DW01 route ELSE DH24 route
    CASE WHEN TAGGED_RDC = 'DW01'
         THEN ISNULL(DW01_DC_TO_HUB_INTRA, 0) + ISNULL(DW01_HUB_TO_ST_INTRA, 0)
         ELSE ISNULL(DH24_DC_TO_HUB_INTRA, 0) + ISNULL(DH24_HUB_TO_ST_INTRA, 0)
    END AS INTRA_DAYS,

    -- TTL_ALC_DAYS = SALE_COVER_DAYS + PRD_DAYS + INTRA_DAYS
    ISNULL(SALE_COVER_DAYS, 0) + ISNULL(PRD_DAYS, 0) +
    CASE WHEN TAGGED_RDC = 'DW01'
         THEN ISNULL(DW01_DC_TO_HUB_INTRA, 0) + ISNULL(DW01_HUB_TO_ST_INTRA, 0)
         ELSE ISNULL(DH24_DC_TO_HUB_INTRA, 0) + ISNULL(DH24_HUB_TO_ST_INTRA, 0)
    END AS TTL_ALC_DAYS

FROM dbo.ARS_ST_MASTER;
GO

PRINT '>> ARS_ST_MASTER + VW_ARS_ST_MASTER created on datav2.';
GO
