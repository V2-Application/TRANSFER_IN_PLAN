-- ============================================================
-- ARS ALLOCATION MODULE — Article Auto Sale Table
-- Database: datav2 (Server 192.168.151.28)
-- Unique: ST + GEN-ART + CLR + MJ
-- ============================================================
USE datav2;
GO

-- Drop old constraint/index if upgrading
IF OBJECT_ID('dbo.ARS_ST_ART_AUTO_SALE', 'U') IS NOT NULL
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UQ_ARS_ART_SALE_ST_ART_CLR' AND object_id = OBJECT_ID('dbo.ARS_ST_ART_AUTO_SALE'))
        ALTER TABLE dbo.ARS_ST_ART_AUTO_SALE DROP CONSTRAINT UQ_ARS_ART_SALE_ST_ART_CLR;
    -- Add MJ column if missing
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ARS_ST_ART_AUTO_SALE' AND COLUMN_NAME = 'MJ')
        ALTER TABLE dbo.ARS_ST_ART_AUTO_SALE ADD MJ VARCHAR(100) NULL;
    -- Add new unique constraint
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UQ_ARS_ART_SALE_ST_ART_CLR_MJ' AND object_id = OBJECT_ID('dbo.ARS_ST_ART_AUTO_SALE'))
        ALTER TABLE dbo.ARS_ST_ART_AUTO_SALE ADD CONSTRAINT UQ_ARS_ART_SALE_ST_ART_CLR_MJ UNIQUE (ST, [GEN-ART], CLR, MJ);
END
ELSE
BEGIN
    CREATE TABLE dbo.ARS_ST_ART_AUTO_SALE (
        ID                  INT IDENTITY(1,1) NOT NULL,
        ST                  VARCHAR(20)       NOT NULL,
        [GEN-ART]           VARCHAR(50)       NOT NULL,
        CLR                 VARCHAR(50)       NOT NULL,
        MJ                  VARCHAR(100)      NULL,
        [CM-REM-DAYS]       DECIMAL(18,4)     NULL DEFAULT 0,
        [NM-DAYS]           DECIMAL(18,4)     NULL DEFAULT 0,
        [CM-AUTO-SALE-Q]    DECIMAL(18,4)     NULL DEFAULT 0,
        [NM-AUTO-SALE-Q]    DECIMAL(18,4)     NULL DEFAULT 0,
        CONSTRAINT PK_ARS_ART_AUTO_SALE PRIMARY KEY (ID),
        CONSTRAINT UQ_ARS_ART_SALE_ST_ART_CLR_MJ UNIQUE (ST, [GEN-ART], CLR, MJ)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ART_SALE_ST')
    CREATE INDEX IX_ARS_ART_SALE_ST ON dbo.ARS_ST_ART_AUTO_SALE (ST, [GEN-ART], CLR, MJ);
GO

PRINT '>> ARS_ST_ART_AUTO_SALE created/updated on datav2 (with MJ column).';
GO
