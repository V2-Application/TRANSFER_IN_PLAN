-- ============================================================
-- ARS ALLOCATION MODULE — Article Auto Sale Table
-- Database: datav2 (Server 192.168.151.28)
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.ARS_ST_ART_AUTO_SALE', 'U') IS NULL
CREATE TABLE dbo.ARS_ST_ART_AUTO_SALE (
    ID                  INT IDENTITY(1,1) NOT NULL,
    ST                  VARCHAR(20)       NOT NULL,
    [GEN-ART]           VARCHAR(50)       NOT NULL,
    CLR                 VARCHAR(50)       NOT NULL,
    [CM-REM-DAYS]       DECIMAL(18,4)     NULL DEFAULT 0,
    [NM-DAYS]           DECIMAL(18,4)     NULL DEFAULT 0,
    [CM-AUTO-SALE-Q]    DECIMAL(18,4)     NULL DEFAULT 0,
    [NM-AUTO-SALE-Q]    DECIMAL(18,4)     NULL DEFAULT 0,
    CONSTRAINT PK_ARS_ART_AUTO_SALE PRIMARY KEY (ID),
    CONSTRAINT UQ_ARS_ART_SALE_ST_ART_CLR UNIQUE (ST, [GEN-ART], CLR)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_ART_SALE_ST')
    CREATE INDEX IX_ARS_ART_SALE_ST ON dbo.ARS_ST_ART_AUTO_SALE (ST, [GEN-ART], CLR);
GO

PRINT '>> ARS_ST_ART_AUTO_SALE created on datav2.';
GO
