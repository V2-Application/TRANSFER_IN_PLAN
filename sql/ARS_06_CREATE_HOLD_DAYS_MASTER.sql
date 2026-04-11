-- ============================================================
-- ARS ALLOCATION MODULE — Hold Days Master Table
-- Database: datav2 (Server 192.168.151.28)
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.ARS_HOLD_DAYS_MASTER', 'U') IS NULL
CREATE TABLE dbo.ARS_HOLD_DAYS_MASTER (
    ID              INT IDENTITY(1,1) NOT NULL,
    ST              VARCHAR(20)       NOT NULL,
    MJ              VARCHAR(100)      NOT NULL,
    HOLD_DAYS       DECIMAL(18,4)     NULL DEFAULT 0,
    CONSTRAINT PK_ARS_HOLD_DAYS PRIMARY KEY (ID),
    CONSTRAINT UQ_ARS_HOLD_ST_MJ UNIQUE (ST, MJ)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_HOLD_ST_MJ')
    CREATE INDEX IX_ARS_HOLD_ST_MJ ON dbo.ARS_HOLD_DAYS_MASTER (ST, MJ);
GO

PRINT '>> ARS_HOLD_DAYS_MASTER created on datav2.';
GO
