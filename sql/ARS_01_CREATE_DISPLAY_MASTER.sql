-- ============================================================
-- ARS ALLOCATION MODULE — Display Master Table
-- Database: datav2 (Server 192.168.151.28)
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.ARS_ST_MJ_DISPLAY_MASTER', 'U') IS NULL
CREATE TABLE dbo.ARS_ST_MJ_DISPLAY_MASTER (
    ID              INT IDENTITY(1,1) NOT NULL,
    ST              VARCHAR(20)       NOT NULL,
    MJ              VARCHAR(100)      NOT NULL,
    [ST_MJ_DISP_Q]    DECIMAL(18,4)     NULL DEFAULT 0,
    CONSTRAINT PK_ARS_DISPLAY_MASTER PRIMARY KEY (ID),
    CONSTRAINT UQ_ARS_DISP_ST_MJ UNIQUE (ST, MJ)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_ARS_DISP_ST_MJ')
    CREATE INDEX IX_ARS_DISP_ST_MJ ON dbo.ARS_ST_MJ_DISPLAY_MASTER (ST, MJ);
GO

PRINT '>> ARS_ST_MJ_DISPLAY_MASTER created on datav2.';
GO
