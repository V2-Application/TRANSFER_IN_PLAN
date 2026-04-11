-- ============================================================
-- WEEKLY DISAGGREGATION LOG TABLE
-- Run against 'planning' database on SQL Server
-- ============================================================
USE planning;
GO

IF OBJECT_ID('dbo.WEEKLY_DISAGG_LOG', 'U') IS NULL
CREATE TABLE dbo.WEEKLY_DISAGG_LOG (
    ID                  INT IDENTITY(1,1) NOT NULL,
    RUN_ID              VARCHAR(50)     NOT NULL,
    SOURCE_TABLE        VARCHAR(50)     NOT NULL,
    TARGET_TABLE        VARCHAR(50)     NOT NULL,
    ROWS_WRITTEN        INT             NOT NULL DEFAULT 0,
    MONTHS_PROCESSED    INT             NOT NULL DEFAULT 0,
    WEEKS_PER_MONTH     INT             NOT NULL DEFAULT 0,
    METHOD              VARCHAR(30)     NULL,
    CREATED_DT          DATETIME        NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_WEEKLY_DISAGG_LOG PRIMARY KEY (ID)
);
GO

PRINT 'Weekly Disaggregation log table created.';
GO
