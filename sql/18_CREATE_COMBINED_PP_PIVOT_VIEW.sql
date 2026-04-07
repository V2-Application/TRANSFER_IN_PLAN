/*==============================================================
  FILE: 18_CREATE_COMBINED_PP_PIVOT_VIEW.sql
  PURPOSE: Create a single combined view over the 3 physical
           Purchase Plan pivot tables.

  PROBLEM:
    SQL Server has a 1024-column limit on VIEWs.
    3 tables ÃƒÂ— ~733 cols = ~2193 cols Ã¢Â†Â’ cannot be a VIEW.

  SOLUTION:
    Use a stored procedure with dynamic SQL.
    SP result sets have NO column limit.

  RESULT:
    SP_PP_PIVOT_ALL  Ã¢Â€Â” one SP, all ~2193 columns, instant query

  USAGE:
    EXEC dbo.SP_PP_PIVOT_ALL;                  -- all data
    EXEC dbo.SP_PP_PIVOT_ALL @RdcCode = 'R01'; -- filter by RDC
    EXEC dbo.SP_PP_PIVOT_ALL @MajCat = 'ATTA'; -- filter by MAJ_CAT

  PREREQUISITE: Run 17_OPTIMIZE_PIVOT_PERFORMANCE.sql first
                (creates the 3 physical tables with data)
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.SP_PP_PIVOT_ALL', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_PP_PIVOT_ALL;
GO

CREATE PROCEDURE dbo.SP_PP_PIVOT_ALL
    @RdcCode VARCHAR(20)  = NULL,
    @MajCat  VARCHAR(50)  = NULL,
    @FyYear  INT          = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- =====================================================
    -- Build dynamic column list from INFORMATION_SCHEMA
    -- Take ALL columns from s (stock), then only metric
    -- columns (excluding identifiers) from g and p.
    -- =====================================================

    -- Identifier columns present in all 3 tables
    DECLARE @idCols TABLE (col_name NVARCHAR(128));
    INSERT INTO @idCols VALUES
        ('RDC-CD'),('RDC-NM'),('MAJ-CAT'),
        ('SEG'),('DIV'),('SUB_DIV'),('MAJ_CAT_NM'),
        ('SSN'),('FY_YEAR');

    DECLARE @cols NVARCHAR(MAX) = '';
    DECLARE @sql  NVARCHAR(MAX);

    -- All columns from PP_PIVOT_STOCK_DATA (alias s)
    SELECT @cols = @cols + 's.' + QUOTENAME(COLUMN_NAME) + ', '
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'PP_PIVOT_STOCK_DATA' AND TABLE_SCHEMA = 'dbo'
    ORDER BY ORDINAL_POSITION;

    -- Metric columns only from PP_PIVOT_GRT_TRF_DATA (alias g)
    SELECT @cols = @cols + 'g.' + QUOTENAME(COLUMN_NAME) + ', '
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'PP_PIVOT_GRT_TRF_DATA' AND TABLE_SCHEMA = 'dbo'
      AND COLUMN_NAME NOT IN (SELECT col_name FROM @idCols)
    ORDER BY ORDINAL_POSITION;

    -- Metric columns only from PP_PIVOT_PURCHASE_DATA (alias p)
    SELECT @cols = @cols + 'p.' + QUOTENAME(COLUMN_NAME) + ', '
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'PP_PIVOT_PURCHASE_DATA' AND TABLE_SCHEMA = 'dbo'
      AND COLUMN_NAME NOT IN (SELECT col_name FROM @idCols)
    ORDER BY ORDINAL_POSITION;

    -- Trim trailing comma+space
    SET @cols = LEFT(@cols, LEN(@cols) - 1);

    -- Build full SELECT
    SET @sql = 'SELECT ' + @cols + CHAR(13)
        + 'FROM dbo.PP_PIVOT_STOCK_DATA s WITH (NOLOCK)' + CHAR(13)
        + 'INNER JOIN dbo.PP_PIVOT_GRT_TRF_DATA g WITH (NOLOCK)' + CHAR(13)
        + '    ON  s.[RDC-CD]  = g.[RDC-CD]' + CHAR(13)
        + '    AND s.[MAJ-CAT] = g.[MAJ-CAT]' + CHAR(13)
        + '    AND s.FY_YEAR   = g.FY_YEAR' + CHAR(13)
        + '    AND s.SSN       = g.SSN' + CHAR(13)
        + 'INNER JOIN dbo.PP_PIVOT_PURCHASE_DATA p WITH (NOLOCK)' + CHAR(13)
        + '    ON  s.[RDC-CD]  = p.[RDC-CD]' + CHAR(13)
        + '    AND s.[MAJ-CAT] = p.[MAJ-CAT]' + CHAR(13)
        + '    AND s.FY_YEAR   = p.FY_YEAR' + CHAR(13)
        + '    AND s.SSN       = p.SSN' + CHAR(13)
        + 'WHERE 1=1' + CHAR(13);

    -- Optional filters
    IF @RdcCode IS NOT NULL
        SET @sql = @sql + '  AND s.[RDC-CD] = ' + QUOTENAME(@RdcCode, '''') + CHAR(13);
    IF @MajCat IS NOT NULL
        SET @sql = @sql + '  AND s.[MAJ-CAT] = ' + QUOTENAME(@MajCat, '''') + CHAR(13);
    IF @FyYear IS NOT NULL
        SET @sql = @sql + '  AND s.FY_YEAR = ' + CAST(@FyYear AS VARCHAR) + CHAR(13);

    SET @sql = @sql + 'ORDER BY s.[RDC-CD], s.[MAJ-CAT];';

    -- Execute
    EXEC sp_executesql @sql;
END;
GO

PRINT 'SP_PP_PIVOT_ALL created successfully.';
PRINT '';
PRINT 'Usage:';
PRINT '  EXEC dbo.SP_PP_PIVOT_ALL;                         -- all data';
PRINT '  EXEC dbo.SP_PP_PIVOT_ALL @RdcCode = ''R01'';        -- filter by RDC';
PRINT '  EXEC dbo.SP_PP_PIVOT_ALL @MajCat  = ''ATTA'';       -- filter by MAJ_CAT';
PRINT '  EXEC dbo.SP_PP_PIVOT_ALL @FyYear  = 2026;          -- filter by year';
GO
