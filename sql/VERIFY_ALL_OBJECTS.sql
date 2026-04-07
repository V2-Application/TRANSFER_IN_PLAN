-- =====================================================
-- VERIFICATION SCRIPT: Check all Transfer In + Purchase Plan objects
-- Run this in SSMS on 192.168.151.28 [planning] database
-- =====================================================

USE [planning];
GO

PRINT '============================================';
PRINT '  VERIFICATION: Transfer In Plan + Purchase Plan';
PRINT '  Server: ' + @@SERVERNAME;
PRINT '  Database: ' + DB_NAME();
PRINT '  Date: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '============================================';
PRINT '';

-- =====================================================
-- STEP 1: CHECK ALL TABLES
-- =====================================================
PRINT '--- STEP 1: TABLES ---';
PRINT '';

DECLARE @Tables TABLE (TableName VARCHAR(100), Phase VARCHAR(20), ExpectedColumns INT);
INSERT INTO @Tables VALUES
    ('WEEK_CALENDAR', 'Phase 1', 8),
    ('MASTER_ST_MASTER', 'Phase 1', 8),
    ('MASTER_BIN_CAPACITY', 'Phase 1', 3),
    ('MASTER_GRT_CONS_percentage', 'Phase 1', 49),
    ('QTY_SALE_QTY', 'Phase 1', 6),
    ('QTY_DISP_QTY', 'Phase 1', 5),
    ('QTY_ST_STK_Q', 'Phase 1', 6),
    ('QTY_MSA_AND_GRT', 'Phase 1', 7),
    ('TRF_IN_PLAN', 'Phase 1', 26),
    ('QTY_DEL_PENDING', 'Phase 2', 5),
    ('PURCHASE_PLAN', 'Phase 2', 56);

DECLARE @tName VARCHAR(100), @tPhase VARCHAR(20), @tExpected INT;
DECLARE @actualCols INT, @actualRows INT;

DECLARE table_cursor CURSOR FOR SELECT TableName, Phase, ExpectedColumns FROM @Tables;
OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @tName, @tPhase, @tExpected;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @tName, 'U') IS NOT NULL
    BEGIN
        SELECT @actualCols = COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @tName AND TABLE_SCHEMA = 'dbo';

        DECLARE @sql NVARCHAR(200) = N'SELECT @cnt = COUNT(*) FROM dbo.[' + @tName + N']';
        EXEC sp_executesql @sql, N'@cnt INT OUTPUT', @cnt = @actualRows OUTPUT;

        IF @actualCols >= @tExpected
            PRINT '  [OK]     ' + @tPhase + ' | ' + @tName + ' | Columns: ' + CAST(@actualCols AS VARCHAR) + ' (expected >=' + CAST(@tExpected AS VARCHAR) + ') | Rows: ' + CAST(@actualRows AS VARCHAR);
        ELSE
            PRINT '  [WARN]   ' + @tPhase + ' | ' + @tName + ' | Columns: ' + CAST(@actualCols AS VARCHAR) + ' (expected ' + CAST(@tExpected AS VARCHAR) + ') | Rows: ' + CAST(@actualRows AS VARCHAR);
    END
    ELSE
        PRINT '  [MISSING] ' + @tPhase + ' | ' + @tName + ' Ã¢Â€Â” TABLE NOT FOUND!';

    FETCH NEXT FROM table_cursor INTO @tName, @tPhase, @tExpected;
END

CLOSE table_cursor;
DEALLOCATE table_cursor;

PRINT '';

-- =====================================================
-- STEP 2: CHECK ALL STORED PROCEDURES
-- =====================================================
PRINT '--- STEP 2: STORED PROCEDURES ---';
PRINT '';

DECLARE @SPs TABLE (SpName VARCHAR(100), Phase VARCHAR(20));
INSERT INTO @SPs VALUES
    ('SP_GENERATE_TRF_IN_PLAN', 'Phase 1'),
    ('SP_GENERATE_PURCHASE_PLAN', 'Phase 2');

DECLARE @spName VARCHAR(100), @spPhase VARCHAR(20);
DECLARE sp_cursor CURSOR FOR SELECT SpName, Phase FROM @SPs;
OPEN sp_cursor;
FETCH NEXT FROM sp_cursor INTO @spName, @spPhase;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @spName, 'P') IS NOT NULL
    BEGIN
        DECLARE @spLines INT;
        SELECT @spLines = LEN(OBJECT_DEFINITION(OBJECT_ID('dbo.' + @spName))) - LEN(REPLACE(OBJECT_DEFINITION(OBJECT_ID('dbo.' + @spName)), CHAR(10), '')) + 1;
        PRINT '  [OK]     ' + @spPhase + ' | ' + @spName + ' | Lines: ~' + CAST(@spLines AS VARCHAR);
    END
    ELSE
        PRINT '  [MISSING] ' + @spPhase + ' | ' + @spName + ' Ã¢Â€Â” SP NOT FOUND!';

    FETCH NEXT FROM sp_cursor INTO @spName, @spPhase;
END

CLOSE sp_cursor;
DEALLOCATE sp_cursor;

PRINT '';

-- =====================================================
-- STEP 3: CHECK ALL VIEWS
-- =====================================================
PRINT '--- STEP 3: VIEWS ---';
PRINT '';

DECLARE @Views TABLE (ViewName VARCHAR(100), Phase VARCHAR(20));
INSERT INTO @Views VALUES
    ('VW_TRF_IN_STORE_SUMMARY', 'Phase 1'),
    ('VW_TRF_IN_RDC_SUMMARY', 'Phase 1'),
    ('VW_TRF_IN_CATEGORY_SUMMARY', 'Phase 1'),
    ('VW_TRF_IN_ALERTS', 'Phase 1'),
    ('VW_TRF_IN_DETAIL', 'Phase 1'),
    ('VW_TRF_IN_PIVOT', 'Phase 1'),
    ('VW_PURCHASE_PLAN_DETAIL', 'Phase 2'),
    ('VW_PURCHASE_PLAN_SUMMARY', 'Phase 2'),
    ('VW_PURCHASE_PLAN_ALERTS', 'Phase 2'),
    ('VW_PURCHASE_PLAN_CATEGORY_SUMMARY', 'Phase 2'),
    ('VW_WEEK_REFERENCE', 'Phase 2');

DECLARE @vName VARCHAR(100), @vPhase VARCHAR(20);
DECLARE view_cursor CURSOR FOR SELECT ViewName, Phase FROM @Views;
OPEN view_cursor;
FETCH NEXT FROM view_cursor INTO @vName, @vPhase;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF OBJECT_ID('dbo.' + @vName, 'V') IS NOT NULL
        PRINT '  [OK]     ' + @vPhase + ' | ' + @vName;
    ELSE
        PRINT '  [MISSING] ' + @vPhase + ' | ' + @vName + ' Ã¢Â€Â” VIEW NOT FOUND!';

    FETCH NEXT FROM view_cursor INTO @vName, @vPhase;
END

CLOSE view_cursor;
DEALLOCATE view_cursor;

PRINT '';

-- =====================================================
-- STEP 4: CHECK ALL INDEXES
-- =====================================================
PRINT '--- STEP 4: INDEXES ---';
PRINT '';

SELECT
    t.name AS [Table],
    i.name AS [Index],
    i.type_desc AS [Type],
    CASE WHEN i.has_filter = 1 THEN 'Filtered: ' + ISNULL(i.filter_definition, '') ELSE '' END AS [Filter]
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
WHERE t.name IN ('TRF_IN_PLAN', 'PURCHASE_PLAN', 'QTY_DEL_PENDING')
    AND i.name IS NOT NULL
ORDER BY t.name, i.name;

PRINT '';

-- =====================================================
-- STEP 5: CHECK REFERENCE DATA (row counts)
-- =====================================================
PRINT '--- STEP 5: REFERENCE DATA ROW COUNTS ---';
PRINT '';

SELECT 'WEEK_CALENDAR' AS [Table], COUNT(*) AS [Rows] FROM dbo.WEEK_CALENDAR
UNION ALL SELECT 'MASTER_ST_MASTER', COUNT(*) FROM dbo.MASTER_ST_MASTER
UNION ALL SELECT 'MASTER_BIN_CAPACITY', COUNT(*) FROM dbo.MASTER_BIN_CAPACITY
UNION ALL SELECT 'MASTER_GRT_CONS_percentage', COUNT(*) FROM dbo.MASTER_GRT_CONS_percentage
UNION ALL SELECT 'QTY_SALE_QTY', COUNT(*) FROM dbo.QTY_SALE_QTY
UNION ALL SELECT 'QTY_DISP_QTY', COUNT(*) FROM dbo.QTY_DISP_QTY
UNION ALL SELECT 'QTY_ST_STK_Q', COUNT(*) FROM dbo.QTY_ST_STK_Q
UNION ALL SELECT 'QTY_MSA_AND_GRT', COUNT(*) FROM dbo.QTY_MSA_AND_GRT
UNION ALL SELECT 'QTY_DEL_PENDING', COUNT(*) FROM dbo.QTY_DEL_PENDING
UNION ALL SELECT 'TRF_IN_PLAN', COUNT(*) FROM dbo.TRF_IN_PLAN
UNION ALL SELECT 'PURCHASE_PLAN', COUNT(*) FROM dbo.PURCHASE_PLAN;

PRINT '';

-- =====================================================
-- STEP 6: TEST SP EXECUTION (dry run Ã¢Â€Â” small range)
-- =====================================================
PRINT '--- STEP 6: SP EXECUTION TEST ---';
PRINT '';
PRINT '  To test Transfer In Plan SP:';
PRINT '    EXEC dbo.SP_GENERATE_TRF_IN_PLAN @StartWeekID=202401, @EndWeekID=202403, @Debug=1;';
PRINT '';
PRINT '  To test Purchase Plan SP:';
PRINT '    EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID=202401, @EndWeekID=202403, @Debug=1;';
PRINT '';

-- =====================================================
-- SUMMARY
-- =====================================================
PRINT '============================================';
PRINT '  SUMMARY';
PRINT '============================================';

DECLARE @tableCount INT, @spCount INT, @viewCount INT;

SELECT @tableCount = COUNT(*) FROM sys.tables WHERE name IN ('WEEK_CALENDAR','MASTER_ST_MASTER','MASTER_BIN_CAPACITY','MASTER_GRT_CONS_percentage','QTY_SALE_QTY','QTY_DISP_QTY','QTY_ST_STK_Q','QTY_MSA_AND_GRT','TRF_IN_PLAN','QTY_DEL_PENDING','PURCHASE_PLAN');
SELECT @spCount = COUNT(*) FROM sys.procedures WHERE name IN ('SP_GENERATE_TRF_IN_PLAN','SP_GENERATE_PURCHASE_PLAN');
SELECT @viewCount = COUNT(*) FROM sys.views WHERE name IN ('VW_TRF_IN_STORE_SUMMARY','VW_TRF_IN_RDC_SUMMARY','VW_TRF_IN_CATEGORY_SUMMARY','VW_TRF_IN_ALERTS','VW_TRF_IN_DETAIL','VW_TRF_IN_PIVOT','VW_PURCHASE_PLAN_DETAIL','VW_PURCHASE_PLAN_SUMMARY','VW_PURCHASE_PLAN_ALERTS','VW_PURCHASE_PLAN_CATEGORY_SUMMARY','VW_WEEK_REFERENCE');

PRINT '  Tables:     ' + CAST(@tableCount AS VARCHAR) + ' / 11';
PRINT '  SPs:        ' + CAST(@spCount AS VARCHAR) + ' / 2';
PRINT '  Views:      ' + CAST(@viewCount AS VARCHAR) + ' / 11';

IF @tableCount = 11 AND @spCount = 2 AND @viewCount = 11
    PRINT '  RESULT:     ALL 24 OBJECTS VERIFIED!';
ELSE
    PRINT '  RESULT:     SOME OBJECTS MISSING Ã¢Â€Â” check details above';

PRINT '============================================';
GO
