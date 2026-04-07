/*==============================================================
  TEST SCRIPT: Run TRF_IN + PP for a few stores,
               validate algorithm, show pivot output.

  RUN THIS IN SSMS (Results to Grid + Messages enabled)
  Each section separated by GO Ã¢Â€Â” run entire script at once.

  Sections:
    1. Pre-checks (tables, SPs, views exist)
    2. Pick 3 test stores + show their RDCs
    3. Run TRF_IN SP for 3 stores (with debug)
    4. Validate TRF_IN algorithm (chaining, shrinkage)
    5. Run PP SP for those RDCs (with debug)
    6. Validate PP algorithm (DC chaining, GRT_CONS)
    7. Broader_Menu integration check (SEG, DIV, SSN)
    8. Pivot output Ã¢Â€Â” Transfer In (all 52 weeks)
    9. Pivot output Ã¢Â€Â” Purchase Plan (all 52 weeks)
   10. Summary
==============================================================*/

USE [planning];
GO

-- =====================================================
-- SECTION 1: PRE-CHECKS
-- =====================================================
PRINT '================================================================';
PRINT '  SECTION 1: PRE-CHECKS';
PRINT '================================================================';

-- Check all required objects exist
SELECT
    ObjectName,
    CASE WHEN OBJECT_ID(ObjectName) IS NOT NULL THEN 'EXISTS' ELSE '*** MISSING ***' END AS Status,
    ObjectType
FROM (VALUES
    ('dbo.SP_GENERATE_TRF_IN_PLAN',     'Stored Procedure'),
    ('dbo.SP_GENERATE_PURCHASE_PLAN',    'Stored Procedure'),
    ('dbo.VW_TRF_IN_PIVOT',             'View'),
    ('dbo.VW_PP_PIVOT_STOCK',           'View'),
    ('dbo.VW_PP_PIVOT_GRT_TRF',         'View'),
    ('dbo.VW_PP_PIVOT_PURCHASE',         'View'),
    ('dbo.TRF_IN_PLAN',                 'Table'),
    ('dbo.PURCHASE_PLAN',               'Table'),
    ('dbo.WEEK_CALENDAR',               'Table'),
    ('dbo.MASTER_ST_MASTER',            'Table'),
    ('dbo.MASTER_BIN_CAPACITY',         'Table'),
    ('dbo.Broader_Menu',                'Table'),
    ('dbo.QTY_SALE_QTY',               'Table'),
    ('dbo.QTY_DISP_QTY',               'Table'),
    ('dbo.QTY_ST_STK_Q',               'Table'),
    ('dbo.QTY_MSA_AND_GRT',            'Table'),
    ('dbo.MASTER_GRT_CONS_percentage',  'Table')
) AS T(ObjectName, ObjectType);

-- Check column existence on TRF_IN_PLAN (Broader_Menu columns)
SELECT 'TRF_IN_PLAN columns' AS CheckType,
    MAX(CASE WHEN COLUMN_NAME = 'SEG' THEN 'YES' ELSE 'NO' END) AS SEG,
    MAX(CASE WHEN COLUMN_NAME = 'DIV' THEN 'YES' ELSE 'NO' END) AS DIV,
    MAX(CASE WHEN COLUMN_NAME = 'SUB_DIV' THEN 'YES' ELSE 'NO' END) AS SUB_DIV,
    MAX(CASE WHEN COLUMN_NAME = 'MAJ_CAT_NM' THEN 'YES' ELSE 'NO' END) AS MAJ_CAT_NM
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'TRF_IN_PLAN';

SELECT 'PURCHASE_PLAN columns' AS CheckType,
    MAX(CASE WHEN COLUMN_NAME = 'SEG' THEN 'YES' ELSE 'NO' END) AS SEG,
    MAX(CASE WHEN COLUMN_NAME = 'DIV' THEN 'YES' ELSE 'NO' END) AS DIV,
    MAX(CASE WHEN COLUMN_NAME = 'SUB_DIV' THEN 'YES' ELSE 'NO' END) AS SUB_DIV,
    MAX(CASE WHEN COLUMN_NAME = 'MAJ_CAT_NM' THEN 'YES' ELSE 'NO' END) AS MAJ_CAT_NM,
    MAX(CASE WHEN COLUMN_NAME = 'CW2_TRF_OUT_Q' THEN 'YES' ELSE 'NO' END) AS CW2_TRF_OUT_Q,
    MAX(CASE WHEN COLUMN_NAME = 'CW3_TRF_OUT_Q' THEN 'YES' ELSE 'NO' END) AS CW3_TRF_OUT_Q,
    MAX(CASE WHEN COLUMN_NAME = 'CW4_TRF_OUT_Q' THEN 'YES' ELSE 'NO' END) AS CW4_TRF_OUT_Q
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'PURCHASE_PLAN';

-- Week range check
SELECT 'WEEK_CALENDAR' AS CheckType,
    MIN(WEEK_ID) AS MinWeekID,
    MAX(WEEK_ID) AS MaxWeekID,
    COUNT(*) AS TotalWeeks
FROM dbo.WEEK_CALENDAR;

PRINT 'Pre-checks complete. Review results above.';
GO


-- =====================================================
-- SECTION 2: PICK 3 TEST STORES
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 2: PICK 3 TEST STORES';
PRINT '================================================================';

-- Pick 3 stores (first, middle, last by ST_CD)
DECLARE @TestStores TABLE (Seq INT IDENTITY, ST_CD VARCHAR(20), RDC_CD VARCHAR(20));

INSERT INTO @TestStores (ST_CD, RDC_CD)
SELECT TOP 3 [ST CD], [RDC_CD]
FROM (
    SELECT [ST CD], [RDC_CD],
        ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN,
        COUNT(*) OVER() AS Total
    FROM dbo.MASTER_ST_MASTER
) x
WHERE RN = 1 OR RN = Total / 2 OR RN = Total
ORDER BY [ST CD];

SELECT 'TEST STORES' AS Info, ST_CD, RDC_CD FROM @TestStores;

-- Show total store and RDC counts
SELECT
    COUNT(DISTINCT [ST CD]) AS Total_Stores,
    COUNT(DISTINCT [RDC_CD]) AS Total_RDCs,
    COUNT(DISTINCT CASE WHEN [STATUS] = 'NEW' THEN [ST CD] END) AS Active_Stores,
    COUNT(DISTINCT CASE WHEN [STATUS] != 'NEW' OR [STATUS] IS NULL THEN [ST CD] END) AS Inactive_Stores
FROM dbo.MASTER_ST_MASTER;

-- Show MAJ_CAT count
SELECT COUNT(DISTINCT [MAJ-CAT]) AS Total_MAJ_CAT FROM dbo.MASTER_BIN_CAPACITY;

-- Show Broader_Menu sample
SELECT TOP 5 MAJ_CAT_NM, SEG, DIV, SUB_DIV, SSN
FROM dbo.Broader_Menu
ORDER BY MAJ_CAT_NM;
GO


-- =====================================================
-- SECTION 3: RUN TRF_IN SP FOR 3 TEST STORES
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 3: RUN TRANSFER IN PLAN (3 stores)';
PRINT '================================================================';

-- Get week range (full year)
DECLARE @StartWk INT, @EndWk INT;
SELECT @StartWk = MIN(WEEK_ID), @EndWk = MAX(WEEK_ID) FROM dbo.WEEK_CALENDAR;

-- Pick 3 stores again
DECLARE @S1 VARCHAR(20), @S2 VARCHAR(20), @S3 VARCHAR(20);
DECLARE @R1 VARCHAR(20), @R2 VARCHAR(20), @R3 VARCHAR(20);

SELECT TOP 1 @S1 = [ST CD], @R1 = [RDC_CD]
FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD];

SELECT TOP 1 @S2 = [ST CD], @R2 = [RDC_CD]
FROM (
    SELECT [ST CD], [RDC_CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
    FROM dbo.MASTER_ST_MASTER
) x WHERE RN = Total / 2;

SELECT TOP 1 @S3 = [ST CD], @R3 = [RDC_CD]
FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD] DESC;

PRINT 'Test Store 1: ' + @S1 + ' (RDC: ' + @R1 + ')';
PRINT 'Test Store 2: ' + @S2 + ' (RDC: ' + @R2 + ')';
PRINT 'Test Store 3: ' + @S3 + ' (RDC: ' + @R3 + ')';
PRINT 'Week range: ' + CAST(@StartWk AS VARCHAR) + ' to ' + CAST(@EndWk AS VARCHAR);
PRINT '';

-- Run Store 1
PRINT '--- Running TRF_IN for Store: ' + @S1 + ' ---';
EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = @StartWk, @EndWeekID = @EndWk,
    @StoreCode = @S1, @Debug = 1;

-- Run Store 2
PRINT '';
PRINT '--- Running TRF_IN for Store: ' + @S2 + ' ---';
EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = @StartWk, @EndWeekID = @EndWk,
    @StoreCode = @S2, @Debug = 1;

-- Run Store 3
PRINT '';
PRINT '--- Running TRF_IN for Store: ' + @S3 + ' ---';
EXEC dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID = @StartWk, @EndWeekID = @EndWk,
    @StoreCode = @S3, @Debug = 1;
GO


-- =====================================================
-- SECTION 4: VALIDATE TRF_IN ALGORITHM
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 4: VALIDATE TRANSFER IN ALGORITHM';
PRINT '================================================================';

-- 4A: Check row counts per store
SELECT 'ROW_COUNT' AS Check_Type,
    ST_CD, COUNT(*) AS Rows,
    COUNT(DISTINCT MAJ_CAT) AS MAJ_CATs,
    COUNT(DISTINCT FY_WEEK) AS Weeks,
    MIN(FY_WEEK) AS MinWeek, MAX(FY_WEEK) AS MaxWeek
FROM dbo.TRF_IN_PLAN
WHERE ST_CD IN (SELECT TOP 3 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
GROUP BY ST_CD;

-- 4B: CHAINING CHECK Ã¢Â€Â” Week N opening stock = Week N-1 closing stock
-- If chaining works, this should return 0 rows (no mismatches)
SELECT 'CHAIN_MISMATCH' AS Check_Type,
    curr.ST_CD, curr.MAJ_CAT,
    prev.FY_WEEK AS PrevWeek, curr.FY_WEEK AS CurrWeek,
    prev.BGT_TTL_CF_CL_STK_Q AS PrevWeek_ClosingStk,
    curr.BGT_TTL_CF_OP_STK_Q AS CurrWeek_OpeningStk,
    prev.BGT_TTL_CF_CL_STK_Q - curr.BGT_TTL_CF_OP_STK_Q AS Difference
FROM dbo.TRF_IN_PLAN curr
INNER JOIN dbo.TRF_IN_PLAN prev
    ON prev.ST_CD = curr.ST_CD
    AND prev.MAJ_CAT = curr.MAJ_CAT
    AND prev.FY_WEEK = curr.FY_WEEK - 1
    AND prev.FY_YEAR = curr.FY_YEAR
WHERE curr.ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
  AND prev.BGT_TTL_CF_CL_STK_Q != curr.BGT_TTL_CF_OP_STK_Q;

-- 4C: SHRINKAGE CHECK Ã¢Â€Â” NT_ACT_Q = ROUND(ROUND(OP_STK * 0.08, 0) * SSN_FACTOR, 0)
-- SSN_FACTOR: S,PS = 1.0; others = 0.5
SELECT TOP 10
    'SHRINKAGE_CHECK' AS Check_Type,
    T.ST_CD, T.MAJ_CAT, T.FY_WEEK, T.SSN,
    T.BGT_TTL_CF_OP_STK_Q AS OP_STK,
    T.NT_ACT_Q AS Actual_Shrinkage,
    ROUND(ROUND(T.BGT_TTL_CF_OP_STK_Q * 0.08, 0)
        * CASE WHEN T.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) AS Expected_Shrinkage,
    CASE WHEN T.NT_ACT_Q = ROUND(ROUND(T.BGT_TTL_CF_OP_STK_Q * 0.08, 0)
        * CASE WHEN T.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.TRF_IN_PLAN T
WHERE T.ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
  AND T.BGT_TTL_CF_OP_STK_Q > 0
ORDER BY T.FY_WEEK;

-- 4D: NET_CF CHECK Ã¢Â€Â” NET_BGT_CF_STK_Q = MAX(OP_STK - Shrinkage, 0)
SELECT TOP 10
    'NETCF_CHECK' AS Check_Type,
    ST_CD, MAJ_CAT, FY_WEEK,
    BGT_TTL_CF_OP_STK_Q AS OP_STK,
    NT_ACT_Q AS Shrinkage,
    NET_BGT_CF_STK_Q AS Actual_NetCF,
    CASE WHEN BGT_TTL_CF_OP_STK_Q - NT_ACT_Q > 0
         THEN BGT_TTL_CF_OP_STK_Q - NT_ACT_Q ELSE 0 END AS Expected_NetCF,
    CASE WHEN NET_BGT_CF_STK_Q =
        CASE WHEN BGT_TTL_CF_OP_STK_Q - NT_ACT_Q > 0
             THEN BGT_TTL_CF_OP_STK_Q - NT_ACT_Q ELSE 0 END
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.TRF_IN_PLAN
WHERE ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
  AND BGT_TTL_CF_OP_STK_Q > 0
ORDER BY FY_WEEK;

-- 4E: EXCESS/SHORT CHECK
SELECT TOP 10
    'EXCESS_SHORT_CHECK' AS Check_Type,
    ST_CD, MAJ_CAT, FY_WEEK,
    NET_ST_CL_STK_Q AS Closing,
    BGT_ST_CL_MBQ AS MBQ,
    ST_CL_EXCESS_Q AS Actual_Excess,
    CASE WHEN NET_ST_CL_STK_Q - BGT_ST_CL_MBQ > 0
         THEN NET_ST_CL_STK_Q - BGT_ST_CL_MBQ ELSE 0 END AS Expected_Excess,
    ST_CL_SHORT_Q AS Actual_Short,
    CASE WHEN BGT_ST_CL_MBQ - NET_ST_CL_STK_Q > 0
         THEN BGT_ST_CL_MBQ - NET_ST_CL_STK_Q ELSE 0 END AS Expected_Short,
    CASE WHEN ST_CL_EXCESS_Q = CASE WHEN NET_ST_CL_STK_Q - BGT_ST_CL_MBQ > 0
              THEN NET_ST_CL_STK_Q - BGT_ST_CL_MBQ ELSE 0 END
         AND ST_CL_SHORT_Q = CASE WHEN BGT_ST_CL_MBQ - NET_ST_CL_STK_Q > 0
              THEN BGT_ST_CL_MBQ - NET_ST_CL_STK_Q ELSE 0 END
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.TRF_IN_PLAN
WHERE ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
  AND (NET_ST_CL_STK_Q > 0 OR BGT_ST_CL_MBQ > 0)
ORDER BY FY_WEEK;

-- 4F: NULL CHECK Ã¢Â€Â” no NULLs allowed (all should be 0 or 'NA')
SELECT 'NULL_CHECK_TRF' AS Check_Type,
    SUM(CASE WHEN SSN IS NULL THEN 1 ELSE 0 END) AS SSN_Nulls,
    SUM(CASE WHEN SEG IS NULL THEN 1 ELSE 0 END) AS SEG_Nulls,
    SUM(CASE WHEN DIV IS NULL THEN 1 ELSE 0 END) AS DIV_Nulls,
    SUM(CASE WHEN TRF_IN_STK_Q IS NULL THEN 1 ELSE 0 END) AS TRFIN_Nulls,
    SUM(CASE WHEN BGT_TTL_CF_CL_STK_Q IS NULL THEN 1 ELSE 0 END) AS CLSTK_Nulls,
    SUM(CASE WHEN NET_BGT_CF_STK_Q IS NULL THEN 1 ELSE 0 END) AS NETCF_Nulls
FROM dbo.TRF_IN_PLAN
WHERE ST_CD IN (SELECT TOP 3 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD]);

PRINT 'TRF_IN validation complete. Check result grids above.';
PRINT '  - CHAIN_MISMATCH: 0 rows = PASS (closing stock flows to next week opening)';
PRINT '  - SHRINKAGE_CHECK: All PASS = correct 8% shrinkage with SSN factor';
PRINT '  - NETCF_CHECK: All PASS = MAX(OP - Shrinkage, 0)';
PRINT '  - EXCESS_SHORT_CHECK: All PASS = correct excess/short vs MBQ';
PRINT '  - NULL_CHECK: All 0s = no NULLs in data';
GO


-- =====================================================
-- SECTION 5: RUN PURCHASE PLAN FOR TEST RDCs
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 5: RUN PURCHASE PLAN (test RDCs)';
PRINT '================================================================';

DECLARE @StartWk INT, @EndWk INT;
SELECT @StartWk = MIN(WEEK_ID), @EndWk = MAX(WEEK_ID) FROM dbo.WEEK_CALENDAR;

-- Get distinct RDCs from our 3 test stores
DECLARE @RDCs TABLE (RDC_CD VARCHAR(20));
INSERT INTO @RDCs
SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER
WHERE [ST CD] IN (
    SELECT TOP 3 [ST CD] FROM (
        SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
        FROM dbo.MASTER_ST_MASTER
    ) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total
);

SELECT 'TEST_RDCs' AS Info, RDC_CD FROM @RDCs;

-- Run PP for each test RDC
DECLARE @rdc VARCHAR(20);
DECLARE rdc_cursor CURSOR LOCAL FAST_FORWARD FOR SELECT RDC_CD FROM @RDCs;
OPEN rdc_cursor;
FETCH NEXT FROM rdc_cursor INTO @rdc;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT '';
    PRINT '--- Running PURCHASE PLAN for RDC: ' + @rdc + ' ---';

    EXEC dbo.SP_GENERATE_PURCHASE_PLAN
        @StartWeekID = @StartWk, @EndWeekID = @EndWk,
        @RdcCode = @rdc, @Debug = 1;

    FETCH NEXT FROM rdc_cursor INTO @rdc;
END

CLOSE rdc_cursor;
DEALLOCATE rdc_cursor;
GO


-- =====================================================
-- SECTION 6: VALIDATE PURCHASE PLAN ALGORITHM
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 6: VALIDATE PURCHASE PLAN ALGORITHM';
PRINT '================================================================';

-- Get test RDCs
DECLARE @TestRDC VARCHAR(20);
SELECT TOP 1 @TestRDC = RDC_CD FROM dbo.PURCHASE_PLAN;

-- 6A: Row counts
SELECT 'PP_ROW_COUNT' AS Check_Type,
    RDC_CD, COUNT(*) AS Rows,
    COUNT(DISTINCT MAJ_CAT) AS MAJ_CATs,
    COUNT(DISTINCT FY_WEEK) AS Weeks
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC
GROUP BY RDC_CD;

-- 6B: DC CHAINING CHECK Ã¢Â€Â” Week N BGT_DC_OP_STK_Q = Week N-1 BGT_DC_CL_STK_Q
SELECT 'PP_DC_CHAIN_MISMATCH' AS Check_Type,
    curr.RDC_CD, curr.MAJ_CAT,
    prev.FY_WEEK AS PrevWeek, curr.FY_WEEK AS CurrWeek,
    prev.BGT_DC_CL_STK_Q AS Prev_DC_Close,
    curr.BGT_DC_OP_STK_Q AS Curr_DC_Open,
    prev.BGT_DC_CL_STK_Q - curr.BGT_DC_OP_STK_Q AS Difference
FROM dbo.PURCHASE_PLAN curr
INNER JOIN dbo.PURCHASE_PLAN prev
    ON prev.RDC_CD = curr.RDC_CD
    AND prev.MAJ_CAT = curr.MAJ_CAT
    AND prev.FY_WEEK = curr.FY_WEEK - 1
    AND prev.FY_YEAR = curr.FY_YEAR
WHERE curr.RDC_CD = @TestRDC
  AND prev.BGT_DC_CL_STK_Q != curr.BGT_DC_OP_STK_Q;

-- 6C: BGT_DC_CL_MBQ CHECK Ã¢Â€Â” MIN(CW1_TRF_OUT, BGT_DC_MBQ_SALE)
SELECT TOP 10
    'PP_DC_CL_MBQ_CHECK' AS Check_Type,
    RDC_CD, MAJ_CAT, FY_WEEK,
    CW1_TRF_OUT_Q, BGT_DC_MBQ_SALE,
    BGT_DC_CL_MBQ AS Actual,
    CASE WHEN CW1_TRF_OUT_Q < BGT_DC_MBQ_SALE THEN CW1_TRF_OUT_Q ELSE BGT_DC_MBQ_SALE END AS Expected,
    CASE WHEN BGT_DC_CL_MBQ = CASE WHEN CW1_TRF_OUT_Q < BGT_DC_MBQ_SALE
         THEN CW1_TRF_OUT_Q ELSE BGT_DC_MBQ_SALE END
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC
  AND (CW1_TRF_OUT_Q > 0 OR BGT_DC_MBQ_SALE > 0)
ORDER BY FY_WEEK;

-- 6D: PP_NET_BGT_CF_STK_Q CHECK = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q
SELECT TOP 10
    'PP_NETCF_CHECK' AS Check_Type,
    RDC_CD, MAJ_CAT, FY_WEEK,
    BGT_CF_STK_Q, GRT_CONS_Q, DEL_PEND_Q,
    PP_NET_BGT_CF_STK_Q AS Actual,
    BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q AS Expected,
    CASE WHEN PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC
ORDER BY FY_WEEK;

-- 6E: BGT_PUR_Q_INIT CHECK = MAX(TTL_TRF_OUT + DC_CL_MBQ - NETCF - DEL_PEND, 0)
SELECT TOP 10
    'PP_PURCHASE_CHECK' AS Check_Type,
    RDC_CD, MAJ_CAT, FY_WEEK,
    TTL_TRF_OUT_Q, BGT_DC_CL_MBQ, PP_NET_BGT_CF_STK_Q, DEL_PEND_Q,
    BGT_PUR_Q_INIT AS Actual,
    CASE WHEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q > 0
         THEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q
         ELSE 0 END AS Expected,
    CASE WHEN BGT_PUR_Q_INIT =
        CASE WHEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q > 0
             THEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q
             ELSE 0 END
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC
ORDER BY FY_WEEK;

-- 6F: DC EXCESS/SHORT CHECK
SELECT TOP 10
    'PP_DC_EXCESS_SHORT' AS Check_Type,
    RDC_CD, MAJ_CAT, FY_WEEK,
    BGT_DC_CL_STK_Q, BGT_DC_CL_MBQ,
    DC_STK_EXCESS_Q AS Actual_Excess,
    CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
         THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END AS Expected_Excess,
    DC_STK_SHORT_Q AS Actual_Short,
    CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
         THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END AS Expected_Short,
    CASE WHEN DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
              THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END
         AND DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
              THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END
        THEN 'PASS' ELSE 'FAIL' END AS Result
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC
  AND (BGT_DC_CL_STK_Q > 0 OR BGT_DC_CL_MBQ > 0)
ORDER BY FY_WEEK;

-- 6G: NULL CHECK for PP
SELECT 'NULL_CHECK_PP' AS Check_Type,
    SUM(CASE WHEN SSN IS NULL THEN 1 ELSE 0 END) AS SSN_Nulls,
    SUM(CASE WHEN SEG IS NULL THEN 1 ELSE 0 END) AS SEG_Nulls,
    SUM(CASE WHEN DIV IS NULL THEN 1 ELSE 0 END) AS DIV_Nulls,
    SUM(CASE WHEN BGT_PUR_Q_INIT IS NULL THEN 1 ELSE 0 END) AS Purchase_Nulls,
    SUM(CASE WHEN BGT_DC_CL_STK_Q IS NULL THEN 1 ELSE 0 END) AS DC_Close_Nulls,
    SUM(CASE WHEN PP_NET_BGT_CF_STK_Q IS NULL THEN 1 ELSE 0 END) AS NETCF_Nulls
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC;

PRINT 'PP validation complete. Check result grids above.';
PRINT '  - PP_DC_CHAIN_MISMATCH: 0 rows = PASS (DC closing flows to next week DC opening)';
PRINT '  - PP_DC_CL_MBQ_CHECK: All PASS = MIN(CW1_TRF_OUT, DC_MBQ_SALE)';
PRINT '  - PP_NETCF_CHECK: All PASS = CF + GRT_CONS + DEL_PEND';
PRINT '  - PP_PURCHASE_CHECK: All PASS = MAX(TRF_OUT + DC_MBQ - NETCF - DELPEND, 0)';
PRINT '  - PP_DC_EXCESS_SHORT: All PASS = correct DC excess/short';
GO


-- =====================================================
-- SECTION 7: BROADER_MENU INTEGRATION CHECK
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 7: BROADER_MENU INTEGRATION CHECK';
PRINT '================================================================';

-- 7A: TRF_IN Ã¢Â€Â” SSN should come from Broader_Menu, not from CASE WHEN FY_WEEK
SELECT TOP 15
    'TRF_BROADER_CHECK' AS Check_Type,
    T.ST_CD, T.MAJ_CAT, T.FY_WEEK,
    T.SSN AS TRF_SSN,
    T.SEG AS TRF_SEG,
    T.DIV AS TRF_DIV,
    T.SUB_DIV AS TRF_SUB_DIV,
    T.MAJ_CAT_NM AS TRF_MAJ_CAT_NM,
    BM.SSN AS BroaderMenu_SSN,
    BM.SEG AS BroaderMenu_SEG,
    CASE WHEN ISNULL(T.SSN, 'NA') = ISNULL(BM.SSN, 'NA') THEN 'PASS' ELSE 'FAIL' END AS SSN_Match,
    CASE WHEN ISNULL(T.SEG, 'NA') = ISNULL(BM.SEG, 'NA') THEN 'PASS' ELSE 'FAIL' END AS SEG_Match
FROM dbo.TRF_IN_PLAN T
LEFT JOIN dbo.Broader_Menu BM ON BM.MAJ_CAT_NM = T.MAJ_CAT
WHERE T.ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
  AND T.FY_WEEK IN (1, 13, 26, 40, 52)
ORDER BY T.MAJ_CAT, T.FY_WEEK;

-- 7B: KEY CHECK Ã¢Â€Â” SSN should be SAME for all weeks of same MAJ_CAT
-- (old bug: SSN changed by week; now it's category-based from Broader_Menu)
SELECT 'SSN_CONSISTENCY' AS Check_Type,
    ST_CD, MAJ_CAT,
    COUNT(DISTINCT SSN) AS Distinct_SSN_Values,
    MIN(SSN) AS SSN_Value,
    CASE WHEN COUNT(DISTINCT SSN) = 1 THEN 'PASS (same SSN all weeks)'
         ELSE 'FAIL (SSN varies by week - old bug!)' END AS Result
FROM dbo.TRF_IN_PLAN
WHERE ST_CD IN (SELECT TOP 1 [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD])
GROUP BY ST_CD, MAJ_CAT
HAVING COUNT(DISTINCT SSN) > 1;
-- 0 rows = PASS (all MAJ_CATs have consistent SSN across weeks)

-- 7C: PP Broader_Menu check
SELECT TOP 10
    'PP_BROADER_CHECK' AS Check_Type,
    P.RDC_CD, P.MAJ_CAT, P.FY_WEEK,
    P.SSN, P.SEG, P.DIV, P.SUB_DIV, P.MAJ_CAT_NM,
    BM.SSN AS BM_SSN, BM.SEG AS BM_SEG,
    CASE WHEN ISNULL(P.SSN, 'NA') = ISNULL(BM.SSN, 'NA') THEN 'PASS' ELSE 'FAIL' END AS SSN_Match
FROM dbo.PURCHASE_PLAN P
LEFT JOIN dbo.Broader_Menu BM ON BM.MAJ_CAT_NM = P.MAJ_CAT
WHERE P.FY_WEEK = 1
ORDER BY P.RDC_CD, P.MAJ_CAT;

PRINT 'Broader_Menu validation complete.';
PRINT '  - SSN_CONSISTENCY: 0 rows = PASS (SSN is category-based, not week-based)';
PRINT '  - SSN_Match / SEG_Match: All PASS = values from Broader_Menu table';
GO


-- =====================================================
-- SECTION 8: PIVOT OUTPUT Ã¢Â€Â” TRANSFER IN (all weeks)
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 8: TRANSFER IN PIVOT OUTPUT';
PRINT '================================================================';

-- Show pivot for test stores Ã¢Â€Â” all 14 metrics ÃƒÂ— 52 weeks
DECLARE @TestStore1 VARCHAR(20);
SELECT TOP 1 @TestStore1 = [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD];

-- Full pivot from the view (for test stores only)
SELECT *
FROM dbo.VW_TRF_IN_PIVOT
WHERE [ST-CD] IN (
    SELECT TOP 3 [ST CD] FROM (
        SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
        FROM dbo.MASTER_ST_MASTER
    ) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total
)
ORDER BY [ST-CD], [MAJ-CAT];

-- Also show a summary: first 5 weeks detail for 1 store + 1 category
SELECT TOP 1 @TestStore1 = [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD];

SELECT 'TRF_DETAIL_WEEKS_1_TO_5' AS View_Type,
    ST_CD, MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM,
    FY_WEEK,
    BGT_DISP_CL_Q AS [DISP],
    BGT_ST_CL_MBQ AS [MBQ],
    BGT_TTL_CF_OP_STK_Q AS [OP_STK],
    NT_ACT_Q AS [SHRINKAGE],
    NET_BGT_CF_STK_Q AS [NET_CF],
    CM_BGT_SALE_Q AS [SALE],
    TRF_IN_STK_Q AS [TRF_IN],
    BGT_TTL_CF_CL_STK_Q AS [CL_STK],
    NET_ST_CL_STK_Q AS [NET_CL],
    ST_CL_EXCESS_Q AS [EXCESS],
    ST_CL_SHORT_Q AS [SHORT]
FROM dbo.TRF_IN_PLAN
WHERE ST_CD = @TestStore1
  AND MAJ_CAT = (SELECT TOP 1 MAJ_CAT FROM dbo.TRF_IN_PLAN
                  WHERE ST_CD = @TestStore1 AND TRF_IN_STK_Q > 0
                  ORDER BY MAJ_CAT)
  AND FY_WEEK BETWEEN 1 AND 5
ORDER BY FY_WEEK;

PRINT 'Transfer In pivot output generated.';
PRINT 'Check the Results grid for full 52-week pivot data.';
GO


-- =====================================================
-- SECTION 9: PIVOT OUTPUT Ã¢Â€Â” PURCHASE PLAN (all weeks)
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 9: PURCHASE PLAN PIVOT OUTPUT';
PRINT '================================================================';

-- Stock pivot
SELECT *
FROM dbo.VW_PP_PIVOT_STOCK
WHERE [RDC-CD] IN (
    SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER
    WHERE [ST CD] IN (
        SELECT TOP 3 [ST CD] FROM (
            SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
            FROM dbo.MASTER_ST_MASTER
        ) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total
    )
)
ORDER BY [RDC-CD], [MAJ-CAT];

-- GRT & Transfer pivot
SELECT *
FROM dbo.VW_PP_PIVOT_GRT_TRF
WHERE [RDC-CD] IN (
    SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER
    WHERE [ST CD] IN (
        SELECT TOP 3 [ST CD] FROM (
            SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
            FROM dbo.MASTER_ST_MASTER
        ) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total
    )
)
ORDER BY [RDC-CD], [MAJ-CAT];

-- Purchase pivot
SELECT *
FROM dbo.VW_PP_PIVOT_PURCHASE
WHERE [RDC-CD] IN (
    SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER
    WHERE [ST CD] IN (
        SELECT TOP 3 [ST CD] FROM (
            SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
            FROM dbo.MASTER_ST_MASTER
        ) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total
    )
)
ORDER BY [RDC-CD], [MAJ-CAT];

-- Detail view: first 5 weeks of PP for 1 RDC + 1 category
DECLARE @TestRDC2 VARCHAR(20);
SELECT TOP 1 @TestRDC2 = RDC_CD FROM dbo.PURCHASE_PLAN;

SELECT 'PP_DETAIL_WEEKS_1_TO_5' AS View_Type,
    RDC_CD, MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM,
    FY_WEEK,
    BGT_DISP_CL_Q AS [DISP],
    CW_BGT_SALE_Q AS [SALE_CW],
    CW1_BGT_SALE_Q AS [SALE_CW1],
    BGT_DC_OP_STK_Q AS [DC_OP],
    BGT_CF_STK_Q AS [CF_STK],
    GRT_CONS_Q AS [GRT_CONS],
    PP_NET_BGT_CF_STK_Q AS [NET_CF],
    TTL_TRF_OUT_Q AS [TRF_OUT],
    CW_TRF_OUT_Q AS [TRF_CW],
    CW1_TRF_OUT_Q AS [TRF_CW1],
    BGT_DC_CL_MBQ AS [DC_MBQ],
    BGT_DC_CL_STK_Q AS [DC_CL],
    BGT_PUR_Q_INIT AS [PURCHASE],
    DC_STK_EXCESS_Q AS [DC_EXCESS],
    DC_STK_SHORT_Q AS [DC_SHORT]
FROM dbo.PURCHASE_PLAN
WHERE RDC_CD = @TestRDC2
  AND MAJ_CAT = (SELECT TOP 1 MAJ_CAT FROM dbo.PURCHASE_PLAN
                  WHERE RDC_CD = @TestRDC2 AND BGT_PUR_Q_INIT > 0
                  ORDER BY MAJ_CAT)
  AND FY_WEEK BETWEEN 1 AND 5
ORDER BY FY_WEEK;

PRINT 'Purchase Plan pivot output generated.';
PRINT 'Check the Results grids for all 3 pivot views (Stock, GRT_TRF, Purchase).';
GO


-- =====================================================
-- SECTION 10: SUMMARY
-- =====================================================
PRINT '';
PRINT '================================================================';
PRINT '  SECTION 10: FINAL SUMMARY';
PRINT '================================================================';

SELECT 'FINAL_SUMMARY' AS Report,
    (SELECT COUNT(*) FROM dbo.TRF_IN_PLAN
     WHERE ST_CD IN (SELECT TOP 3 [ST CD] FROM (
         SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
         FROM dbo.MASTER_ST_MASTER) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total)
    ) AS TRF_IN_Rows,
    (SELECT COUNT(*) FROM dbo.PURCHASE_PLAN
     WHERE RDC_CD IN (SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER
         WHERE [ST CD] IN (SELECT TOP 3 [ST CD] FROM (
             SELECT [ST CD], ROW_NUMBER() OVER (ORDER BY [ST CD]) AS RN, COUNT(*) OVER() AS Total
             FROM dbo.MASTER_ST_MASTER) x WHERE RN = 1 OR RN = Total / 2 OR RN = Total))
    ) AS PP_Rows,
    (SELECT COUNT(DISTINCT ST_CD) FROM dbo.TRF_IN_PLAN) AS TRF_Distinct_Stores,
    (SELECT COUNT(DISTINCT RDC_CD) FROM dbo.PURCHASE_PLAN) AS PP_Distinct_RDCs;

PRINT '';
PRINT 'TEST COMPLETE!';
PRINT '';
PRINT 'What to look for:';
PRINT '  1. All pre-check objects show EXISTS';
PRINT '  2. All SP runs complete without errors';
PRINT '  3. CHAIN_MISMATCH queries return 0 rows (chaining correct)';
PRINT '  4. All validation checks show PASS';
PRINT '  5. SSN_CONSISTENCY returns 0 rows (SSN from Broader_Menu, not week-based)';
PRINT '  6. NULL_CHECK shows all 0s';
PRINT '  7. Pivot outputs show all 52 weeks of data';
PRINT '';
PRINT 'If all checks pass, you can run for ALL stores:';
PRINT '  EXEC dbo.SP_RUN_ALL_PLANS @StartWeekID = 1, @EndWeekID = 52, @Debug = 1;';
PRINT '================================================================';
GO
