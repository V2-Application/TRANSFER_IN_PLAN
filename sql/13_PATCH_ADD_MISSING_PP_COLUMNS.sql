-- =====================================================
-- FILE: 13_PATCH_ADD_MISSING_PP_COLUMNS.sql
-- PURPOSE: Add 3 missing Transfer-Out columns to PURCHASE_PLAN
--          and populate them from TRF_IN_PLAN data
-- DATABASE: [planning]
--
-- MISSING COLUMNS:
--   CW2_TRF_OUT_Q  (Transfer Out CW+2)
--   CW3_TRF_OUT_Q  (Transfer Out CW+3)
--   CW4_TRF_OUT_Q  (Transfer Out CW+4)
--
-- Also fixes TTL_TRF_OUT_Q to be the real total of all 5 weeks
-- =====================================================

USE [planning];
GO

-- =====================================================
-- STEP 1: ADD MISSING COLUMNS TO PURCHASE_PLAN TABLE
-- =====================================================
PRINT '--- STEP 1: Adding missing columns ---';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'CW2_TRF_OUT_Q')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [CW2_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0;
    PRINT '  Added CW2_TRF_OUT_Q';
END
ELSE
    PRINT '  CW2_TRF_OUT_Q already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'CW3_TRF_OUT_Q')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [CW3_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0;
    PRINT '  Added CW3_TRF_OUT_Q';
END
ELSE
    PRINT '  CW3_TRF_OUT_Q already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'CW4_TRF_OUT_Q')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [CW4_TRF_OUT_Q] DECIMAL(18,4) DEFAULT 0;
    PRINT '  Added CW4_TRF_OUT_Q';
END
ELSE
    PRINT '  CW4_TRF_OUT_Q already exists';

GO

-- =====================================================
-- STEP 2: POPULATE NEW COLUMNS FROM TRF_IN_PLAN
-- Uses pre-aggregated temp table (avoids OUTER APPLY /
-- correlated subquery issues with aggregated outer refs)
-- =====================================================
PRINT '';
PRINT '--- STEP 2: Populating new columns from TRF_IN_PLAN ---';

-- Pre-aggregate TRF_IN_PLAN at RDC ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK level
IF OBJECT_ID('tempdb..#TrfAgg') IS NOT NULL DROP TABLE #TrfAgg;

SELECT
    RDC_CD,
    MAJ_CAT,
    WEEK_ID,
    SUM(TRF_IN_STK_Q) AS TRF_OUT_Q
INTO #TrfAgg
FROM dbo.TRF_IN_PLAN
GROUP BY RDC_CD, MAJ_CAT, WEEK_ID;

CREATE INDEX IX_TrfAgg ON #TrfAgg (RDC_CD, MAJ_CAT, WEEK_ID);

-- Update CW+2 transfer out
UPDATE pp
SET [CW2_TRF_OUT_Q] = ISNULL(ta2.TRF_OUT_Q, 0)
FROM dbo.PURCHASE_PLAN pp
LEFT JOIN #TrfAgg ta2
    ON ta2.RDC_CD  = pp.RDC_CD
    AND ta2.MAJ_CAT = pp.MAJ_CAT
    AND ta2.WEEK_ID = pp.WEEK_ID + 2;

PRINT '  CW2_TRF_OUT_Q updated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

-- Update CW+3 transfer out
UPDATE pp
SET [CW3_TRF_OUT_Q] = ISNULL(ta3.TRF_OUT_Q, 0)
FROM dbo.PURCHASE_PLAN pp
LEFT JOIN #TrfAgg ta3
    ON ta3.RDC_CD  = pp.RDC_CD
    AND ta3.MAJ_CAT = pp.MAJ_CAT
    AND ta3.WEEK_ID = pp.WEEK_ID + 3;

PRINT '  CW3_TRF_OUT_Q updated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

-- Update CW+4 transfer out
UPDATE pp
SET [CW4_TRF_OUT_Q] = ISNULL(ta4.TRF_OUT_Q, 0)
FROM dbo.PURCHASE_PLAN pp
LEFT JOIN #TrfAgg ta4
    ON ta4.RDC_CD  = pp.RDC_CD
    AND ta4.MAJ_CAT = pp.MAJ_CAT
    AND ta4.WEEK_ID = pp.WEEK_ID + 4;

PRINT '  CW4_TRF_OUT_Q updated: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';

DROP TABLE #TrfAgg;
GO

-- =====================================================
-- STEP 3: FIX TTL_TRF_OUT_Q = SUM of all 5 week transfers
-- =====================================================
PRINT '';
PRINT '--- STEP 3: Updating TTL_TRF_OUT_Q to be true total ---';

UPDATE dbo.PURCHASE_PLAN
SET [TTL_TRF_OUT_Q] = ISNULL([CW_TRF_OUT_Q], 0)
                     + ISNULL([CW1_TRF_OUT_Q], 0)
                     + ISNULL([CW2_TRF_OUT_Q], 0)
                     + ISNULL([CW3_TRF_OUT_Q], 0)
                     + ISNULL([CW4_TRF_OUT_Q], 0);

PRINT '  Updated ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows.';
GO

-- =====================================================
-- STEP 4: VERIFY
-- =====================================================
PRINT '';
PRINT '--- STEP 4: Verification ---';

SELECT TOP 5
    RDC_CD, MAJ_CAT, WEEK_ID,
    CW_TRF_OUT_Q   AS [TRF CW],
    CW1_TRF_OUT_Q  AS [TRF CW+1],
    CW2_TRF_OUT_Q  AS [TRF CW+2],
    CW3_TRF_OUT_Q  AS [TRF CW+3],
    CW4_TRF_OUT_Q  AS [TRF CW+4],
    TTL_TRF_OUT_Q  AS [TRF TOTAL]
FROM dbo.PURCHASE_PLAN
WHERE CW_TRF_OUT_Q > 0
ORDER BY RDC_CD, MAJ_CAT, WEEK_ID;

PRINT '';
PRINT 'Patch complete. 3 columns added and populated.';
GO
