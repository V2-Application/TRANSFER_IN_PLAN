-- =====================================================
-- FILE: 16_PATCH_BROADER_MENU_COLUMNS.sql
-- PURPOSE: Add SEG, DIV, SUB_DIV, MAJ_CAT_NM from Broader_Menu
--          SSN now comes from Broader_Menu (per category)
--          instead of CASE WHEN FY_WEEK (per week)
-- DATABASE: [planning]
-- RUN THIS BEFORE re-running the SPs
-- =====================================================

USE [planning];
GO

-- =====================================================
-- STEP 1: ADD COLUMNS TO TRF_IN_PLAN
-- =====================================================
PRINT '--- STEP 1: Adding Broader_Menu columns to TRF_IN_PLAN ---';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TRF_IN_PLAN' AND COLUMN_NAME = 'SEG')
BEGIN
    ALTER TABLE dbo.TRF_IN_PLAN ADD [SEG] VARCHAR(100) NULL;
    PRINT '  Added SEG';
END
ELSE PRINT '  SEG already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TRF_IN_PLAN' AND COLUMN_NAME = 'DIV')
BEGIN
    ALTER TABLE dbo.TRF_IN_PLAN ADD [DIV] VARCHAR(100) NULL;
    PRINT '  Added DIV';
END
ELSE PRINT '  DIV already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TRF_IN_PLAN' AND COLUMN_NAME = 'SUB_DIV')
BEGIN
    ALTER TABLE dbo.TRF_IN_PLAN ADD [SUB_DIV] VARCHAR(100) NULL;
    PRINT '  Added SUB_DIV';
END
ELSE PRINT '  SUB_DIV already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TRF_IN_PLAN' AND COLUMN_NAME = 'MAJ_CAT_NM')
BEGIN
    ALTER TABLE dbo.TRF_IN_PLAN ADD [MAJ_CAT_NM] VARCHAR(100) NULL;
    PRINT '  Added MAJ_CAT_NM';
END
ELSE PRINT '  MAJ_CAT_NM already exists';

GO

-- =====================================================
-- STEP 2: ADD COLUMNS TO PURCHASE_PLAN
-- =====================================================
PRINT '';
PRINT '--- STEP 2: Adding Broader_Menu columns to PURCHASE_PLAN ---';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'SEG')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [SEG] VARCHAR(100) NULL;
    PRINT '  Added SEG';
END
ELSE PRINT '  SEG already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'DIV')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [DIV] VARCHAR(100) NULL;
    PRINT '  Added DIV';
END
ELSE PRINT '  DIV already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'SUB_DIV')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [SUB_DIV] VARCHAR(100) NULL;
    PRINT '  Added SUB_DIV';
END
ELSE PRINT '  SUB_DIV already exists';

IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'PURCHASE_PLAN' AND COLUMN_NAME = 'MAJ_CAT_NM')
BEGIN
    ALTER TABLE dbo.PURCHASE_PLAN ADD [MAJ_CAT_NM] VARCHAR(100) NULL;
    PRINT '  Added MAJ_CAT_NM';
END
ELSE PRINT '  MAJ_CAT_NM already exists';

GO

-- =====================================================
-- STEP 3: VERIFY Broader_Menu table exists and has data
-- =====================================================
PRINT '';
PRINT '--- STEP 3: Verify Broader_Menu ---';

IF OBJECT_ID('dbo.Broader_Menu', 'U') IS NOT NULL
BEGIN
    PRINT '  Broader_Menu table found.';
    SELECT COUNT(*) AS [Broader_Menu_Rows] FROM dbo.Broader_Menu;
    SELECT TOP 5 SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN FROM dbo.Broader_Menu;
END
ELSE
    PRINT '  WARNING: Broader_Menu table NOT FOUND in planning database!';

GO

PRINT '';
PRINT 'Patch complete. Now re-run:';
PRINT '  1. 03_CREATE_SP_GENERATE_TRF_IN_PLAN.sql';
PRINT '  2. 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql';
PRINT '  3. 07_CREATE_VW_TRF_IN_PIVOT.sql';
PRINT '  4. 12_CREATE_VW_PURCHASE_PLAN_PIVOT.sql';
GO
