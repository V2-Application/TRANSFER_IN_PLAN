-- ============================================================
-- ARS ALLOCATION — SP 3: FINALIZE
-- Tags NEW-L from remaining MSA, writes to ARS_ALLOCATION_OUTPUT
-- Runs after SP_ARS_ALLOCATE (#Prepared must exist)
-- Database: datav2
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.SP_ARS_FINALIZE', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_ARS_FINALIZE;
GO

CREATE PROCEDURE dbo.SP_ARS_FINALIZE
    @RunId VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 1: Tag NEW-L articles
    --   After ARS allocation, remaining MJ-REQ can be filled by
    --   new articles from MSA (ST-MJ-REM-REQ for NEW-L tagging)
    -- ═══════════════════════════════════════════════════════════

    -- Calculate remaining MJ-REQ after allocation
    ;WITH MjAllocated AS (
        SELECT ST, MJ, SUM(ART_ALC_Q) AS TTL_ALC
        FROM #Prepared
        WHERE ART_ALC_Q > 0
        GROUP BY ST, MJ
    )
    UPDATE p SET p.ART_CLASS = 'NEW-L'
    FROM #Prepared p
    INNER JOIN MjAllocated ma ON ma.ST = p.ST AND ma.MJ = p.MJ
    WHERE p.ART_CLASS = 'MIX-ART'
      AND p.REM_MSA > 0
      AND (p.ST_MJ_MBQ - ma.TTL_ALC) > 0;  -- Still has MJ demand

    -- ═══════════════════════════════════════════════════════════
    -- STEP 2: Delete old run + INSERT into ARS_ALLOCATION_OUTPUT
    -- ═══════════════════════════════════════════════════════════
    DELETE FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId;

    INSERT INTO dbo.ARS_ALLOCATION_OUTPUT (
        RUN_ID, ST, ST_NM, TAGGED_RDC, HUB_CD,
        MJ, [GEN-ART], CLR, ART_CLASS,
        TTL_ST_STK_Q, STK_0001, STK_0002, STK_0004, STK_0006,
        STK_HUB_INTRA, STK_HUB_PRD, STK_INTRA, STK_PRD, MSA_QTY,
        TTL_ALC_DAYS, HOLD_DAYS, CM_PD_SALE_Q, NM_PD_SALE_Q, ART_AUTO_SALE_PD,
        ACC_DENSITY, ST_MJ_DISP_Q, ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
        ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ,
        ART_ALC_Q, ART_HOLD_Q, REM_MSA,
        CREATED_DT
    )
    SELECT
        RUN_ID, ST, ST_NM, TAGGED_RDC, HUB_CD,
        MJ, [GEN-ART], '', ART_CLASS,
        TTL_ST_STK_Q, STK_0001, STK_0002, STK_0004, STK_0006,
        STK_HUB_INTRA, STK_HUB_PRD, STK_INTRA, STK_PRD, MSA_QTY,
        TTL_ALC_DAYS, HOLD_DAYS, CM_PD_SALE_Q, NM_PD_SALE_Q, ART_AUTO_SALE_PD,
        ACC_DENSITY, ST_MJ_DISP_Q, ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
        ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ,
        ART_ALC_Q, ART_HOLD_Q, REM_MSA,
        GETDATE()
    FROM #Prepared;

    DECLARE @RowsInserted INT = @@ROWCOUNT;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 3: Update ARS_RUN_LOG
    -- ═══════════════════════════════════════════════════════════
    UPDATE dbo.ARS_RUN_LOG SET
        COMPLETED_DT = GETDATE(),
        STATUS = 'COMPLETED',
        TOTAL_STORES = (SELECT COUNT(DISTINCT ST) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId),
        TOTAL_ARTICLES = (SELECT COUNT(DISTINCT [GEN-ART]) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId),
        TOTAL_ALLOCATED = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_ALC_Q > 0),
        TOTAL_HELD = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_HOLD_Q > 0),
        L_ART_COUNT = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_CLASS = 'L-ART'),
        MIX_ART_COUNT = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_CLASS = 'MIX-ART'),
        OLD_ART_COUNT = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_CLASS = 'OLD-ART'),
        NEW_L_COUNT = (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_CLASS = 'NEW-L')
    WHERE RUN_ID = @RunId;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 4: Cleanup + Output summary
    -- ═══════════════════════════════════════════════════════════
    DROP TABLE IF EXISTS #Prepared, #StockPivot, #MSA, #StoreMaster, #Display, #MjSale, #ArtSale, #MBQ;

    SELECT @RowsInserted AS RowsInserted,
           (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_ALC_Q > 0) AS Allocated,
           (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_HOLD_Q > 0) AS Held,
           (SELECT COUNT(*) FROM dbo.ARS_ALLOCATION_OUTPUT WHERE RUN_ID = @RunId AND ART_CLASS = 'NEW-L') AS NewL;
END;
GO

PRINT '>> SP_ARS_FINALIZE created on datav2.';
GO
