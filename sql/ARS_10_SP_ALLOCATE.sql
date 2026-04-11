-- ============================================================
-- ARS ALLOCATION — SP 2: ALLOCATE (Core Iterative Algorithm)
-- Runs after SP_ARS_PREPARE_DATA (#Prepared must exist)
-- Process: OLD-L articles with HOLD-MSA > 0
--   ALC-Q = MIN(REM_MSA, ART-REQ)
--   HOLD-Q = MIN(REM_MSA, HOLD-REQ - ALC-Q)
--   MAX CAP of MJ-MBQ applied
--   MJ-REQ recalculated at each step
-- Database: datav2
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.SP_ARS_ALLOCATE', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_ARS_ALLOCATE;
GO

CREATE PROCEDURE dbo.SP_ARS_ALLOCATE
    @RunId VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 1: Build working allocation table from #Prepared
    --   Filter: OLD-ART or L-ART with MSA > 0
    --   Ordered by store, then ART_REQ DESC (highest need first)
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#AlcWork') IS NOT NULL DROP TABLE #AlcWork;

    SELECT
        ROW_NUMBER() OVER (ORDER BY ST, ST_ART_REQ DESC) AS SEQ,
        ST, ST_NM, TAGGED_RDC, HUB_CD, MJ, [GEN-ART],
        TTL_ST_STK_Q, MSA_QTY,
        ART_CLASS,
        TTL_ALC_DAYS, HOLD_DAYS,
        CM_PD_SALE_Q, NM_PD_SALE_Q, ART_AUTO_SALE_PD,
        ACC_DENSITY, ST_MJ_DISP_Q,
        ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
        ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ,
        CAST(0 AS DECIMAL(18,4)) AS ART_ALC_Q,
        CAST(0 AS DECIMAL(18,4)) AS ART_HOLD_Q,
        MSA_QTY AS REM_MSA
    INTO #AlcWork
    FROM #Prepared
    WHERE ART_CLASS IN ('L-ART', 'OLD-ART')
      AND MSA_QTY > 0
      AND ST_ART_REQ > 0;

    CREATE CLUSTERED INDEX CX ON #AlcWork (SEQ);
    CREATE INDEX IX_ST_MJ ON #AlcWork (ST, MJ);

    DECLARE @TotalRows INT = (SELECT COUNT(*) FROM #AlcWork);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 2: Track MJ-level remaining REQ (for capping)
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#MjRemReq') IS NOT NULL DROP TABLE #MjRemReq;

    SELECT ST, MJ, MAX(ST_MJ_MBQ) AS ST_MJ_MBQ, MAX(ST_MJ_REQ) AS MJ_REM_REQ,
           CAST(0 AS DECIMAL(18,4)) AS MJ_ALC_TOTAL
    INTO #MjRemReq
    FROM #AlcWork
    GROUP BY ST, MJ;

    CREATE UNIQUE CLUSTERED INDEX CX ON #MjRemReq (ST, MJ);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 3: Iterative Allocation (set-based per batch)
    --   Process all rows in one pass:
    --   ALC-Q = MIN(REM_MSA, ART-REQ, MJ_REM_REQ)
    --   HOLD-Q = MIN(REM_MSA - ALC-Q, HOLD_REQ - ALC_Q)
    --   Update MJ_REM_REQ after each article
    -- ═══════════════════════════════════════════════════════════

    -- For 600 stores × articles, a set-based approach per store is faster than row-by-row
    DECLARE @StoreCursor TABLE (Seq INT IDENTITY(1,1), ST VARCHAR(20));
    INSERT INTO @StoreCursor (ST)
    SELECT DISTINCT ST FROM #AlcWork ORDER BY ST;

    DECLARE @StoreCount INT = @@ROWCOUNT;
    DECLARE @si INT = 1;
    DECLARE @currSt VARCHAR(20);

    WHILE @si <= @StoreCount
    BEGIN
        SELECT @currSt = ST FROM @StoreCursor WHERE Seq = @si;

        -- Process articles for this store (ordered by ART_REQ DESC)
        DECLARE @ArtCursor TABLE (Seq INT IDENTITY(1,1), [GEN-ART] VARCHAR(50), MJ VARCHAR(100));
        DELETE FROM @ArtCursor;
        INSERT INTO @ArtCursor ([GEN-ART], MJ)
        SELECT [GEN-ART], MJ FROM #AlcWork
        WHERE ST = @currSt ORDER BY ST_ART_REQ DESC;

        DECLARE @ArtCount INT = @@ROWCOUNT;
        DECLARE @ai INT = 1;
        DECLARE @artCode VARCHAR(50), @artMj VARCHAR(100);

        WHILE @ai <= @ArtCount
        BEGIN
            SELECT @artCode = [GEN-ART], @artMj = MJ FROM @ArtCursor WHERE Seq = @ai;

            -- Get current values
            DECLARE @artReq DECIMAL(18,4), @holdReq DECIMAL(18,4), @remMsa DECIMAL(18,4), @mjRemReq DECIMAL(18,4);

            SELECT @artReq = ST_ART_REQ, @holdReq = ST_ART_HOLD_REQ, @remMsa = REM_MSA
            FROM #AlcWork WHERE ST = @currSt AND [GEN-ART] = @artCode;

            SELECT @mjRemReq = MJ_REM_REQ FROM #MjRemReq WHERE ST = @currSt AND MJ = @artMj;

            -- ALLOCATE: ALC-Q = MIN(REM_MSA, ART-REQ, MJ-REM-REQ)
            DECLARE @alcQ DECIMAL(18,4) = 0;
            SET @alcQ = CASE
                WHEN @remMsa <= 0 OR @artReq <= 0 OR @mjRemReq <= 0 THEN 0
                ELSE (SELECT MIN(v) FROM (VALUES (@remMsa), (@artReq), (@mjRemReq)) AS T(v))
            END;

            -- HOLD: HOLD-Q = MIN(REM_MSA - ALC-Q, HOLD-REQ - ALC-Q)
            DECLARE @holdQ DECIMAL(18,4) = 0;
            IF @alcQ > 0 AND @holdReq > @alcQ AND @remMsa > @alcQ
            BEGIN
                SET @holdQ = CASE
                    WHEN (@remMsa - @alcQ) < (@holdReq - @alcQ) THEN @remMsa - @alcQ
                    ELSE @holdReq - @alcQ
                END;
                IF @holdQ < 0 SET @holdQ = 0;
            END

            -- Update allocation
            UPDATE #AlcWork SET
                ART_ALC_Q = @alcQ,
                ART_HOLD_Q = @holdQ,
                REM_MSA = REM_MSA - @alcQ - @holdQ
            WHERE ST = @currSt AND [GEN-ART] = @artCode;

            -- Update MJ remaining (cap enforcement)
            UPDATE #MjRemReq SET
                MJ_ALC_TOTAL = MJ_ALC_TOTAL + @alcQ,
                MJ_REM_REQ = CASE WHEN MJ_REM_REQ - @alcQ > 0 THEN MJ_REM_REQ - @alcQ ELSE 0 END
            WHERE ST = @currSt AND MJ = @artMj;

            SET @ai = @ai + 1;
        END

        SET @si = @si + 1;
    END

    -- ═══════════════════════════════════════════════════════════
    -- STEP 4: Merge allocation results back to #Prepared
    -- ═══════════════════════════════════════════════════════════
    UPDATE p SET
        p.ART_ALC_Q = ISNULL(w.ART_ALC_Q, 0),
        p.ART_HOLD_Q = ISNULL(w.ART_HOLD_Q, 0),
        p.REM_MSA = ISNULL(w.REM_MSA, p.REM_MSA)
    FROM #Prepared p
    LEFT JOIN #AlcWork w ON w.ST = p.ST AND w.[GEN-ART] = p.[GEN-ART] AND w.MJ = p.MJ;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 5: Output stats
    -- ═══════════════════════════════════════════════════════════
    SELECT
        @TotalRows AS CandidateRows,
        SUM(CASE WHEN ART_ALC_Q > 0 THEN 1 ELSE 0 END) AS AllocatedCount,
        SUM(CASE WHEN ART_HOLD_Q > 0 THEN 1 ELSE 0 END) AS HeldCount,
        SUM(ART_ALC_Q) AS TotalAlcQty,
        SUM(ART_HOLD_Q) AS TotalHoldQty,
        COUNT(DISTINCT ST) AS StoresProcessed
    FROM #AlcWork;

    -- #Prepared remains alive for SP_ARS_FINALIZE
END;
GO

PRINT '>> SP_ARS_ALLOCATE created on datav2.';
GO
