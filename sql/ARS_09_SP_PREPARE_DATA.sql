-- ============================================================
-- ARS ALLOCATION — SP 1: PREPARE DATA
-- Reads all masters + stock, computes PD sales, MBQ, classifies
-- Database: datav2
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.SP_ARS_PREPARE_DATA', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_ARS_PREPARE_DATA;
GO

CREATE PROCEDURE dbo.SP_ARS_PREPARE_DATA
    @RunId VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 1: Pivot ET_STOCK_DATA by LGORT → TTL_ST_STK_Q
    --         per ST (WERKS) x Article (MATNR)
    --         TTL = 0001 + 0002 + 0004 + 0006 + HUB_INTRA + HUB_PRD + INTRA + PRD
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#StockPivot') IS NOT NULL DROP TABLE #StockPivot;

    SELECT
        WERKS AS ST,
        MATNR AS [GEN-ART],
        ISNULL(SUM(CASE WHEN LGORT = '0001' THEN LABST END), 0) AS STK_0001,
        ISNULL(SUM(CASE WHEN LGORT = '0002' THEN LABST END), 0) AS STK_0002,
        ISNULL(SUM(CASE WHEN LGORT = '0004' THEN LABST END), 0) AS STK_0004,
        ISNULL(SUM(CASE WHEN LGORT = '0006' THEN LABST END), 0) AS STK_0006,
        ISNULL(SUM(CASE WHEN LGORT = 'HUB_INTRA' THEN LABST END), 0) AS STK_HUB_INTRA,
        ISNULL(SUM(CASE WHEN LGORT = 'HUB_PRD' THEN LABST END), 0) AS STK_HUB_PRD,
        ISNULL(SUM(CASE WHEN LGORT = 'INTRA' THEN LABST END), 0) AS STK_INTRA,
        ISNULL(SUM(CASE WHEN LGORT = 'PRD' THEN LABST END), 0) AS STK_PRD,
        -- TTL_ST_STK_Q = sum of all locations
        ISNULL(SUM(CASE WHEN LGORT IN ('0001','0002','0004','0006','HUB_INTRA','HUB_PRD','INTRA','PRD') THEN LABST END), 0) AS TTL_ST_STK_Q
    INTO #StockPivot
    FROM dbo.ET_STOCK_DATA WITH (NOLOCK)
    WHERE stock_Date = (SELECT MAX(stock_Date) FROM dbo.ET_STOCK_DATA WITH (NOLOCK))
    GROUP BY WERKS, MATNR;

    CREATE CLUSTERED INDEX CX ON #StockPivot (ST, [GEN-ART]);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 2: Load MSA data (latest date, MSA_QTY > 50 = available)
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#MSA') IS NOT NULL DROP TABLE #MSA;

    SELECT
        STORE_CODE AS ST,
        Article_Number AS [GEN-ART],
        SUM(ISNULL(QTY, 0)) AS MSA_QTY
    INTO #MSA
    FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK)
    WHERE MSA_Stock_Date = (SELECT MAX(MSA_Stock_Date) FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK))
    GROUP BY STORE_CODE, Article_Number;

    CREATE CLUSTERED INDEX CX ON #MSA (ST, [GEN-ART]);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 3: Load Store Master with computed TTL_ALC_DAYS
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#StoreMaster') IS NOT NULL DROP TABLE #StoreMaster;

    SELECT
        ST_CD AS ST, ST_NM, TAGGED_RDC, HUB_CD,
        ISNULL(SALE_COVER_DAYS, 0) AS SALE_COVER_DAYS,
        ISNULL(PRD_DAYS, 0) AS PRD_DAYS,
        CASE WHEN TAGGED_RDC = 'DW01'
             THEN ISNULL(DW01_DC_TO_HUB_INTRA, 0) + ISNULL(DW01_HUB_TO_ST_INTRA, 0)
             ELSE ISNULL(DH24_DC_TO_HUB_INTRA, 0) + ISNULL(DH24_HUB_TO_ST_INTRA, 0)
        END AS INTRA_DAYS,
        ISNULL(SALE_COVER_DAYS, 0) + ISNULL(PRD_DAYS, 0) +
        CASE WHEN TAGGED_RDC = 'DW01'
             THEN ISNULL(DW01_DC_TO_HUB_INTRA, 0) + ISNULL(DW01_HUB_TO_ST_INTRA, 0)
             ELSE ISNULL(DH24_DC_TO_HUB_INTRA, 0) + ISNULL(DH24_HUB_TO_ST_INTRA, 0)
        END AS TTL_ALC_DAYS
    INTO #StoreMaster
    FROM dbo.ARS_ST_MASTER WITH (NOLOCK);

    CREATE UNIQUE CLUSTERED INDEX CX ON #StoreMaster (ST);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 4: Load Display Master + Hold Days
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#Display') IS NOT NULL DROP TABLE #Display;

    SELECT
        d.ST, d.MJ,
        ISNULL(d.ST_MJ_DISP_Q, 0) AS ST_MJ_DISP_Q,
        ISNULL(h.HOLD_DAYS, 0) AS HOLD_DAYS
    INTO #Display
    FROM dbo.ARS_ST_MJ_DISPLAY_MASTER d WITH (NOLOCK)
    LEFT JOIN dbo.ARS_HOLD_DAYS_MASTER h WITH (NOLOCK)
        ON h.ST = d.ST AND h.MJ = d.MJ;

    CREATE CLUSTERED INDEX CX ON #Display (ST, MJ);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 5: Calculate PD Sales at MJ level
    --   CM_PD_SALE_Q = CM_AUTO_SALE / CM_REM_DAYS
    --   NM_PD_SALE_Q = NM_AUTO_SALE / NM_DAYS
    --   ST_MJ_ALC_SALE_Q_PD = MIN(ALC+HOLD, CM_REM)*CM_PD
    --                       + (ALC+HOLD - MIN(ALC+HOLD, CM_REM))*NM_PD
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#MjSale') IS NOT NULL DROP TABLE #MjSale;

    SELECT
        s.ST, s.MJ,
        CASE WHEN s.[CM-REM-DAYS] > 0 THEN s.[CM-AUTO-SALE-Q] / s.[CM-REM-DAYS] ELSE 0 END AS CM_PD_SALE_Q,
        CASE WHEN s.[NM-DAYS] > 0 THEN s.[NM-AUTO-SALE-Q] / s.[NM-DAYS] ELSE 0 END AS NM_PD_SALE_Q,
        s.[CM-REM-DAYS],
        -- Full PD sale considering ALC_DAYS + HOLD_DAYS crossover
        CASE WHEN s.[CM-REM-DAYS] > 0 THEN
            -- Part covered by CM
            CASE WHEN (sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) <= s.[CM-REM-DAYS]
                 THEN (sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) * (s.[CM-AUTO-SALE-Q] / s.[CM-REM-DAYS])
                 ELSE s.[CM-REM-DAYS] * (s.[CM-AUTO-SALE-Q] / s.[CM-REM-DAYS])
                    + ((sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) - s.[CM-REM-DAYS])
                    * CASE WHEN s.[NM-DAYS] > 0 THEN s.[NM-AUTO-SALE-Q] / s.[NM-DAYS] ELSE 0 END
            END
        ELSE 0 END AS ST_MJ_ALC_SALE_Q_PD
    INTO #MjSale
    FROM dbo.ARS_ST_MJ_AUTO_SALE s WITH (NOLOCK)
    INNER JOIN #StoreMaster sm ON sm.ST = s.ST
    LEFT JOIN #Display dp ON dp.ST = s.ST AND dp.MJ = s.MJ;

    CREATE CLUSTERED INDEX CX ON #MjSale (ST, MJ);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 6: Calculate PD Sales at Article level
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#ArtSale') IS NOT NULL DROP TABLE #ArtSale;

    SELECT
        a.ST, a.[GEN-ART], a.CLR,
        CASE WHEN a.[CM-REM-DAYS] > 0 THEN a.[CM-AUTO-SALE-Q] / a.[CM-REM-DAYS] ELSE 0 END AS ART_CM_PD,
        CASE WHEN a.[NM-DAYS] > 0 THEN a.[NM-AUTO-SALE-Q] / a.[NM-DAYS] ELSE 0 END AS ART_NM_PD,
        -- Full article PD sale
        CASE WHEN a.[CM-REM-DAYS] > 0 THEN
            CASE WHEN (sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) <= a.[CM-REM-DAYS]
                 THEN (sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) * (a.[CM-AUTO-SALE-Q] / a.[CM-REM-DAYS])
                 ELSE a.[CM-REM-DAYS] * (a.[CM-AUTO-SALE-Q] / a.[CM-REM-DAYS])
                    + ((sm.TTL_ALC_DAYS + ISNULL(dp.HOLD_DAYS, 0)) - a.[CM-REM-DAYS])
                    * CASE WHEN a.[NM-DAYS] > 0 THEN a.[NM-AUTO-SALE-Q] / a.[NM-DAYS] ELSE 0 END
            END
        ELSE 0 END AS ART_AUTO_SALE_PD
    INTO #ArtSale
    FROM dbo.ARS_ST_ART_AUTO_SALE a WITH (NOLOCK)
    INNER JOIN #StoreMaster sm ON sm.ST = a.ST
    -- Need MJ for this article to get hold days — join via stock or display
    OUTER APPLY (
        SELECT TOP 1 dp2.HOLD_DAYS FROM #Display dp2 WHERE dp2.ST = a.ST
    ) dp;

    CREATE CLUSTERED INDEX CX ON #ArtSale (ST, [GEN-ART], CLR);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 7: Calculate MBQ at all levels
    -- ═══════════════════════════════════════════════════════════
    -- MJ-MBQ = DISP + SALE_PD * TTL_ALC_DAYS
    -- ART_MBQ = ACC_DENSITY + ART_AUTO_SALE_PD * TTL_ALC_DAYS
    -- ART_HOLD_MBQ = ACC_DENSITY + ART_SALE_PD * (TTL_ALC_DAYS + HOLD_DAYS)

    IF OBJECT_ID('tempdb..#MBQ') IS NOT NULL DROP TABLE #MBQ;

    SELECT
        dp.ST, dp.MJ,
        dp.ST_MJ_DISP_Q,
        dp.HOLD_DAYS,
        sm.TTL_ALC_DAYS,
        ISNULL(ms.CM_PD_SALE_Q, 0) AS CM_PD_SALE_Q,
        ISNULL(ms.NM_PD_SALE_Q, 0) AS NM_PD_SALE_Q,
        -- ST_MJ_MBQ = DISP + SALE_PD * TTL_ALC_DAYS
        dp.ST_MJ_DISP_Q + ISNULL(ms.ST_MJ_ALC_SALE_Q_PD, 0) AS ST_MJ_MBQ
    INTO #MBQ
    FROM #Display dp
    INNER JOIN #StoreMaster sm ON sm.ST = dp.ST
    LEFT JOIN #MjSale ms ON ms.ST = dp.ST AND ms.MJ = dp.MJ;

    CREATE CLUSTERED INDEX CX ON #MBQ (ST, MJ);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 8: Build master #PREPARED table (Article level)
    --   Join Stock + MSA + ArtSale + MBQ + StoreMaster
    --   Classify articles: L-ART, MIX-ART, OLD-ART
    -- ═══════════════════════════════════════════════════════════
    IF OBJECT_ID('tempdb..#Prepared') IS NOT NULL DROP TABLE #Prepared;

    SELECT
        @RunId AS RUN_ID,
        sm.ST, sm.ST_NM, sm.TAGGED_RDC, sm.HUB_CD,
        mbq.MJ,
        stk.[GEN-ART], stk.STK_0001, stk.STK_0002, stk.STK_0004, stk.STK_0006,
        stk.STK_HUB_INTRA, stk.STK_HUB_PRD, stk.STK_INTRA, stk.STK_PRD,
        stk.TTL_ST_STK_Q,
        ISNULL(msa.MSA_QTY, 0) AS MSA_QTY,

        -- Days
        sm.TTL_ALC_DAYS,
        ISNULL(mbq.HOLD_DAYS, 0) AS HOLD_DAYS,

        -- PD Sales
        ISNULL(mbq.CM_PD_SALE_Q, 0) AS CM_PD_SALE_Q,
        ISNULL(mbq.NM_PD_SALE_Q, 0) AS NM_PD_SALE_Q,
        ISNULL(asale.ART_AUTO_SALE_PD, 0) AS ART_AUTO_SALE_PD,

        -- Display
        ISNULL(mbq.ST_MJ_DISP_Q, 0) AS ACC_DENSITY,
        ISNULL(mbq.ST_MJ_DISP_Q, 0) AS ST_MJ_DISP_Q,

        -- MBQ
        ISNULL(mbq.ST_MJ_MBQ, 0) AS ST_MJ_MBQ,
        -- ART_MBQ = ACC_DENSITY + ART_SALE_PD * TTL_ALC_DAYS
        ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * sm.TTL_ALC_DAYS AS ART_MBQ,
        -- ART_HOLD_MBQ = ACC_DENSITY + ART_SALE_PD * (TTL_ALC_DAYS + HOLD_DAYS)
        ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * (sm.TTL_ALC_DAYS + ISNULL(mbq.HOLD_DAYS, 0)) AS ART_HOLD_MBQ,

        -- Requirement
        -- ST_MJ_REQ = ST_MJ_MBQ - TTL_ST_STK_Q (at MJ level, will be aggregated)
        CAST(0 AS DECIMAL(18,4)) AS ST_MJ_REQ,
        -- ST_ART_REQ = ART_MBQ - TTL_ST_STK_Q
        CASE WHEN (ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * sm.TTL_ALC_DAYS) - stk.TTL_ST_STK_Q > 0
             THEN (ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * sm.TTL_ALC_DAYS) - stk.TTL_ST_STK_Q
             ELSE 0 END AS ST_ART_REQ,
        -- ST_ART_HOLD_REQ = ART_HOLD_MBQ - TTL_ST_STK_Q
        CASE WHEN (ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * (sm.TTL_ALC_DAYS + ISNULL(mbq.HOLD_DAYS, 0))) - stk.TTL_ST_STK_Q > 0
             THEN (ISNULL(mbq.ST_MJ_DISP_Q, 0) + ISNULL(asale.ART_AUTO_SALE_PD, 0) * (sm.TTL_ALC_DAYS + ISNULL(mbq.HOLD_DAYS, 0))) - stk.TTL_ST_STK_Q
             ELSE 0 END AS ST_ART_HOLD_REQ,

        -- Classification
        CASE
            -- L-ART: STK >= 60% of ACC_DENSITY
            WHEN stk.TTL_ST_STK_Q >= ISNULL(mbq.ST_MJ_DISP_Q, 0) * 0.60 THEN 'L-ART'
            -- MIX-ART conditions:
            -- STK < 60% of ACC_DENSITY & MSA<50
            WHEN stk.TTL_ST_STK_Q < ISNULL(mbq.ST_MJ_DISP_Q, 0) * 0.60 AND ISNULL(msa.MSA_QTY, 0) < 50 THEN 'MIX-ART'
            -- Zero MSA
            WHEN ISNULL(msa.MSA_QTY, 0) = 0 THEN 'MIX-ART'
            -- OLD-ART: TTL_ST_STK > 0
            WHEN stk.TTL_ST_STK_Q > 0 THEN 'OLD-ART'
            ELSE 'MIX-ART'
        END AS ART_CLASS,

        -- Allocation placeholders
        CAST(0 AS DECIMAL(18,4)) AS ART_ALC_Q,
        CAST(0 AS DECIMAL(18,4)) AS ART_HOLD_Q,
        ISNULL(msa.MSA_QTY, 0) AS REM_MSA

    INTO #Prepared
    FROM #StockPivot stk
    INNER JOIN #StoreMaster sm ON sm.ST = stk.ST
    -- Need to map article to MJ — via art auto sale or a MJ lookup
    LEFT JOIN #ArtSale asale ON asale.ST = stk.ST AND asale.[GEN-ART] = stk.[GEN-ART]
    -- Cross to all MJ combos this store has display for
    INNER JOIN #MBQ mbq ON mbq.ST = stk.ST
    LEFT JOIN #MSA msa ON msa.ST = stk.ST AND msa.[GEN-ART] = stk.[GEN-ART];

    CREATE CLUSTERED INDEX CX ON #Prepared (ST, [GEN-ART]);
    CREATE INDEX IX_CLASS ON #Prepared (ART_CLASS);

    -- ═══════════════════════════════════════════════════════════
    -- STEP 9: Calculate ST_MJ_REQ (aggregate stock at MJ level)
    -- ST_MJ_REQ = ST_MJ_MBQ - SUM(TTL_ST_STK_Q) for this ST+MJ
    -- ═══════════════════════════════════════════════════════════
    ;WITH MjStk AS (
        SELECT ST, MJ, SUM(TTL_ST_STK_Q) AS MJ_TTL_STK
        FROM #Prepared GROUP BY ST, MJ
    )
    UPDATE p SET p.ST_MJ_REQ = CASE WHEN p.ST_MJ_MBQ - ms.MJ_TTL_STK > 0
                                     THEN p.ST_MJ_MBQ - ms.MJ_TTL_STK ELSE 0 END
    FROM #Prepared p INNER JOIN MjStk ms ON ms.ST = p.ST AND ms.MJ = p.MJ;

    -- ═══════════════════════════════════════════════════════════
    -- STEP 10: Output stats
    -- ═══════════════════════════════════════════════════════════
    SELECT
        COUNT(*) AS TotalRows,
        COUNT(DISTINCT ST) AS Stores,
        COUNT(DISTINCT [GEN-ART]) AS Articles,
        SUM(CASE WHEN ART_CLASS = 'L-ART' THEN 1 ELSE 0 END) AS L_Count,
        SUM(CASE WHEN ART_CLASS = 'MIX-ART' THEN 1 ELSE 0 END) AS MIX_Count,
        SUM(CASE WHEN ART_CLASS = 'OLD-ART' THEN 1 ELSE 0 END) AS OLD_Count
    FROM #Prepared;

    -- Keep #Prepared alive for SP_ARS_ALLOCATE (same connection)
    -- NOTE: If called from C# in same SqlConnection, temp table persists
END;
GO

PRINT '>> SP_ARS_PREPARE_DATA created on datav2.';
GO
