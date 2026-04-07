-- =====================================================
-- FILE: 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql
-- PURPOSE: Stored procedure to generate Purchase Plan
-- DATABASE: [planning]
--
-- V2 - OPTIMIZED: No cursors, temp tables, set-based
--   1. Table variables Ã¢Â†Â’ temp tables (indexes + statistics)
--   2. Correlated subqueries Ã¢Â†Â’ pre-built week-offset map
--   3. Nested cursor Ã¢Â†Â’ set-based WHILE loop per week
--   4. ~300 UPDATEs total instead of ~3M
-- =====================================================

USE [planning];
GO

IF OBJECT_ID('dbo.SP_GENERATE_PURCHASE_PLAN', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_GENERATE_PURCHASE_PLAN;
GO

CREATE PROCEDURE dbo.SP_GENERATE_PURCHASE_PLAN (
    @StartWeekID INT,
    @EndWeekID INT,
    @RdcCode VARCHAR(20) = NULL,
    @MajCat VARCHAR(50) = NULL,
    @Debug BIT = 0
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowsInserted INT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @StepTime  DATETIME;

    BEGIN TRY

        -- ===== STEP 1: Build working weeks with sequence =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 1: Building working weeks...';

        IF OBJECT_ID('tempdb..#Weeks') IS NOT NULL DROP TABLE #Weeks;

        SELECT
            [WEEK_ID],
            [WEEK_SEQ],
            [FY_WEEK],
            [FY_YEAR],
            [WK_ST_DT],
            [WK_END_DT]
        INTO #Weeks
        FROM dbo.WEEK_CALENDAR
        WHERE [WEEK_ID] BETWEEN @StartWeekID AND @EndWeekID;

        CREATE UNIQUE CLUSTERED INDEX CX ON #Weeks ([WEEK_ID]);
        CREATE UNIQUE INDEX IX_SEQ ON #Weeks ([WEEK_SEQ]);

        -- Ordered week list for loop
        DECLARE @WeekList TABLE (Seq INT IDENTITY(1,1) PRIMARY KEY, WID INT);
        INSERT INTO @WeekList (WID) SELECT [WEEK_ID] FROM #Weeks ORDER BY [WEEK_ID];
        DECLARE @TotalWeeks INT = @@ROWCOUNT;
        DECLARE @FirstWkSeq INT;
        SELECT @FirstWkSeq = [WEEK_SEQ] FROM #Weeks WHERE [WEEK_ID] = (SELECT WID FROM @WeekList WHERE Seq = 1);

        -- Pre-build week offset map: for each week, what's the WEEK_ID at +1, +3, +4, +5 offset
        IF OBJECT_ID('tempdb..#WkMap') IS NOT NULL DROP TABLE #WkMap;
        SELECT
            w.[WEEK_ID],
            w.[WEEK_SEQ],
            wP.[WEEK_ID] AS PREV_WEEK_ID,   -- offset -1
            w1.[WEEK_ID] AS NEXT1_WEEK_ID,   -- offset +1
            w2.[WEEK_ID] AS NEXT2_WEEK_ID,   -- offset +2
            w3.[WEEK_ID] AS NEXT3_WEEK_ID,   -- offset +3
            w4.[WEEK_ID] AS NEXT4_WEEK_ID,   -- offset +4
            w5.[WEEK_ID] AS NEXT5_WEEK_ID    -- offset +5
        INTO #WkMap
        FROM #Weeks w
        LEFT JOIN #Weeks wP ON wP.[WEEK_SEQ] = w.[WEEK_SEQ] - 1
        LEFT JOIN #Weeks w1 ON w1.[WEEK_SEQ] = w.[WEEK_SEQ] + 1
        LEFT JOIN #Weeks w2 ON w2.[WEEK_SEQ] = w.[WEEK_SEQ] + 2
        LEFT JOIN #Weeks w3 ON w3.[WEEK_SEQ] = w.[WEEK_SEQ] + 3
        LEFT JOIN #Weeks w4 ON w4.[WEEK_SEQ] = w.[WEEK_SEQ] + 4
        LEFT JOIN #Weeks w5 ON w5.[WEEK_SEQ] = w.[WEEK_SEQ] + 5;

        CREATE UNIQUE CLUSTERED INDEX CX ON #WkMap ([WEEK_ID]);

        IF @Debug = 1
            PRINT '  -> ' + CAST(@TotalWeeks AS VARCHAR) + ' weeks. Step 1: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- ===== STEP 2: Build RDC x MAJ_CAT combinations =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 2: Building RDC x MAJ_CAT combinations...';

        IF OBJECT_ID('tempdb..#RdcCat') IS NOT NULL DROP TABLE #RdcCat;

        SELECT DISTINCT
            m.[RDC_CD],
            m.[RDC_NM],
            b.[MAJ-CAT] AS MAJ_CAT,
            ISNULL(BM.SEG, 'NA') AS SEG,
            ISNULL(BM.DIV, 'NA') AS DIV,
            ISNULL(BM.SUB_DIV, 'NA') AS SUB_DIV,
            ISNULL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM,
            ISNULL(BM.SSN, 'NA') AS SSN
        INTO #RdcCat
        FROM (
            SELECT DISTINCT [RDC_CD], [RDC_NM]
            FROM dbo.MASTER_ST_MASTER
        ) m
        CROSS JOIN (
            SELECT DISTINCT [MAJ-CAT]
            FROM dbo.MASTER_BIN_CAPACITY
        ) b
        OUTER APPLY (
            SELECT TOP 1 BM.SEG, BM.DIV, BM.SUB_DIV, BM.MAJ_CAT_NM, BM.SSN
            FROM dbo.MASTER_PRODUCT_HIERARCHY BM
            WHERE BM.MAJ_CAT_NM = b.[MAJ-CAT]
        ) BM
        WHERE (@RdcCode IS NULL OR m.[RDC_CD] = @RdcCode)
          AND (@MajCat IS NULL OR b.[MAJ-CAT] = @MajCat);

        CREATE INDEX IX ON #RdcCat ([RDC_CD], [MAJ_CAT]);

        IF @Debug = 1
            PRINT '  -> ' + CAST(@@ROWCOUNT AS VARCHAR) + ' combos. Step 2: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- ===== STEP 3: Aggregate from TRF_IN_PLAN (set-based, no correlated subqueries) =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 3: Aggregating TRF_IN_PLAN metrics...';

        IF OBJECT_ID('tempdb..#TrfAgg') IS NOT NULL DROP TABLE #TrfAgg;

        SELECT
            t.[RDC_CD],
            t.[MAJ_CAT],
            t.[WEEK_ID],
            SUM(t.[S_GRT_STK_Q])          AS S_GRT_STK_Q,
            SUM(t.[W_GRT_STK_Q])          AS W_GRT_STK_Q,
            SUM(t.[BGT_DISP_CL_Q])        AS BGT_DISP_CL_Q,
            SUM(t.[CM_BGT_SALE_Q])         AS CW_BGT_SALE_Q,
            SUM(t.[CM1_BGT_SALE_Q])        AS CW1_BGT_SALE_Q,
            SUM(t.[BGT_ST_CL_MBQ])         AS BGT_ST_CL_MBQ,
            SUM(t.[BGT_TTL_CF_OP_STK_Q])   AS NET_ST_OP_STK_Q,
            SUM(t.[TRF_IN_STK_Q])          AS CW_TRF_OUT_Q,
            SUM(t.[NET_ST_CL_STK_Q])       AS NET_BGT_ST_CL_STK_Q,
            SUM(t.[ST_CL_EXCESS_Q])        AS ST_STK_EXCESS_Q,
            SUM(t.[ST_CL_SHORT_Q])         AS ST_STK_SHORT_Q
        INTO #TrfAgg
        FROM dbo.TRF_IN_PLAN t
        WHERE t.[WEEK_ID] BETWEEN @StartWeekID AND @EndWeekID
          AND (@RdcCode IS NULL OR t.[RDC_CD] = @RdcCode)
          AND (@MajCat IS NULL OR t.[MAJ_CAT] = @MajCat)
        GROUP BY t.[RDC_CD], t.[MAJ_CAT], t.[WEEK_ID];

        CREATE CLUSTERED INDEX CX ON #TrfAgg ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        -- Prev week's BGT_ST_CL_MBQ (for BGT_ST_OP_MBQ)
        IF OBJECT_ID('tempdb..#TrfPrev') IS NOT NULL DROP TABLE #TrfPrev;
        SELECT
            ta.[RDC_CD], ta.[MAJ_CAT], ta.[WEEK_ID],
            ISNULL(tp.BGT_ST_CL_MBQ, 0) AS BGT_ST_OP_MBQ
        INTO #TrfPrev
        FROM #TrfAgg ta
        LEFT JOIN #WkMap wm ON wm.[WEEK_ID] = ta.[WEEK_ID]
        LEFT JOIN #TrfAgg tp ON tp.[RDC_CD] = ta.[RDC_CD]
                               AND tp.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND tp.[WEEK_ID] = wm.PREV_WEEK_ID;
        CREATE CLUSTERED INDEX CX ON #TrfPrev ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        -- Next week's TRF_OUT (CW1_TRF_OUT_Q)
        IF OBJECT_ID('tempdb..#TrfNext') IS NOT NULL DROP TABLE #TrfNext;
        SELECT
            ta.[RDC_CD], ta.[MAJ_CAT], ta.[WEEK_ID],
            ISNULL(tn.CW_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q
        INTO #TrfNext
        FROM #TrfAgg ta
        LEFT JOIN #WkMap wm ON wm.[WEEK_ID] = ta.[WEEK_ID]
        LEFT JOIN #TrfAgg tn ON tn.[RDC_CD] = ta.[RDC_CD]
                               AND tn.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND tn.[WEEK_ID] = wm.NEXT1_WEEK_ID;
        CREATE CLUSTERED INDEX CX ON #TrfNext ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        -- Future weeks TRF_OUT (CW2, CW3, CW4)
        IF OBJECT_ID('tempdb..#TrfNxt234') IS NOT NULL DROP TABLE #TrfNxt234;
        SELECT
            ta.[RDC_CD], ta.[MAJ_CAT], ta.[WEEK_ID],
            ISNULL(t2.CW_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q,
            ISNULL(t3.CW_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q,
            ISNULL(t4.CW_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q
        INTO #TrfNxt234
        FROM #TrfAgg ta
        LEFT JOIN #WkMap wm ON wm.[WEEK_ID] = ta.[WEEK_ID]
        LEFT JOIN #TrfAgg t2 ON t2.[RDC_CD] = ta.[RDC_CD]
                               AND t2.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t2.[WEEK_ID] = wm.NEXT2_WEEK_ID
        LEFT JOIN #TrfAgg t3 ON t3.[RDC_CD] = ta.[RDC_CD]
                               AND t3.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t3.[WEEK_ID] = wm.NEXT3_WEEK_ID
        LEFT JOIN #TrfAgg t4 ON t4.[RDC_CD] = ta.[RDC_CD]
                               AND t4.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t4.[WEEK_ID] = wm.NEXT4_WEEK_ID;
        CREATE CLUSTERED INDEX CX ON #TrfNxt234 ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        -- Future week sales (all sourced from TRF_IN_PLAN via CW1_BGT_SALE_Q at offset weeks)
        -- CW2 = SUM(CM1_BGT_SALE_Q) at week+1 = week+2 sale from Transfer In
        -- CW3 = SUM(CM1_BGT_SALE_Q) at week+2 = week+3 sale from Transfer In
        -- CW4 = SUM(CM1_BGT_SALE_Q) at week+3 = week+4 sale from Transfer In
        -- CW5 = SUM(CM1_BGT_SALE_Q) at week+4 = week+5 sale from Transfer In
        IF OBJECT_ID('tempdb..#TrfFuture') IS NOT NULL DROP TABLE #TrfFuture;
        SELECT
            ta.[RDC_CD], ta.[MAJ_CAT], ta.[WEEK_ID],
            ISNULL(t1.CW1_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q,
            ISNULL(t2.CW1_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q,
            ISNULL(t3.CW1_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q,
            ISNULL(t4.CW1_BGT_SALE_Q, 0) AS CW5_BGT_SALE_Q
        INTO #TrfFuture
        FROM #TrfAgg ta
        LEFT JOIN #WkMap wm ON wm.[WEEK_ID] = ta.[WEEK_ID]
        LEFT JOIN #TrfAgg t1 ON t1.[RDC_CD] = ta.[RDC_CD]
                               AND t1.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t1.[WEEK_ID] = wm.NEXT1_WEEK_ID
        LEFT JOIN #TrfAgg t2 ON t2.[RDC_CD] = ta.[RDC_CD]
                               AND t2.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t2.[WEEK_ID] = wm.NEXT2_WEEK_ID
        LEFT JOIN #TrfAgg t3 ON t3.[RDC_CD] = ta.[RDC_CD]
                               AND t3.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t3.[WEEK_ID] = wm.NEXT3_WEEK_ID
        LEFT JOIN #TrfAgg t4 ON t4.[RDC_CD] = ta.[RDC_CD]
                               AND t4.[MAJ_CAT] = ta.[MAJ_CAT]
                               AND t4.[WEEK_ID] = wm.NEXT4_WEEK_ID;
        CREATE CLUSTERED INDEX CX ON #TrfFuture ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        IF @Debug = 1
            PRINT '  -> Aggregation done. Step 3: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- ===== STEP 4: Reference data =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 4: Loading reference data...';

        -- 4a: UNPIVOT MASTER_GRT_CONTRIBUTION into (SSN, WkNum, GrtPct) rows
        IF OBJECT_ID('tempdb..#GrtUnpivot') IS NOT NULL DROP TABLE #GrtUnpivot;

        SELECT upv.[SSN], x.WkNum, upv.GrtPct
        INTO #GrtUnpivot
        FROM dbo.MASTER_GRT_CONTRIBUTION
        UNPIVOT (
            GrtPct FOR WkCol IN (
                [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],
                [WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],
                [WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
                [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],
                [WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],
                [WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]
            )
        ) AS upv
        CROSS APPLY (SELECT CAST(REPLACE(upv.WkCol, 'WK-', '') AS INT)) AS x(WkNum);

        CREATE CLUSTERED INDEX CX_GRT ON #GrtUnpivot ([SSN], [WkNum]);

        IF @Debug = 1
            PRINT '  -> GRT unpivoted. Rows: ' + CAST(@@ROWCOUNT AS VARCHAR);

        -- 4b: Build reference data with dynamic GRT lookup by SSN + week position
        IF OBJECT_ID('tempdb..#RefData') IS NOT NULL DROP TABLE #RefData;

        SELECT
            rc.[RDC_CD],
            rc.[MAJ_CAT],
            ww.[WEEK_ID],
            ISNULL(qmg.[DC-STK-Q], 0) AS DC_STK_Q,
            ISNULL(qmg.[GRT-STK-Q], 0) AS GRT_STK_Q,
            ISNULL(mbc.[BIN CAP DC TEAM], 0) AS BIN_CAP_DC_TEAM,
            ISNULL(mbc.[BIN CAP], 0) AS BIN_CAP,
            ISNULL(gu.GrtPct, 0) AS GRT_CONS_PCT,
            ISNULL(qdp.DEL_PEND_Q, 0) AS DEL_PEND_Q,
            rc.[SSN]
        INTO #RefData
        FROM #RdcCat rc
        CROSS JOIN #Weeks ww
        LEFT JOIN dbo.QTY_MSA_AND_GRT qmg ON qmg.[RDC_CD] = rc.[RDC_CD]
                                            AND qmg.[MAJ-CAT] = rc.[MAJ_CAT]
                                            AND qmg.[DATE] = (SELECT MAX([DATE])
                                                              FROM dbo.QTY_MSA_AND_GRT
                                                              WHERE [RDC_CD] = rc.[RDC_CD]
                                                                AND [MAJ-CAT] = rc.[MAJ_CAT]
                                                                AND [DATE] <= ww.[WK_END_DT])
        LEFT JOIN dbo.MASTER_BIN_CAPACITY mbc ON mbc.[MAJ-CAT] = rc.[MAJ_CAT]
        LEFT JOIN #GrtUnpivot gu ON gu.[SSN] = rc.[SSN]
                                  AND gu.[WkNum] = (ww.[WEEK_SEQ] - @FirstWkSeq + 1)
        -- QTY_DEL_PENDING: LEFT JOIN so SP works if table doesn't exist yet
        LEFT JOIN (
            SELECT dp1.[RDC_CD], dp1.[MAJ_CAT], dp1.[DATE], dp1.[DEL_PEND_Q]
            FROM dbo.QTY_DEL_PENDING dp1
            WHERE dp1.[DATE] = (SELECT MAX(dp2.[DATE])
                                FROM dbo.QTY_DEL_PENDING dp2
                                WHERE dp2.[RDC_CD] = dp1.[RDC_CD]
                                  AND dp2.[MAJ_CAT] = dp1.[MAJ_CAT])
        ) qdp ON qdp.[RDC_CD] = rc.[RDC_CD]
               AND qdp.[MAJ_CAT] = rc.[MAJ_CAT];

        CREATE CLUSTERED INDEX CX ON #RefData ([RDC_CD], [MAJ_CAT], [WEEK_ID]);

        IF @Debug = 1
            PRINT '  -> Reference loaded. Step 4: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- ===== STEP 5: Build full calculation table (temp, not table variable) =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 5: Building calculation table...';

        IF OBJECT_ID('tempdb..#PP') IS NOT NULL DROP TABLE #PP;

        SELECT
            rc.[RDC_CD],
            ISNULL(rc.[RDC_NM], 'NA') AS [RDC_NM],
            rc.[MAJ_CAT],
            ww.[WEEK_ID],
            ww.[WEEK_SEQ],
            rc.[SSN],
            rc.[SEG],
            rc.[DIV],
            rc.[SUB_DIV],
            rc.[MAJ_CAT_NM],
            ww.[FY_WEEK],
            ww.[FY_YEAR],
            ww.[WK_ST_DT],
            ww.[WK_END_DT],
            ISNULL(rd.DC_STK_Q, 0) AS DC_STK_Q,
            ISNULL(rd.GRT_STK_Q, 0) AS GRT_STK_Q,
            ISNULL(ta.S_GRT_STK_Q, 0) AS S_GRT_STK_Q,
            ISNULL(ta.W_GRT_STK_Q, 0) AS W_GRT_STK_Q,
            ISNULL(rd.BIN_CAP_DC_TEAM, 0) AS BIN_CAP_DC_TEAM,
            ISNULL(rd.BIN_CAP, 0) AS BIN_CAP,
            ISNULL(ta.BGT_DISP_CL_Q, 0) AS BGT_DISP_CL_Q,
            ISNULL(ta.CW_BGT_SALE_Q, 0) AS CW_BGT_SALE_Q,
            ISNULL(ta.CW1_BGT_SALE_Q, 0) AS CW1_BGT_SALE_Q,
            ISNULL(tf.CW2_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q,
            ISNULL(tf.CW3_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q,
            ISNULL(tf.CW4_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q,
            ISNULL(tf.CW5_BGT_SALE_Q, 0) AS CW5_BGT_SALE_Q,
            ISNULL(tpv.BGT_ST_OP_MBQ, 0) AS BGT_ST_OP_MBQ,
            ISNULL(ta.NET_ST_OP_STK_Q, 0) AS NET_ST_OP_STK_Q,
            -- BGT_DC_OP_STK_Q: Week 1 = DC_STK_Q, others chained
            ISNULL(CASE WHEN rd.DC_STK_Q > 0 THEN rd.DC_STK_Q ELSE 0 END, 0) AS BGT_DC_OP_STK_Q,
            CAST(0 AS DECIMAL(18,4)) AS PP_NT_ACT_Q,
            ISNULL(CASE WHEN rd.DC_STK_Q > 0 THEN rd.DC_STK_Q ELSE 0 END, 0) AS BGT_CF_STK_Q,
            ISNULL(rd.GRT_STK_Q, 0) AS TTL_STK,
            ISNULL(rd.GRT_STK_Q, 0) AS OP_STK,
            CASE WHEN rc.[SSN] IN ('S', 'OC', 'A') THEN ISNULL(rd.GRT_STK_Q, 0) * 0.10 ELSE 0 END AS NT_ACT_STK,
            ISNULL(rd.GRT_CONS_PCT, 0) AS GRT_CONS_PCT,
            CAST(0 AS DECIMAL(18,4)) AS GRT_CONS_Q,
            ISNULL(rd.DEL_PEND_Q, 0) AS DEL_PEND_Q,
            CAST(0 AS DECIMAL(18,4)) AS PP_NET_BGT_CF_STK_Q,
            ISNULL(ta.CW_TRF_OUT_Q, 0) AS CW_TRF_OUT_Q,
            ISNULL(tn.CW1_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q,
            ISNULL(tn234.CW2_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q,
            ISNULL(tn234.CW3_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q,
            ISNULL(tn234.CW4_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q,
            ISNULL(ta.CW_TRF_OUT_Q, 0) AS TTL_TRF_OUT_Q,  -- will be recalculated below
            ISNULL(ta.BGT_ST_CL_MBQ, 0) AS BGT_ST_CL_MBQ,
            ISNULL(ta.NET_BGT_ST_CL_STK_Q, 0) AS NET_BGT_ST_CL_STK_Q,
            CAST(0 AS DECIMAL(18,4)) AS NET_SSNL_CL_STK_Q,
            -- BGT_DC_MBQ_SALE = CW1 + CW2 + CW3 + CW4 (next 4 weeks sale)
            ISNULL(ta.CW1_BGT_SALE_Q, 0)
                + ISNULL(tf.CW2_BGT_SALE_Q, 0)
                + ISNULL(tf.CW3_BGT_SALE_Q, 0)
                + ISNULL(tf.CW4_BGT_SALE_Q, 0) AS BGT_DC_MBQ_SALE,
            CAST(0 AS DECIMAL(18,4)) AS BGT_DC_CL_MBQ,
            CAST(0 AS DECIMAL(18,4)) AS BGT_DC_CL_STK_Q,
            CAST(0 AS DECIMAL(18,4)) AS BGT_PUR_Q_INIT,
            CAST(0 AS DECIMAL(18,4)) AS POS_PO_RAISED,
            CAST(0 AS DECIMAL(18,4)) AS NEG_PO_RAISED,
            CAST(0 AS DECIMAL(18,4)) AS BGT_CO_CL_STK_Q,
            CAST(0 AS DECIMAL(18,4)) AS DC_STK_EXCESS_Q,
            CAST(0 AS DECIMAL(18,4)) AS DC_STK_SHORT_Q,
            ISNULL(ta.ST_STK_EXCESS_Q, 0) AS ST_STK_EXCESS_Q,
            ISNULL(ta.ST_STK_SHORT_Q, 0) AS ST_STK_SHORT_Q,
            CAST(0 AS DECIMAL(18,4)) AS CO_STK_EXCESS_Q,
            CAST(0 AS DECIMAL(18,4)) AS CO_STK_SHORT_Q,
            CAST(0 AS DECIMAL(18,4)) AS FRESH_BIN_REQ,
            CAST(0 AS DECIMAL(18,4)) AS GRT_BIN_REQ
        INTO #PP
        FROM #RdcCat rc
        CROSS JOIN #Weeks ww
        LEFT JOIN #RefData rd ON rd.[RDC_CD] = rc.[RDC_CD]
                                AND rd.[MAJ_CAT] = rc.[MAJ_CAT]
                                AND rd.[WEEK_ID] = ww.[WEEK_ID]
        LEFT JOIN #TrfAgg ta ON ta.[RDC_CD] = rc.[RDC_CD]
                               AND ta.[MAJ_CAT] = rc.[MAJ_CAT]
                               AND ta.[WEEK_ID] = ww.[WEEK_ID]
        LEFT JOIN #TrfPrev tpv ON tpv.[RDC_CD] = rc.[RDC_CD]
                                 AND tpv.[MAJ_CAT] = rc.[MAJ_CAT]
                                 AND tpv.[WEEK_ID] = ww.[WEEK_ID]
        LEFT JOIN #TrfNext tn ON tn.[RDC_CD] = rc.[RDC_CD]
                                AND tn.[MAJ_CAT] = rc.[MAJ_CAT]
                                AND tn.[WEEK_ID] = ww.[WEEK_ID]
        LEFT JOIN #TrfFuture tf ON tf.[RDC_CD] = rc.[RDC_CD]
                                  AND tf.[MAJ_CAT] = rc.[MAJ_CAT]
                                  AND tf.[WEEK_ID] = ww.[WEEK_ID]
        LEFT JOIN #TrfNxt234 tn234 ON tn234.[RDC_CD] = rc.[RDC_CD]
                                     AND tn234.[MAJ_CAT] = rc.[MAJ_CAT]
                                     AND tn234.[WEEK_ID] = ww.[WEEK_ID];

        -- Critical index for week chaining
        CREATE CLUSTERED INDEX CX ON #PP ([RDC_CD], [MAJ_CAT], [WEEK_ID]);
        CREATE INDEX IX_WEEK ON #PP ([WEEK_ID]);

        -- Recalculate TTL_TRF_OUT_Q = sum of all CW transfer outs
        UPDATE #PP SET TTL_TRF_OUT_Q = CW_TRF_OUT_Q + CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q;

        IF @Debug = 1
        BEGIN
            SELECT COUNT(*) AS PPRows FROM #PP;
            PRINT '  -> Calc table built. Step 5: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';
        END

        -- ===== STEP 6: Calculate static columns + Week 1, then chain =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 6: Calculating all weeks (set-based)...';

        -- 6a: BGT_DC_CL_MBQ = MIN(sum of next 4 weeks transfer out, BGT_DC_MBQ_SALE)
        UPDATE #PP
        SET BGT_DC_CL_MBQ = CASE WHEN (CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q) < BGT_DC_MBQ_SALE
                                  THEN (CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q)
                                  ELSE BGT_DC_MBQ_SALE END;

        -- 6b: Calculate GRT_CONS_Q for ALL weeks at once (initial pass)
        UPDATE #PP
        SET GRT_CONS_Q = CASE WHEN TTL_TRF_OUT_Q = 0 THEN 0
            ELSE (
                SELECT MIN(v) FROM (VALUES
                    (pp.TTL_TRF_OUT_Q * 0.30),
                    (CASE WHEN pp.OP_STK - pp.NT_ACT_STK > 0
                          THEN pp.OP_STK - pp.NT_ACT_STK ELSE 0 END),
                    (CASE WHEN pp.TTL_TRF_OUT_Q -
                              CASE WHEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ > 0
                                   THEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ ELSE 0 END > 0
                          THEN pp.TTL_TRF_OUT_Q -
                              CASE WHEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ > 0
                                   THEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ ELSE 0 END
                          ELSE 0 END),
                    (CASE WHEN pp.TTL_STK - pp.NT_ACT_STK > 0
                          THEN (pp.TTL_STK - pp.NT_ACT_STK) * pp.GRT_CONS_PCT ELSE 0 END)
                ) AS T(v)
            ) END
        FROM #PP pp;

        -- 6c: Cascading calculations for ALL weeks
        UPDATE #PP
        SET PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q,
            NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0
                                      THEN OP_STK - GRT_CONS_Q ELSE 0 END;

        UPDATE #PP
        SET BGT_PUR_Q_INIT = CASE WHEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q > 0
                                  THEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q
                                  ELSE 0 END;

        UPDATE #PP
        SET POS_PO_RAISED = CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q > 0
                                  THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END,
            NEG_PO_RAISED = CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q < 0
                                  THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END;

        UPDATE #PP
        SET BGT_DC_CL_STK_Q = CASE WHEN PP_NET_BGT_CF_STK_Q + POS_PO_RAISED - TTL_TRF_OUT_Q > 0
                                    THEN PP_NET_BGT_CF_STK_Q + POS_PO_RAISED - TTL_TRF_OUT_Q
                                    ELSE 0 END;

        UPDATE #PP
        SET DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
                                    THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END,
            DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
                                   THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END,
            CO_STK_EXCESS_Q = ST_STK_EXCESS_Q +
                              CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
                                    THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END,
            CO_STK_SHORT_Q = ST_STK_SHORT_Q +
                             CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
                                   THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END,
            BGT_CO_CL_STK_Q = NET_BGT_ST_CL_STK_Q + NET_SSNL_CL_STK_Q + BGT_DC_CL_STK_Q,
            FRESH_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN BGT_DC_CL_STK_Q / BIN_CAP ELSE 0 END,
            GRT_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN OP_STK / BIN_CAP ELSE 0 END;

        -- ===== STEP 6d: Week chaining loop (set-based, all combos at once) =====
        -- Only BGT_DC_OP_STK_Q chains: week N = prev week's BGT_DC_CL_STK_Q
        -- Then recalculate downstream columns

        DECLARE @i INT = 2, @tw INT, @pw INT;

        WHILE @i <= @TotalWeeks
        BEGIN
            SELECT @tw = WID FROM @WeekList WHERE Seq = @i;
            SELECT @pw = WID FROM @WeekList WHERE Seq = @i - 1;

            -- Chain: DC_STK_Q, BGT_DC_OP_STK_Q and BGT_CF_STK_Q from prev week's BGT_DC_CL_STK_Q
            UPDATE c
            SET c.DC_STK_Q = p.BGT_DC_CL_STK_Q,
                c.BGT_DC_OP_STK_Q = p.BGT_DC_CL_STK_Q,
                c.BGT_CF_STK_Q = CASE WHEN p.BGT_DC_CL_STK_Q > 0
                                       THEN p.BGT_DC_CL_STK_Q ELSE 0 END
            FROM #PP c
            INNER JOIN #PP p ON p.[RDC_CD] = c.[RDC_CD]
                              AND p.[MAJ_CAT] = c.[MAJ_CAT]
                              AND p.[WEEK_ID] = @pw
            WHERE c.[WEEK_ID] = @tw;

            -- Recalculate GRT_CONS_Q for this week (uses updated BGT_CF_STK_Q)
            UPDATE pp
            SET GRT_CONS_Q = CASE WHEN pp.TTL_TRF_OUT_Q = 0 THEN 0
                ELSE (
                    SELECT MIN(v) FROM (VALUES
                        (pp.TTL_TRF_OUT_Q * 0.30),
                        (CASE WHEN pp.OP_STK - pp.NT_ACT_STK > 0
                              THEN pp.OP_STK - pp.NT_ACT_STK ELSE 0 END),
                        (CASE WHEN pp.TTL_TRF_OUT_Q -
                                  CASE WHEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ > 0
                                       THEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ ELSE 0 END > 0
                              THEN pp.TTL_TRF_OUT_Q -
                                  CASE WHEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ > 0
                                       THEN pp.BGT_CF_STK_Q - pp.BGT_DC_CL_MBQ ELSE 0 END
                              ELSE 0 END),
                        (CASE WHEN pp.TTL_STK - pp.NT_ACT_STK > 0
                              THEN (pp.TTL_STK - pp.NT_ACT_STK) * pp.GRT_CONS_PCT ELSE 0 END)
                    ) AS T(v)
                ) END
            FROM #PP pp
            WHERE pp.[WEEK_ID] = @tw;

            -- Recalculate cascading columns
            UPDATE #PP
            SET PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q,
                NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0
                                          THEN OP_STK - GRT_CONS_Q ELSE 0 END
            WHERE [WEEK_ID] = @tw;

            UPDATE #PP
            SET BGT_PUR_Q_INIT = CASE WHEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q > 0
                                      THEN TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q
                                      ELSE 0 END
            WHERE [WEEK_ID] = @tw;

            UPDATE #PP
            SET POS_PO_RAISED = CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q > 0
                                      THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END,
                NEG_PO_RAISED = CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q < 0
                                      THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END
            WHERE [WEEK_ID] = @tw;

            UPDATE #PP
            SET BGT_DC_CL_STK_Q = CASE WHEN PP_NET_BGT_CF_STK_Q + POS_PO_RAISED - TTL_TRF_OUT_Q > 0
                                        THEN PP_NET_BGT_CF_STK_Q + POS_PO_RAISED - TTL_TRF_OUT_Q
                                        ELSE 0 END
            WHERE [WEEK_ID] = @tw;

            UPDATE #PP
            SET DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
                                        THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END,
                DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
                                       THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END,
                CO_STK_EXCESS_Q = ST_STK_EXCESS_Q +
                                  CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0
                                        THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END,
                CO_STK_SHORT_Q = ST_STK_SHORT_Q +
                                 CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0
                                       THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END,
                BGT_CO_CL_STK_Q = NET_BGT_ST_CL_STK_Q + NET_SSNL_CL_STK_Q + BGT_DC_CL_STK_Q,
                FRESH_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN BGT_DC_CL_STK_Q / BIN_CAP ELSE 0 END
            WHERE [WEEK_ID] = @tw;

            SET @i = @i + 1;
        END

        IF @Debug = 1
            PRINT '  -> Chaining done. Step 6: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- ===== STEP 7: Insert into PURCHASE_PLAN table =====
        SET @StepTime = GETDATE();
        IF @Debug = 1 PRINT 'STEP 7: Inserting into PURCHASE_PLAN...';

        DELETE FROM dbo.PURCHASE_PLAN
        WHERE [WEEK_ID] BETWEEN @StartWeekID AND @EndWeekID
          AND (@RdcCode IS NULL OR [RDC_CD] = @RdcCode)
          AND (@MajCat IS NULL OR [MAJ_CAT] = @MajCat);

        INSERT INTO dbo.PURCHASE_PLAN WITH (TABLOCK) (
            [RDC_CD], [RDC_NM], [MAJ_CAT], [SSN], [SEG], [DIV], [SUB_DIV], [MAJ_CAT_NM],
            [WEEK_ID], [FY_WEEK], [FY_YEAR], [WK_ST_DT], [WK_END_DT],
            [DC_STK_Q], [GRT_STK_Q], [S_GRT_STK_Q], [W_GRT_STK_Q],
            [BIN_CAP_DC_TEAM], [BIN_CAP],
            [BGT_DISP_CL_Q],
            [CW_BGT_SALE_Q], [CW1_BGT_SALE_Q], [CW2_BGT_SALE_Q], [CW3_BGT_SALE_Q], [CW4_BGT_SALE_Q], [CW5_BGT_SALE_Q],
            [BGT_ST_OP_MBQ], [NET_ST_OP_STK_Q],
            [BGT_DC_OP_STK_Q], [PP_NT_ACT_Q], [BGT_CF_STK_Q],
            [TTL_STK], [OP_STK], [NT_ACT_STK],
            [GRT_CONS_PCT], [GRT_CONS_Q],
            [DEL_PEND_Q], [PP_NET_BGT_CF_STK_Q],
            [CW_TRF_OUT_Q], [CW1_TRF_OUT_Q], [CW2_TRF_OUT_Q], [CW3_TRF_OUT_Q], [CW4_TRF_OUT_Q], [TTL_TRF_OUT_Q],
            [BGT_ST_CL_MBQ], [NET_BGT_ST_CL_STK_Q], [NET_SSNL_CL_STK_Q],
            [BGT_DC_MBQ_SALE], [BGT_DC_CL_MBQ], [BGT_DC_CL_STK_Q],
            [BGT_PUR_Q_INIT], [POS_PO_RAISED], [NEG_PO_RAISED],
            [BGT_CO_CL_STK_Q],
            [DC_STK_EXCESS_Q], [DC_STK_SHORT_Q], [ST_STK_EXCESS_Q], [ST_STK_SHORT_Q], [CO_STK_EXCESS_Q], [CO_STK_SHORT_Q],
            [FRESH_BIN_REQ], [GRT_BIN_REQ]
        )
        SELECT
            ISNULL([RDC_CD], 'NA'),
            ISNULL([RDC_NM], 'NA'),
            ISNULL([MAJ_CAT], 'NA'),
            ISNULL([SSN], 'NA'),
            ISNULL([SEG], 'NA'),
            ISNULL([DIV], 'NA'),
            ISNULL([SUB_DIV], 'NA'),
            ISNULL([MAJ_CAT_NM], 'NA'),
            ISNULL([WEEK_ID], 0),
            ISNULL([FY_WEEK], 0),
            ISNULL([FY_YEAR], 0),
            [WK_ST_DT],
            [WK_END_DT],
            ISNULL([DC_STK_Q], 0),
            ISNULL([GRT_STK_Q], 0),
            ISNULL([S_GRT_STK_Q], 0),
            ISNULL([W_GRT_STK_Q], 0),
            ISNULL([BIN_CAP_DC_TEAM], 0),
            ISNULL([BIN_CAP], 0),
            ISNULL([BGT_DISP_CL_Q], 0),
            ISNULL([CW_BGT_SALE_Q], 0),
            ISNULL([CW1_BGT_SALE_Q], 0),
            ISNULL([CW2_BGT_SALE_Q], 0),
            ISNULL([CW3_BGT_SALE_Q], 0),
            ISNULL([CW4_BGT_SALE_Q], 0),
            ISNULL([CW5_BGT_SALE_Q], 0),
            ISNULL([BGT_ST_OP_MBQ], 0),
            ISNULL([NET_ST_OP_STK_Q], 0),
            ISNULL([BGT_DC_OP_STK_Q], 0),
            ISNULL([PP_NT_ACT_Q], 0),
            ISNULL([BGT_CF_STK_Q], 0),
            ISNULL([TTL_STK], 0),
            ISNULL([OP_STK], 0),
            ISNULL([NT_ACT_STK], 0),
            ISNULL([GRT_CONS_PCT], 0),
            ISNULL([GRT_CONS_Q], 0),
            ISNULL([DEL_PEND_Q], 0),
            ISNULL([PP_NET_BGT_CF_STK_Q], 0),
            ISNULL([CW_TRF_OUT_Q], 0),
            ISNULL([CW1_TRF_OUT_Q], 0),
            ISNULL([CW2_TRF_OUT_Q], 0),
            ISNULL([CW3_TRF_OUT_Q], 0),
            ISNULL([CW4_TRF_OUT_Q], 0),
            ISNULL([TTL_TRF_OUT_Q], 0),
            ISNULL([BGT_ST_CL_MBQ], 0),
            ISNULL([NET_BGT_ST_CL_STK_Q], 0),
            ISNULL([NET_SSNL_CL_STK_Q], 0),
            ISNULL([BGT_DC_MBQ_SALE], 0),
            ISNULL([BGT_DC_CL_MBQ], 0),
            ISNULL([BGT_DC_CL_STK_Q], 0),
            ISNULL([BGT_PUR_Q_INIT], 0),
            ISNULL([POS_PO_RAISED], 0),
            ISNULL([NEG_PO_RAISED], 0),
            ISNULL([BGT_CO_CL_STK_Q], 0),
            ISNULL([DC_STK_EXCESS_Q], 0),
            ISNULL([DC_STK_SHORT_Q], 0),
            ISNULL([ST_STK_EXCESS_Q], 0),
            ISNULL([ST_STK_SHORT_Q], 0),
            ISNULL([CO_STK_EXCESS_Q], 0),
            ISNULL([CO_STK_SHORT_Q], 0),
            ISNULL([FRESH_BIN_REQ], 0),
            ISNULL([GRT_BIN_REQ], 0)
        FROM #PP
        ORDER BY [RDC_CD], [MAJ_CAT], [WEEK_SEQ];

        SET @RowsInserted = @@ROWCOUNT;

        IF @Debug = 1
            PRINT '  -> ' + CAST(@RowsInserted AS VARCHAR) + ' rows inserted. Step 7: '
                + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

        -- Cleanup
        DROP TABLE IF EXISTS #Weeks, #WkMap, #RdcCat, #TrfAgg, #TrfPrev, #TrfNext, #TrfNxt234,
                             #TrfFuture, #RefData, #PP;

        -- ===== SUMMARY =====
        PRINT '';
        PRINT '========================================';
        PRINT 'PURCHASE PLAN GENERATION SUMMARY';
        PRINT '========================================';
        PRINT 'Rows inserted: ' + CAST(@RowsInserted AS VARCHAR);
        PRINT 'Execution time: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds';
        PRINT '========================================';

        SELECT
            @RowsInserted AS RowsInserted,
            @StartWeekID AS StartWeek,
            @EndWeekID AS EndWeek,
            DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionSeconds;

    END TRY
    BEGIN CATCH
        -- Cleanup on error
        DROP TABLE IF EXISTS #Weeks, #WkMap, #RdcCat, #TrfAgg, #TrfPrev, #TrfNext, #TrfNxt234,
                             #TrfFuture, #RefData, #PP;
        PRINT 'ERROR: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;

END;
GO

PRINT 'SP_GENERATE_PURCHASE_PLAN (V2-OPTIMIZED) created successfully.';
GO
