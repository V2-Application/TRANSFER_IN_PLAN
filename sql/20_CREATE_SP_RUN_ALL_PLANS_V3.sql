/*==============================================================
  SP_RUN_ALL_PLANS - V3 BULK (ALL STORES AT ONCE)

  Before V2: 657 stores x 52 weeks x 2 UPDATEs = 68,328 UPDATEs → ~27 min
  V3:        52 weeks x 2 UPDATEs = 104 UPDATEs (all stores in parallel) → ~30-60s

  Strategy: Build ONE #Chain for ALL stores at once,
  run week chaining ONCE across all combos.
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.SP_RUN_ALL_PLANS', 'P') IS NOT NULL
    DROP PROCEDURE dbo.SP_RUN_ALL_PLANS;
GO

CREATE PROCEDURE dbo.SP_RUN_ALL_PLANS
    @StartWeekID    INT,
    @EndWeekID      INT,
    @MajCat         VARCHAR(50)  = NULL,
    @CoverDaysCM1   DECIMAL(18,4) = 14,
    @CoverDaysCM2   DECIMAL(18,4) = 0,
    @Debug          BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @MasterStart DATETIME = GETDATE();
    DECLARE @StepStart DATETIME;
    DECLARE @TotalWeeks INT, @FirstWk INT;
    DECLARE @trfRows BIGINT, @ppRows BIGINT;

    PRINT '========================================================';
    PRINT '  V3 BULK PLAN GENERATION - ALL STORES AT ONCE';
    PRINT '  Started: ' + CONVERT(VARCHAR, @MasterStart, 120);
    PRINT '========================================================';

    -- ==========================================================
    -- STEP 1: WEEKS
    -- ==========================================================
    SET @StepStart = GETDATE();

    IF OBJECT_ID('tempdb..#Weeks') IS NOT NULL DROP TABLE #Weeks;
    SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT,
           'WK-' + CAST(FY_WEEK AS VARCHAR) AS WK_COL_NAME
    INTO #Weeks
    FROM dbo.WEEK_CALENDAR
    WHERE WEEK_ID BETWEEN @StartWeekID AND @EndWeekID;
    CREATE UNIQUE CLUSTERED INDEX CX ON #Weeks (WEEK_ID);
    CREATE UNIQUE INDEX IX_SEQ ON #Weeks (WEEK_SEQ);

    DECLARE @WeekList TABLE (Seq INT IDENTITY(1,1) PRIMARY KEY, WID INT);
    INSERT INTO @WeekList (WID) SELECT WEEK_ID FROM #Weeks ORDER BY WEEK_ID;
    SET @TotalWeeks = @@ROWCOUNT;
    SELECT @FirstWk = WID FROM @WeekList WHERE Seq = 1;

    IF @Debug = 1 PRINT 'Step 1 (weeks): ' + CAST(@TotalWeeks AS VARCHAR) + ' weeks in '
        + CAST(DATEDIFF(MS, @StepStart, GETDATE()) AS VARCHAR) + 'ms';

    -- ==========================================================
    -- STEP 2: SHARED LOOKUPS (ONCE)
    -- ==========================================================
    SET @StepStart = GETDATE();

    -- Unpivot Sale Qty
    IF OBJECT_ID('tempdb..#SQ') IS NOT NULL DROP TABLE #SQ;
    SELECT [ST-CD] AS ST_CD, [MAJ-CAT] AS MAJ_CAT, WK_NAME, SALE_QTY
    INTO #SQ
    FROM dbo.QTY_SALE_QTY
    UNPIVOT (SALE_QTY FOR WK_NAME IN (
        [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],
        [WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],
        [WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
        [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],
        [WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],
        [WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]
    )) u;
    CREATE INDEX IX ON #SQ (ST_CD, MAJ_CAT, WK_NAME) INCLUDE (SALE_QTY);

    -- Unpivot Display Qty
    IF OBJECT_ID('tempdb..#DQ') IS NOT NULL DROP TABLE #DQ;
    SELECT [ST-CD] AS ST_CD, [MAJ-CAT] AS MAJ_CAT, WK_NAME, DISP_QTY
    INTO #DQ
    FROM dbo.QTY_DISP_QTY
    UNPIVOT (DISP_QTY FOR WK_NAME IN (
        [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],
        [WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],
        [WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
        [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],
        [WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],
        [WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]
    )) u;
    CREATE INDEX IX ON #DQ (ST_CD, MAJ_CAT, WK_NAME) INCLUDE (DISP_QTY);

    -- Latest store stock
    IF OBJECT_ID('tempdb..#LS') IS NOT NULL DROP TABLE #LS;
    SELECT ST_CD, MAJ_CAT, STK_QTY
    INTO #LS
    FROM (
        SELECT ST_CD, MAJ_CAT, STK_QTY,
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY [DATE] DESC) RN
        FROM dbo.QTY_ST_STK_Q
    ) r WHERE RN = 1;
    CREATE INDEX IX ON #LS (ST_CD, MAJ_CAT);

    IF @Debug = 1 PRINT 'Step 2 (lookups): ' + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- STEP 3: ALL STORE x CATEGORY COMBOS (ONE TABLE)
    -- ==========================================================
    SET @StepStart = GETDATE();

    IF OBJECT_ID('tempdb..#SC') IS NOT NULL DROP TABLE #SC;
    SELECT SM.[ST CD] AS ST_CD, SM.[ST NM] AS ST_NM,
           SM.[RDC_CD], SM.[RDC_NM], SM.[HUB_CD], SM.[HUB_NM], SM.[AREA],
           BC.[MAJ-CAT] AS MAJ_CAT, BC.[BIN CAP] AS BIN_CAP,
           ISNULL(BM.SEG, 'NA') AS SEG, ISNULL(BM.DIV, 'NA') AS DIV,
           ISNULL(BM.SUB_DIV, 'NA') AS SUB_DIV,
           ISNULL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM,
           ISNULL(BM.SSN, 'NA') AS SSN
    INTO #SC
    FROM dbo.MASTER_ST_MASTER SM
    CROSS JOIN dbo.MASTER_BIN_CAPACITY BC
    OUTER APPLY (
        SELECT TOP 1 BM2.SEG, BM2.DIV, BM2.SUB_DIV, BM2.MAJ_CAT_NM, BM2.SSN
        FROM dbo.MASTER_PRODUCT_HIERARCHY BM2 WHERE BM2.MAJ_CAT_NM = BC.[MAJ-CAT]
    ) BM
    WHERE (@MajCat IS NULL OR BC.[MAJ-CAT] = @MajCat);

    CREATE INDEX IX ON #SC (ST_CD, MAJ_CAT);

    DECLARE @totalCombos INT;
    SELECT @totalCombos = COUNT(*) FROM #SC;

    IF @Debug = 1 PRINT 'Step 3 (combos): ' + CAST(@totalCombos AS VARCHAR) + ' in '
        + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- STEP 4: BUILD BULK #Chain (ALL stores x categories x weeks)
    -- ==========================================================
    SET @StepStart = GETDATE();
    PRINT 'Building chain table: ' + CAST(@totalCombos AS VARCHAR) + ' combos x '
        + CAST(@TotalWeeks AS VARCHAR) + ' weeks = '
        + CAST(@totalCombos * @TotalWeeks AS VARCHAR) + ' rows...';

    IF OBJECT_ID('tempdb..#Chain') IS NOT NULL DROP TABLE #Chain;

    SELECT
        SC.ST_CD, SC.MAJ_CAT, W.WEEK_ID, SC.SSN,
        ISNULL(DQ.DISP_QTY, 0) + ISNULL(SQ1.SALE_QTY, 0) AS MBQ,
        ISNULL(SQ0.SALE_QTY, 0) AS SALE,
        ISNULL(LS.STK_QTY, 0) AS OP_STK,
        CAST(0 AS DECIMAL(18,4)) AS NET_CF,
        CAST(0 AS DECIMAL(18,4)) AS TRF_IN,
        CAST(0 AS DECIMAL(18,4)) AS CL_STK
    INTO #Chain
    FROM #SC SC
    CROSS JOIN #Weeks W
    LEFT JOIN #SQ SQ0 ON SQ0.ST_CD = SC.ST_CD AND SQ0.MAJ_CAT = SC.MAJ_CAT
        AND SQ0.WK_NAME = W.WK_COL_NAME
    LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1
    LEFT JOIN #SQ SQ1 ON SQ1.ST_CD = SC.ST_CD AND SQ1.MAJ_CAT = SC.MAJ_CAT
        AND SQ1.WK_NAME = W1.WK_COL_NAME
    LEFT JOIN #DQ DQ ON DQ.ST_CD = SC.ST_CD AND DQ.MAJ_CAT = SC.MAJ_CAT
        AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME)
    LEFT JOIN #LS LS ON LS.ST_CD = SC.ST_CD AND LS.MAJ_CAT = SC.MAJ_CAT;

    CREATE CLUSTERED INDEX CX ON #Chain (WEEK_ID, ST_CD, MAJ_CAT);

    PRINT 'Chain table: ' + CAST(@@ROWCOUNT AS VARCHAR) + ' rows in '
        + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- STEP 5: WEEK 1 CALCULATION (ALL combos at once)
    -- ==========================================================
    SET @StepStart = GETDATE();

    UPDATE #Chain SET NET_CF = CASE
        WHEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0)
            * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
        THEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0)
            * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
        ELSE 0 END
    WHERE WEEK_ID = @FirstWk;

    UPDATE #Chain SET
        TRF_IN = CASE WHEN MBQ = 0 AND SALE = 0 THEN 0
                      WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF ELSE 0 END,
        CL_STK = CASE WHEN MBQ = 0 AND SALE = 0 THEN NET_CF
                      WHEN MBQ + SALE > NET_CF THEN MBQ
                      ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END END
    WHERE WEEK_ID = @FirstWk;

    IF @Debug = 1 PRINT 'Step 5 (week 1): ' + CAST(DATEDIFF(MS, @StepStart, GETDATE()) AS VARCHAR) + 'ms';

    -- ==========================================================
    -- STEP 6: CHAIN WEEKS 2..N (ALL combos at once per week)
    -- This is 52 x 2 = 104 UPDATEs instead of 68,328
    -- ==========================================================
    SET @StepStart = GETDATE();
    PRINT 'Chaining ' + CAST(@TotalWeeks AS VARCHAR) + ' weeks (all stores in parallel)...';

    DECLARE @wi INT = 2, @tw INT, @pw INT;

    WHILE @wi <= @TotalWeeks
    BEGIN
        SELECT @tw = WID FROM @WeekList WHERE Seq = @wi;
        SELECT @pw = WID FROM @WeekList WHERE Seq = @wi - 1;

        -- 6a: OP_STK = prev.CL_STK, NET_CF from shrinkage
        UPDATE c SET
            c.OP_STK = p.CL_STK,
            c.NET_CF = CASE
                WHEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0)
                    * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
                THEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0)
                    * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
                ELSE 0 END
        FROM #Chain c
        INNER JOIN #Chain p ON p.ST_CD = c.ST_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.WEEK_ID = @pw
        WHERE c.WEEK_ID = @tw;

        -- 6b: TRF_IN + CL_STK
        UPDATE #Chain SET
            TRF_IN = CASE WHEN MBQ = 0 AND SALE = 0 THEN 0
                          WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF ELSE 0 END,
            CL_STK = CASE WHEN MBQ = 0 AND SALE = 0 THEN NET_CF
                          WHEN MBQ + SALE > NET_CF THEN MBQ
                          ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END END
        WHERE WEEK_ID = @tw;

        IF @Debug = 1 AND @wi % 10 = 0
            PRINT '  Week ' + CAST(@wi AS VARCHAR) + '/' + CAST(@TotalWeeks AS VARCHAR);

        SET @wi = @wi + 1;
    END

    PRINT 'Chaining done in ' + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- STEP 7: TRUNCATE + BULK INSERT INTO TRF_IN_PLAN
    -- ==========================================================
    SET @StepStart = GETDATE();
    PRINT 'Inserting into TRF_IN_PLAN...';

    TRUNCATE TABLE dbo.TRF_IN_PLAN;

    INSERT INTO dbo.TRF_IN_PLAN WITH (TABLOCK) (
        ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA,
        MAJ_CAT, SSN, WEEK_ID, WK_ST_DT, WK_END_DT, FY_YEAR, FY_WEEK,
        SEG, DIV, SUB_DIV, MAJ_CAT_NM,
        S_GRT_STK_Q, W_GRT_STK_Q, BGT_DISP_CL_Q, BGT_DISP_CL_OPT,
        CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY,
        BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ,
        BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q,
        CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q,
        TRF_IN_STK_Q, TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ, DC_MBQ,
        BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q,
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q
    )
    SELECT
        SC.ST_CD, SC.ST_NM, SC.RDC_CD, SC.RDC_NM, SC.HUB_CD, SC.HUB_NM, SC.AREA,
        SC.MAJ_CAT, SC.SSN, W.WEEK_ID, W.WK_ST_DT, W.WK_END_DT, W.FY_YEAR, W.FY_WEEK,
        SC.SEG, SC.DIV, SC.SUB_DIV, SC.MAJ_CAT_NM,

        -- S_GRT_STK_Q
        0,
        -- W_GRT_STK_Q
        CASE WHEN SC.SSN IN ('W','PW') THEN ISNULL(pch.CL_STK, 0) ELSE 0 END,
        -- BGT_DISP_CL_Q
        ISNULL(DQ.DISP_QTY, 0),
        -- BGT_DISP_CL_OPT
        CASE WHEN SC.BIN_CAP > 0 THEN ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0) ELSE 0 END,
        -- Cover days
        @CoverDaysCM1, @CoverDaysCM2,
        -- COVER_SALE_QTY
        ISNULL(SQ1.SALE_QTY, 0),
        -- BGT_ST_CL_MBQ
        ch.MBQ,
        -- BGT_DISP_CL_OPT_MBQ
        CASE WHEN SC.BIN_CAP > 0 AND ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0) > 0
            THEN ROUND(ch.MBQ * 1000.0 / NULLIF(ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0), 0), 0) ELSE 0 END,
        -- BGT_TTL_CF_OP_STK_Q
        ch.OP_STK,
        -- NT_ACT_Q
        ROUND(ROUND(ch.OP_STK * 0.08, 0) * CASE WHEN SC.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0),
        -- NET_BGT_CF_STK_Q
        ch.NET_CF,
        -- CM_BGT_SALE_Q, CM1, CM2
        ch.SALE,
        ISNULL(SQ1.SALE_QTY, 0),
        ISNULL(SQ2.SALE_QTY, 0),
        -- TRF_IN_STK_Q
        ch.TRF_IN,
        -- TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ (calculated after insert)
        0, 0,
        -- DC_MBQ = sum of next 4 weeks sale
        ISNULL(SQ1.SALE_QTY, 0) + ISNULL(SQN2.SALE_QTY, 0) + ISNULL(SQN3.SALE_QTY, 0) + ISNULL(SQN4.SALE_QTY, 0),
        -- BGT_TTL_CF_CL_STK_Q
        ch.CL_STK,
        -- BGT_NT_ACT_Q
        0,
        -- NET_ST_CL_STK_Q
        ch.CL_STK,
        -- ST_CL_EXCESS_Q
        CASE WHEN ch.CL_STK - ch.MBQ > 0 THEN ch.CL_STK - ch.MBQ ELSE 0 END,
        -- ST_CL_SHORT_Q
        CASE WHEN ch.MBQ - ch.CL_STK > 0 THEN ch.MBQ - ch.CL_STK ELSE 0 END

    FROM #Chain ch
    INNER JOIN #SC SC ON SC.ST_CD = ch.ST_CD AND SC.MAJ_CAT = ch.MAJ_CAT
    INNER JOIN #Weeks W ON W.WEEK_ID = ch.WEEK_ID
    -- Previous week for W_GRT_STK_Q
    LEFT JOIN @WeekList wl ON wl.WID = ch.WEEK_ID
    LEFT JOIN @WeekList wlp ON wlp.Seq = wl.Seq - 1
    LEFT JOIN #Chain pch ON pch.ST_CD = ch.ST_CD AND pch.MAJ_CAT = ch.MAJ_CAT AND pch.WEEK_ID = wlp.WID
    -- Sale lookups
    LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1
    LEFT JOIN #SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT AND SQ1.WK_NAME = W1.WK_COL_NAME
    LEFT JOIN #Weeks W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8
    LEFT JOIN #SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT AND SQ2.WK_NAME = W2.WK_COL_NAME
    -- Next 2/3/4 week sale for DC_MBQ
    LEFT JOIN #Weeks WN2 ON WN2.WEEK_SEQ = W.WEEK_SEQ + 2
    LEFT JOIN #SQ SQN2 ON SQN2.ST_CD = ch.ST_CD AND SQN2.MAJ_CAT = ch.MAJ_CAT AND SQN2.WK_NAME = WN2.WK_COL_NAME
    LEFT JOIN #Weeks WN3 ON WN3.WEEK_SEQ = W.WEEK_SEQ + 3
    LEFT JOIN #SQ SQN3 ON SQN3.ST_CD = ch.ST_CD AND SQN3.MAJ_CAT = ch.MAJ_CAT AND SQN3.WK_NAME = WN3.WK_COL_NAME
    LEFT JOIN #Weeks WN4 ON WN4.WEEK_SEQ = W.WEEK_SEQ + 4
    LEFT JOIN #SQ SQN4 ON SQN4.ST_CD = ch.ST_CD AND SQN4.MAJ_CAT = ch.MAJ_CAT AND SQN4.WK_NAME = WN4.WK_COL_NAME
    -- Display
    LEFT JOIN #DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT
        AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME);

    SET @trfRows = @@ROWCOUNT;

    -- Post-insert: TRF_IN_OPT_CNT and TRF_IN_OPT_MBQ
    UPDATE dbo.TRF_IN_PLAN
    SET TRF_IN_OPT_CNT = CASE
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0
            THEN ROUND(TRF_IN_STK_Q * 1000.0 / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0) ELSE 0 END,
        TRF_IN_OPT_MBQ = CASE
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0
            THEN ISNULL(TRF_IN_STK_Q * 1000.0
                / NULLIF(ROUND(TRF_IN_STK_Q * 1000.0 / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0), 0), 0) ELSE 0 END;

    PRINT 'TRF_IN_PLAN: ' + CAST(@trfRows AS VARCHAR) + ' rows in '
        + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- STEP 8: PURCHASE PLAN (single call, all RDCs at once)
    -- ==========================================================
    SET @StepStart = GETDATE();
    PRINT 'Running Purchase Plan (all RDCs at once)...';

    EXEC dbo.SP_GENERATE_PURCHASE_PLAN
        @StartWeekID = @StartWeekID,
        @EndWeekID = @EndWeekID,
        @RdcCode = NULL,
        @MajCat = @MajCat,
        @Debug = @Debug;

    SELECT @ppRows = COUNT(*) FROM dbo.PURCHASE_PLAN;
    PRINT 'PURCHASE_PLAN: ' + CAST(@ppRows AS VARCHAR) + ' rows in '
        + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';

    -- ==========================================================
    -- CLEANUP & SUMMARY
    -- ==========================================================
    DROP TABLE IF EXISTS #Weeks, #SQ, #DQ, #LS, #SC, #Chain;

    DECLARE @totalTime INT = DATEDIFF(SECOND, @MasterStart, GETDATE());

    PRINT '';
    PRINT '========================================================';
    PRINT '  COMPLETE: ' + CAST(@trfRows AS VARCHAR) + ' TRF + ' + CAST(@ppRows AS VARCHAR) + ' PP rows';
    PRINT '  Total: ' + CAST(@totalTime AS VARCHAR) + ' seconds (' + CAST(@totalTime/60 AS VARCHAR) + 'm ' + CAST(@totalTime%60 AS VARCHAR) + 's)';
    PRINT '========================================================';

    SELECT @trfRows AS TRF_Rows, @ppRows AS PP_Rows, @totalTime AS Total_Seconds;
END;
GO

PRINT 'SP_RUN_ALL_PLANS V3 (BULK) created.';
GO
