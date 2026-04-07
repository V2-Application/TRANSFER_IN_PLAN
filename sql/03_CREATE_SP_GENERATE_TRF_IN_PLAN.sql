/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 3: SP_GENERATE_TRF_IN_PLAN

  V6 - FIXED & FAST VERSION
  Strategy:
    1. Build LEAN chaining table (10 cols: +NET_CF, +TRF_IN)
    2. Chain weeks: 2 UPDATEs per week on lean table
    3. Single JOIN to produce full output in one INSERT
    4. No nested CASE expressions - clean, readable logic

  Expected: seconds, not minutes.
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.SP_GENERATE_TRF_IN_PLAN','P') IS NOT NULL
    DROP PROCEDURE dbo.SP_GENERATE_TRF_IN_PLAN;
GO

CREATE PROCEDURE dbo.SP_GENERATE_TRF_IN_PLAN
    @StartWeekID    INT,
    @EndWeekID      INT,
    @StoreCode      VARCHAR(20)  = NULL,
    @MajCat         VARCHAR(50)  = NULL,
    @CoverDaysCM1   DECIMAL(18,4) = 14,
    @CoverDaysCM2   DECIMAL(18,4) = 0,
    @Debug          BIT = 0
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RowCount BIGINT = 0;
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @StepTime  DATETIME;

    IF @Debug = 1
        PRINT '>> V6-SPEED started at ' + CONVERT(VARCHAR, @StartTime, 120);

    -----------------------------------------------------------
    -- STEP 0: Validate
    -----------------------------------------------------------
    IF NOT EXISTS (SELECT 1 FROM dbo.WEEK_CALENDAR WHERE WEEK_ID = @StartWeekID)
    BEGIN
        RAISERROR('Invalid @StartWeekID: %d', 16, 1, @StartWeekID);
        RETURN;
    END
    IF NOT EXISTS (SELECT 1 FROM dbo.WEEK_CALENDAR WHERE WEEK_ID = @EndWeekID)
    BEGIN
        RAISERROR('Invalid @EndWeekID: %d', 16, 1, @EndWeekID);
        RETURN;
    END

    -----------------------------------------------------------
    -- STEP 1: Weeks
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    IF OBJECT_ID('tempdb..#Weeks') IS NOT NULL DROP TABLE #Weeks;

    SELECT
        W.WEEK_ID, W.WEEK_SEQ, W.FY_WEEK, W.FY_YEAR,
        W.WK_ST_DT, W.WK_END_DT,
        'WK-' + CAST(W.FY_WEEK AS VARCHAR) AS WK_COL_NAME
    INTO #Weeks
    FROM dbo.WEEK_CALENDAR W
    WHERE W.WEEK_ID BETWEEN @StartWeekID AND @EndWeekID;

    CREATE UNIQUE CLUSTERED INDEX CX ON #Weeks (WEEK_ID);
    CREATE UNIQUE INDEX IX_SEQ ON #Weeks (WEEK_SEQ);

    DECLARE @WeekList TABLE (Seq INT IDENTITY(1,1) PRIMARY KEY, WID INT);
    INSERT INTO @WeekList (WID) SELECT WEEK_ID FROM #Weeks ORDER BY WEEK_ID;

    DECLARE @TotalWeeks INT = @@ROWCOUNT;
    DECLARE @FirstWk INT = (SELECT WID FROM @WeekList WHERE Seq = 1);

    IF @Debug = 1
    BEGIN
        SELECT @TotalWeeks AS WeekCount;
        PRINT '>> Step 1: ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';
    END

    -----------------------------------------------------------
    -- STEP 2: Store x Category combos
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    IF OBJECT_ID('tempdb..#SC') IS NOT NULL DROP TABLE #SC;

    SELECT
        SM.[ST CD] AS ST_CD, SM.[ST NM] AS ST_NM,
        SM.[RDC_CD], SM.[RDC_NM], SM.[HUB_CD], SM.[HUB_NM], SM.[AREA],
        BC.[MAJ-CAT] AS MAJ_CAT, BC.[BIN CAP] AS BIN_CAP,
        ISNULL(BM.SEG, 'NA') AS SEG,
        ISNULL(BM.DIV, 'NA') AS DIV,
        ISNULL(BM.SUB_DIV, 'NA') AS SUB_DIV,
        ISNULL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM,
        ISNULL(BM.SSN, 'NA') AS SSN
    INTO #SC
    FROM dbo.MASTER_ST_MASTER SM
    CROSS JOIN dbo.MASTER_BIN_CAPACITY BC
    OUTER APPLY (
        SELECT TOP 1 BM.SEG, BM.DIV, BM.SUB_DIV, BM.MAJ_CAT_NM, BM.SSN
        FROM dbo.MASTER_PRODUCT_HIERARCHY BM
        WHERE BM.MAJ_CAT_NM = BC.[MAJ-CAT]
    ) BM
    WHERE (@StoreCode IS NULL OR SM.[ST CD] = @StoreCode)
      AND (@MajCat    IS NULL OR BC.[MAJ-CAT] = @MajCat);

    CREATE INDEX IX ON #SC (ST_CD, MAJ_CAT);

    IF @Debug = 1
    BEGIN
        SELECT COUNT(*) AS StoreCatCombos FROM #SC;
        PRINT '>> Step 2: ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';
    END

    -----------------------------------------------------------
    -- STEP 3: Unpivot sales & display (shared lookups)
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

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
    CREATE INDEX IX ON #SQ (ST_CD, MAJ_CAT, WK_NAME);

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
    CREATE INDEX IX ON #DQ (ST_CD, MAJ_CAT, WK_NAME);

    IF OBJECT_ID('tempdb..#LS') IS NOT NULL DROP TABLE #LS;
    SELECT ST_CD, MAJ_CAT, STK_QTY
    INTO #LS
    FROM (
        SELECT ST_CD, MAJ_CAT, STK_QTY,
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY [DATE] DESC) RN
        FROM dbo.QTY_ST_STK_Q
    ) r WHERE RN = 1;
    CREATE INDEX IX ON #LS (ST_CD, MAJ_CAT);

    IF @Debug = 1
        PRINT '>> Step 3 (lookups): ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

    -----------------------------------------------------------
    -- STEP 4: Build LEAN chaining table
    -- 10 columns: ST_CD, MAJ_CAT, WEEK_ID, SSN, MBQ, SALE,
    --             OP_STK, NET_CF, TRF_IN, CL_STK
    -- ~257K combos x 52 weeks = ~13M rows x ~80 bytes = ~1GB
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    IF OBJECT_ID('tempdb..#Chain') IS NOT NULL DROP TABLE #Chain;

    SELECT
        SC.ST_CD, SC.MAJ_CAT, W.WEEK_ID,
        SC.SSN,
        -- MBQ (static per week, not chained)
        ISNULL(DQ.DISP_QTY, 0) + ISNULL(SQ1.SALE_QTY, 0) AS MBQ,
        -- Current week sale (static)
        ISNULL(SQ0.SALE_QTY, 0) AS SALE,
        -- Opening stock (chained: week N OP = week N-1 CL)
        ISNULL(LS.STK_QTY, 0)   AS OP_STK,
        -- NET_CF: NET carry-forward stock after shrinkage
        CAST(0 AS DECIMAL(18,4)) AS NET_CF,
        -- TRF_IN: Transfer in quantity
        CAST(0 AS DECIMAL(18,4)) AS TRF_IN,
        -- Closing stock (calculated from above)
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

    -- THE critical index for chaining speed
    CREATE CLUSTERED INDEX CX ON #Chain (ST_CD, MAJ_CAT, WEEK_ID);

    IF @Debug = 1
    BEGIN
        SELECT COUNT(*) AS ChainRows FROM #Chain;
        PRINT '>> Step 4 (lean table): ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';
    END

    -----------------------------------------------------------
    -- STEP 5: Calculate Week 1
    -- Split into 2 clean UPDATEs (no nesting!)
    --   5a: NET_CF = MAX(OP_STK - shrinkage, 0)
    --   5b: TRF_IN + CL_STK from NET_CF
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    -- 5a: Compute NET carry-forward after shrinkage
    UPDATE #Chain
    SET NET_CF = CASE
        WHEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0)
            * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
        THEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0)
            * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
        ELSE 0
        END
    WHERE WEEK_ID = @FirstWk;

    -- 5b: Compute TRF_IN and CL_STK (both depend only on NET_CF, MBQ, SALE)
    UPDATE #Chain
    SET
        TRF_IN = CASE
            WHEN MBQ = 0 AND SALE = 0 THEN 0
            WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF
            ELSE 0
            END,
        CL_STK = CASE
            WHEN MBQ = 0 AND SALE = 0 THEN NET_CF
            WHEN MBQ + SALE > NET_CF THEN MBQ
            ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END
            END
    WHERE WEEK_ID = @FirstWk;

    IF @Debug = 1
        PRINT '>> Step 5 (week 1): ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

    -----------------------------------------------------------
    -- STEP 6: Chain weeks 2..N
    -- TWO UPDATEs per week on lean 10-col table = FAST
    --   6a: OP_STK = prev.CL_STK, NET_CF from OP_STK
    --   6b: TRF_IN + CL_STK from NET_CF
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    DECLARE @i INT = 2, @tw INT, @pw INT;

    WHILE @i <= @TotalWeeks
    BEGIN
        SELECT @tw = WID FROM @WeekList WHERE Seq = @i;
        SELECT @pw = WID FROM @WeekList WHERE Seq = @i - 1;

        -- 6a: Set OP_STK from previous week's CL_STK, compute NET_CF
        UPDATE c
        SET
            c.OP_STK = p.CL_STK,
            c.NET_CF = CASE
                WHEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0)
                    * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
                THEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0)
                    * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0)
                ELSE 0
                END
        FROM #Chain c
        INNER JOIN #Chain p ON p.ST_CD = c.ST_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.WEEK_ID = @pw
        WHERE c.WEEK_ID = @tw;

        -- 6b: Compute TRF_IN and CL_STK (uses NET_CF just set above)
        UPDATE #Chain
        SET
            TRF_IN = CASE
                WHEN MBQ = 0 AND SALE = 0 THEN 0
                WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF
                ELSE 0
                END,
            CL_STK = CASE
                WHEN MBQ = 0 AND SALE = 0 THEN NET_CF
                WHEN MBQ + SALE > NET_CF THEN MBQ
                ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END
                END
        WHERE WEEK_ID = @tw;

        SET @i = @i + 1;
    END

    IF @Debug = 1
        PRINT '>> Step 6 (chain ' + CAST(@TotalWeeks AS VARCHAR) + ' wks): ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

    -----------------------------------------------------------
    -- STEP 7: Delete old data & INSERT full result
    -- Single JOIN from #Chain + #SC + #Weeks to produce all cols
    -----------------------------------------------------------
    SET @StepTime = GETDATE();

    DELETE FROM dbo.TRF_IN_PLAN
    WHERE WEEK_ID BETWEEN @StartWeekID AND @EndWeekID
      AND (@StoreCode IS NULL OR ST_CD = @StoreCode)
      AND (@MajCat    IS NULL OR MAJ_CAT = @MajCat);

    INSERT INTO dbo.TRF_IN_PLAN WITH (TABLOCK) (
        ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA,
        MAJ_CAT, SSN, WEEK_ID, WK_ST_DT, WK_END_DT, FY_YEAR, FY_WEEK,
        SEG, DIV, SUB_DIV, MAJ_CAT_NM,
        S_GRT_STK_Q, W_GRT_STK_Q,
        BGT_DISP_CL_Q, BGT_DISP_CL_OPT,
        CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY,
        BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ,
        BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q,
        CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q,
        TRF_IN_STK_Q, TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ,
        DC_MBQ,
        BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q,
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q
    )
    SELECT
        SC.ST_CD, SC.ST_NM, SC.RDC_CD, SC.RDC_NM, SC.HUB_CD, SC.HUB_NM, SC.AREA,
        SC.MAJ_CAT,
        SC.SSN, W.WEEK_ID, W.WK_ST_DT, W.WK_END_DT, W.FY_YEAR, W.FY_WEEK,
        SC.SEG, SC.DIV, SC.SUB_DIV, SC.MAJ_CAT_NM,

        -- S_GRT_STK_Q
        CAST(0 AS DECIMAL(18,4)),

        -- W_GRT_STK_Q (uses prev week CL from chain)
        CASE WHEN SC.SSN IN ('W','PW') THEN ISNULL(prev_ch.CL_STK, 0) ELSE 0 END,

        -- BGT_DISP_CL_Q
        ISNULL(DQ.DISP_QTY, 0),

        -- BGT_DISP_CL_OPT
        CASE WHEN SC.BIN_CAP > 0
            THEN ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0)
            ELSE 0 END,

        -- Cover days
        @CoverDaysCM1, @CoverDaysCM2,

        -- COVER_SALE_QTY
        ISNULL(SQ1.SALE_QTY, 0),

        -- BGT_ST_CL_MBQ = ch.MBQ
        ch.MBQ,

        -- BGT_DISP_CL_OPT_MBQ
        CASE WHEN SC.BIN_CAP > 0
                AND ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0) > 0
            THEN ROUND(ch.MBQ * 1000.0
                / NULLIF(ROUND(ISNULL(DQ.DISP_QTY,0) * 1000.0 / SC.BIN_CAP, 0), 0), 0)
            ELSE 0 END,

        -- BGT_TTL_CF_OP_STK_Q (from chain)
        ch.OP_STK,

        -- NT_ACT_Q (shrinkage)
        ROUND(ROUND(ch.OP_STK * 0.08, 0)
            * CASE WHEN SC.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0),

        -- NET_BGT_CF_STK_Q (from chain)
        ch.NET_CF,

        -- CM_BGT_SALE_Q, CM1, CM2
        ch.SALE,
        ISNULL(SQ1.SALE_QTY, 0),
        ISNULL(SQ2.SALE_QTY, 0),

        -- TRF_IN_STK_Q (from chain)
        ch.TRF_IN,

        -- TRF_IN_OPT_CNT (computed from TRF_IN and OPT_MBQ)
        CAST(0 AS DECIMAL(18,4)),  -- will update below
        CAST(0 AS DECIMAL(18,4)),  -- will update below

        -- DC_MBQ = sum of next 4 weeks sale
        ISNULL(SQ1.SALE_QTY, 0) + ISNULL(SQN2.SALE_QTY, 0) + ISNULL(SQN3.SALE_QTY, 0) + ISNULL(SQN4.SALE_QTY, 0),

        -- BGT_TTL_CF_CL_STK_Q (from chain)
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

    -- Previous week chain for W_GRT_STK_Q
    LEFT JOIN @WeekList wl ON wl.WID = ch.WEEK_ID
    LEFT JOIN @WeekList wlp ON wlp.Seq = wl.Seq - 1
    LEFT JOIN #Chain prev_ch ON prev_ch.ST_CD = ch.ST_CD
        AND prev_ch.MAJ_CAT = ch.MAJ_CAT AND prev_ch.WEEK_ID = wlp.WID

    -- Sale lookups for CM1, CM2
    LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1
    LEFT JOIN #SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT
        AND SQ1.WK_NAME = W1.WK_COL_NAME
    LEFT JOIN #Weeks W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8
    LEFT JOIN #SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT
        AND SQ2.WK_NAME = W2.WK_COL_NAME

    -- Next 2/3/4 week sale for DC_MBQ
    LEFT JOIN #Weeks WN2 ON WN2.WEEK_SEQ = W.WEEK_SEQ + 2
    LEFT JOIN #SQ SQN2 ON SQN2.ST_CD = ch.ST_CD AND SQN2.MAJ_CAT = ch.MAJ_CAT
        AND SQN2.WK_NAME = WN2.WK_COL_NAME
    LEFT JOIN #Weeks WN3 ON WN3.WEEK_SEQ = W.WEEK_SEQ + 3
    LEFT JOIN #SQ SQN3 ON SQN3.ST_CD = ch.ST_CD AND SQN3.MAJ_CAT = ch.MAJ_CAT
        AND SQN3.WK_NAME = WN3.WK_COL_NAME
    LEFT JOIN #Weeks WN4 ON WN4.WEEK_SEQ = W.WEEK_SEQ + 4
    LEFT JOIN #SQ SQN4 ON SQN4.ST_CD = ch.ST_CD AND SQN4.MAJ_CAT = ch.MAJ_CAT
        AND SQN4.WK_NAME = WN4.WK_COL_NAME

    -- Display
    LEFT JOIN #DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT
        AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME);

    SET @RowCount = @@ROWCOUNT;

    -- Quick update for TRF_IN_OPT_CNT and TRF_IN_OPT_MBQ
    UPDATE dbo.TRF_IN_PLAN
    SET TRF_IN_OPT_CNT = CASE
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0
            THEN ROUND(TRF_IN_STK_Q * 1000.0 / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0)
            ELSE 0 END,
        TRF_IN_OPT_MBQ = CASE
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0
            THEN ISNULL(TRF_IN_STK_Q * 1000.0
                / NULLIF(ROUND(TRF_IN_STK_Q * 1000.0
                    / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0), 0), 0)
            ELSE 0 END
    WHERE WEEK_ID BETWEEN @StartWeekID AND @EndWeekID
      AND (@StoreCode IS NULL OR ST_CD = @StoreCode)
      AND (@MajCat    IS NULL OR MAJ_CAT = @MajCat);

    IF @Debug = 1
        PRINT '>> Step 7 (insert): ' + CAST(DATEDIFF(MS, @StepTime, GETDATE()) AS VARCHAR) + 'ms';

    -----------------------------------------------------------
    -- DONE
    -----------------------------------------------------------
    DROP TABLE IF EXISTS #Weeks, #SC, #SQ, #DQ, #LS, #Chain;

    IF @Debug = 1
        PRINT '>> TOTAL: ' + CAST(DATEDIFF(SECOND, @StartTime, GETDATE()) AS VARCHAR) + ' seconds, '
            + CAST(@RowCount AS VARCHAR) + ' rows';

    SELECT
        @RowCount AS RowsInserted,
        @StartWeekID AS StartWeek,
        @EndWeekID AS EndWeek,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS ExecutionSeconds;
END;
GO

PRINT '>> SP_GENERATE_TRF_IN_PLAN (V6-SPEED) created successfully.';
GO
