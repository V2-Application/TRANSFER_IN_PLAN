/*==============================================================
  SP_RUN_ALL_PLANS - OPTIMIZED VERSION

  Strategy: Build shared lookups ONCE, loop only the tiny per-store
  chaining (383 cats x 52 weeks = 20K rows per store).

  Before: 657 stores x 25s = 4.5 hours
  After:  15s setup + 657 stores x 2-3s = ~25-35 minutes
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

    DECLARE @MasterStart DATETIME;
    DECLARE @StepStart DATETIME;
    DECLARE @i INT, @total INT, @code VARCHAR(20);
    DECLARE @wi INT, @tw2 INT, @pw2 INT;
    DECLARE @el INT, @eta VARCHAR(30);
    DECLARE @TotalWeeks INT, @FirstWk INT;
    DECLARE @trfTime INT, @trfRows BIGINT;
    DECLARE @ppTime INT, @ppRows BIGINT;
    DECLARE @totalTime INT;

    SET @MasterStart = GETDATE();

    PRINT '========================================================';
    PRINT '  OPTIMIZED PLAN GENERATION - ALL STORES & RDCs';
    PRINT '  Started: ' + CONVERT(VARCHAR, @MasterStart, 120);
    PRINT '========================================================';

    -- ==========================================================
    -- PHASE 0: BUILD SHARED LOOKUPS (ONCE)
    -- ==========================================================
    SET @StepStart = GETDATE();
    PRINT '>>> Building shared lookup tables (one-time)...';

    -- Weeks
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

    -- Unpivot Sale Qty (ONCE for all stores)
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

    -- Unpivot Display Qty (ONCE)
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

    -- Latest store stock (ONCE)
    IF OBJECT_ID('tempdb..#LS') IS NOT NULL DROP TABLE #LS;
    SELECT ST_CD, MAJ_CAT, STK_QTY
    INTO #LS
    FROM (
        SELECT ST_CD, MAJ_CAT, STK_QTY,
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY [DATE] DESC) RN
        FROM dbo.QTY_ST_STK_Q
    ) r WHERE RN = 1;
    CREATE INDEX IX ON #LS (ST_CD, MAJ_CAT);

    -- Category master (ONCE)
    IF OBJECT_ID('tempdb..#Cats') IS NOT NULL DROP TABLE #Cats;
    SELECT BC.[MAJ-CAT] AS MAJ_CAT, BC.[BIN CAP] AS BIN_CAP,
           ISNULL(BM.SEG, 'NA') AS SEG, ISNULL(BM.DIV, 'NA') AS DIV,
           ISNULL(BM.SUB_DIV, 'NA') AS SUB_DIV,
           ISNULL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM,
           ISNULL(BM.SSN, 'NA') AS SSN
    INTO #Cats
    FROM dbo.MASTER_BIN_CAPACITY BC
    OUTER APPLY (
        SELECT TOP 1 BM.SEG, BM.DIV, BM.SUB_DIV, BM.MAJ_CAT_NM, BM.SSN
        FROM dbo.MASTER_PRODUCT_HIERARCHY BM WHERE BM.MAJ_CAT_NM = BC.[MAJ-CAT]
    ) BM
    WHERE (@MajCat IS NULL OR BC.[MAJ-CAT] = @MajCat);
    CREATE UNIQUE INDEX IX ON #Cats (MAJ_CAT);

    PRINT '  Lookup tables built in ' + CAST(DATEDIFF(SECOND, @StepStart, GETDATE()) AS VARCHAR) + 's';
    DECLARE @cnt INT;
    SELECT @cnt = COUNT(*) FROM #SQ;  PRINT '  #SQ rows: ' + CAST(@cnt AS VARCHAR);
    SELECT @cnt = COUNT(*) FROM #DQ;  PRINT '  #DQ rows: ' + CAST(@cnt AS VARCHAR);
    SELECT @cnt = COUNT(*) FROM #Cats; PRINT '  Categories: ' + CAST(@cnt AS VARCHAR);
    PRINT '';

    -- ==========================================================
    -- PHASE 1: TRANSFER IN PLAN (per store, using shared lookups)
    -- ==========================================================
    PRINT '>>> PHASE 1: TRANSFER IN PLAN <<<';

    DECLARE @Stores TABLE (Seq INT IDENTITY(1,1) PRIMARY KEY, ST_CD VARCHAR(20));
    INSERT INTO @Stores (ST_CD)
    SELECT DISTINCT [ST CD] FROM dbo.MASTER_ST_MASTER ORDER BY [ST CD];
    SET @total = @@ROWCOUNT;
    PRINT 'Processing ' + CAST(@total AS VARCHAR) + ' stores...';

    SET @StepStart = GETDATE();
    SET @i = 1;
    WHILE @i <= @total
    BEGIN
        SELECT @code = ST_CD FROM @Stores WHERE Seq = @i;

        -- Progress every 25 stores
        IF @i = 1 OR @i % 25 = 0 OR @i = @total
        BEGIN
            SET @el = DATEDIFF(SECOND, @StepStart, GETDATE());
            SET @eta = CASE WHEN @i > 1
                THEN CAST(CAST((@el * 1.0 / (@i-1)) * (@total-@i+1) AS INT) AS VARCHAR) + 's'
                ELSE '...' END;
            PRINT '  [' + CAST(@i AS VARCHAR) + '/' + CAST(@total AS VARCHAR) + '] '
                + @code + ' | ' + CAST(@el AS VARCHAR) + 's elapsed | ETA: ' + @eta;
        END

        -- Build per-store #SC (tiny: ~383 rows)
        IF OBJECT_ID('tempdb..#SC') IS NOT NULL DROP TABLE #SC;
        SELECT SM.[ST CD] AS ST_CD, SM.[ST NM] AS ST_NM,
               SM.[RDC_CD], SM.[RDC_NM], SM.[HUB_CD], SM.[HUB_NM], SM.[AREA],
               C.MAJ_CAT, C.BIN_CAP, C.SEG, C.DIV, C.SUB_DIV, C.MAJ_CAT_NM, C.SSN
        INTO #SC
        FROM dbo.MASTER_ST_MASTER SM
        CROSS JOIN #Cats C
        WHERE SM.[ST CD] = @code;

        -- Build #Chain (tiny: ~383 x 52 = 20K rows)
        IF OBJECT_ID('tempdb..#Chain') IS NOT NULL DROP TABLE #Chain;
        SELECT SC.ST_CD, SC.MAJ_CAT, W.WEEK_ID, SC.SSN,
               ISNULL(DQ.DISP_QTY, 0) + ISNULL(SQ1.SALE_QTY, 0) AS MBQ,
               ISNULL(SQ0.SALE_QTY, 0) AS SALE,
               ISNULL(LS.STK_QTY, 0) AS OP_STK,
               CAST(0 AS DECIMAL(18,4)) AS NET_CF,
               CAST(0 AS DECIMAL(18,4)) AS TRF_IN,
               CAST(0 AS DECIMAL(18,4)) AS CL_STK
        INTO #Chain
        FROM #SC SC
        CROSS JOIN #Weeks W
        LEFT JOIN #SQ SQ0 ON SQ0.ST_CD = SC.ST_CD AND SQ0.MAJ_CAT = SC.MAJ_CAT AND SQ0.WK_NAME = W.WK_COL_NAME
        LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1
        LEFT JOIN #SQ SQ1 ON SQ1.ST_CD = SC.ST_CD AND SQ1.MAJ_CAT = SC.MAJ_CAT AND SQ1.WK_NAME = W1.WK_COL_NAME
        LEFT JOIN #DQ DQ ON DQ.ST_CD = SC.ST_CD AND DQ.MAJ_CAT = SC.MAJ_CAT AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME)
        LEFT JOIN #LS LS ON LS.ST_CD = SC.ST_CD AND LS.MAJ_CAT = SC.MAJ_CAT;

        CREATE CLUSTERED INDEX CX ON #Chain (ST_CD, MAJ_CAT, WEEK_ID);

        -- Week 1 calculations
        UPDATE #Chain SET NET_CF = CASE
            WHEN OP_STK - ROUND(ROUND(OP_STK*0.08,0) * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
            THEN OP_STK - ROUND(ROUND(OP_STK*0.08,0) * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) ELSE 0 END
        WHERE WEEK_ID = @FirstWk;

        UPDATE #Chain SET
            TRF_IN = CASE WHEN MBQ=0 AND SALE=0 THEN 0 WHEN MBQ+SALE-NET_CF > 0 THEN MBQ+SALE-NET_CF ELSE 0 END,
            CL_STK = CASE WHEN MBQ=0 AND SALE=0 THEN NET_CF WHEN MBQ+SALE > NET_CF THEN MBQ ELSE CASE WHEN NET_CF-SALE > 0 THEN NET_CF-SALE ELSE 0 END END
        WHERE WEEK_ID = @FirstWk;

        -- Chain weeks 2..N
        SET @wi = 2;
        WHILE @wi <= @TotalWeeks
        BEGIN
            SELECT @tw2 = WID FROM @WeekList WHERE Seq = @wi;
            SELECT @pw2 = WID FROM @WeekList WHERE Seq = @wi - 1;

            UPDATE c SET c.OP_STK = p.CL_STK,
                c.NET_CF = CASE WHEN p.CL_STK - ROUND(ROUND(p.CL_STK*0.08,0) * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0
                    THEN p.CL_STK - ROUND(ROUND(p.CL_STK*0.08,0) * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) ELSE 0 END
            FROM #Chain c INNER JOIN #Chain p ON p.ST_CD = c.ST_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.WEEK_ID = @pw2
            WHERE c.WEEK_ID = @tw2;

            UPDATE #Chain SET
                TRF_IN = CASE WHEN MBQ=0 AND SALE=0 THEN 0 WHEN MBQ+SALE-NET_CF > 0 THEN MBQ+SALE-NET_CF ELSE 0 END,
                CL_STK = CASE WHEN MBQ=0 AND SALE=0 THEN NET_CF WHEN MBQ+SALE > NET_CF THEN MBQ ELSE CASE WHEN NET_CF-SALE > 0 THEN NET_CF-SALE ELSE 0 END END
            WHERE WEEK_ID = @tw2;

            SET @wi = @wi + 1;
        END

        -- Delete old data for this store
        DELETE FROM dbo.TRF_IN_PLAN WHERE ST_CD = @code AND WEEK_ID BETWEEN @StartWeekID AND @EndWeekID;

        -- INSERT final result
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
        SELECT SC.ST_CD, SC.ST_NM, SC.RDC_CD, SC.RDC_NM, SC.HUB_CD, SC.HUB_NM, SC.AREA,
            SC.MAJ_CAT, SC.SSN, W.WEEK_ID, W.WK_ST_DT, W.WK_END_DT, W.FY_YEAR, W.FY_WEEK,
            SC.SEG, SC.DIV, SC.SUB_DIV, SC.MAJ_CAT_NM,
            0, CASE WHEN SC.SSN IN ('W','PW') THEN ISNULL(pch.CL_STK,0) ELSE 0 END,
            ISNULL(DQ.DISP_QTY,0),
            CASE WHEN SC.BIN_CAP > 0 THEN ROUND(ISNULL(DQ.DISP_QTY,0)*1000.0/SC.BIN_CAP,0) ELSE 0 END,
            @CoverDaysCM1, @CoverDaysCM2, ISNULL(SQ1.SALE_QTY,0),
            ch.MBQ,
            CASE WHEN SC.BIN_CAP > 0 AND ROUND(ISNULL(DQ.DISP_QTY,0)*1000.0/SC.BIN_CAP,0) > 0
                THEN ROUND(ch.MBQ*1000.0/NULLIF(ROUND(ISNULL(DQ.DISP_QTY,0)*1000.0/SC.BIN_CAP,0),0),0) ELSE 0 END,
            ch.OP_STK,
            ROUND(ROUND(ch.OP_STK*0.08,0)*CASE WHEN SC.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END,0),
            ch.NET_CF, ch.SALE, ISNULL(SQ1.SALE_QTY,0), ISNULL(SQ2.SALE_QTY,0),
            ch.TRF_IN, 0, 0,
            CASE WHEN ROUND(ISNULL(SQ1.SALE_QTY,0)/30.0*15,0) > 0 THEN ROUND(ISNULL(SQ1.SALE_QTY,0)/30.0*15,0) ELSE 0 END,
            ch.CL_STK, 0, ch.CL_STK,
            CASE WHEN ch.CL_STK - ch.MBQ > 0 THEN ch.CL_STK - ch.MBQ ELSE 0 END,
            CASE WHEN ch.MBQ - ch.CL_STK > 0 THEN ch.MBQ - ch.CL_STK ELSE 0 END
        FROM #Chain ch
        INNER JOIN #SC SC ON SC.ST_CD = ch.ST_CD AND SC.MAJ_CAT = ch.MAJ_CAT
        INNER JOIN #Weeks W ON W.WEEK_ID = ch.WEEK_ID
        LEFT JOIN @WeekList wl ON wl.WID = ch.WEEK_ID
        LEFT JOIN @WeekList wlp ON wlp.Seq = wl.Seq - 1
        LEFT JOIN #Chain pch ON pch.ST_CD = ch.ST_CD AND pch.MAJ_CAT = ch.MAJ_CAT AND pch.WEEK_ID = wlp.WID
        LEFT JOIN #Weeks W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1
        LEFT JOIN #SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT AND SQ1.WK_NAME = W1.WK_COL_NAME
        LEFT JOIN #Weeks W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8
        LEFT JOIN #SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT AND SQ2.WK_NAME = W2.WK_COL_NAME
        LEFT JOIN #DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT AND DQ.WK_NAME = ISNULL(W1.WK_COL_NAME, W.WK_COL_NAME);

        SET @i = @i + 1;
    END

    SET @trfTime = DATEDIFF(SECOND, @StepStart, GETDATE());
    SELECT @trfRows = COUNT(*) FROM dbo.TRF_IN_PLAN;
    PRINT '';
    PRINT '  TRF_IN COMPLETE: ' + CAST(@trfRows AS VARCHAR) + ' rows in ' + CAST(@trfTime AS VARCHAR) + 's';

    -- ==========================================================
    -- PHASE 2: PURCHASE PLAN (per RDC - calls existing SP)
    -- ==========================================================
    PRINT '';
    PRINT '>>> PHASE 2: PURCHASE PLAN <<<';

    DECLARE @RDCs TABLE (Seq INT IDENTITY(1,1) PRIMARY KEY, RDC_CD VARCHAR(20));
    INSERT INTO @RDCs (RDC_CD) SELECT DISTINCT [RDC_CD] FROM dbo.MASTER_ST_MASTER ORDER BY [RDC_CD];
    SET @total = @@ROWCOUNT;
    PRINT 'Processing ' + CAST(@total AS VARCHAR) + ' RDCs...';

    SET @StepStart = GETDATE();
    SET @i = 1;
    WHILE @i <= @total
    BEGIN
        SELECT @code = RDC_CD FROM @RDCs WHERE Seq = @i;
        PRINT '  PP [' + CAST(@i AS VARCHAR) + '/' + CAST(@total AS VARCHAR) + '] ' + @code;
        EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID=@StartWeekID, @EndWeekID=@EndWeekID, @RdcCode=@code, @MajCat=@MajCat, @Debug=0;
        SET @i = @i + 1;
    END

    SET @ppTime = DATEDIFF(SECOND, @StepStart, GETDATE());
    SELECT @ppRows = COUNT(*) FROM dbo.PURCHASE_PLAN;
    PRINT '  PP COMPLETE: ' + CAST(@ppRows AS VARCHAR) + ' rows in ' + CAST(@ppTime AS VARCHAR) + 's';

    -- ==========================================================
    -- CLEANUP & SUMMARY
    -- ==========================================================
    DROP TABLE IF EXISTS #Weeks, #SQ, #DQ, #LS, #Cats, #SC, #Chain;

    SET @totalTime = DATEDIFF(SECOND, @MasterStart, GETDATE());
    PRINT '';
    PRINT '========================================================';
    PRINT '  COMPLETE: ' + CAST(@trfRows AS VARCHAR) + ' TRF rows + ' + CAST(@ppRows AS VARCHAR) + ' PP rows';
    PRINT '  Total: ' + CAST(@totalTime/60 AS VARCHAR) + 'm ' + CAST(@totalTime%60 AS VARCHAR) + 's';
    PRINT '========================================================';

    SELECT @trfRows AS TRF_Rows, @trfTime AS TRF_Seconds, @ppRows AS PP_Rows, @ppTime AS PP_Seconds,
           @totalTime AS Total_Seconds;
END;
GO

PRINT 'SP_RUN_ALL_PLANS (OPTIMIZED) created.';
GO
