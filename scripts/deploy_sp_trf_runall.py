import snowflake.connector
import sys

conn = snowflake.connector.connect(
    account='iafphkw-hh80816',
    user='akashv2kart',
    password='SVXqEe5pDdamMb9',
    database='V2RETAIL',
    schema='GOLD',
    warehouse='V2_WH'
)
cur = conn.cursor()

# ================================================================
# SP 1: SF_SP_GENERATE_TRF_IN_PLAN
# Converted from SQL Server SP_GENERATE_TRF_IN_PLAN (V6-SPEED)
# ================================================================
sp_trf_sql = r"""
CREATE OR REPLACE PROCEDURE SF_SP_GENERATE_TRF_IN_PLAN(
    START_WEEK_ID FLOAT,
    END_WEEK_ID FLOAT,
    STORE_CODE VARCHAR DEFAULT NULL,
    MAJ_CAT_PARAM VARCHAR DEFAULT NULL,
    COVER_DAYS_CM1 FLOAT DEFAULT 14,
    COVER_DAYS_CM2 FLOAT DEFAULT 0
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function exec(sql) { return snowflake.execute({sqlText: sql}); }
    function execScalar(sql) { var rs = exec(sql); rs.next(); return rs.getColumnValue(1); }

    var startTime = Date.now();
    var startWeek = START_WEEK_ID;
    var endWeek = END_WEEK_ID;
    var storeCode = STORE_CODE;
    var majCatParam = MAJ_CAT_PARAM;
    var coverCM1 = COVER_DAYS_CM1;
    var coverCM2 = COVER_DAYS_CM2;

    // ═══════════════════════════════════════════════════════
    // STEP 0: Validate week IDs
    // ═══════════════════════════════════════════════════════
    var validStart = execScalar("SELECT COUNT(*) FROM WEEK_CALENDAR WHERE WEEK_ID = " + startWeek);
    if (validStart == 0) { throw "Invalid START_WEEK_ID: " + startWeek; }
    var validEnd = execScalar("SELECT COUNT(*) FROM WEEK_CALENDAR WHERE WEEK_ID = " + endWeek);
    if (validEnd == 0) { throw "Invalid END_WEEK_ID: " + endWeek; }

    // ═══════════════════════════════════════════════════════
    // STEP 1: Build weeks
    // ═══════════════════════════════════════════════════════
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_WEEKS AS \
    SELECT \
        W.WEEK_ID, W.WEEK_SEQ, W.FY_WEEK, W.FY_YEAR, \
        W.WK_ST_DT, W.WK_END_DT, \
        'WK_' || CAST(W.FY_WEEK AS VARCHAR) AS WK_COL_NAME \
    FROM WEEK_CALENDAR W \
    WHERE W.WEEK_ID BETWEEN " + startWeek + " AND " + endWeek);

    // Build ordered week list
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_WEEK_LIST AS \
    SELECT ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ, WEEK_ID AS WID \
    FROM TMP_WEEKS");

    var totalWeeks = execScalar("SELECT COUNT(*) FROM TMP_WEEK_LIST");
    var firstWk = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = 1");

    // ═══════════════════════════════════════════════════════
    // STEP 2: Store x Category combos
    // ═══════════════════════════════════════════════════════
    var scWhere = "";
    if (storeCode !== null && storeCode !== "") {
        scWhere = scWhere + " AND SM.ST_CD = ''" + storeCode + "''";
    }
    if (majCatParam !== null && majCatParam !== "") {
        scWhere = scWhere + " AND BC.MAJ_CAT = ''" + majCatParam + "''";
    }

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SC AS \
    SELECT \
        SM.ST_CD, SM.ST_NM, \
        SM.RDC_CD, SM.RDC_NM, SM.HUB_CD, SM.HUB_NM, SM.AREA, \
        BC.MAJ_CAT, BC.BIN_CAP, \
        NVL(BM.SEG, ''NA'') AS SEG, \
        NVL(BM.DIV, ''NA'') AS DIV, \
        NVL(BM.SUB_DIV, ''NA'') AS SUB_DIV, \
        NVL(BM.MAJ_CAT_NM, ''NA'') AS MAJ_CAT_NM, \
        NVL(BM.SSN, ''NA'') AS SSN \
    FROM MASTER_ST_MASTER SM \
    CROSS JOIN MASTER_BIN_CAPACITY BC \
    LEFT JOIN ( \
        SELECT SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN \
        FROM MASTER_PRODUCT_HIERARCHY \
        QUALIFY ROW_NUMBER() OVER (PARTITION BY MAJ_CAT_NM ORDER BY MAJ_CAT_NM) = 1 \
    ) BM ON BM.MAJ_CAT_NM = BC.MAJ_CAT \
    WHERE 1=1" + scWhere);

    // ═══════════════════════════════════════════════════════
    // STEP 3: Unpivot sale, display, latest stock
    // ═══════════════════════════════════════════════════════
    var wkCols = "";
    for (var k = 1; k <= 48; k++) {
        if (k > 1) wkCols += ", ";
        wkCols += "WK_" + k;
    }

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SQ AS \
    SELECT ST_CD, MAJ_CAT, WK_NAME, SALE_QTY \
    FROM QTY_SALE_QTY \
    UNPIVOT (SALE_QTY FOR WK_NAME IN (" + wkCols + "))");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_DQ AS \
    SELECT ST_CD, MAJ_CAT, WK_NAME, DISP_QTY \
    FROM QTY_DISP_QTY \
    UNPIVOT (DISP_QTY FOR WK_NAME IN (" + wkCols + "))");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_LS AS \
    SELECT ST_CD, MAJ_CAT, STK_QTY \
    FROM ( \
        SELECT ST_CD, MAJ_CAT, STK_QTY, \
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY DATE DESC) AS RN \
        FROM QTY_ST_STK_Q \
    ) r WHERE RN = 1");

    // ═══════════════════════════════════════════════════════
    // STEP 4: Build LEAN chaining table
    //   ST_CD, MAJ_CAT, WEEK_ID, SSN, MBQ, SALE,
    //   OP_STK, NET_CF, TRF_IN, CL_STK
    // ═══════════════════════════════════════════════════════
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_CHAIN AS \
    SELECT \
        SC.ST_CD, SC.MAJ_CAT, W.WEEK_ID, \
        SC.SSN, \
        NVL(DQ.DISP_QTY, 0) + NVL(SQ1.SALE_QTY, 0) AS MBQ, \
        NVL(SQ0.SALE_QTY, 0) AS SALE, \
        NVL(LS.STK_QTY, 0) AS OP_STK, \
        0::NUMBER(18,4) AS NET_CF, \
        0::NUMBER(18,4) AS TRF_IN, \
        0::NUMBER(18,4) AS CL_STK \
    FROM TMP_SC SC \
    CROSS JOIN TMP_WEEKS W \
    LEFT JOIN TMP_SQ SQ0 ON SQ0.ST_CD = SC.ST_CD AND SQ0.MAJ_CAT = SC.MAJ_CAT \
        AND SQ0.WK_NAME = W.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SQ SQ1 ON SQ1.ST_CD = SC.ST_CD AND SQ1.MAJ_CAT = SC.MAJ_CAT \
        AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_DQ DQ ON DQ.ST_CD = SC.ST_CD AND DQ.MAJ_CAT = SC.MAJ_CAT \
        AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME) \
    LEFT JOIN TMP_LS LS ON LS.ST_CD = SC.ST_CD AND LS.MAJ_CAT = SC.MAJ_CAT");

    // ═══════════════════════════════════════════════════════
    // STEP 5: Calculate Week 1
    //   5a: NET_CF = MAX(OP_STK - shrinkage, 0)
    //   5b: TRF_IN + CL_STK from NET_CF
    // ═══════════════════════════════════════════════════════

    // 5a: Compute NET carry-forward after shrinkage
    exec("UPDATE TMP_CHAIN SET NET_CF = CASE \
        WHEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) \
            * CASE WHEN SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) > 0 \
        THEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) \
            * CASE WHEN SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) \
        ELSE 0 \
        END \
    WHERE WEEK_ID = " + firstWk);

    // 5b: Compute TRF_IN and CL_STK
    exec("UPDATE TMP_CHAIN SET \
        TRF_IN = CASE \
            WHEN MBQ = 0 AND SALE = 0 THEN 0 \
            WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF \
            ELSE 0 \
            END, \
        CL_STK = CASE \
            WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
            WHEN MBQ + SALE > NET_CF THEN MBQ \
            ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END \
            END \
    WHERE WEEK_ID = " + firstWk);

    // ═══════════════════════════════════════════════════════
    // STEP 6: Chain weeks 2..N
    //   TWO UPDATEs per week on lean 10-col table
    //   6a: OP_STK = prev.CL_STK, NET_CF from OP_STK
    //   6b: TRF_IN + CL_STK from NET_CF
    // ═══════════════════════════════════════════════════════
    for (var i = 2; i <= totalWeeks; i++) {
        var tw = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = " + i);
        var pw = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = " + (i - 1));

        // 6a: Set OP_STK from previous week CL_STK, compute NET_CF
        exec("UPDATE TMP_CHAIN c SET \
            c.OP_STK = p.CL_STK, \
            c.NET_CF = CASE \
                WHEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) \
                    * CASE WHEN c.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) > 0 \
                THEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) \
                    * CASE WHEN c.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) \
                ELSE 0 \
                END \
        FROM TMP_CHAIN p \
        WHERE c.ST_CD = p.ST_CD AND c.MAJ_CAT = p.MAJ_CAT \
            AND p.WEEK_ID = " + pw + " AND c.WEEK_ID = " + tw);

        // 6b: Compute TRF_IN and CL_STK
        exec("UPDATE TMP_CHAIN SET \
            TRF_IN = CASE \
                WHEN MBQ = 0 AND SALE = 0 THEN 0 \
                WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF \
                ELSE 0 \
                END, \
            CL_STK = CASE \
                WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
                WHEN MBQ + SALE > NET_CF THEN MBQ \
                ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END \
                END \
        WHERE WEEK_ID = " + tw);
    }

    // ═══════════════════════════════════════════════════════
    // STEP 7: Delete old data + INSERT full result into TRF_IN_PLAN
    // ═══════════════════════════════════════════════════════
    var delWhere = " WHERE WEEK_ID BETWEEN " + startWeek + " AND " + endWeek;
    if (storeCode !== null && storeCode !== "") {
        delWhere += " AND ST_CD = ''" + storeCode + "''";
    }
    if (majCatParam !== null && majCatParam !== "") {
        delWhere += " AND MAJ_CAT = ''" + majCatParam + "''";
    }
    exec("DELETE FROM TRF_IN_PLAN" + delWhere);

    exec("INSERT INTO TRF_IN_PLAN ( \
        ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA, \
        MAJ_CAT, SSN, WEEK_ID, WK_ST_DT, WK_END_DT, FY_YEAR, FY_WEEK, \
        SEG, DIV, SUB_DIV, MAJ_CAT_NM, \
        S_GRT_STK_Q, W_GRT_STK_Q, \
        BGT_DISP_CL_Q, BGT_DISP_CL_OPT, \
        CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY, \
        BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ, \
        BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q, \
        CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q, \
        TRF_IN_STK_Q, TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ, \
        DC_MBQ, \
        BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q, \
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q \
    ) \
    SELECT \
        SC.ST_CD, SC.ST_NM, SC.RDC_CD, SC.RDC_NM, SC.HUB_CD, SC.HUB_NM, SC.AREA, \
        SC.MAJ_CAT, SC.SSN, W.WEEK_ID, W.WK_ST_DT, W.WK_END_DT, W.FY_YEAR, W.FY_WEEK, \
        SC.SEG, SC.DIV, SC.SUB_DIV, SC.MAJ_CAT_NM, \
        0, \
        CASE WHEN SC.SSN IN (''W'',''PW'') THEN NVL(prev_ch.CL_STK, 0) ELSE 0 END, \
        NVL(DQ.DISP_QTY, 0), \
        CASE WHEN SC.BIN_CAP > 0 \
            THEN ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0) \
            ELSE 0 END, \
        " + coverCM1 + ", " + coverCM2 + ", \
        NVL(SQ1.SALE_QTY, 0), \
        ch.MBQ, \
        CASE WHEN SC.BIN_CAP > 0 \
                AND ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0) > 0 \
            THEN ROUND(ch.MBQ * 1000.0 \
                / NULLIF(ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0), 0), 0) \
            ELSE 0 END, \
        ch.OP_STK, \
        ROUND(ROUND(ch.OP_STK * 0.08, 0) \
            * CASE WHEN SC.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0), \
        ch.NET_CF, \
        ch.SALE, \
        NVL(SQ1.SALE_QTY, 0), \
        NVL(SQ2.SALE_QTY, 0), \
        ch.TRF_IN, \
        0, 0, \
        NVL(SQ1.SALE_QTY, 0) + NVL(SQN2.SALE_QTY, 0) + NVL(SQN3.SALE_QTY, 0) + NVL(SQN4.SALE_QTY, 0), \
        ch.CL_STK, \
        0, \
        ch.CL_STK, \
        CASE WHEN ch.CL_STK - ch.MBQ > 0 THEN ch.CL_STK - ch.MBQ ELSE 0 END, \
        CASE WHEN ch.MBQ - ch.CL_STK > 0 THEN ch.MBQ - ch.CL_STK ELSE 0 END \
    FROM TMP_CHAIN ch \
    INNER JOIN TMP_SC SC ON SC.ST_CD = ch.ST_CD AND SC.MAJ_CAT = ch.MAJ_CAT \
    INNER JOIN TMP_WEEKS W ON W.WEEK_ID = ch.WEEK_ID \
    LEFT JOIN TMP_WEEK_LIST wl ON wl.WID = ch.WEEK_ID \
    LEFT JOIN TMP_WEEK_LIST wlp ON wlp.SEQ = wl.SEQ - 1 \
    LEFT JOIN TMP_CHAIN prev_ch ON prev_ch.ST_CD = ch.ST_CD \
        AND prev_ch.MAJ_CAT = ch.MAJ_CAT AND prev_ch.WEEK_ID = wlp.WID \
    LEFT JOIN TMP_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT \
        AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8 \
    LEFT JOIN TMP_SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT \
        AND SQ2.WK_NAME = W2.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN2 ON WN2.WEEK_SEQ = W.WEEK_SEQ + 2 \
    LEFT JOIN TMP_SQ SQN2 ON SQN2.ST_CD = ch.ST_CD AND SQN2.MAJ_CAT = ch.MAJ_CAT \
        AND SQN2.WK_NAME = WN2.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN3 ON WN3.WEEK_SEQ = W.WEEK_SEQ + 3 \
    LEFT JOIN TMP_SQ SQN3 ON SQN3.ST_CD = ch.ST_CD AND SQN3.MAJ_CAT = ch.MAJ_CAT \
        AND SQN3.WK_NAME = WN3.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN4 ON WN4.WEEK_SEQ = W.WEEK_SEQ + 4 \
    LEFT JOIN TMP_SQ SQN4 ON SQN4.ST_CD = ch.ST_CD AND SQN4.MAJ_CAT = ch.MAJ_CAT \
        AND SQN4.WK_NAME = WN4.WK_COL_NAME \
    LEFT JOIN TMP_DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT \
        AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME)");

    var rowsInserted = execScalar("SELECT COUNT(*) FROM TRF_IN_PLAN \
        WHERE WEEK_ID BETWEEN " + startWeek + " AND " + endWeek + \
        (storeCode !== null && storeCode !== "" ? " AND ST_CD = ''" + storeCode + "''" : "") + \
        (majCatParam !== null && majCatParam !== "" ? " AND MAJ_CAT = ''" + majCatParam + "''" : ""));

    // Post-update: TRF_IN_OPT_CNT and TRF_IN_OPT_MBQ
    var postWhere = " WHERE WEEK_ID BETWEEN " + startWeek + " AND " + endWeek;
    if (storeCode !== null && storeCode !== "") {
        postWhere += " AND ST_CD = ''" + storeCode + "''";
    }
    if (majCatParam !== null && majCatParam !== "") {
        postWhere += " AND MAJ_CAT = ''" + majCatParam + "''";
    }
    exec("UPDATE TRF_IN_PLAN SET \
        TRF_IN_OPT_CNT = CASE \
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0 \
            THEN ROUND(TRF_IN_STK_Q * 1000.0 / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0) \
            ELSE 0 END, \
        TRF_IN_OPT_MBQ = CASE \
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0 \
            THEN NVL(TRF_IN_STK_Q * 1000.0 \
                / NULLIF(ROUND(TRF_IN_STK_Q * 1000.0 \
                    / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0), 0), 0) \
            ELSE 0 END" + postWhere);

    // Cleanup
    exec("DROP TABLE IF EXISTS TMP_WEEKS");
    exec("DROP TABLE IF EXISTS TMP_WEEK_LIST");
    exec("DROP TABLE IF EXISTS TMP_SC");
    exec("DROP TABLE IF EXISTS TMP_SQ");
    exec("DROP TABLE IF EXISTS TMP_DQ");
    exec("DROP TABLE IF EXISTS TMP_LS");
    exec("DROP TABLE IF EXISTS TMP_CHAIN");

    var elapsedSec = Math.round((Date.now() - startTime) / 1000);

    return {
        rows_inserted: rowsInserted,
        start_week: startWeek,
        end_week: endWeek,
        seconds: elapsedSec
    };
$$;
"""

# ================================================================
# SP 2: SF_SP_RUN_ALL_PLANS
# Converted from SQL Server SP_RUN_ALL_PLANS V3 (BULK)
# Same TRF logic as SP1 but: all stores, TRUNCATE first, then PP
# ================================================================
sp_runall_sql = r"""
CREATE OR REPLACE PROCEDURE SF_SP_RUN_ALL_PLANS(
    START_WEEK_ID FLOAT,
    END_WEEK_ID FLOAT,
    MAJ_CAT_PARAM VARCHAR DEFAULT NULL,
    COVER_DAYS_CM1 FLOAT DEFAULT 14,
    COVER_DAYS_CM2 FLOAT DEFAULT 0
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function exec(sql) { return snowflake.execute({sqlText: sql}); }
    function execScalar(sql) { var rs = exec(sql); rs.next(); return rs.getColumnValue(1); }

    var startTime = Date.now();
    var startWeek = START_WEEK_ID;
    var endWeek = END_WEEK_ID;
    var majCatParam = MAJ_CAT_PARAM;
    var coverCM1 = COVER_DAYS_CM1;
    var coverCM2 = COVER_DAYS_CM2;

    // ═══════════════════════════════════════════════════════
    // STEP 1: Build weeks
    // ═══════════════════════════════════════════════════════
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_WEEKS AS \
    SELECT \
        W.WEEK_ID, W.WEEK_SEQ, W.FY_WEEK, W.FY_YEAR, \
        W.WK_ST_DT, W.WK_END_DT, \
        'WK_' || CAST(W.FY_WEEK AS VARCHAR) AS WK_COL_NAME \
    FROM WEEK_CALENDAR W \
    WHERE W.WEEK_ID BETWEEN " + startWeek + " AND " + endWeek);

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_WEEK_LIST AS \
    SELECT ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ, WEEK_ID AS WID \
    FROM TMP_WEEKS");

    var totalWeeks = execScalar("SELECT COUNT(*) FROM TMP_WEEK_LIST");
    var firstWk = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = 1");

    // ═══════════════════════════════════════════════════════
    // STEP 2: Shared lookups (unpivot sale, display, stock)
    // ═══════════════════════════════════════════════════════
    var wkCols = "";
    for (var k = 1; k <= 48; k++) {
        if (k > 1) wkCols += ", ";
        wkCols += "WK_" + k;
    }

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SQ AS \
    SELECT ST_CD, MAJ_CAT, WK_NAME, SALE_QTY \
    FROM QTY_SALE_QTY \
    UNPIVOT (SALE_QTY FOR WK_NAME IN (" + wkCols + "))");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_DQ AS \
    SELECT ST_CD, MAJ_CAT, WK_NAME, DISP_QTY \
    FROM QTY_DISP_QTY \
    UNPIVOT (DISP_QTY FOR WK_NAME IN (" + wkCols + "))");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_LS AS \
    SELECT ST_CD, MAJ_CAT, STK_QTY \
    FROM ( \
        SELECT ST_CD, MAJ_CAT, STK_QTY, \
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT ORDER BY DATE DESC) AS RN \
        FROM QTY_ST_STK_Q \
    ) r WHERE RN = 1");

    // ═══════════════════════════════════════════════════════
    // STEP 3: ALL Store x Category combos (no store filter)
    // ═══════════════════════════════════════════════════════
    var scWhere = "";
    if (majCatParam !== null && majCatParam !== "") {
        scWhere = " AND BC.MAJ_CAT = ''" + majCatParam + "''";
    }

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SC AS \
    SELECT \
        SM.ST_CD, SM.ST_NM, \
        SM.RDC_CD, SM.RDC_NM, SM.HUB_CD, SM.HUB_NM, SM.AREA, \
        BC.MAJ_CAT, BC.BIN_CAP, \
        NVL(BM.SEG, ''NA'') AS SEG, \
        NVL(BM.DIV, ''NA'') AS DIV, \
        NVL(BM.SUB_DIV, ''NA'') AS SUB_DIV, \
        NVL(BM.MAJ_CAT_NM, ''NA'') AS MAJ_CAT_NM, \
        NVL(BM.SSN, ''NA'') AS SSN \
    FROM MASTER_ST_MASTER SM \
    CROSS JOIN MASTER_BIN_CAPACITY BC \
    LEFT JOIN ( \
        SELECT SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN \
        FROM MASTER_PRODUCT_HIERARCHY \
        QUALIFY ROW_NUMBER() OVER (PARTITION BY MAJ_CAT_NM ORDER BY MAJ_CAT_NM) = 1 \
    ) BM ON BM.MAJ_CAT_NM = BC.MAJ_CAT \
    WHERE 1=1" + scWhere);

    // ═══════════════════════════════════════════════════════
    // STEP 4: Build BULK chain table (ALL stores x categories x weeks)
    // ═══════════════════════════════════════════════════════
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_CHAIN AS \
    SELECT \
        SC.ST_CD, SC.MAJ_CAT, W.WEEK_ID, \
        SC.SSN, \
        NVL(DQ.DISP_QTY, 0) + NVL(SQ1.SALE_QTY, 0) AS MBQ, \
        NVL(SQ0.SALE_QTY, 0) AS SALE, \
        NVL(LS.STK_QTY, 0) AS OP_STK, \
        0::NUMBER(18,4) AS NET_CF, \
        0::NUMBER(18,4) AS TRF_IN, \
        0::NUMBER(18,4) AS CL_STK \
    FROM TMP_SC SC \
    CROSS JOIN TMP_WEEKS W \
    LEFT JOIN TMP_SQ SQ0 ON SQ0.ST_CD = SC.ST_CD AND SQ0.MAJ_CAT = SC.MAJ_CAT \
        AND SQ0.WK_NAME = W.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SQ SQ1 ON SQ1.ST_CD = SC.ST_CD AND SQ1.MAJ_CAT = SC.MAJ_CAT \
        AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_DQ DQ ON DQ.ST_CD = SC.ST_CD AND DQ.MAJ_CAT = SC.MAJ_CAT \
        AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME) \
    LEFT JOIN TMP_LS LS ON LS.ST_CD = SC.ST_CD AND LS.MAJ_CAT = SC.MAJ_CAT");

    // ═══════════════════════════════════════════════════════
    // STEP 5: Week 1 calculation (ALL combos at once)
    // ═══════════════════════════════════════════════════════

    // 5a: NET_CF after shrinkage
    exec("UPDATE TMP_CHAIN SET NET_CF = CASE \
        WHEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) \
            * CASE WHEN SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) > 0 \
        THEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) \
            * CASE WHEN SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) \
        ELSE 0 \
        END \
    WHERE WEEK_ID = " + firstWk);

    // 5b: TRF_IN and CL_STK
    exec("UPDATE TMP_CHAIN SET \
        TRF_IN = CASE \
            WHEN MBQ = 0 AND SALE = 0 THEN 0 \
            WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF \
            ELSE 0 \
            END, \
        CL_STK = CASE \
            WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
            WHEN MBQ + SALE > NET_CF THEN MBQ \
            ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END \
            END \
    WHERE WEEK_ID = " + firstWk);

    // ═══════════════════════════════════════════════════════
    // STEP 6: Chain weeks 2..N (ALL combos at once per week)
    // ═══════════════════════════════════════════════════════
    for (var i = 2; i <= totalWeeks; i++) {
        var tw = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = " + i);
        var pw = execScalar("SELECT WID FROM TMP_WEEK_LIST WHERE SEQ = " + (i - 1));

        // 6a: OP_STK = prev.CL_STK, NET_CF from shrinkage
        exec("UPDATE TMP_CHAIN c SET \
            c.OP_STK = p.CL_STK, \
            c.NET_CF = CASE \
                WHEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) \
                    * CASE WHEN c.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) > 0 \
                THEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) \
                    * CASE WHEN c.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0) \
                ELSE 0 \
                END \
        FROM TMP_CHAIN p \
        WHERE c.ST_CD = p.ST_CD AND c.MAJ_CAT = p.MAJ_CAT \
            AND p.WEEK_ID = " + pw + " AND c.WEEK_ID = " + tw);

        // 6b: TRF_IN + CL_STK
        exec("UPDATE TMP_CHAIN SET \
            TRF_IN = CASE \
                WHEN MBQ = 0 AND SALE = 0 THEN 0 \
                WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF \
                ELSE 0 \
                END, \
            CL_STK = CASE \
                WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
                WHEN MBQ + SALE > NET_CF THEN MBQ \
                ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END \
                END \
        WHERE WEEK_ID = " + tw);
    }

    // ═══════════════════════════════════════════════════════
    // STEP 7: TRUNCATE + BULK INSERT into TRF_IN_PLAN
    // ═══════════════════════════════════════════════════════
    exec("TRUNCATE TABLE IF EXISTS TRF_IN_PLAN");

    exec("INSERT INTO TRF_IN_PLAN ( \
        ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA, \
        MAJ_CAT, SSN, WEEK_ID, WK_ST_DT, WK_END_DT, FY_YEAR, FY_WEEK, \
        SEG, DIV, SUB_DIV, MAJ_CAT_NM, \
        S_GRT_STK_Q, W_GRT_STK_Q, \
        BGT_DISP_CL_Q, BGT_DISP_CL_OPT, \
        CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY, \
        BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ, \
        BGT_TTL_CF_OP_STK_Q, NT_ACT_Q, NET_BGT_CF_STK_Q, \
        CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q, \
        TRF_IN_STK_Q, TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ, \
        DC_MBQ, \
        BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q, \
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q \
    ) \
    SELECT \
        SC.ST_CD, SC.ST_NM, SC.RDC_CD, SC.RDC_NM, SC.HUB_CD, SC.HUB_NM, SC.AREA, \
        SC.MAJ_CAT, SC.SSN, W.WEEK_ID, W.WK_ST_DT, W.WK_END_DT, W.FY_YEAR, W.FY_WEEK, \
        SC.SEG, SC.DIV, SC.SUB_DIV, SC.MAJ_CAT_NM, \
        0, \
        CASE WHEN SC.SSN IN (''W'',''PW'') THEN NVL(pch.CL_STK, 0) ELSE 0 END, \
        NVL(DQ.DISP_QTY, 0), \
        CASE WHEN SC.BIN_CAP > 0 \
            THEN ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0) \
            ELSE 0 END, \
        " + coverCM1 + ", " + coverCM2 + ", \
        NVL(SQ1.SALE_QTY, 0), \
        ch.MBQ, \
        CASE WHEN SC.BIN_CAP > 0 \
                AND ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0) > 0 \
            THEN ROUND(ch.MBQ * 1000.0 \
                / NULLIF(ROUND(NVL(DQ.DISP_QTY, 0) * 1000.0 / SC.BIN_CAP, 0), 0), 0) \
            ELSE 0 END, \
        ch.OP_STK, \
        ROUND(ROUND(ch.OP_STK * 0.08, 0) \
            * CASE WHEN SC.SSN IN (''S'',''PS'') THEN 1.0 ELSE 0.5 END, 0), \
        ch.NET_CF, \
        ch.SALE, \
        NVL(SQ1.SALE_QTY, 0), \
        NVL(SQ2.SALE_QTY, 0), \
        ch.TRF_IN, \
        0, 0, \
        NVL(SQ1.SALE_QTY, 0) + NVL(SQN2.SALE_QTY, 0) + NVL(SQN3.SALE_QTY, 0) + NVL(SQN4.SALE_QTY, 0), \
        ch.CL_STK, \
        0, \
        ch.CL_STK, \
        CASE WHEN ch.CL_STK - ch.MBQ > 0 THEN ch.CL_STK - ch.MBQ ELSE 0 END, \
        CASE WHEN ch.MBQ - ch.CL_STK > 0 THEN ch.MBQ - ch.CL_STK ELSE 0 END \
    FROM TMP_CHAIN ch \
    INNER JOIN TMP_SC SC ON SC.ST_CD = ch.ST_CD AND SC.MAJ_CAT = ch.MAJ_CAT \
    INNER JOIN TMP_WEEKS W ON W.WEEK_ID = ch.WEEK_ID \
    LEFT JOIN TMP_WEEK_LIST wl ON wl.WID = ch.WEEK_ID \
    LEFT JOIN TMP_WEEK_LIST wlp ON wlp.SEQ = wl.SEQ - 1 \
    LEFT JOIN TMP_CHAIN pch ON pch.ST_CD = ch.ST_CD \
        AND pch.MAJ_CAT = ch.MAJ_CAT AND pch.WEEK_ID = wlp.WID \
    LEFT JOIN TMP_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT \
        AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8 \
    LEFT JOIN TMP_SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT \
        AND SQ2.WK_NAME = W2.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN2 ON WN2.WEEK_SEQ = W.WEEK_SEQ + 2 \
    LEFT JOIN TMP_SQ SQN2 ON SQN2.ST_CD = ch.ST_CD AND SQN2.MAJ_CAT = ch.MAJ_CAT \
        AND SQN2.WK_NAME = WN2.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN3 ON WN3.WEEK_SEQ = W.WEEK_SEQ + 3 \
    LEFT JOIN TMP_SQ SQN3 ON SQN3.ST_CD = ch.ST_CD AND SQN3.MAJ_CAT = ch.MAJ_CAT \
        AND SQN3.WK_NAME = WN3.WK_COL_NAME \
    LEFT JOIN TMP_WEEKS WN4 ON WN4.WEEK_SEQ = W.WEEK_SEQ + 4 \
    LEFT JOIN TMP_SQ SQN4 ON SQN4.ST_CD = ch.ST_CD AND SQN4.MAJ_CAT = ch.MAJ_CAT \
        AND SQN4.WK_NAME = WN4.WK_COL_NAME \
    LEFT JOIN TMP_DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT \
        AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME)");

    var trfRows = execScalar("SELECT COUNT(*) FROM TRF_IN_PLAN");

    // Post-update: TRF_IN_OPT_CNT and TRF_IN_OPT_MBQ
    exec("UPDATE TRF_IN_PLAN SET \
        TRF_IN_OPT_CNT = CASE \
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0 \
            THEN ROUND(TRF_IN_STK_Q * 1000.0 / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0) \
            ELSE 0 END, \
        TRF_IN_OPT_MBQ = CASE \
            WHEN BGT_DISP_CL_OPT_MBQ > 0 AND TRF_IN_STK_Q > 0 \
            THEN NVL(TRF_IN_STK_Q * 1000.0 \
                / NULLIF(ROUND(TRF_IN_STK_Q * 1000.0 \
                    / NULLIF(BGT_DISP_CL_OPT_MBQ, 0), 0), 0), 0) \
            ELSE 0 END");

    // ═══════════════════════════════════════════════════════
    // STEP 8: PURCHASE PLAN (call the PP stored procedure)
    // ═══════════════════════════════════════════════════════
    var ppCall = "CALL SF_SP_GENERATE_PURCHASE_PLAN(" + startWeek + ", " + endWeek + ", NULL, NULL)";
    if (majCatParam !== null && majCatParam !== "") {
        ppCall = "CALL SF_SP_GENERATE_PURCHASE_PLAN(" + startWeek + ", " + endWeek + ", NULL, ''" + majCatParam + "'')";
    }
    exec(ppCall);

    var ppRows = execScalar("SELECT COUNT(*) FROM PURCHASE_PLAN");

    // Cleanup
    exec("DROP TABLE IF EXISTS TMP_WEEKS");
    exec("DROP TABLE IF EXISTS TMP_WEEK_LIST");
    exec("DROP TABLE IF EXISTS TMP_SC");
    exec("DROP TABLE IF EXISTS TMP_SQ");
    exec("DROP TABLE IF EXISTS TMP_DQ");
    exec("DROP TABLE IF EXISTS TMP_LS");
    exec("DROP TABLE IF EXISTS TMP_CHAIN");

    var totalSec = Math.round((Date.now() - startTime) / 1000);

    return {
        trf_rows: trfRows,
        pp_rows: ppRows,
        total_seconds: totalSec
    };
$$;
"""

# ================================================================
# DEPLOY BOTH SPs
# ================================================================
print("Deploying SF_SP_GENERATE_TRF_IN_PLAN...")
try:
    cur.execute(sp_trf_sql)
    print("  -> SF_SP_GENERATE_TRF_IN_PLAN deployed successfully.")
except Exception as e:
    print(f"  -> FAILED: {e}")
    sys.exit(1)

print("Deploying SF_SP_RUN_ALL_PLANS...")
try:
    cur.execute(sp_runall_sql)
    print("  -> SF_SP_RUN_ALL_PLANS deployed successfully.")
except Exception as e:
    print(f"  -> FAILED: {e}")
    sys.exit(1)

print("\nBoth stored procedures deployed to V2RETAIL.GOLD")
print("  CALL SF_SP_GENERATE_TRF_IN_PLAN(start_wk, end_wk, 'ST001', NULL, 14, 0);")
print("  CALL SF_SP_RUN_ALL_PLANS(start_wk, end_wk, NULL, 14, 0);")

conn.close()
