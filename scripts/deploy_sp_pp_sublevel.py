"""
Deploy Snowflake Stored Procedures:
  1. SF_SP_GENERATE_PURCHASE_PLAN
  2. SF_SP_GENERATE_SUB_LEVEL_TRF
  3. SF_SP_GENERATE_SUB_LEVEL_PP

Converted from T-SQL (sql/10, sql/24, sql/25) to Snowflake JavaScript SPs.
Column names normalized: hyphens/spaces -> underscores.
"""

import snowflake.connector

conn = snowflake.connector.connect(
    account='iafphkw-hh80816',
    user='akashv2kart',
    password='SVXqEe5pDdamMb9',
    database='V2RETAIL',
    schema='GOLD',
    warehouse='V2_WH'
)
cur = conn.cursor()

# ═══════════════════════════════════════════════════════════════
# SP 1: SF_SP_GENERATE_PURCHASE_PLAN
# ═══════════════════════════════════════════════════════════════
sp_pp = r"""
CREATE OR REPLACE PROCEDURE SF_SP_GENERATE_PURCHASE_PLAN(
    START_WEEK_ID FLOAT,
    END_WEEK_ID FLOAT,
    RDC_CODE VARCHAR DEFAULT NULL,
    MAJ_CAT_PARAM VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function exec(sql) { return snowflake.execute({sqlText: sql}); }
    function execScalar(sql) { var rs = exec(sql); rs.next(); return rs.getColumnValue(1); }

    var startWk = START_WEEK_ID;
    var endWk   = END_WEEK_ID;
    var rdcCode  = RDC_CODE;
    var majCat   = MAJ_CAT_PARAM;
    var startTs  = Date.now();

    // ═══ STEP 1: Build working weeks with sequence ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_WEEKS AS \
    SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT \
    FROM WEEK_CALENDAR \
    WHERE WEEK_ID BETWEEN " + startWk + " AND " + endWk);

    // Ordered week list
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_WKLIST AS \
    SELECT ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ, WEEK_ID AS WID \
    FROM TMP_PP_WEEKS");

    var totalWeeks = execScalar("SELECT COUNT(*) FROM TMP_PP_WKLIST");
    var firstWkSeq = execScalar("SELECT w.WEEK_SEQ FROM TMP_PP_WEEKS w \
    INNER JOIN TMP_PP_WKLIST wl ON wl.WID = w.WEEK_ID WHERE wl.SEQ = 1");

    // Week offset map: for each week, PREV, NEXT1..NEXT5
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_WKMAP AS \
    SELECT w.WEEK_ID, w.WEEK_SEQ, \
        wP.WEEK_ID AS PREV_WEEK_ID, \
        w1.WEEK_ID AS NEXT1_WEEK_ID, \
        w2.WEEK_ID AS NEXT2_WEEK_ID, \
        w3.WEEK_ID AS NEXT3_WEEK_ID, \
        w4.WEEK_ID AS NEXT4_WEEK_ID, \
        w5.WEEK_ID AS NEXT5_WEEK_ID \
    FROM TMP_PP_WEEKS w \
    LEFT JOIN TMP_PP_WEEKS wP ON wP.WEEK_SEQ = w.WEEK_SEQ - 1 \
    LEFT JOIN TMP_PP_WEEKS w1 ON w1.WEEK_SEQ = w.WEEK_SEQ + 1 \
    LEFT JOIN TMP_PP_WEEKS w2 ON w2.WEEK_SEQ = w.WEEK_SEQ + 2 \
    LEFT JOIN TMP_PP_WEEKS w3 ON w3.WEEK_SEQ = w.WEEK_SEQ + 3 \
    LEFT JOIN TMP_PP_WEEKS w4 ON w4.WEEK_SEQ = w.WEEK_SEQ + 4 \
    LEFT JOIN TMP_PP_WEEKS w5 ON w5.WEEK_SEQ = w.WEEK_SEQ + 5");

    // ═══ STEP 2: Build RDC x MAJ_CAT combinations ═══
    var rdcFilter = rdcCode ? " AND m.RDC_CD = '" + rdcCode + "'" : "";
    var catFilter  = majCat ? " AND b.MAJ_CAT = '" + majCat + "'" : "";

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_RDCCAT AS \
    SELECT DISTINCT m.RDC_CD, m.RDC_NM, b.MAJ_CAT, \
        NVL(BM.SEG, 'NA') AS SEG, NVL(BM.DIV, 'NA') AS DIV, \
        NVL(BM.SUB_DIV, 'NA') AS SUB_DIV, NVL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM, \
        NVL(BM.SSN, 'NA') AS SSN \
    FROM (SELECT DISTINCT RDC_CD, RDC_NM FROM MASTER_ST_MASTER) m \
    CROSS JOIN (SELECT DISTINCT MAJ_CAT FROM MASTER_BIN_CAPACITY) b \
    LEFT JOIN (SELECT SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN, \
        ROW_NUMBER() OVER (PARTITION BY MAJ_CAT_NM ORDER BY ID) AS RN \
        FROM MASTER_PRODUCT_HIERARCHY) BM ON BM.MAJ_CAT_NM = b.MAJ_CAT AND BM.RN = 1 \
    WHERE 1=1" + rdcFilter + catFilter);

    // ═══ STEP 3: Aggregate from TRF_IN_PLAN ═══
    var rdcFilterTrf = rdcCode ? " AND t.RDC_CD = '" + rdcCode + "'" : "";
    var catFilterTrf  = majCat ? " AND t.MAJ_CAT = '" + majCat + "'" : "";

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TRFAGG AS \
    SELECT t.RDC_CD, t.MAJ_CAT, t.WEEK_ID, \
        SUM(t.S_GRT_STK_Q) AS S_GRT_STK_Q, \
        SUM(t.W_GRT_STK_Q) AS W_GRT_STK_Q, \
        SUM(t.BGT_DISP_CL_Q) AS BGT_DISP_CL_Q, \
        SUM(t.CM_BGT_SALE_Q) AS CW_BGT_SALE_Q, \
        SUM(t.CM1_BGT_SALE_Q) AS CW1_BGT_SALE_Q, \
        SUM(t.BGT_ST_CL_MBQ) AS BGT_ST_CL_MBQ, \
        SUM(t.BGT_TTL_CF_OP_STK_Q) AS NET_ST_OP_STK_Q, \
        SUM(t.TRF_IN_STK_Q) AS CW_TRF_OUT_Q, \
        SUM(t.NET_ST_CL_STK_Q) AS NET_BGT_ST_CL_STK_Q, \
        SUM(t.ST_CL_EXCESS_Q) AS ST_STK_EXCESS_Q, \
        SUM(t.ST_CL_SHORT_Q) AS ST_STK_SHORT_Q \
    FROM TRF_IN_PLAN t \
    WHERE t.WEEK_ID BETWEEN " + startWk + " AND " + endWk +
    rdcFilterTrf + catFilterTrf +
    " GROUP BY t.RDC_CD, t.MAJ_CAT, t.WEEK_ID");

    // Prev week's BGT_ST_CL_MBQ
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TRFPREV AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.WEEK_ID, \
        NVL(tp.BGT_ST_CL_MBQ, 0) AS BGT_ST_OP_MBQ \
    FROM TMP_PP_TRFAGG ta \
    LEFT JOIN TMP_PP_WKMAP wm ON wm.WEEK_ID = ta.WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG tp ON tp.RDC_CD = ta.RDC_CD \
        AND tp.MAJ_CAT = ta.MAJ_CAT AND tp.WEEK_ID = wm.PREV_WEEK_ID");

    // Next week's TRF_OUT
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TRFNEXT AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.WEEK_ID, \
        NVL(tn.CW_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q \
    FROM TMP_PP_TRFAGG ta \
    LEFT JOIN TMP_PP_WKMAP wm ON wm.WEEK_ID = ta.WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG tn ON tn.RDC_CD = ta.RDC_CD \
        AND tn.MAJ_CAT = ta.MAJ_CAT AND tn.WEEK_ID = wm.NEXT1_WEEK_ID");

    // Future TRF_OUT (CW2, CW3, CW4)
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TRFNXT234 AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.WEEK_ID, \
        NVL(t2.CW_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q, \
        NVL(t3.CW_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q, \
        NVL(t4.CW_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q \
    FROM TMP_PP_TRFAGG ta \
    LEFT JOIN TMP_PP_WKMAP wm ON wm.WEEK_ID = ta.WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t2 ON t2.RDC_CD = ta.RDC_CD AND t2.MAJ_CAT = ta.MAJ_CAT AND t2.WEEK_ID = wm.NEXT2_WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t3 ON t3.RDC_CD = ta.RDC_CD AND t3.MAJ_CAT = ta.MAJ_CAT AND t3.WEEK_ID = wm.NEXT3_WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t4 ON t4.RDC_CD = ta.RDC_CD AND t4.MAJ_CAT = ta.MAJ_CAT AND t4.WEEK_ID = wm.NEXT4_WEEK_ID");

    // Future week sales (CW2..CW5)
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TRFFUTURE AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.WEEK_ID, \
        NVL(t1.CW1_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q, \
        NVL(t2.CW1_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q, \
        NVL(t3.CW1_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q, \
        NVL(t4.CW1_BGT_SALE_Q, 0) AS CW5_BGT_SALE_Q \
    FROM TMP_PP_TRFAGG ta \
    LEFT JOIN TMP_PP_WKMAP wm ON wm.WEEK_ID = ta.WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t1 ON t1.RDC_CD = ta.RDC_CD AND t1.MAJ_CAT = ta.MAJ_CAT AND t1.WEEK_ID = wm.NEXT1_WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t2 ON t2.RDC_CD = ta.RDC_CD AND t2.MAJ_CAT = ta.MAJ_CAT AND t2.WEEK_ID = wm.NEXT2_WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t3 ON t3.RDC_CD = ta.RDC_CD AND t3.MAJ_CAT = ta.MAJ_CAT AND t3.WEEK_ID = wm.NEXT3_WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG t4 ON t4.RDC_CD = ta.RDC_CD AND t4.MAJ_CAT = ta.MAJ_CAT AND t4.WEEK_ID = wm.NEXT4_WEEK_ID");

    // ═══ STEP 4: Reference data (GRT unpivot, DC stock, BIN_CAP, DEL_PEND) ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_GRT AS \
    SELECT SSN, REPLACE(WK_COL, 'WK_', '')::INTEGER AS WK_NUM, GRT_PCT \
    FROM MASTER_GRT_CONTRIBUTION \
    UNPIVOT (GRT_PCT FOR WK_COL IN ( \
        WK_1,WK_2,WK_3,WK_4,WK_5,WK_6,WK_7,WK_8,WK_9,WK_10,WK_11,WK_12, \
        WK_13,WK_14,WK_15,WK_16,WK_17,WK_18,WK_19,WK_20,WK_21,WK_22,WK_23,WK_24, \
        WK_25,WK_26,WK_27,WK_28,WK_29,WK_30,WK_31,WK_32,WK_33,WK_34,WK_35,WK_36, \
        WK_37,WK_38,WK_39,WK_40,WK_41,WK_42,WK_43,WK_44,WK_45,WK_46,WK_47,WK_48 \
    ))");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP_REFDATA AS \
    SELECT rc.RDC_CD, rc.MAJ_CAT, ww.WEEK_ID, \
        NVL(qmg.DC_STK_Q, 0) AS DC_STK_Q, \
        NVL(qmg.GRT_STK_Q, 0) AS GRT_STK_Q, \
        NVL(mbc.BIN_CAP_DC_TEAM, 0) AS BIN_CAP_DC_TEAM, \
        NVL(mbc.BIN_CAP, 0) AS BIN_CAP, \
        NVL(gu.GRT_PCT, 0) AS GRT_CONS_PCT, \
        NVL(qdp.DEL_PEND_Q, 0) AS DEL_PEND_Q, \
        rc.SSN \
    FROM TMP_PP_RDCCAT rc \
    CROSS JOIN TMP_PP_WEEKS ww \
    LEFT JOIN ( \
        SELECT RDC_CD, MAJ_CAT, DC_STK_Q, GRT_STK_Q, DATE, \
            ROW_NUMBER() OVER (PARTITION BY RDC_CD, MAJ_CAT ORDER BY DATE DESC) AS RN \
        FROM QTY_MSA_AND_GRT \
    ) qmg ON qmg.RDC_CD = rc.RDC_CD AND qmg.MAJ_CAT = rc.MAJ_CAT AND qmg.RN = 1 \
    LEFT JOIN MASTER_BIN_CAPACITY mbc ON mbc.MAJ_CAT = rc.MAJ_CAT \
    LEFT JOIN TMP_PP_GRT gu ON gu.SSN = rc.SSN \
        AND gu.WK_NUM = (ww.WEEK_SEQ - " + firstWkSeq + " + 1) \
    LEFT JOIN ( \
        SELECT RDC_CD, MAJ_CAT, DEL_PEND_Q, \
            ROW_NUMBER() OVER (PARTITION BY RDC_CD, MAJ_CAT ORDER BY DATE DESC) AS RN \
        FROM QTY_DEL_PENDING \
    ) qdp ON qdp.RDC_CD = rc.RDC_CD AND qdp.MAJ_CAT = rc.MAJ_CAT AND qdp.RN = 1");

    // ═══ STEP 5: Build full calculation table ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_PP AS \
    SELECT rc.RDC_CD, NVL(rc.RDC_NM, 'NA') AS RDC_NM, rc.MAJ_CAT, \
        ww.WEEK_ID, ww.WEEK_SEQ, rc.SSN, rc.SEG, rc.DIV, rc.SUB_DIV, rc.MAJ_CAT_NM, \
        ww.FY_WEEK, ww.FY_YEAR, ww.WK_ST_DT, ww.WK_END_DT, \
        NVL(rd.DC_STK_Q, 0) AS DC_STK_Q, \
        NVL(rd.GRT_STK_Q, 0) AS GRT_STK_Q, \
        NVL(ta.S_GRT_STK_Q, 0) AS S_GRT_STK_Q, \
        NVL(ta.W_GRT_STK_Q, 0) AS W_GRT_STK_Q, \
        NVL(rd.BIN_CAP_DC_TEAM, 0) AS BIN_CAP_DC_TEAM, \
        NVL(rd.BIN_CAP, 0) AS BIN_CAP, \
        NVL(ta.BGT_DISP_CL_Q, 0) AS BGT_DISP_CL_Q, \
        NVL(ta.CW_BGT_SALE_Q, 0) AS CW_BGT_SALE_Q, \
        NVL(ta.CW1_BGT_SALE_Q, 0) AS CW1_BGT_SALE_Q, \
        NVL(tf.CW2_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q, \
        NVL(tf.CW3_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q, \
        NVL(tf.CW4_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q, \
        NVL(tf.CW5_BGT_SALE_Q, 0) AS CW5_BGT_SALE_Q, \
        NVL(tpv.BGT_ST_OP_MBQ, 0) AS BGT_ST_OP_MBQ, \
        NVL(ta.NET_ST_OP_STK_Q, 0) AS NET_ST_OP_STK_Q, \
        NVL(CASE WHEN rd.DC_STK_Q > 0 THEN rd.DC_STK_Q ELSE 0 END, 0) AS BGT_DC_OP_STK_Q, \
        0::NUMBER(18,4) AS PP_NT_ACT_Q, \
        NVL(CASE WHEN rd.DC_STK_Q > 0 THEN rd.DC_STK_Q ELSE 0 END, 0) AS BGT_CF_STK_Q, \
        NVL(rd.GRT_STK_Q, 0) AS TTL_STK, \
        NVL(rd.GRT_STK_Q, 0) AS OP_STK, \
        CASE WHEN rc.SSN IN ('S','OC','A') THEN NVL(rd.GRT_STK_Q, 0) * 0.10 ELSE 0 END AS NT_ACT_STK, \
        NVL(rd.GRT_CONS_PCT, 0) AS GRT_CONS_PCT, \
        0::NUMBER(18,4) AS GRT_CONS_Q, \
        NVL(rd.DEL_PEND_Q, 0) AS DEL_PEND_Q, \
        0::NUMBER(18,4) AS PP_NET_BGT_CF_STK_Q, \
        NVL(ta.CW_TRF_OUT_Q, 0) AS CW_TRF_OUT_Q, \
        NVL(tn.CW1_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q, \
        NVL(tn234.CW2_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q, \
        NVL(tn234.CW3_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q, \
        NVL(tn234.CW4_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q, \
        NVL(ta.CW_TRF_OUT_Q, 0) AS TTL_TRF_OUT_Q, \
        NVL(ta.BGT_ST_CL_MBQ, 0) AS BGT_ST_CL_MBQ, \
        NVL(ta.NET_BGT_ST_CL_STK_Q, 0) AS NET_BGT_ST_CL_STK_Q, \
        0::NUMBER(18,4) AS NET_SSNL_CL_STK_Q, \
        NVL(ta.CW1_BGT_SALE_Q, 0) + NVL(tf.CW2_BGT_SALE_Q, 0) + NVL(tf.CW3_BGT_SALE_Q, 0) + NVL(tf.CW4_BGT_SALE_Q, 0) AS BGT_DC_MBQ_SALE, \
        0::NUMBER(18,4) AS BGT_DC_CL_MBQ, \
        0::NUMBER(18,4) AS BGT_DC_CL_STK_Q, \
        0::NUMBER(18,4) AS BGT_PUR_Q_INIT, \
        0::NUMBER(18,4) AS POS_PO_RAISED, \
        0::NUMBER(18,4) AS NEG_PO_RAISED, \
        0::NUMBER(18,4) AS BGT_CO_CL_STK_Q, \
        0::NUMBER(18,4) AS DC_STK_EXCESS_Q, \
        0::NUMBER(18,4) AS DC_STK_SHORT_Q, \
        NVL(ta.ST_STK_EXCESS_Q, 0) AS ST_STK_EXCESS_Q, \
        NVL(ta.ST_STK_SHORT_Q, 0) AS ST_STK_SHORT_Q, \
        0::NUMBER(18,4) AS CO_STK_EXCESS_Q, \
        0::NUMBER(18,4) AS CO_STK_SHORT_Q, \
        0::NUMBER(18,4) AS FRESH_BIN_REQ, \
        0::NUMBER(18,4) AS GRT_BIN_REQ \
    FROM TMP_PP_RDCCAT rc \
    CROSS JOIN TMP_PP_WEEKS ww \
    LEFT JOIN TMP_PP_REFDATA rd ON rd.RDC_CD = rc.RDC_CD AND rd.MAJ_CAT = rc.MAJ_CAT AND rd.WEEK_ID = ww.WEEK_ID \
    LEFT JOIN TMP_PP_TRFAGG ta ON ta.RDC_CD = rc.RDC_CD AND ta.MAJ_CAT = rc.MAJ_CAT AND ta.WEEK_ID = ww.WEEK_ID \
    LEFT JOIN TMP_PP_TRFPREV tpv ON tpv.RDC_CD = rc.RDC_CD AND tpv.MAJ_CAT = rc.MAJ_CAT AND tpv.WEEK_ID = ww.WEEK_ID \
    LEFT JOIN TMP_PP_TRFNEXT tn ON tn.RDC_CD = rc.RDC_CD AND tn.MAJ_CAT = rc.MAJ_CAT AND tn.WEEK_ID = ww.WEEK_ID \
    LEFT JOIN TMP_PP_TRFFUTURE tf ON tf.RDC_CD = rc.RDC_CD AND tf.MAJ_CAT = rc.MAJ_CAT AND tf.WEEK_ID = ww.WEEK_ID \
    LEFT JOIN TMP_PP_TRFNXT234 tn234 ON tn234.RDC_CD = rc.RDC_CD AND tn234.MAJ_CAT = rc.MAJ_CAT AND tn234.WEEK_ID = ww.WEEK_ID");

    // Recalculate TTL_TRF_OUT_Q
    exec("UPDATE TMP_PP SET TTL_TRF_OUT_Q = CW_TRF_OUT_Q + CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q");

    // ═══ STEP 6: Calculate — static columns + Week 1 then chain ═══

    // 6a: BGT_DC_CL_MBQ = MIN(next 4 wks TRF_OUT, BGT_DC_MBQ_SALE)
    exec("UPDATE TMP_PP SET BGT_DC_CL_MBQ = \
        CASE WHEN (CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q) < BGT_DC_MBQ_SALE \
             THEN (CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q) \
             ELSE BGT_DC_MBQ_SALE END");

    // 6b: GRT_CONS_Q = MIN of 4 values (all weeks initial pass)
    exec("UPDATE TMP_PP SET GRT_CONS_Q = \
        CASE WHEN TTL_TRF_OUT_Q = 0 THEN 0 ELSE LEAST( \
            TTL_TRF_OUT_Q * 0.30, \
            GREATEST(OP_STK - NT_ACT_STK, 0), \
            GREATEST(TTL_TRF_OUT_Q - GREATEST(BGT_CF_STK_Q - BGT_DC_CL_MBQ, 0), 0), \
            GREATEST(TTL_STK - NT_ACT_STK, 0) * GRT_CONS_PCT \
        ) END");

    // 6c: Cascading
    exec("UPDATE TMP_PP SET \
        PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q, \
        NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0 THEN OP_STK - GRT_CONS_Q ELSE 0 END");

    exec("UPDATE TMP_PP SET POS_PO_RAISED = \
        CASE WHEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q > 0 \
             THEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q ELSE 0 END");

    exec("UPDATE TMP_PP SET BGT_PUR_Q_INIT = POS_PO_RAISED");

    exec("UPDATE TMP_PP SET NEG_PO_RAISED = \
        CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q < 0 THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END");

    exec("UPDATE TMP_PP SET BGT_DC_CL_STK_Q = \
        CASE WHEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q > 0 \
             THEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q ELSE 0 END");

    exec("UPDATE TMP_PP SET \
        DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
        DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END, \
        CO_STK_EXCESS_Q = ST_STK_EXCESS_Q + CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
        CO_STK_SHORT_Q = ST_STK_SHORT_Q + CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END, \
        BGT_CO_CL_STK_Q = NET_BGT_ST_CL_STK_Q + NET_SSNL_CL_STK_Q + BGT_DC_CL_STK_Q, \
        FRESH_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN BGT_DC_CL_STK_Q / BIN_CAP ELSE 0 END, \
        GRT_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN OP_STK / BIN_CAP ELSE 0 END");

    // ═══ STEP 6d: Week chaining loop ═══
    for (var i = 2; i <= totalWeeks; i++) {
        var tw = execScalar("SELECT WID FROM TMP_PP_WKLIST WHERE SEQ = " + i);
        var pw = execScalar("SELECT WID FROM TMP_PP_WKLIST WHERE SEQ = " + (i - 1));

        // Chain: OP_STK = prev NET_SSNL_CL_STK_Q, DC stocks from prev BGT_DC_CL_STK_Q
        exec("UPDATE TMP_PP c SET \
            c.OP_STK = p.NET_SSNL_CL_STK_Q, \
            c.DC_STK_Q = p.BGT_DC_CL_STK_Q, \
            c.BGT_DC_OP_STK_Q = p.BGT_DC_CL_STK_Q, \
            c.BGT_CF_STK_Q = CASE WHEN p.BGT_DC_CL_STK_Q > 0 THEN p.BGT_DC_CL_STK_Q ELSE 0 END \
        FROM TMP_PP p \
        WHERE p.RDC_CD = c.RDC_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.WEEK_ID = " + pw +
        " AND c.WEEK_ID = " + tw);

        // Recalculate GRT_CONS_Q
        exec("UPDATE TMP_PP SET GRT_CONS_Q = \
            CASE WHEN TTL_TRF_OUT_Q = 0 THEN 0 ELSE LEAST( \
                TTL_TRF_OUT_Q * 0.30, \
                GREATEST(OP_STK - NT_ACT_STK, 0), \
                GREATEST(TTL_TRF_OUT_Q - GREATEST(BGT_CF_STK_Q - BGT_DC_CL_MBQ, 0), 0), \
                GREATEST(TTL_STK - NT_ACT_STK, 0) * GRT_CONS_PCT \
            ) END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_PP SET \
            PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q, \
            NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0 THEN OP_STK - GRT_CONS_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_PP SET POS_PO_RAISED = \
            CASE WHEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q > 0 \
                 THEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_PP SET BGT_PUR_Q_INIT = POS_PO_RAISED WHERE WEEK_ID = " + tw);

        // NEG_PO_RAISED chains from prev
        exec("UPDATE TMP_PP c SET c.NEG_PO_RAISED = \
            CASE WHEN c.BGT_PUR_Q_INIT - c.DEL_PEND_Q + p.NEG_PO_RAISED < 0 \
                 THEN c.BGT_PUR_Q_INIT - c.DEL_PEND_Q + p.NEG_PO_RAISED ELSE 0 END \
        FROM TMP_PP p \
        WHERE p.RDC_CD = c.RDC_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.WEEK_ID = " + pw +
        " AND c.WEEK_ID = " + tw);

        exec("UPDATE TMP_PP SET BGT_DC_CL_STK_Q = \
            CASE WHEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q > 0 \
                 THEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_PP SET \
            DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
            DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END, \
            CO_STK_EXCESS_Q = ST_STK_EXCESS_Q + CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
            CO_STK_SHORT_Q = ST_STK_SHORT_Q + CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END, \
            BGT_CO_CL_STK_Q = NET_BGT_ST_CL_STK_Q + NET_SSNL_CL_STK_Q + BGT_DC_CL_STK_Q, \
            FRESH_BIN_REQ = CASE WHEN BIN_CAP > 0 THEN BGT_DC_CL_STK_Q / BIN_CAP ELSE 0 END \
        WHERE WEEK_ID = " + tw);
    }

    // ═══ STEP 7: Delete old + INSERT into PURCHASE_PLAN ═══
    var delFilter = " WHERE WEEK_ID BETWEEN " + startWk + " AND " + endWk;
    if (rdcCode) delFilter += " AND RDC_CD = '" + rdcCode + "'";
    if (majCat)  delFilter += " AND MAJ_CAT = '" + majCat + "'";
    exec("DELETE FROM PURCHASE_PLAN" + delFilter);

    exec("INSERT INTO PURCHASE_PLAN ( \
        RDC_CD, RDC_NM, MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM, \
        WEEK_ID, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT, \
        DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q, \
        BIN_CAP_DC_TEAM, BIN_CAP, BGT_DISP_CL_Q, \
        CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, CW5_BGT_SALE_Q, \
        BGT_ST_OP_MBQ, NET_ST_OP_STK_Q, \
        BGT_DC_OP_STK_Q, PP_NT_ACT_Q, BGT_CF_STK_Q, \
        TTL_STK, OP_STK, NT_ACT_STK, \
        GRT_CONS_PCT, GRT_CONS_Q, DEL_PEND_Q, PP_NET_BGT_CF_STK_Q, \
        CW_TRF_OUT_Q, CW1_TRF_OUT_Q, CW2_TRF_OUT_Q, CW3_TRF_OUT_Q, CW4_TRF_OUT_Q, TTL_TRF_OUT_Q, \
        BGT_ST_CL_MBQ, NET_BGT_ST_CL_STK_Q, NET_SSNL_CL_STK_Q, \
        BGT_DC_MBQ_SALE, BGT_DC_CL_MBQ, BGT_DC_CL_STK_Q, \
        BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED, \
        BGT_CO_CL_STK_Q, \
        DC_STK_EXCESS_Q, DC_STK_SHORT_Q, ST_STK_EXCESS_Q, ST_STK_SHORT_Q, CO_STK_EXCESS_Q, CO_STK_SHORT_Q, \
        FRESH_BIN_REQ, GRT_BIN_REQ \
    ) \
    SELECT \
        NVL(RDC_CD,'NA'), NVL(RDC_NM,'NA'), NVL(MAJ_CAT,'NA'), NVL(SSN,'NA'), \
        NVL(SEG,'NA'), NVL(DIV,'NA'), NVL(SUB_DIV,'NA'), NVL(MAJ_CAT_NM,'NA'), \
        NVL(WEEK_ID,0), NVL(FY_WEEK,0), NVL(FY_YEAR,0), WK_ST_DT, WK_END_DT, \
        NVL(DC_STK_Q,0), NVL(GRT_STK_Q,0), NVL(S_GRT_STK_Q,0), NVL(W_GRT_STK_Q,0), \
        NVL(BIN_CAP_DC_TEAM,0), NVL(BIN_CAP,0), NVL(BGT_DISP_CL_Q,0), \
        NVL(CW_BGT_SALE_Q,0), NVL(CW1_BGT_SALE_Q,0), NVL(CW2_BGT_SALE_Q,0), \
        NVL(CW3_BGT_SALE_Q,0), NVL(CW4_BGT_SALE_Q,0), NVL(CW5_BGT_SALE_Q,0), \
        NVL(BGT_ST_OP_MBQ,0), NVL(NET_ST_OP_STK_Q,0), \
        NVL(BGT_DC_OP_STK_Q,0), NVL(PP_NT_ACT_Q,0), NVL(BGT_CF_STK_Q,0), \
        NVL(TTL_STK,0), NVL(OP_STK,0), NVL(NT_ACT_STK,0), \
        NVL(GRT_CONS_PCT,0), NVL(GRT_CONS_Q,0), NVL(DEL_PEND_Q,0), NVL(PP_NET_BGT_CF_STK_Q,0), \
        NVL(CW_TRF_OUT_Q,0), NVL(CW1_TRF_OUT_Q,0), NVL(CW2_TRF_OUT_Q,0), \
        NVL(CW3_TRF_OUT_Q,0), NVL(CW4_TRF_OUT_Q,0), NVL(TTL_TRF_OUT_Q,0), \
        NVL(BGT_ST_CL_MBQ,0), NVL(NET_BGT_ST_CL_STK_Q,0), NVL(NET_SSNL_CL_STK_Q,0), \
        NVL(BGT_DC_MBQ_SALE,0), NVL(BGT_DC_CL_MBQ,0), NVL(BGT_DC_CL_STK_Q,0), \
        NVL(BGT_PUR_Q_INIT,0), NVL(POS_PO_RAISED,0), NVL(NEG_PO_RAISED,0), \
        NVL(BGT_CO_CL_STK_Q,0), \
        NVL(DC_STK_EXCESS_Q,0), NVL(DC_STK_SHORT_Q,0), \
        NVL(ST_STK_EXCESS_Q,0), NVL(ST_STK_SHORT_Q,0), \
        NVL(CO_STK_EXCESS_Q,0), NVL(CO_STK_SHORT_Q,0), \
        NVL(FRESH_BIN_REQ,0), NVL(GRT_BIN_REQ,0) \
    FROM TMP_PP ORDER BY RDC_CD, MAJ_CAT, WEEK_SEQ");

    var rowsInserted = execScalar("SELECT COUNT(*) FROM PURCHASE_PLAN" + delFilter.replace("DELETE FROM PURCHASE_PLAN", ""));
    // Use the rows from TMP_PP as inserted count
    rowsInserted = execScalar("SELECT COUNT(*) FROM TMP_PP");

    // Cleanup
    exec("DROP TABLE IF EXISTS TMP_PP_WEEKS");
    exec("DROP TABLE IF EXISTS TMP_PP_WKLIST");
    exec("DROP TABLE IF EXISTS TMP_PP_WKMAP");
    exec("DROP TABLE IF EXISTS TMP_PP_RDCCAT");
    exec("DROP TABLE IF EXISTS TMP_PP_TRFAGG");
    exec("DROP TABLE IF EXISTS TMP_PP_TRFPREV");
    exec("DROP TABLE IF EXISTS TMP_PP_TRFNEXT");
    exec("DROP TABLE IF EXISTS TMP_PP_TRFNXT234");
    exec("DROP TABLE IF EXISTS TMP_PP_TRFFUTURE");
    exec("DROP TABLE IF EXISTS TMP_PP_GRT");
    exec("DROP TABLE IF EXISTS TMP_PP_REFDATA");
    exec("DROP TABLE IF EXISTS TMP_PP");

    var elapsed = Math.round((Date.now() - startTs) / 1000);
    return {"rows_inserted": rowsInserted, "seconds": elapsed};
$$;
"""

# ═══════════════════════════════════════════════════════════════
# SP 2: SF_SP_GENERATE_SUB_LEVEL_TRF
# ═══════════════════════════════════════════════════════════════
sp_sub_trf = r"""
CREATE OR REPLACE PROCEDURE SF_SP_GENERATE_SUB_LEVEL_TRF(
    LEVEL_PARAM VARCHAR,
    START_WEEK_ID FLOAT,
    END_WEEK_ID FLOAT,
    STORE_CODE VARCHAR DEFAULT NULL,
    MAJ_CAT_PARAM VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function exec(sql) { return snowflake.execute({sqlText: sql}); }
    function execScalar(sql) { var rs = exec(sql); rs.next(); return rs.getColumnValue(1); }

    var level    = LEVEL_PARAM;
    var startWk  = START_WEEK_ID;
    var endWk    = END_WEEK_ID;
    var storeCode = STORE_CODE;
    var majCat    = MAJ_CAT_PARAM;
    var startTs   = Date.now();

    // Map level to tables
    var stockTable, contTable, contCol;
    if (level === "MVGR") {
        stockTable = "SUB_ST_STK_MVGR"; contTable = "ST_MAJ_CAT_MACRO_MVGR_PLAN"; contCol = "DISP_MVGR_MATRIX";
    } else if (level === "SZ") {
        stockTable = "SUB_ST_STK_SZ"; contTable = "ST_MAJ_CAT_SZ_PLAN"; contCol = "SZ";
    } else if (level === "SEG") {
        stockTable = "SUB_ST_STK_SEG"; contTable = "ST_MAJ_CAT_SEG_PLAN"; contCol = "SEG";
    } else if (level === "VND") {
        stockTable = "SUB_ST_STK_VND"; contTable = "ST_MAJ_CAT_VND_PLAN"; contCol = "M_VND_CD";
    } else {
        throw "Invalid LEVEL: " + level + ". Must be MVGR, SZ, SEG, or VND.";
    }

    // ═══ STEP 1: Weeks ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_WEEKS AS \
    SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT, \
        'WK_' || CAST(FY_WEEK AS VARCHAR) AS WK_COL_NAME \
    FROM WEEK_CALENDAR \
    WHERE WEEK_ID BETWEEN " + startWk + " AND " + endWk);

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_WKLIST AS \
    SELECT ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ, WEEK_ID AS WID \
    FROM TMP_SLT_WEEKS");

    var totalWeeks = execScalar("SELECT COUNT(*) FROM TMP_SLT_WKLIST");
    var firstWk = execScalar("SELECT WID FROM TMP_SLT_WKLIST WHERE SEQ = 1");

    // ═══ STEP 2: Store x Category x SubValue combos ═══
    var storeFilter = storeCode ? " AND SM.ST_CD = '" + storeCode + "'" : "";
    var catFilter   = majCat   ? " AND C.MAJ_CAT_CD = '" + majCat + "'" : "";

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_SC AS \
    SELECT SM.ST_CD, SM.ST_NM, SM.RDC_CD, SM.RDC_NM, SM.HUB_CD, SM.HUB_NM, SM.AREA, \
        C.MAJ_CAT_CD AS MAJ_CAT, C." + contCol + " AS SUB_VALUE, C.CONT_PCT, \
        NVL(BC.BIN_CAP, 0) AS BIN_CAP, \
        NVL(BM.SEG, 'NA') AS SEG, NVL(BM.DIV, 'NA') AS DIV, \
        NVL(BM.SUB_DIV, 'NA') AS SUB_DIV, NVL(BM.MAJ_CAT_NM, 'NA') AS MAJ_CAT_NM, \
        NVL(BM.SSN, 'NA') AS SSN \
    FROM " + contTable + " C \
    INNER JOIN MASTER_ST_MASTER SM ON SM.ST_CD = C.ST_CD \
    LEFT JOIN MASTER_BIN_CAPACITY BC ON BC.MAJ_CAT = C.MAJ_CAT_CD \
    LEFT JOIN (SELECT SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN, \
        ROW_NUMBER() OVER (PARTITION BY MAJ_CAT_NM ORDER BY ID) AS RN \
        FROM MASTER_PRODUCT_HIERARCHY) BM ON BM.MAJ_CAT_NM = C.MAJ_CAT_CD AND BM.RN = 1 \
    WHERE 1=1" + storeFilter + catFilter);

    // ═══ STEP 3: Sale x CONT_PCT, Display x CONT_PCT, Stock ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_SQ AS \
    SELECT u.ST_CD, u.MAJ_CAT, sc.SUB_VALUE, u.WK_COL_NAME AS WK_NAME, \
        u.SALE_QTY * sc.CONT_PCT AS SALE_QTY \
    FROM ( \
        SELECT ST_CD, MAJ_CAT, WK_COL_NAME, SALE_QTY \
        FROM QTY_SALE_QTY \
        UNPIVOT (SALE_QTY FOR WK_COL_NAME IN ( \
            WK_1,WK_2,WK_3,WK_4,WK_5,WK_6,WK_7,WK_8,WK_9,WK_10,WK_11,WK_12, \
            WK_13,WK_14,WK_15,WK_16,WK_17,WK_18,WK_19,WK_20,WK_21,WK_22,WK_23,WK_24, \
            WK_25,WK_26,WK_27,WK_28,WK_29,WK_30,WK_31,WK_32,WK_33,WK_34,WK_35,WK_36, \
            WK_37,WK_38,WK_39,WK_40,WK_41,WK_42,WK_43,WK_44,WK_45,WK_46,WK_47,WK_48 \
        )) \
    ) u \
    INNER JOIN TMP_SLT_SC sc ON sc.ST_CD = u.ST_CD AND sc.MAJ_CAT = u.MAJ_CAT");

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_DQ AS \
    SELECT u.ST_CD, u.MAJ_CAT, sc.SUB_VALUE, u.WK_COL_NAME AS WK_NAME, \
        u.DISP_QTY * sc.CONT_PCT AS DISP_QTY \
    FROM ( \
        SELECT ST_CD, MAJ_CAT, WK_COL_NAME, DISP_QTY \
        FROM QTY_DISP_QTY \
        UNPIVOT (DISP_QTY FOR WK_COL_NAME IN ( \
            WK_1,WK_2,WK_3,WK_4,WK_5,WK_6,WK_7,WK_8,WK_9,WK_10,WK_11,WK_12, \
            WK_13,WK_14,WK_15,WK_16,WK_17,WK_18,WK_19,WK_20,WK_21,WK_22,WK_23,WK_24, \
            WK_25,WK_26,WK_27,WK_28,WK_29,WK_30,WK_31,WK_32,WK_33,WK_34,WK_35,WK_36, \
            WK_37,WK_38,WK_39,WK_40,WK_41,WK_42,WK_43,WK_44,WK_45,WK_46,WK_47,WK_48 \
        )) \
    ) u \
    INNER JOIN TMP_SLT_SC sc ON sc.ST_CD = u.ST_CD AND sc.MAJ_CAT = u.MAJ_CAT");

    // Level-specific store stock (latest date per combo)
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_LS AS \
    SELECT ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY FROM ( \
        SELECT ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY, \
            ROW_NUMBER() OVER (PARTITION BY ST_CD, MAJ_CAT, SUB_VALUE ORDER BY DATE DESC) AS RN \
        FROM " + stockTable + " \
    ) r WHERE RN = 1");

    // ═══ STEP 4: Build chain table (ST_CD + MAJ_CAT + SUB_VALUE + WEEK) ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLT_CHAIN AS \
    SELECT SC.ST_CD, SC.MAJ_CAT, SC.SUB_VALUE, W.WEEK_ID, SC.SSN, \
        NVL(DQ.DISP_QTY, 0) + NVL(SQ1.SALE_QTY, 0) AS MBQ, \
        NVL(SQ0.SALE_QTY, 0) AS SALE, \
        NVL(LS.STK_QTY, 0) AS OP_STK, \
        0::NUMBER(18,4) AS NET_CF, \
        0::NUMBER(18,4) AS TRF_IN, \
        0::NUMBER(18,4) AS CL_STK \
    FROM (SELECT DISTINCT ST_CD, MAJ_CAT, SUB_VALUE, SSN FROM TMP_SLT_SC) SC \
    CROSS JOIN TMP_SLT_WEEKS W \
    LEFT JOIN TMP_SLT_SQ SQ0 ON SQ0.ST_CD = SC.ST_CD AND SQ0.MAJ_CAT = SC.MAJ_CAT \
        AND SQ0.SUB_VALUE = SC.SUB_VALUE AND SQ0.WK_NAME = W.WK_COL_NAME \
    LEFT JOIN TMP_SLT_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SLT_SQ SQ1 ON SQ1.ST_CD = SC.ST_CD AND SQ1.MAJ_CAT = SC.MAJ_CAT \
        AND SQ1.SUB_VALUE = SC.SUB_VALUE AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_SLT_DQ DQ ON DQ.ST_CD = SC.ST_CD AND DQ.MAJ_CAT = SC.MAJ_CAT \
        AND DQ.SUB_VALUE = SC.SUB_VALUE AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME) \
    LEFT JOIN TMP_SLT_LS LS ON LS.ST_CD = SC.ST_CD AND LS.MAJ_CAT = SC.MAJ_CAT \
        AND LS.SUB_VALUE = SC.SUB_VALUE");

    // ═══ STEP 5: Week 1 calculations ═══
    exec("UPDATE TMP_SLT_CHAIN SET NET_CF = CASE \
        WHEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0 \
        THEN OP_STK - ROUND(ROUND(OP_STK * 0.08, 0) * CASE WHEN SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) \
        ELSE 0 END \
    WHERE WEEK_ID = " + firstWk);

    exec("UPDATE TMP_SLT_CHAIN SET \
        TRF_IN = CASE WHEN MBQ = 0 AND SALE = 0 THEN 0 \
                      WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF ELSE 0 END, \
        CL_STK = CASE WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
                      WHEN MBQ + SALE > NET_CF THEN MBQ \
                      ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END END \
    WHERE WEEK_ID = " + firstWk);

    // ═══ STEP 6: Chain weeks 2..N ═══
    for (var i = 2; i <= totalWeeks; i++) {
        var tw = execScalar("SELECT WID FROM TMP_SLT_WKLIST WHERE SEQ = " + i);
        var pw = execScalar("SELECT WID FROM TMP_SLT_WKLIST WHERE SEQ = " + (i - 1));

        exec("UPDATE TMP_SLT_CHAIN c SET \
            c.OP_STK = p.CL_STK, \
            c.NET_CF = CASE \
                WHEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) > 0 \
                THEN p.CL_STK - ROUND(ROUND(p.CL_STK * 0.08, 0) * CASE WHEN c.SSN IN ('S','PS') THEN 1.0 ELSE 0.5 END, 0) \
                ELSE 0 END \
        FROM TMP_SLT_CHAIN p \
        WHERE p.ST_CD = c.ST_CD AND p.MAJ_CAT = c.MAJ_CAT AND p.SUB_VALUE = c.SUB_VALUE AND p.WEEK_ID = " + pw +
        " AND c.WEEK_ID = " + tw);

        exec("UPDATE TMP_SLT_CHAIN SET \
            TRF_IN = CASE WHEN MBQ = 0 AND SALE = 0 THEN 0 \
                          WHEN MBQ + SALE - NET_CF > 0 THEN MBQ + SALE - NET_CF ELSE 0 END, \
            CL_STK = CASE WHEN MBQ = 0 AND SALE = 0 THEN NET_CF \
                          WHEN MBQ + SALE > NET_CF THEN MBQ \
                          ELSE CASE WHEN NET_CF - SALE > 0 THEN NET_CF - SALE ELSE 0 END END \
        WHERE WEEK_ID = " + tw);
    }

    // ═══ STEP 7: Delete old + INSERT into SUB_LEVEL_TRF_PLAN ═══
    exec("DELETE FROM SUB_LEVEL_TRF_PLAN WHERE LEVEL = '" + level + "'");

    exec("INSERT INTO SUB_LEVEL_TRF_PLAN ( \
        LEVEL, SUB_VALUE, ST_CD, MAJ_CAT, CONT_PCT, FY_YEAR, FY_WEEK, \
        BGT_DISP_CL_Q, CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q, COVER_SALE_QTY, \
        TRF_IN_STK_Q, DC_MBQ, \
        BGT_TTL_CF_OP_STK_Q, BGT_TTL_CF_CL_STK_Q, BGT_ST_CL_MBQ, \
        ST_CL_EXCESS_Q, ST_CL_SHORT_Q, CREATED_DT \
    ) \
    SELECT \
        '" + level + "', ch.SUB_VALUE, ch.ST_CD, ch.MAJ_CAT, \
        NVL(SC.CONT_PCT, 0), W.FY_YEAR, W.FY_WEEK, \
        NVL(DQ.DISP_QTY, 0), \
        ch.SALE, \
        NVL(SQ1.SALE_QTY, 0), \
        NVL(SQ2.SALE_QTY, 0), \
        NVL(SQ1.SALE_QTY, 0), \
        ch.TRF_IN, \
        NVL(SQ1.SALE_QTY, 0) + NVL(SQN2.SALE_QTY, 0) + NVL(SQN3.SALE_QTY, 0) + NVL(SQN4.SALE_QTY, 0), \
        ch.OP_STK, ch.CL_STK, ch.MBQ, \
        CASE WHEN ch.CL_STK - ch.MBQ > 0 THEN ch.CL_STK - ch.MBQ ELSE 0 END, \
        CASE WHEN ch.MBQ - ch.CL_STK > 0 THEN ch.MBQ - ch.CL_STK ELSE 0 END, \
        CURRENT_TIMESTAMP() \
    FROM TMP_SLT_CHAIN ch \
    INNER JOIN TMP_SLT_SC SC ON SC.ST_CD = ch.ST_CD AND SC.MAJ_CAT = ch.MAJ_CAT AND SC.SUB_VALUE = ch.SUB_VALUE \
    INNER JOIN TMP_SLT_WEEKS W ON W.WEEK_ID = ch.WEEK_ID \
    LEFT JOIN TMP_SLT_WEEKS W1 ON W1.WEEK_SEQ = W.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SLT_SQ SQ1 ON SQ1.ST_CD = ch.ST_CD AND SQ1.MAJ_CAT = ch.MAJ_CAT AND SQ1.SUB_VALUE = ch.SUB_VALUE AND SQ1.WK_NAME = W1.WK_COL_NAME \
    LEFT JOIN TMP_SLT_WEEKS W2 ON W2.WEEK_SEQ = W.WEEK_SEQ + 8 \
    LEFT JOIN TMP_SLT_SQ SQ2 ON SQ2.ST_CD = ch.ST_CD AND SQ2.MAJ_CAT = ch.MAJ_CAT AND SQ2.SUB_VALUE = ch.SUB_VALUE AND SQ2.WK_NAME = W2.WK_COL_NAME \
    LEFT JOIN TMP_SLT_WEEKS WN2 ON WN2.WEEK_SEQ = W.WEEK_SEQ + 2 \
    LEFT JOIN TMP_SLT_SQ SQN2 ON SQN2.ST_CD = ch.ST_CD AND SQN2.MAJ_CAT = ch.MAJ_CAT AND SQN2.SUB_VALUE = ch.SUB_VALUE AND SQN2.WK_NAME = WN2.WK_COL_NAME \
    LEFT JOIN TMP_SLT_WEEKS WN3 ON WN3.WEEK_SEQ = W.WEEK_SEQ + 3 \
    LEFT JOIN TMP_SLT_SQ SQN3 ON SQN3.ST_CD = ch.ST_CD AND SQN3.MAJ_CAT = ch.MAJ_CAT AND SQN3.SUB_VALUE = ch.SUB_VALUE AND SQN3.WK_NAME = WN3.WK_COL_NAME \
    LEFT JOIN TMP_SLT_WEEKS WN4 ON WN4.WEEK_SEQ = W.WEEK_SEQ + 4 \
    LEFT JOIN TMP_SLT_SQ SQN4 ON SQN4.ST_CD = ch.ST_CD AND SQN4.MAJ_CAT = ch.MAJ_CAT AND SQN4.SUB_VALUE = ch.SUB_VALUE AND SQN4.WK_NAME = WN4.WK_COL_NAME \
    LEFT JOIN TMP_SLT_DQ DQ ON DQ.ST_CD = ch.ST_CD AND DQ.MAJ_CAT = ch.MAJ_CAT AND DQ.SUB_VALUE = ch.SUB_VALUE AND DQ.WK_NAME = NVL(W1.WK_COL_NAME, W.WK_COL_NAME)");

    var rowsInserted = execScalar("SELECT COUNT(*) FROM TMP_SLT_CHAIN");

    // Cleanup
    exec("DROP TABLE IF EXISTS TMP_SLT_WEEKS");
    exec("DROP TABLE IF EXISTS TMP_SLT_WKLIST");
    exec("DROP TABLE IF EXISTS TMP_SLT_SC");
    exec("DROP TABLE IF EXISTS TMP_SLT_SQ");
    exec("DROP TABLE IF EXISTS TMP_SLT_DQ");
    exec("DROP TABLE IF EXISTS TMP_SLT_LS");
    exec("DROP TABLE IF EXISTS TMP_SLT_CHAIN");

    var elapsed = Math.round((Date.now() - startTs) / 1000);
    return {"level": level, "rows_inserted": rowsInserted, "seconds": elapsed};
$$;
"""

# ═══════════════════════════════════════════════════════════════
# SP 3: SF_SP_GENERATE_SUB_LEVEL_PP
# ═══════════════════════════════════════════════════════════════
sp_sub_pp = r"""
CREATE OR REPLACE PROCEDURE SF_SP_GENERATE_SUB_LEVEL_PP(
    LEVEL_PARAM VARCHAR,
    START_WEEK_ID FLOAT,
    END_WEEK_ID FLOAT,
    RDC_CODE VARCHAR DEFAULT NULL,
    MAJ_CAT_PARAM VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    function exec(sql) { return snowflake.execute({sqlText: sql}); }
    function execScalar(sql) { var rs = exec(sql); rs.next(); return rs.getColumnValue(1); }

    var level   = LEVEL_PARAM;
    var startWk = START_WEEK_ID;
    var endWk   = END_WEEK_ID;
    var rdcCode = RDC_CODE;
    var majCat  = MAJ_CAT_PARAM;
    var startTs = Date.now();

    // Map level to DC stock table
    var dcStockTable;
    if (level === "MVGR")     dcStockTable = "SUB_DC_STK_MVGR";
    else if (level === "SZ")  dcStockTable = "SUB_DC_STK_SZ";
    else if (level === "SEG") dcStockTable = "SUB_DC_STK_SEG";
    else if (level === "VND") dcStockTable = "SUB_DC_STK_VND";
    else throw "Invalid LEVEL: " + level + ". Must be MVGR, SZ, SEG, or VND.";

    // ═══ STEP 1: Weeks ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_WEEKS AS \
    SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT \
    FROM WEEK_CALENDAR \
    WHERE WEEK_ID BETWEEN " + startWk + " AND " + endWk);

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_WKLIST AS \
    SELECT ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ, WEEK_ID AS WID \
    FROM TMP_SLP_WEEKS");

    var totalWeeks = execScalar("SELECT COUNT(*) FROM TMP_SLP_WKLIST");

    // Week offset map
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_WKMAP AS \
    SELECT w.WEEK_ID, wP.WEEK_ID AS PREV_WEEK_ID, \
        w1.WEEK_ID AS NEXT1_WEEK_ID, w2.WEEK_ID AS NEXT2_WEEK_ID, \
        w3.WEEK_ID AS NEXT3_WEEK_ID, w4.WEEK_ID AS NEXT4_WEEK_ID \
    FROM TMP_SLP_WEEKS w \
    LEFT JOIN TMP_SLP_WEEKS wP ON wP.WEEK_SEQ = w.WEEK_SEQ - 1 \
    LEFT JOIN TMP_SLP_WEEKS w1 ON w1.WEEK_SEQ = w.WEEK_SEQ + 1 \
    LEFT JOIN TMP_SLP_WEEKS w2 ON w2.WEEK_SEQ = w.WEEK_SEQ + 2 \
    LEFT JOIN TMP_SLP_WEEKS w3 ON w3.WEEK_SEQ = w.WEEK_SEQ + 3 \
    LEFT JOIN TMP_SLP_WEEKS w4 ON w4.WEEK_SEQ = w.WEEK_SEQ + 4");

    // ═══ STEP 2: Aggregate from SUB_LEVEL_TRF_PLAN (by RDC + MAJ_CAT + SUB_VALUE) ═══
    var rdcFilter = rdcCode ? " AND sm.RDC_CD = '" + rdcCode + "'" : "";
    var catFilter  = majCat ? " AND t.MAJ_CAT = '" + majCat + "'" : "";

    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_TRFAGG AS \
    SELECT sm.RDC_CD, t.MAJ_CAT, t.SUB_VALUE, w.WEEK_ID, \
        SUM(t.CM_BGT_SALE_Q) AS CW_BGT_SALE_Q, \
        SUM(t.CM1_BGT_SALE_Q) AS CW1_BGT_SALE_Q, \
        SUM(t.BGT_DISP_CL_Q) AS BGT_DISP_CL_Q, \
        SUM(t.BGT_ST_CL_MBQ) AS BGT_ST_CL_MBQ, \
        SUM(t.BGT_TTL_CF_OP_STK_Q) AS NET_ST_OP_STK_Q, \
        SUM(t.TRF_IN_STK_Q) AS CW_TRF_OUT_Q, \
        SUM(t.BGT_TTL_CF_CL_STK_Q) AS NET_BGT_ST_CL_STK_Q, \
        SUM(t.ST_CL_EXCESS_Q) AS ST_STK_EXCESS_Q, \
        SUM(t.ST_CL_SHORT_Q) AS ST_STK_SHORT_Q \
    FROM SUB_LEVEL_TRF_PLAN t \
    INNER JOIN MASTER_ST_MASTER sm ON sm.ST_CD = t.ST_CD \
    INNER JOIN TMP_SLP_WEEKS w ON w.FY_YEAR = t.FY_YEAR AND w.FY_WEEK = t.FY_WEEK \
    WHERE t.LEVEL = '" + level + "'" + rdcFilter + catFilter +
    " GROUP BY sm.RDC_CD, t.MAJ_CAT, t.SUB_VALUE, w.WEEK_ID");

    // Future TRF_OUT + future sales
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_TRFNEXT AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.SUB_VALUE, ta.WEEK_ID, \
        NVL(t1.CW_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q, \
        NVL(t2.CW_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q, \
        NVL(t3.CW_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q, \
        NVL(t4.CW_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q, \
        NVL(t1.CW1_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q, \
        NVL(t2.CW1_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q, \
        NVL(t3.CW1_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q, \
        NVL(t4.CW1_BGT_SALE_Q, 0) AS CW5_BGT_SALE_Q \
    FROM TMP_SLP_TRFAGG ta \
    LEFT JOIN TMP_SLP_WKMAP wm ON wm.WEEK_ID = ta.WEEK_ID \
    LEFT JOIN TMP_SLP_TRFAGG t1 ON t1.RDC_CD=ta.RDC_CD AND t1.MAJ_CAT=ta.MAJ_CAT AND t1.SUB_VALUE=ta.SUB_VALUE AND t1.WEEK_ID=wm.NEXT1_WEEK_ID \
    LEFT JOIN TMP_SLP_TRFAGG t2 ON t2.RDC_CD=ta.RDC_CD AND t2.MAJ_CAT=ta.MAJ_CAT AND t2.SUB_VALUE=ta.SUB_VALUE AND t2.WEEK_ID=wm.NEXT2_WEEK_ID \
    LEFT JOIN TMP_SLP_TRFAGG t3 ON t3.RDC_CD=ta.RDC_CD AND t3.MAJ_CAT=ta.MAJ_CAT AND t3.SUB_VALUE=ta.SUB_VALUE AND t3.WEEK_ID=wm.NEXT3_WEEK_ID \
    LEFT JOIN TMP_SLP_TRFAGG t4 ON t4.RDC_CD=ta.RDC_CD AND t4.MAJ_CAT=ta.MAJ_CAT AND t4.SUB_VALUE=ta.SUB_VALUE AND t4.WEEK_ID=wm.NEXT4_WEEK_ID");

    // ═══ STEP 3: DC Stock ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_DCSTK AS \
    SELECT RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, W_GRT_STK_Q FROM ( \
        SELECT RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, W_GRT_STK_Q, \
            ROW_NUMBER() OVER (PARTITION BY RDC_CD, MAJ_CAT, SUB_VALUE ORDER BY DATE DESC) AS RN \
        FROM " + dcStockTable + " \
    ) r WHERE RN = 1");

    // ═══ STEP 4: Build #PP calculation table ═══
    exec("CREATE OR REPLACE TEMPORARY TABLE TMP_SLP_PP AS \
    SELECT ta.RDC_CD, ta.MAJ_CAT, ta.SUB_VALUE, ww.WEEK_ID, ww.FY_WEEK, ww.FY_YEAR, \
        NVL(dc.DC_STK_Q, 0) AS DC_STK_Q, \
        NVL(dc.GRT_STK_Q, 0) AS GRT_STK_Q, \
        NVL(dc.GRT_STK_Q, 0) AS TTL_STK, \
        NVL(dc.GRT_STK_Q, 0) AS OP_STK, \
        0::NUMBER(18,4) AS NT_ACT_STK, \
        NVL(ta.BGT_DISP_CL_Q, 0) AS BGT_DISP_CL_Q, \
        NVL(ta.CW_BGT_SALE_Q, 0) AS CW_BGT_SALE_Q, \
        NVL(ta.CW1_BGT_SALE_Q, 0) AS CW1_BGT_SALE_Q, \
        NVL(tn.CW2_BGT_SALE_Q, 0) AS CW2_BGT_SALE_Q, \
        NVL(tn.CW3_BGT_SALE_Q, 0) AS CW3_BGT_SALE_Q, \
        NVL(tn.CW4_BGT_SALE_Q, 0) AS CW4_BGT_SALE_Q, \
        NVL(dc.DC_STK_Q, 0) AS BGT_DC_OP_STK_Q, \
        NVL(dc.DC_STK_Q, 0) AS BGT_CF_STK_Q, \
        NVL(ta.CW_TRF_OUT_Q, 0) AS CW_TRF_OUT_Q, \
        NVL(tn.CW1_TRF_OUT_Q, 0) AS CW1_TRF_OUT_Q, \
        NVL(tn.CW2_TRF_OUT_Q, 0) AS CW2_TRF_OUT_Q, \
        NVL(tn.CW3_TRF_OUT_Q, 0) AS CW3_TRF_OUT_Q, \
        NVL(tn.CW4_TRF_OUT_Q, 0) AS CW4_TRF_OUT_Q, \
        0::NUMBER(18,4) AS TTL_TRF_OUT_Q, \
        NVL(ta.CW1_BGT_SALE_Q,0)+NVL(tn.CW2_BGT_SALE_Q,0)+NVL(tn.CW3_BGT_SALE_Q,0)+NVL(tn.CW4_BGT_SALE_Q,0) AS BGT_DC_MBQ_SALE, \
        0::NUMBER(18,4) AS BGT_DC_CL_MBQ, \
        0::NUMBER(18,4) AS GRT_CONS_Q, \
        0::NUMBER(18,4) AS PP_NET_BGT_CF_STK_Q, \
        0::NUMBER(18,4) AS NET_SSNL_CL_STK_Q, \
        0::NUMBER(18,4) AS POS_PO_RAISED, \
        0::NUMBER(18,4) AS NEG_PO_RAISED, \
        0::NUMBER(18,4) AS DEL_PEND_Q, \
        0::NUMBER(18,4) AS BGT_DC_CL_STK_Q, \
        0::NUMBER(18,4) AS BGT_PUR_Q_INIT, \
        0::NUMBER(18,4) AS DC_STK_EXCESS_Q, \
        0::NUMBER(18,4) AS DC_STK_SHORT_Q \
    FROM (SELECT DISTINCT RDC_CD, MAJ_CAT, SUB_VALUE FROM TMP_SLP_TRFAGG) AS combos \
    CROSS JOIN TMP_SLP_WEEKS ww \
    LEFT JOIN TMP_SLP_TRFAGG ta ON ta.RDC_CD=combos.RDC_CD AND ta.MAJ_CAT=combos.MAJ_CAT AND ta.SUB_VALUE=combos.SUB_VALUE AND ta.WEEK_ID=ww.WEEK_ID \
    LEFT JOIN TMP_SLP_TRFNEXT tn ON tn.RDC_CD=combos.RDC_CD AND tn.MAJ_CAT=combos.MAJ_CAT AND tn.SUB_VALUE=combos.SUB_VALUE AND tn.WEEK_ID=ww.WEEK_ID \
    LEFT JOIN TMP_SLP_DCSTK dc ON dc.RDC_CD=combos.RDC_CD AND dc.MAJ_CAT=combos.MAJ_CAT AND dc.SUB_VALUE=combos.SUB_VALUE");

    // TTL_TRF_OUT_Q
    exec("UPDATE TMP_SLP_PP SET TTL_TRF_OUT_Q = CW_TRF_OUT_Q + CW1_TRF_OUT_Q + CW2_TRF_OUT_Q + CW3_TRF_OUT_Q + CW4_TRF_OUT_Q");

    // ═══ STEP 5: Week 1 calculations ═══
    exec("UPDATE TMP_SLP_PP SET BGT_DC_CL_MBQ = \
        CASE WHEN (CW1_TRF_OUT_Q+CW2_TRF_OUT_Q+CW3_TRF_OUT_Q+CW4_TRF_OUT_Q) < BGT_DC_MBQ_SALE \
             THEN (CW1_TRF_OUT_Q+CW2_TRF_OUT_Q+CW3_TRF_OUT_Q+CW4_TRF_OUT_Q) \
             ELSE BGT_DC_MBQ_SALE END");

    exec("UPDATE TMP_SLP_PP SET GRT_CONS_Q = 0");

    exec("UPDATE TMP_SLP_PP SET \
        PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q, \
        NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0 THEN OP_STK - GRT_CONS_Q ELSE 0 END");

    exec("UPDATE TMP_SLP_PP SET POS_PO_RAISED = \
        CASE WHEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q > 0 \
             THEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q ELSE 0 END");

    exec("UPDATE TMP_SLP_PP SET BGT_PUR_Q_INIT = POS_PO_RAISED");

    exec("UPDATE TMP_SLP_PP SET NEG_PO_RAISED = \
        CASE WHEN BGT_PUR_Q_INIT - DEL_PEND_Q < 0 THEN BGT_PUR_Q_INIT - DEL_PEND_Q ELSE 0 END");

    exec("UPDATE TMP_SLP_PP SET BGT_DC_CL_STK_Q = \
        CASE WHEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q > 0 \
             THEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q ELSE 0 END");

    exec("UPDATE TMP_SLP_PP SET \
        DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
        DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END");

    // ═══ STEP 6: Chain weeks 2..N ═══
    for (var i = 2; i <= totalWeeks; i++) {
        var tw = execScalar("SELECT WID FROM TMP_SLP_WKLIST WHERE SEQ = " + i);
        var pw = execScalar("SELECT WID FROM TMP_SLP_WKLIST WHERE SEQ = " + (i - 1));

        // Chain: OP_STK = prev NET_SSNL_CL_STK_Q
        exec("UPDATE TMP_SLP_PP c SET \
            c.OP_STK = p.NET_SSNL_CL_STK_Q, \
            c.DC_STK_Q = p.BGT_DC_CL_STK_Q, \
            c.BGT_DC_OP_STK_Q = p.BGT_DC_CL_STK_Q, \
            c.BGT_CF_STK_Q = CASE WHEN p.BGT_DC_CL_STK_Q > 0 THEN p.BGT_DC_CL_STK_Q ELSE 0 END \
        FROM TMP_SLP_PP p \
        WHERE p.RDC_CD=c.RDC_CD AND p.MAJ_CAT=c.MAJ_CAT AND p.SUB_VALUE=c.SUB_VALUE AND p.WEEK_ID=" + pw +
        " AND c.WEEK_ID=" + tw);

        exec("UPDATE TMP_SLP_PP SET \
            PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q, \
            NET_SSNL_CL_STK_Q = CASE WHEN OP_STK - GRT_CONS_Q > 0 THEN OP_STK - GRT_CONS_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_SLP_PP SET POS_PO_RAISED = \
            CASE WHEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q > 0 \
                 THEN BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_SLP_PP SET BGT_PUR_Q_INIT = POS_PO_RAISED WHERE WEEK_ID = " + tw);

        // NEG_PO_RAISED chains from prev
        exec("UPDATE TMP_SLP_PP c SET c.NEG_PO_RAISED = \
            CASE WHEN c.BGT_PUR_Q_INIT - c.DEL_PEND_Q + p.NEG_PO_RAISED < 0 \
                 THEN c.BGT_PUR_Q_INIT - c.DEL_PEND_Q + p.NEG_PO_RAISED ELSE 0 END \
        FROM TMP_SLP_PP p \
        WHERE p.RDC_CD=c.RDC_CD AND p.MAJ_CAT=c.MAJ_CAT AND p.SUB_VALUE=c.SUB_VALUE AND p.WEEK_ID=" + pw +
        " AND c.WEEK_ID=" + tw);

        exec("UPDATE TMP_SLP_PP SET BGT_DC_CL_STK_Q = \
            CASE WHEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q > 0 \
                 THEN BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);

        exec("UPDATE TMP_SLP_PP SET \
            DC_STK_EXCESS_Q = CASE WHEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ > 0 THEN BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ ELSE 0 END, \
            DC_STK_SHORT_Q = CASE WHEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q > 0 THEN BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q ELSE 0 END \
        WHERE WEEK_ID = " + tw);
    }

    // ═══ STEP 7: Delete old + INSERT into SUB_LEVEL_PP_PLAN ═══
    exec("DELETE FROM SUB_LEVEL_PP_PLAN WHERE LEVEL = '" + level + "'");

    exec("INSERT INTO SUB_LEVEL_PP_PLAN ( \
        LEVEL, SUB_VALUE, RDC_CD, MAJ_CAT, CONT_PCT, FY_YEAR, FY_WEEK, \
        BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, \
        BGT_PUR_Q_INIT, BGT_DC_CL_STK_Q, BGT_DC_CL_MBQ, BGT_DC_MBQ_SALE, \
        DC_STK_EXCESS_Q, DC_STK_SHORT_Q, CREATED_DT \
    ) \
    SELECT '" + level + "', SUB_VALUE, RDC_CD, MAJ_CAT, 0, FY_YEAR, FY_WEEK, \
        BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, \
        BGT_PUR_Q_INIT, BGT_DC_CL_STK_Q, BGT_DC_CL_MBQ, BGT_DC_MBQ_SALE, \
        DC_STK_EXCESS_Q, DC_STK_SHORT_Q, CURRENT_TIMESTAMP() \
    FROM TMP_SLP_PP");

    var rowsInserted = execScalar("SELECT COUNT(*) FROM TMP_SLP_PP");

    // Cleanup
    exec("DROP TABLE IF EXISTS TMP_SLP_WEEKS");
    exec("DROP TABLE IF EXISTS TMP_SLP_WKLIST");
    exec("DROP TABLE IF EXISTS TMP_SLP_WKMAP");
    exec("DROP TABLE IF EXISTS TMP_SLP_TRFAGG");
    exec("DROP TABLE IF EXISTS TMP_SLP_TRFNEXT");
    exec("DROP TABLE IF EXISTS TMP_SLP_DCSTK");
    exec("DROP TABLE IF EXISTS TMP_SLP_PP");

    var elapsed = Math.round((Date.now() - startTs) / 1000);
    return {"level": level, "rows_inserted": rowsInserted, "seconds": elapsed};
$$;
"""

# ═══════════════════════════════════════════════════════════════
# DEPLOY ALL 3 SPs
# ═══════════════════════════════════════════════════════════════
print("Deploying SF_SP_GENERATE_PURCHASE_PLAN...")
cur.execute(sp_pp)
print("  -> OK")

print("Deploying SF_SP_GENERATE_SUB_LEVEL_TRF...")
cur.execute(sp_sub_trf)
print("  -> OK")

print("Deploying SF_SP_GENERATE_SUB_LEVEL_PP...")
cur.execute(sp_sub_pp)
print("  -> OK")

print("\nAll 3 stored procedures deployed successfully.")
print("  CALL SF_SP_GENERATE_PURCHASE_PLAN(202401, 202448, NULL, NULL);")
print("  CALL SF_SP_GENERATE_SUB_LEVEL_TRF('MVGR', 202401, 202448, NULL, NULL);")
print("  CALL SF_SP_GENERATE_SUB_LEVEL_PP('MVGR', 202401, 202448, NULL, NULL);")

conn.close()
