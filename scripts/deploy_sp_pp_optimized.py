"""
Deploy optimized SF_SP_GENERATE_PURCHASE_PLAN (SQL Scripting + Recursive CTE).
Replaces ~408-UPDATE JavaScript loop with ONE recursive CTE query.
Only updates the PP SP — sub-level SPs are in deploy_sp_pp_sublevel.py.
"""
import snowflake.connector, sys

conn = snowflake.connector.connect(
    account='iafphkw-hh80816', user='akashv2kart', password='SVXqEe5pDdamMb9',
    database='V2RETAIL', schema='GOLD', warehouse='V2_WH'
)
cur = conn.cursor()

WK = ",".join([f"WK_{i}" for i in range(1, 49)])

sp_pp = f"""
CREATE OR REPLACE PROCEDURE SF_SP_GENERATE_PURCHASE_PLAN(
    START_WEEK_ID FLOAT, END_WEEK_ID FLOAT,
    RDC_CODE VARCHAR DEFAULT NULL, MAJ_CAT_PARAM VARCHAR DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_rows INTEGER;
    v_ts   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_first_wk_seq INTEGER;
BEGIN
    -- ═══ STEP 1: Week range ═══
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_W AS
    SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT,
        ROW_NUMBER() OVER (ORDER BY WEEK_ID) AS SEQ
    FROM WEEK_CALENDAR
    WHERE WEEK_ID BETWEEN :START_WEEK_ID AND :END_WEEK_ID;

    v_first_wk_seq := (SELECT WEEK_SEQ FROM TMP_PP_W WHERE SEQ = 1);

    -- Week offset map: each week → prev, next1..next5
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_WKMAP AS
    SELECT w.WEEK_ID, w.WEEK_SEQ, w.SEQ,
        wP.WEEK_ID AS PREV_WID,
        w1.WEEK_ID AS NEXT1_WID, w2.WEEK_ID AS NEXT2_WID,
        w3.WEEK_ID AS NEXT3_WID, w4.WEEK_ID AS NEXT4_WID, w5.WEEK_ID AS NEXT5_WID
    FROM TMP_PP_W w
    LEFT JOIN TMP_PP_W wP ON wP.WEEK_SEQ = w.WEEK_SEQ-1
    LEFT JOIN TMP_PP_W w1 ON w1.WEEK_SEQ = w.WEEK_SEQ+1
    LEFT JOIN TMP_PP_W w2 ON w2.WEEK_SEQ = w.WEEK_SEQ+2
    LEFT JOIN TMP_PP_W w3 ON w3.WEEK_SEQ = w.WEEK_SEQ+3
    LEFT JOIN TMP_PP_W w4 ON w4.WEEK_SEQ = w.WEEK_SEQ+4
    LEFT JOIN TMP_PP_W w5 ON w5.WEEK_SEQ = w.WEEK_SEQ+5;

    -- ═══ STEP 2: RDC x Category combos ═══
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_RC AS
    SELECT DISTINCT m.RDC_CD, m.RDC_NM, b.MAJ_CAT,
        NVL(BM.SEG,'NA') AS SEG, NVL(BM.DIV,'NA') AS DIV,
        NVL(BM.SUB_DIV,'NA') AS SUB_DIV, NVL(BM.MAJ_CAT_NM,'NA') AS MAJ_CAT_NM,
        NVL(BM.SSN,'NA') AS SSN
    FROM (SELECT DISTINCT RDC_CD, RDC_NM FROM MASTER_ST_MASTER) m
    CROSS JOIN (SELECT DISTINCT MAJ_CAT FROM MASTER_BIN_CAPACITY) b
    LEFT JOIN (
        SELECT SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN,
            ROW_NUMBER() OVER (PARTITION BY MAJ_CAT_NM ORDER BY ID) AS RN
        FROM MASTER_PRODUCT_HIERARCHY
    ) BM ON BM.MAJ_CAT_NM = b.MAJ_CAT AND BM.RN = 1
    WHERE (:RDC_CODE IS NULL OR m.RDC_CD = :RDC_CODE)
      AND (:MAJ_CAT_PARAM IS NULL OR b.MAJ_CAT = :MAJ_CAT_PARAM);

    -- ═══ STEP 3: Aggregate TRF_IN_PLAN by RDC/MAJ_CAT/WEEK ═══
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_TA AS
    SELECT t.RDC_CD, t.MAJ_CAT, t.WEEK_ID,
        SUM(t.S_GRT_STK_Q) AS S_GRT_STK_Q,   SUM(t.W_GRT_STK_Q) AS W_GRT_STK_Q,
        SUM(t.BGT_DISP_CL_Q) AS BGT_DISP_CL_Q,
        SUM(t.CM_BGT_SALE_Q) AS CW_BGT_SALE_Q, SUM(t.CM1_BGT_SALE_Q) AS CW1_BGT_SALE_Q,
        SUM(t.BGT_ST_CL_MBQ) AS BGT_ST_CL_MBQ,
        SUM(t.BGT_TTL_CF_OP_STK_Q) AS NET_ST_OP_STK_Q,
        SUM(t.TRF_IN_STK_Q) AS CW_TRF_OUT_Q,
        SUM(t.NET_ST_CL_STK_Q) AS NET_BGT_ST_CL_STK_Q,
        SUM(t.ST_CL_EXCESS_Q) AS ST_STK_EXCESS_Q,
        SUM(t.ST_CL_SHORT_Q) AS ST_STK_SHORT_Q
    FROM TRF_IN_PLAN t
    WHERE t.WEEK_ID BETWEEN :START_WEEK_ID AND :END_WEEK_ID
      AND (:RDC_CODE IS NULL OR t.RDC_CD = :RDC_CODE)
      AND (:MAJ_CAT_PARAM IS NULL OR t.MAJ_CAT = :MAJ_CAT_PARAM)
    GROUP BY t.RDC_CD, t.MAJ_CAT, t.WEEK_ID;

    -- ═══ STEP 4: GRT contribution unpivot ═══
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_GRT AS
    SELECT SSN, REPLACE(WK_COL, 'WK_', '')::INTEGER AS WK_NUM, GRT_PCT
    FROM MASTER_GRT_CONTRIBUTION
    UNPIVOT (GRT_PCT FOR WK_COL IN ({WK}));

    -- ═══ STEP 5: Build comprehensive base data ═══
    CREATE OR REPLACE TEMPORARY TABLE TMP_PP_BASE AS
    SELECT
        rc.RDC_CD, NVL(rc.RDC_NM,'NA') AS RDC_NM, rc.MAJ_CAT,
        wm.WEEK_ID, wm.WEEK_SEQ, wm.SEQ, rc.SSN,
        rc.SEG, rc.DIV, rc.SUB_DIV, rc.MAJ_CAT_NM,
        w.FY_WEEK, w.FY_YEAR, w.WK_ST_DT, w.WK_END_DT,
        -- Static from TRF aggregation
        NVL(ta.S_GRT_STK_Q,0) AS S_GRT_STK_Q,
        NVL(ta.W_GRT_STK_Q,0) AS W_GRT_STK_Q,
        NVL(mbc.BIN_CAP_DC_TEAM,0) AS BIN_CAP_DC_TEAM,
        NVL(mbc.BIN_CAP,0) AS BIN_CAP,
        NVL(ta.BGT_DISP_CL_Q,0) AS BGT_DISP_CL_Q,
        NVL(ta.CW_BGT_SALE_Q,0) AS CW_BGT_SALE_Q,
        NVL(ta.CW1_BGT_SALE_Q,0) AS CW1_BGT_SALE_Q,
        NVL(tf1.CW1_BGT_SALE_Q,0) AS CW2_BGT_SALE_Q,
        NVL(tf2.CW1_BGT_SALE_Q,0) AS CW3_BGT_SALE_Q,
        NVL(tf3.CW1_BGT_SALE_Q,0) AS CW4_BGT_SALE_Q,
        NVL(tf4.CW1_BGT_SALE_Q,0) AS CW5_BGT_SALE_Q,
        NVL(tap.BGT_ST_CL_MBQ,0) AS BGT_ST_OP_MBQ,
        NVL(ta.NET_ST_OP_STK_Q,0) AS NET_ST_OP_STK_Q,
        NVL(ta.BGT_ST_CL_MBQ,0) AS BGT_ST_CL_MBQ,
        NVL(ta.NET_BGT_ST_CL_STK_Q,0) AS NET_BGT_ST_CL_STK_Q,
        NVL(ta.ST_STK_EXCESS_Q,0) AS ST_STK_EXCESS_Q,
        NVL(ta.ST_STK_SHORT_Q,0) AS ST_STK_SHORT_Q,
        -- DC/GRT stock (initial, for week 1)
        NVL(qmg.DC_STK_Q,0) AS INIT_DC_STK,
        NVL(qmg.GRT_STK_Q,0) AS INIT_GRT_STK,
        -- GRT parameters
        NVL(gu.GRT_PCT,0) AS GRT_CONS_PCT,
        CASE WHEN rc.SSN IN ('S','OC','A') THEN NVL(qmg.GRT_STK_Q,0)*0.10 ELSE 0 END AS NT_ACT_STK,
        NVL(qmg.GRT_STK_Q,0) AS TTL_STK,
        NVL(qdp.DEL_PEND_Q,0) AS DEL_PEND_Q,
        -- TRF outbound (current + future 4 weeks)
        NVL(ta.CW_TRF_OUT_Q,0) AS CW_TRF_OUT_Q,
        NVL(tn1.CW_TRF_OUT_Q,0) AS CW1_TRF_OUT_Q,
        NVL(tn2.CW_TRF_OUT_Q,0) AS CW2_TRF_OUT_Q,
        NVL(tn3.CW_TRF_OUT_Q,0) AS CW3_TRF_OUT_Q,
        NVL(tn4.CW_TRF_OUT_Q,0) AS CW4_TRF_OUT_Q,
        -- Derived statics
        NVL(ta.CW_TRF_OUT_Q,0)+NVL(tn1.CW_TRF_OUT_Q,0)+NVL(tn2.CW_TRF_OUT_Q,0)+NVL(tn3.CW_TRF_OUT_Q,0)+NVL(tn4.CW_TRF_OUT_Q,0) AS TTL_TRF_OUT_Q,
        NVL(ta.CW1_BGT_SALE_Q,0)+NVL(tf1.CW1_BGT_SALE_Q,0)+NVL(tf2.CW1_BGT_SALE_Q,0)+NVL(tf3.CW1_BGT_SALE_Q,0) AS BGT_DC_MBQ_SALE,
        LEAST(
            NVL(tn1.CW_TRF_OUT_Q,0)+NVL(tn2.CW_TRF_OUT_Q,0)+NVL(tn3.CW_TRF_OUT_Q,0)+NVL(tn4.CW_TRF_OUT_Q,0),
            NVL(ta.CW1_BGT_SALE_Q,0)+NVL(tf1.CW1_BGT_SALE_Q,0)+NVL(tf2.CW1_BGT_SALE_Q,0)+NVL(tf3.CW1_BGT_SALE_Q,0)
        ) AS BGT_DC_CL_MBQ
    FROM TMP_PP_RC rc
    CROSS JOIN TMP_PP_WKMAP wm
    INNER JOIN TMP_PP_W w ON w.WEEK_ID = wm.WEEK_ID
    LEFT JOIN TMP_PP_TA ta ON ta.RDC_CD=rc.RDC_CD AND ta.MAJ_CAT=rc.MAJ_CAT AND ta.WEEK_ID=wm.WEEK_ID
    LEFT JOIN TMP_PP_TA tap ON tap.RDC_CD=rc.RDC_CD AND tap.MAJ_CAT=rc.MAJ_CAT AND tap.WEEK_ID=wm.PREV_WID
    LEFT JOIN TMP_PP_TA tn1 ON tn1.RDC_CD=rc.RDC_CD AND tn1.MAJ_CAT=rc.MAJ_CAT AND tn1.WEEK_ID=wm.NEXT1_WID
    LEFT JOIN TMP_PP_TA tn2 ON tn2.RDC_CD=rc.RDC_CD AND tn2.MAJ_CAT=rc.MAJ_CAT AND tn2.WEEK_ID=wm.NEXT2_WID
    LEFT JOIN TMP_PP_TA tn3 ON tn3.RDC_CD=rc.RDC_CD AND tn3.MAJ_CAT=rc.MAJ_CAT AND tn3.WEEK_ID=wm.NEXT3_WID
    LEFT JOIN TMP_PP_TA tn4 ON tn4.RDC_CD=rc.RDC_CD AND tn4.MAJ_CAT=rc.MAJ_CAT AND tn4.WEEK_ID=wm.NEXT4_WID
    LEFT JOIN TMP_PP_TA tf1 ON tf1.RDC_CD=rc.RDC_CD AND tf1.MAJ_CAT=rc.MAJ_CAT AND tf1.WEEK_ID=wm.NEXT1_WID
    LEFT JOIN TMP_PP_TA tf2 ON tf2.RDC_CD=rc.RDC_CD AND tf2.MAJ_CAT=rc.MAJ_CAT AND tf2.WEEK_ID=wm.NEXT2_WID
    LEFT JOIN TMP_PP_TA tf3 ON tf3.RDC_CD=rc.RDC_CD AND tf3.MAJ_CAT=rc.MAJ_CAT AND tf3.WEEK_ID=wm.NEXT3_WID
    LEFT JOIN TMP_PP_TA tf4 ON tf4.RDC_CD=rc.RDC_CD AND tf4.MAJ_CAT=rc.MAJ_CAT AND tf4.WEEK_ID=wm.NEXT4_WID
    LEFT JOIN (
        SELECT RDC_CD, MAJ_CAT, DC_STK_Q, GRT_STK_Q
        FROM (SELECT RDC_CD, MAJ_CAT, DC_STK_Q, GRT_STK_Q, ROW_NUMBER() OVER (PARTITION BY RDC_CD, MAJ_CAT ORDER BY DATE DESC) AS RN FROM QTY_MSA_AND_GRT) WHERE RN=1
    ) qmg ON qmg.RDC_CD=rc.RDC_CD AND qmg.MAJ_CAT=rc.MAJ_CAT
    LEFT JOIN MASTER_BIN_CAPACITY mbc ON mbc.MAJ_CAT=rc.MAJ_CAT
    LEFT JOIN TMP_PP_GRT gu ON gu.SSN=rc.SSN AND gu.WK_NUM = (wm.WEEK_SEQ - :v_first_wk_seq + 1)
    LEFT JOIN (
        SELECT RDC_CD, MAJ_CAT, DEL_PEND_Q
        FROM (SELECT RDC_CD, MAJ_CAT, DEL_PEND_Q, ROW_NUMBER() OVER (PARTITION BY RDC_CD, MAJ_CAT ORDER BY DATE DESC) AS RN FROM QTY_DEL_PENDING) WHERE RN=1
    ) qdp ON qdp.RDC_CD=rc.RDC_CD AND qdp.MAJ_CAT=rc.MAJ_CAT;

    -- ═══ STEP 6: Delete old + Recursive CTE → INSERT ═══
    DELETE FROM PURCHASE_PLAN
    WHERE WEEK_ID BETWEEN :START_WEEK_ID AND :END_WEEK_ID
      AND (:RDC_CODE IS NULL OR RDC_CD = :RDC_CODE)
      AND (:MAJ_CAT_PARAM IS NULL OR MAJ_CAT = :MAJ_CAT_PARAM);

    INSERT INTO PURCHASE_PLAN (
        RDC_CD, RDC_NM, MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM,
        WEEK_ID, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT,
        DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q,
        BIN_CAP_DC_TEAM, BIN_CAP, BGT_DISP_CL_Q,
        CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, CW5_BGT_SALE_Q,
        BGT_ST_OP_MBQ, NET_ST_OP_STK_Q,
        BGT_DC_OP_STK_Q, PP_NT_ACT_Q, BGT_CF_STK_Q,
        TTL_STK, OP_STK, NT_ACT_STK,
        GRT_CONS_PCT, GRT_CONS_Q, DEL_PEND_Q, PP_NET_BGT_CF_STK_Q,
        CW_TRF_OUT_Q, CW1_TRF_OUT_Q, CW2_TRF_OUT_Q, CW3_TRF_OUT_Q, CW4_TRF_OUT_Q, TTL_TRF_OUT_Q,
        BGT_ST_CL_MBQ, NET_BGT_ST_CL_STK_Q, NET_SSNL_CL_STK_Q,
        BGT_DC_MBQ_SALE, BGT_DC_CL_MBQ, BGT_DC_CL_STK_Q,
        BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED,
        BGT_CO_CL_STK_Q,
        DC_STK_EXCESS_Q, DC_STK_SHORT_Q, ST_STK_EXCESS_Q, ST_STK_SHORT_Q,
        CO_STK_EXCESS_Q, CO_STK_SHORT_Q, FRESH_BIN_REQ, GRT_BIN_REQ
    )
    WITH RECURSIVE chain(
        RDC_CD, MAJ_CAT, SEQ,
        OP_STK, BGT_CF_STK_Q, NEG_PO_RAISED,
        GRT_CONS_Q, PP_NET_BGT_CF_STK_Q, NET_SSNL_CL_STK_Q,
        POS_PO_RAISED, BGT_PUR_Q_INIT, BGT_DC_CL_STK_Q
    ) AS (
        -- ── Week 1: anchor ──
        SELECT
            b.RDC_CD, b.MAJ_CAT, b.SEQ,
            b.INIT_GRT_STK,                                                          -- OP_STK
            GREATEST(b.INIT_DC_STK, 0),                                              -- BGT_CF_STK_Q
            -- NEG_PO_RAISED (week 1)
            LEAST(
                GREATEST(b.BGT_DC_CL_MBQ + b.CW_TRF_OUT_Q
                    - (GREATEST(b.INIT_DC_STK,0) +
                       CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                           b.TTL_TRF_OUT_Q*0.30,
                           GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                           GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                           GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                       ) END
                       + b.DEL_PEND_Q), 0)
                - b.DEL_PEND_Q, 0),
            -- GRT_CONS_Q
            CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                b.TTL_TRF_OUT_Q*0.30,
                GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
            ) END,
            -- PP_NET_BGT_CF_STK_Q
            GREATEST(b.INIT_DC_STK,0) +
            CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                b.TTL_TRF_OUT_Q*0.30,
                GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
            ) END + b.DEL_PEND_Q,
            -- NET_SSNL_CL_STK_Q
            GREATEST(b.INIT_GRT_STK -
                CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                    b.TTL_TRF_OUT_Q*0.30,
                    GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                    GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                    GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                ) END, 0),
            -- POS_PO_RAISED
            GREATEST(b.BGT_DC_CL_MBQ + b.CW_TRF_OUT_Q
                - (GREATEST(b.INIT_DC_STK,0) +
                   CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       b.TTL_TRF_OUT_Q*0.30,
                       GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                       GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                   ) END
                   + b.DEL_PEND_Q), 0),
            -- BGT_PUR_Q_INIT = POS_PO_RAISED
            GREATEST(b.BGT_DC_CL_MBQ + b.CW_TRF_OUT_Q
                - (GREATEST(b.INIT_DC_STK,0) +
                   CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       b.TTL_TRF_OUT_Q*0.30,
                       GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                       GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                   ) END
                   + b.DEL_PEND_Q), 0),
            -- BGT_DC_CL_STK_Q
            GREATEST(
                GREATEST(b.BGT_DC_CL_MBQ + b.CW_TRF_OUT_Q
                    - (GREATEST(b.INIT_DC_STK,0) +
                       CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                           b.TTL_TRF_OUT_Q*0.30,
                           GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                           GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                           GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                       ) END
                       + b.DEL_PEND_Q), 0)
                + (GREATEST(b.INIT_DC_STK,0) +
                   CASE WHEN b.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       b.TTL_TRF_OUT_Q*0.30,
                       GREATEST(b.INIT_GRT_STK - b.NT_ACT_STK, 0),
                       GREATEST(b.TTL_TRF_OUT_Q - GREATEST(GREATEST(b.INIT_DC_STK,0) - b.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(b.TTL_STK - b.NT_ACT_STK, 0) * b.GRT_CONS_PCT
                   ) END + b.DEL_PEND_Q)
                - b.CW_TRF_OUT_Q, 0)
        FROM TMP_PP_BASE b WHERE b.SEQ = 1

        UNION ALL

        -- ── Week N: recursive (chain from prev) ──
        SELECT
            n.RDC_CD, n.MAJ_CAT, n.SEQ,
            p.NET_SSNL_CL_STK_Q,                                                    -- OP_STK
            GREATEST(p.BGT_DC_CL_STK_Q, 0),                                         -- BGT_CF_STK_Q
            -- NEG_PO_RAISED (accumulates)
            LEAST(
                GREATEST(n.BGT_DC_CL_MBQ + n.CW_TRF_OUT_Q
                    - (GREATEST(p.BGT_DC_CL_STK_Q,0) +
                       CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                           n.TTL_TRF_OUT_Q*0.30,
                           GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                           GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                           GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                       ) END
                       + n.DEL_PEND_Q), 0)
                - n.DEL_PEND_Q + p.NEG_PO_RAISED, 0),
            -- GRT_CONS_Q
            CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                n.TTL_TRF_OUT_Q*0.30,
                GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
            ) END,
            -- PP_NET_BGT_CF_STK_Q
            GREATEST(p.BGT_DC_CL_STK_Q,0) +
            CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                n.TTL_TRF_OUT_Q*0.30,
                GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
            ) END + n.DEL_PEND_Q,
            -- NET_SSNL_CL_STK_Q
            GREATEST(p.NET_SSNL_CL_STK_Q -
                CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                    n.TTL_TRF_OUT_Q*0.30,
                    GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                    GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                    GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                ) END, 0),
            -- POS_PO_RAISED
            GREATEST(n.BGT_DC_CL_MBQ + n.CW_TRF_OUT_Q
                - (GREATEST(p.BGT_DC_CL_STK_Q,0) +
                   CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       n.TTL_TRF_OUT_Q*0.30,
                       GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                       GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                   ) END
                   + n.DEL_PEND_Q), 0),
            -- BGT_PUR_Q_INIT
            GREATEST(n.BGT_DC_CL_MBQ + n.CW_TRF_OUT_Q
                - (GREATEST(p.BGT_DC_CL_STK_Q,0) +
                   CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       n.TTL_TRF_OUT_Q*0.30,
                       GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                       GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                   ) END
                   + n.DEL_PEND_Q), 0),
            -- BGT_DC_CL_STK_Q
            GREATEST(
                GREATEST(n.BGT_DC_CL_MBQ + n.CW_TRF_OUT_Q
                    - (GREATEST(p.BGT_DC_CL_STK_Q,0) +
                       CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                           n.TTL_TRF_OUT_Q*0.30,
                           GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                           GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                           GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                       ) END
                       + n.DEL_PEND_Q), 0)
                + (GREATEST(p.BGT_DC_CL_STK_Q,0) +
                   CASE WHEN n.TTL_TRF_OUT_Q=0 THEN 0 ELSE LEAST(
                       n.TTL_TRF_OUT_Q*0.30,
                       GREATEST(p.NET_SSNL_CL_STK_Q - n.NT_ACT_STK, 0),
                       GREATEST(n.TTL_TRF_OUT_Q - GREATEST(GREATEST(p.BGT_DC_CL_STK_Q,0) - n.BGT_DC_CL_MBQ, 0), 0),
                       GREATEST(n.TTL_STK - n.NT_ACT_STK, 0) * n.GRT_CONS_PCT
                   ) END + n.DEL_PEND_Q)
                - n.CW_TRF_OUT_Q, 0)
        FROM TMP_PP_BASE n
        INNER JOIN chain p ON p.RDC_CD=n.RDC_CD AND p.MAJ_CAT=n.MAJ_CAT AND n.SEQ=p.SEQ+1
    )
    SELECT
        b.RDC_CD, b.RDC_NM, b.MAJ_CAT, b.SSN, b.SEG, b.DIV, b.SUB_DIV, b.MAJ_CAT_NM,
        b.WEEK_ID, b.FY_WEEK, b.FY_YEAR, b.WK_ST_DT, b.WK_END_DT,
        CASE WHEN ch.SEQ=1 THEN b.INIT_DC_STK ELSE ch.BGT_CF_STK_Q END,  -- DC_STK_Q
        b.INIT_GRT_STK,                                                    -- GRT_STK_Q
        b.S_GRT_STK_Q, b.W_GRT_STK_Q,
        b.BIN_CAP_DC_TEAM, b.BIN_CAP, b.BGT_DISP_CL_Q,
        b.CW_BGT_SALE_Q, b.CW1_BGT_SALE_Q, b.CW2_BGT_SALE_Q,
        b.CW3_BGT_SALE_Q, b.CW4_BGT_SALE_Q, b.CW5_BGT_SALE_Q,
        b.BGT_ST_OP_MBQ, b.NET_ST_OP_STK_Q,
        ch.BGT_CF_STK_Q AS BGT_DC_OP_STK_Q,
        0 AS PP_NT_ACT_Q,
        ch.BGT_CF_STK_Q,
        b.TTL_STK, ch.OP_STK, b.NT_ACT_STK,
        b.GRT_CONS_PCT, ch.GRT_CONS_Q, b.DEL_PEND_Q, ch.PP_NET_BGT_CF_STK_Q,
        b.CW_TRF_OUT_Q, b.CW1_TRF_OUT_Q, b.CW2_TRF_OUT_Q, b.CW3_TRF_OUT_Q, b.CW4_TRF_OUT_Q,
        b.TTL_TRF_OUT_Q,
        b.BGT_ST_CL_MBQ, b.NET_BGT_ST_CL_STK_Q, ch.NET_SSNL_CL_STK_Q,
        b.BGT_DC_MBQ_SALE, b.BGT_DC_CL_MBQ, ch.BGT_DC_CL_STK_Q,
        ch.BGT_PUR_Q_INIT, ch.POS_PO_RAISED, ch.NEG_PO_RAISED,
        b.NET_BGT_ST_CL_STK_Q + ch.NET_SSNL_CL_STK_Q + ch.BGT_DC_CL_STK_Q,  -- BGT_CO_CL_STK_Q
        GREATEST(ch.BGT_DC_CL_STK_Q - b.BGT_DC_CL_MBQ, 0),                   -- DC_STK_EXCESS_Q
        GREATEST(b.BGT_DC_CL_MBQ - ch.BGT_DC_CL_STK_Q, 0),                   -- DC_STK_SHORT_Q
        b.ST_STK_EXCESS_Q, b.ST_STK_SHORT_Q,
        b.ST_STK_EXCESS_Q + GREATEST(ch.BGT_DC_CL_STK_Q - b.BGT_DC_CL_MBQ, 0),  -- CO_STK_EXCESS_Q
        b.ST_STK_SHORT_Q + GREATEST(b.BGT_DC_CL_MBQ - ch.BGT_DC_CL_STK_Q, 0),   -- CO_STK_SHORT_Q
        CASE WHEN b.BIN_CAP>0 THEN ch.BGT_DC_CL_STK_Q/b.BIN_CAP ELSE 0 END,     -- FRESH_BIN_REQ
        CASE WHEN b.BIN_CAP>0 THEN ch.OP_STK/b.BIN_CAP ELSE 0 END                -- GRT_BIN_REQ
    FROM chain ch
    INNER JOIN TMP_PP_BASE b ON b.RDC_CD=ch.RDC_CD AND b.MAJ_CAT=ch.MAJ_CAT AND b.SEQ=ch.SEQ;

    -- Count
    v_rows := (SELECT COUNT(*) FROM PURCHASE_PLAN
        WHERE WEEK_ID BETWEEN :START_WEEK_ID AND :END_WEEK_ID
          AND (:RDC_CODE IS NULL OR RDC_CD = :RDC_CODE)
          AND (:MAJ_CAT_PARAM IS NULL OR MAJ_CAT = :MAJ_CAT_PARAM));

    -- Cleanup
    DROP TABLE IF EXISTS TMP_PP_W;
    DROP TABLE IF EXISTS TMP_PP_WKMAP;
    DROP TABLE IF EXISTS TMP_PP_RC;
    DROP TABLE IF EXISTS TMP_PP_TA;
    DROP TABLE IF EXISTS TMP_PP_GRT;
    DROP TABLE IF EXISTS TMP_PP_BASE;

    RETURN OBJECT_CONSTRUCT(
        'rows_inserted', :v_rows,
        'seconds', DATEDIFF('second', :v_ts, CURRENT_TIMESTAMP())
    );
END;
"""

print("Deploying SF_SP_GENERATE_PURCHASE_PLAN (SQL Scripting + Recursive CTE)...")
try:
    cur.execute(sp_pp)
    print("  -> SF_SP_GENERATE_PURCHASE_PLAN deployed successfully.")
except Exception as e:
    print(f"  -> FAILED: {e}")
    sys.exit(1)

print("\nDone. Call: SF_SP_GENERATE_PURCHASE_PLAN(1, 52, NULL, 'M_JEANS');")
conn.close()
