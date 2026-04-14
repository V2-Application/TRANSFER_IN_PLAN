-- ============================================================
-- ARS ALLOCATION — Snowflake Stored Procedure (JavaScript)
-- SP_ARS_ALLOCATION_RUN: Prepare → Allocate → Finalize
-- Using JavaScript handler for reliable execution
-- Database: V2RETAIL  Schema: GOLD
-- ============================================================

USE DATABASE V2RETAIL;
USE SCHEMA GOLD;

CREATE OR REPLACE PROCEDURE SP_ARS_ALLOCATION_RUN(RUN_ID_PARAM VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // Helper to run SQL and return results
    function exec(sql) {
        return snowflake.execute({sqlText: sql});
    }
    function execScalar(sql) {
        var rs = exec(sql);
        rs.next();
        return rs.getColumnValue(1);
    }

    var runId = RUN_ID_PARAM;

    // ═══ STEP 1: Pivot ET_STOCK_DATA by LGORT ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_STOCK_PIVOT AS
    SELECT WERKS AS ST, MATNR AS GEN_ART,
        NVL(SUM(CASE WHEN LGORT = '0001' THEN LABST END), 0) AS STK_0001,
        NVL(SUM(CASE WHEN LGORT = '0002' THEN LABST END), 0) AS STK_0002,
        NVL(SUM(CASE WHEN LGORT = '0004' THEN LABST END), 0) AS STK_0004,
        NVL(SUM(CASE WHEN LGORT = '0006' THEN LABST END), 0) AS STK_0006,
        NVL(SUM(CASE WHEN LGORT = 'HUB_INTRA' THEN LABST END), 0) AS STK_HUB_INTRA,
        NVL(SUM(CASE WHEN LGORT = 'HUB_PRD' THEN LABST END), 0) AS STK_HUB_PRD,
        NVL(SUM(CASE WHEN LGORT = 'INTRA' THEN LABST END), 0) AS STK_INTRA,
        NVL(SUM(CASE WHEN LGORT = 'PRD' THEN LABST END), 0) AS STK_PRD,
        NVL(SUM(CASE WHEN LGORT IN ('0001','0002','0004','0006','HUB_INTRA','HUB_PRD','INTRA','PRD') THEN LABST END), 0) AS TTL_ST_STK_Q
    FROM ET_STOCK_DATA
    WHERE STOCK_DATE = CURRENT_DATE() - 1
    GROUP BY WERKS, MATNR`);

    // ═══ STEP 2: MSA ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_MSA AS
    SELECT STORE_CODE AS ST, ARTICLE_NUMBER AS GEN_ART, NVL(SUM(QTY), 0) AS MSA_QTY
    FROM ET_MSA_STOCK
    WHERE MSA_STOCK_DATE = CURRENT_DATE() - 1
    GROUP BY STORE_CODE, ARTICLE_NUMBER`);

    // ═══ STEP 3: Store Master ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_STORE AS
    SELECT ST_CD AS ST, ST_NM, TAGGED_RDC, HUB_CD,
        NVL(SALE_COVER_DAYS,0) + NVL(PRD_DAYS,0) +
        CASE WHEN TAGGED_RDC = 'DW01'
             THEN NVL(DW01_DC_TO_HUB_INTRA,0) + NVL(DW01_HUB_TO_ST_INTRA,0)
             ELSE NVL(DH24_DC_TO_HUB_INTRA,0) + NVL(DH24_HUB_TO_ST_INTRA,0) END AS TTL_ALC_DAYS
    FROM ARS_ST_MASTER`);

    // ═══ STEP 4: Display + Hold + MJ Sale ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_MJ AS
    SELECT d.ST, d.MJ,
        NVL(d.ST_MJ_DISP_Q,0) AS ST_MJ_DISP_Q,
        NVL(h.HOLD_DAYS,0) AS HOLD_DAYS,
        sm.TTL_ALC_DAYS,
        CASE WHEN NVL(s.CM_REM_DAYS,0) > 0 THEN s.CM_AUTO_SALE_Q / s.CM_REM_DAYS ELSE 0 END AS CM_PD,
        CASE WHEN NVL(s.NM_DAYS,0) > 0 THEN s.NM_AUTO_SALE_Q / s.NM_DAYS ELSE 0 END AS NM_PD,
        NVL(d.ST_MJ_DISP_Q,0) +
        CASE WHEN NVL(s.CM_REM_DAYS,0) > 0 THEN
            LEAST(sm.TTL_ALC_DAYS + NVL(h.HOLD_DAYS,0), s.CM_REM_DAYS) * (s.CM_AUTO_SALE_Q / s.CM_REM_DAYS)
            + GREATEST((sm.TTL_ALC_DAYS + NVL(h.HOLD_DAYS,0)) - s.CM_REM_DAYS, 0)
              * CASE WHEN NVL(s.NM_DAYS,0) > 0 THEN s.NM_AUTO_SALE_Q / s.NM_DAYS ELSE 0 END
        ELSE 0 END AS ST_MJ_MBQ
    FROM ARS_ST_MJ_DISPLAY_MASTER d
    INNER JOIN TMP_STORE sm ON sm.ST = d.ST
    LEFT JOIN ARS_HOLD_DAYS_MASTER h ON h.ST = d.ST AND h.MJ = d.MJ
    LEFT JOIN ARS_ST_MJ_AUTO_SALE s ON s.ST = d.ST AND s.MJ = d.MJ`);

    // ═══ STEP 5: Article Sale ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_ART AS
    SELECT a.ST, a.GEN_ART, a.CLR, a.MJ,
        CASE WHEN NVL(a.CM_REM_DAYS,0) > 0 THEN
            LEAST(sm.TTL_ALC_DAYS + NVL(h.HOLD_DAYS,0), a.CM_REM_DAYS) * (a.CM_AUTO_SALE_Q / a.CM_REM_DAYS)
            + GREATEST((sm.TTL_ALC_DAYS + NVL(h.HOLD_DAYS,0)) - a.CM_REM_DAYS, 0)
              * CASE WHEN NVL(a.NM_DAYS,0) > 0 THEN a.NM_AUTO_SALE_Q / a.NM_DAYS ELSE 0 END
        ELSE 0 END AS ART_SALE_PD
    FROM ARS_ST_ART_AUTO_SALE a
    INNER JOIN TMP_STORE sm ON sm.ST = a.ST
    LEFT JOIN ARS_HOLD_DAYS_MASTER h ON h.ST = a.ST AND h.MJ = a.MJ`);

    // ═══ STEP 6: Build PREPARED + Classify ═══
    exec(`CREATE OR REPLACE TEMPORARY TABLE TMP_PREP AS
    SELECT
        '` + runId + `' AS RUN_ID, sm.ST, sm.ST_NM, sm.TAGGED_RDC, sm.HUB_CD,
        mc.MJ, stk.GEN_ART, '' AS CLR,
        stk.STK_0001, stk.STK_0002, stk.STK_0004, stk.STK_0006,
        stk.STK_HUB_INTRA, stk.STK_HUB_PRD, stk.STK_INTRA, stk.STK_PRD,
        stk.TTL_ST_STK_Q, NVL(msa.MSA_QTY,0) AS MSA_QTY,
        sm.TTL_ALC_DAYS, NVL(mc.HOLD_DAYS,0) AS HOLD_DAYS,
        NVL(mc.CM_PD,0) AS CM_PD_SALE_Q, NVL(mc.NM_PD,0) AS NM_PD_SALE_Q,
        NVL(art.ART_SALE_PD,0) AS ART_AUTO_SALE_PD,
        NVL(mc.ST_MJ_DISP_Q,0) AS ACC_DENSITY,
        NVL(mc.ST_MJ_DISP_Q,0) AS ST_MJ_DISP_Q,
        NVL(mc.ST_MJ_MBQ,0) AS ST_MJ_MBQ,
        NVL(mc.ST_MJ_DISP_Q,0) + NVL(art.ART_SALE_PD,0) * sm.TTL_ALC_DAYS AS ART_MBQ,
        NVL(mc.ST_MJ_DISP_Q,0) + NVL(art.ART_SALE_PD,0) * (sm.TTL_ALC_DAYS + NVL(mc.HOLD_DAYS,0)) AS ART_HOLD_MBQ,
        GREATEST(NVL(mc.ST_MJ_DISP_Q,0) + NVL(art.ART_SALE_PD,0) * sm.TTL_ALC_DAYS - stk.TTL_ST_STK_Q, 0) AS ST_ART_REQ,
        GREATEST(NVL(mc.ST_MJ_DISP_Q,0) + NVL(art.ART_SALE_PD,0) * (sm.TTL_ALC_DAYS + NVL(mc.HOLD_DAYS,0)) - stk.TTL_ST_STK_Q, 0) AS ST_ART_HOLD_REQ,
        CASE
            WHEN stk.TTL_ST_STK_Q >= NVL(mc.ST_MJ_DISP_Q,0) * 0.60 THEN 'L-ART'
            WHEN stk.TTL_ST_STK_Q < NVL(mc.ST_MJ_DISP_Q,0) * 0.60 AND NVL(msa.MSA_QTY,0) < 50 THEN 'MIX-ART'
            WHEN NVL(msa.MSA_QTY,0) = 0 THEN 'MIX-ART'
            WHEN stk.TTL_ST_STK_Q > 0 THEN 'OLD-ART'
            ELSE 'MIX-ART'
        END AS ART_CLASS,
        0::NUMBER(18,4) AS ART_ALC_Q,
        0::NUMBER(18,4) AS ART_HOLD_Q,
        NVL(msa.MSA_QTY,0)::NUMBER(18,4) AS REM_MSA,
        0::NUMBER(18,4) AS ST_MJ_REQ
    FROM TMP_STOCK_PIVOT stk
    INNER JOIN TMP_STORE sm ON sm.ST = stk.ST
    INNER JOIN TMP_MJ mc ON mc.ST = stk.ST
    LEFT JOIN TMP_ART art ON art.ST = stk.ST AND art.GEN_ART = stk.GEN_ART
    LEFT JOIN TMP_MSA msa ON msa.ST = stk.ST AND msa.GEN_ART = stk.GEN_ART`);

    // MJ REQ
    exec(`UPDATE TMP_PREP p SET ST_MJ_REQ = GREATEST(p.ST_MJ_MBQ - a.MJ_STK, 0)
    FROM (SELECT ST, MJ, SUM(TTL_ST_STK_Q) AS MJ_STK FROM TMP_PREP GROUP BY ST, MJ) a
    WHERE p.ST = a.ST AND p.MJ = a.MJ`);

    // Stats
    var rs = exec(`SELECT COUNT(*), COUNT(DISTINCT ST), COUNT(DISTINCT GEN_ART),
        NVL(SUM(CASE WHEN ART_CLASS='L-ART' THEN 1 ELSE 0 END),0),
        NVL(SUM(CASE WHEN ART_CLASS='MIX-ART' THEN 1 ELSE 0 END),0),
        NVL(SUM(CASE WHEN ART_CLASS='OLD-ART' THEN 1 ELSE 0 END),0)
    FROM TMP_PREP`);
    rs.next();
    var prepared_rows = rs.getColumnValue(1);
    var stores = rs.getColumnValue(2);
    var articles = rs.getColumnValue(3);
    var l_count = rs.getColumnValue(4);
    var mix_count = rs.getColumnValue(5);
    var old_count = rs.getColumnValue(6);

    // ═══ STEP 7: ALLOCATE ═══
    exec(`UPDATE TMP_PREP SET ART_ALC_Q = LEAST(REM_MSA, ST_ART_REQ, ST_MJ_REQ)
    WHERE ART_CLASS IN ('L-ART','OLD-ART') AND MSA_QTY > 0 AND ST_ART_REQ > 0 AND ST_MJ_REQ > 0 AND REM_MSA > 0`);

    exec(`UPDATE TMP_PREP SET
        ART_HOLD_Q = CASE WHEN ST_ART_HOLD_REQ > ART_ALC_Q AND REM_MSA > ART_ALC_Q
                     THEN LEAST(REM_MSA - ART_ALC_Q, ST_ART_HOLD_REQ - ART_ALC_Q) ELSE 0 END,
        REM_MSA = REM_MSA - ART_ALC_Q
    WHERE ART_ALC_Q > 0`);

    exec(`UPDATE TMP_PREP SET REM_MSA = REM_MSA - ART_HOLD_Q WHERE ART_HOLD_Q > 0`);

    rs = exec(`SELECT NVL(COUNT(*),0), NVL(SUM(CASE WHEN ART_HOLD_Q > 0 THEN 1 ELSE 0 END),0)
    FROM TMP_PREP WHERE ART_ALC_Q > 0`);
    rs.next();
    var allocated_count = rs.getColumnValue(1);
    var held_count = rs.getColumnValue(2);

    // ═══ STEP 8: NEW-L ═══
    exec(`UPDATE TMP_PREP p SET ART_CLASS = 'NEW-L'
    FROM (SELECT ST, MJ, SUM(ART_ALC_Q) AS TTL_ALC FROM TMP_PREP WHERE ART_ALC_Q > 0 GROUP BY ST, MJ) a
    WHERE p.ST = a.ST AND p.MJ = a.MJ AND p.ART_CLASS = 'MIX-ART' AND p.REM_MSA > 0 AND (p.ST_MJ_MBQ - a.TTL_ALC) > 0`);

    var new_l_count = execScalar(`SELECT NVL(COUNT(*),0) FROM TMP_PREP WHERE ART_CLASS = 'NEW-L'`);

    // ═══ STEP 9: Write output ═══
    exec(`DELETE FROM ARS_ALLOCATION_OUTPUT WHERE RUN_ID = '` + runId + `'`);

    exec(`INSERT INTO ARS_ALLOCATION_OUTPUT
    SELECT RUN_ID, ST, ST_NM, TAGGED_RDC, HUB_CD, MJ, GEN_ART, CLR, ART_CLASS,
        TTL_ST_STK_Q, STK_0001, STK_0002, STK_0004, STK_0006,
        STK_HUB_INTRA, STK_HUB_PRD, STK_INTRA, STK_PRD, MSA_QTY,
        TTL_ALC_DAYS, HOLD_DAYS, CM_PD_SALE_Q, NM_PD_SALE_Q, ART_AUTO_SALE_PD,
        ACC_DENSITY, ST_MJ_DISP_Q, ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
        ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ,
        ART_ALC_Q, ART_HOLD_Q, REM_MSA, CURRENT_TIMESTAMP()
    FROM TMP_PREP`);

    var output_rows = execScalar(`SELECT COUNT(*) FROM ARS_ALLOCATION_OUTPUT WHERE RUN_ID = '` + runId + `'`);

    // Update run log
    exec(`UPDATE ARS_RUN_LOG SET COMPLETED_DT = CURRENT_TIMESTAMP(), STATUS = 'COMPLETED',
        TOTAL_STORES = ` + stores + `, TOTAL_ARTICLES = ` + articles + `,
        TOTAL_ALLOCATED = ` + allocated_count + `, TOTAL_HELD = ` + held_count + `,
        L_ART_COUNT = ` + l_count + `, MIX_ART_COUNT = ` + mix_count + `,
        OLD_ART_COUNT = ` + old_count + `, NEW_L_COUNT = ` + new_l_count + `
    WHERE RUN_ID = '` + runId + `'`);

    // Cleanup
    exec(`DROP TABLE IF EXISTS TMP_STOCK_PIVOT`);
    exec(`DROP TABLE IF EXISTS TMP_MSA`);
    exec(`DROP TABLE IF EXISTS TMP_STORE`);
    exec(`DROP TABLE IF EXISTS TMP_MJ`);
    exec(`DROP TABLE IF EXISTS TMP_ART`);
    exec(`DROP TABLE IF EXISTS TMP_PREP`);

    return {
        prepared_rows: prepared_rows,
        stores: stores,
        articles: articles,
        l_count: l_count,
        mix_count: mix_count,
        old_count: old_count,
        allocated_count: allocated_count,
        held_count: held_count,
        output_rows: output_rows,
        new_l_count: new_l_count
    };
$$;
