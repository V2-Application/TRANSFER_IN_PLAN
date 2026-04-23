-- ============================================================
-- VW_SALE_PLAN_WEEKLY  (reference SQL — controller uses parameterized CTEs)
--
-- The controller (SalePlanOutputController) builds this query
-- with FY + FY_WEEK + ST baked in for sub-second execution.
-- This file documents the logic for Snowflake reference.
--
-- Current  = SUM of budget month days in selected FY_WEEK
-- LYSM     = Last Year Same Month = sales on BGT_MNTH_DATE - 1 year
-- LYSP     = Last Year Same Period = sales on LY_SAME_DATE
-- Stock    = latest date stock for LGORT 0001 + 0006
-- ============================================================

-- Example parameterized query (replace :fy, :fyWeek, :st):
-- WITH store_cal AS (
--   SELECT ST_CD,BGT_MNTH_DATE,LY_SAME_DATE FROM STORE_CALENDAR sc
--   WHERE FY=:fy AND FY_WEEK=:fyWeek [AND sc.ST_CD=:st]
-- ), ...

CREATE OR REPLACE VIEW VW_SALE_PLAN_WEEKLY AS
WITH
current_sale AS (
    SELECT
        sc.ST_CD                                       AS STORE_CODE,
        s.MATNR                                        AS ARTICLE_NUMBER,
        sc.FY,
        sc.FY_WEEK,
        SUM(s.FKIMG)                                   AS SALE_QTY,
        SUM(s.NETWR)                                   AS SALE_VAL,
        SUM(NVL(s.NET_VAL, 0) - NVL(s.KWERT_VPRS, 0)) AS SALE_GM
    FROM STORE_CALENDAR sc
    JOIN ET_SALES_DATA s
        ON s.WERKS = sc.ST_CD
        AND s.SALES_DATE::DATE = sc.BGT_MNTH_DATE
    GROUP BY sc.ST_CD, s.MATNR, sc.FY, sc.FY_WEEK
),
lysm_sale AS (
    SELECT
        sc.ST_CD                                       AS STORE_CODE,
        s.MATNR                                        AS ARTICLE_NUMBER,
        sc.FY,
        sc.FY_WEEK,
        SUM(s.FKIMG)                                   AS LYSM_QTY,
        SUM(s.NETWR)                                   AS LYSM_VAL,
        SUM(NVL(s.NET_VAL, 0) - NVL(s.KWERT_VPRS, 0)) AS LYSM_GM
    FROM STORE_CALENDAR sc
    JOIN ET_SALES_DATA s
        ON  s.WERKS            = sc.ST_CD
        AND s.SALES_DATE::DATE = DATEADD('year', -1, sc.BGT_MNTH_DATE)
    GROUP BY sc.ST_CD, s.MATNR, sc.FY, sc.FY_WEEK
),
lysp_sale AS (
    SELECT
        sc.ST_CD                                       AS STORE_CODE,
        s.MATNR                                        AS ARTICLE_NUMBER,
        sc.FY,
        sc.FY_WEEK,
        SUM(s.FKIMG)                                   AS LYSP_QTY,
        SUM(s.NETWR)                                   AS LYSP_VAL,
        SUM(NVL(s.NET_VAL, 0) - NVL(s.KWERT_VPRS, 0)) AS LYSP_GM
    FROM STORE_CALENDAR sc
    JOIN ET_SALES_DATA s
        ON  s.WERKS            = sc.ST_CD
        AND s.SALES_DATE::DATE = sc.LY_SAME_DATE
    WHERE sc.LY_SAME_DATE IS NOT NULL
    GROUP BY sc.ST_CD, s.MATNR, sc.FY, sc.FY_WEEK
),
stock_0001 AS (
    SELECT WERKS, MATNR, SUM(LABST) AS QTY, SUM(LABST_DMBTR) AS VAL
    FROM ET_STOCK_DATA
    WHERE LGORT = '0001'
      AND STOCK_DATE = (SELECT MAX(STOCK_DATE) FROM ET_STOCK_DATA WHERE LGORT = '0001')
    GROUP BY WERKS, MATNR
),
stock_0006 AS (
    SELECT WERKS, MATNR, SUM(LABST) AS QTY, SUM(LABST_DMBTR) AS VAL
    FROM ET_STOCK_DATA
    WHERE LGORT = '0006'
      AND STOCK_DATE = (SELECT MAX(STOCK_DATE) FROM ET_STOCK_DATA WHERE LGORT = '0006')
    GROUP BY WERKS, MATNR
)

SELECT
    cs.STORE_CODE,
    cs.FY,
    cs.FY_WEEK,
    p.MAJ_CAT, p.SUB_DIV, p.DIV, p.SEG,
    p.COLOR, p.SIZE_1, p.MC_CODE, p.MC_DESC, p.REG_SEG, p.ART_TYPE, p.SSN,
    p.VENDOR_NAME, p.VENDOR_CITY, p.MACRO_MVGR, p.MAIN_MVGR, p.M_BUYING_TYPE,
    p.M_FAB_1, p.M_FAB_2, p.IS_ACTIVE,

    cs.SALE_QTY,  cs.SALE_VAL,  cs.SALE_GM,
    NVL(lm.LYSM_QTY, 0) AS LYSM_QTY, NVL(lm.LYSM_VAL, 0) AS LYSM_VAL, NVL(lm.LYSM_GM, 0) AS LYSM_GM,
    NVL(lp.LYSP_QTY, 0) AS LYSP_QTY, NVL(lp.LYSP_VAL, 0) AS LYSP_VAL, NVL(lp.LYSP_GM, 0) AS LYSP_GM,
    NVL(s1.QTY, 0) AS STK_0001_QTY, NVL(s1.VAL, 0) AS STK_0001_VAL,
    NVL(s6.QTY, 0) AS STK_0006_QTY, NVL(s6.VAL, 0) AS STK_0006_VAL

FROM current_sale cs
LEFT JOIN DIM_PRODUCT p    ON p.MATNR = cs.ARTICLE_NUMBER
LEFT JOIN lysm_sale lm     ON lm.STORE_CODE = cs.STORE_CODE AND lm.ARTICLE_NUMBER = cs.ARTICLE_NUMBER
                           AND lm.FY = cs.FY AND lm.FY_WEEK = cs.FY_WEEK
LEFT JOIN lysp_sale lp     ON lp.STORE_CODE = cs.STORE_CODE AND lp.ARTICLE_NUMBER = cs.ARTICLE_NUMBER
                           AND lp.FY = cs.FY AND lp.FY_WEEK = cs.FY_WEEK
LEFT JOIN stock_0001 s1    ON s1.WERKS = cs.STORE_CODE AND s1.MATNR = cs.ARTICLE_NUMBER
LEFT JOIN stock_0006 s6    ON s6.WERKS = cs.STORE_CODE AND s6.MATNR = cs.ARTICLE_NUMBER;
