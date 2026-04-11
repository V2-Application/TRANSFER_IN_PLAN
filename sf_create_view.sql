CREATE OR REPLACE VIEW V_MRST_REPORT_ENRICHED AS
WITH sale_budget_monthly AS (
    SELECT
        "ST_CD",
        "MAJ_CAT",
        DATE_TRUNC('MONTH', TO_DATE("DATE")) AS BGT_MONTH,
        SUM("BGT_VAL") AS BGT_VAL_MTH,
        SUM("BGT_QTY") AS BGT_QTY_MTH
    FROM SALE_BUDGET
    GROUP BY "ST_CD", "MAJ_CAT", DATE_TRUNC('MONTH', TO_DATE("DATE"))
),
base AS (
    SELECT
        m.*,
        dp."AVG_DENSITY",
        dp."MAJ_CAT",
        dp."SEG",
        dp."DIV",
        dp."SUB_DIV",
        dp."GEN_ART_DESC",
        dp."VENDOR_NAME",
        dp."MACRO_MVGR",
        dp."SSN",
        sp."AVG_AREA_PER_FIX",
        sp."ST_NM",
        sp."ZONE",
        sp."REG",
        sp."AREA" AS ST_AREA_NAME,
        sp."ST_TYP",
        sp."ST_FIX",
        DATE_TRUNC('MONTH', TO_DATE(m."LOGDATE")) AS REPORT_MONTH
    FROM MRST_REPORT_TABLE_DUMP m
    LEFT JOIN DIM_PRODUCT dp
        ON LPAD(m."MATNR", 18, '0') = dp."MATNR"
    LEFT JOIN STORE_PLANT_MASTER sp
        ON m."STORE_CODE" = sp."ST_CD"
),
computed AS (
    SELECT
        b.*,
        -- PPA (Per Piece Area)
        CASE
            WHEN NULLIF(b."AVG_DENSITY", 0) IS NOT NULL
            THEN b."AVG_AREA_PER_FIX" / b."AVG_DENSITY"
            ELSE NULL
        END AS PPA,
        -- FINAL_AREA = (0001_ST_STK_Q + 0006_ST_STK_Q) * PPA
        CASE
            WHEN NULLIF(b."AVG_DENSITY", 0) IS NOT NULL
            THEN (COALESCE(b."0001_ST_STK_Q", 0) + COALESCE(b."0006_ST_STK_Q", 0))
                 * (b."AVG_AREA_PER_FIX" / b."AVG_DENSITY")
            ELSE NULL
        END AS FINAL_AREA,
        -- Monthly SALE_BUDGET
        sb.BGT_VAL_MTH,
        sb.BGT_QTY_MTH
    FROM base b
    LEFT JOIN sale_budget_monthly sb
        ON b."STORE_CODE" = sb."ST_CD"
        AND b."MAJ_CAT" = sb."MAJ_CAT"
        AND b.REPORT_MONTH = sb.BGT_MONTH
)
SELECT
    c.*,
    -- TD_PSF = TD_SALE_V / FINAL_AREA
    CASE
        WHEN NULLIF(c.FINAL_AREA, 0) IS NOT NULL
        THEN c."TD_SALE_V" / c.FINAL_AREA
        ELSE NULL
    END AS TD_PSF,
    -- GP_SALE = TD_GM_V / FINAL_AREA
    CASE
        WHEN NULLIF(c.FINAL_AREA, 0) IS NOT NULL
        THEN c."TD_GM_V" / c.FINAL_AREA
        ELSE NULL
    END AS GP_SALE,
    -- BGT_AREA = BGT_QTY_MTH * PPA
    CASE
        WHEN c.PPA IS NOT NULL AND c.BGT_QTY_MTH IS NOT NULL
        THEN c.BGT_QTY_MTH * c.PPA
        ELSE NULL
    END AS BGT_AREA,
    -- BGT_PSF = BGT_VAL_MTH / BGT_AREA
    CASE
        WHEN c.PPA IS NOT NULL
             AND NULLIF(c.BGT_QTY_MTH, 0) IS NOT NULL
             AND c.BGT_VAL_MTH IS NOT NULL
        THEN c.BGT_VAL_MTH / (c.BGT_QTY_MTH * c.PPA)
        ELSE NULL
    END AS BGT_PSF
FROM computed c;
