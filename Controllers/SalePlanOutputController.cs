using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;

namespace TRANSFER_IN_PLAN.Controllers;

public class SalePlanOutputController : Controller
{
    private readonly string _sfConnStr;

    private static readonly (string Col, string Label, string Group)[] GroupableColumns = {
        ("FY",               "FY",                 "Base"),
        ("FY_WEEK",          "FY Week",            "Base"),
        ("WK_ST_DT",         "Week Start",         "Base"),
        ("WK_END_DT",        "Week End",           "Base"),
        ("STORE_CODE",       "Store Code",         "Base"),
        ("MAJ_CAT",          "Major Category",     "Base"),
        ("SUB_DIV",          "Sub Division",       "Base"),
        ("DIV",              "Division",           "Base"),
        ("SEG",              "Segment",            "Base"),
        ("COLOR",            "Color",              "Article"),
        ("SIZE_1",           "Size",               "Article"),
        ("MC_CODE",          "MC Code",            "Article"),
        ("MC_DESC",          "MC Description",     "Article"),
        ("REG_SEG",          "Regional Segment",   "Article"),
        ("ART_TYPE",         "Article Type",       "Article"),
        ("SSN",              "Season",             "Article"),
        ("VENDOR_NAME",      "Vendor Name",        "Vendor"),
        ("VENDOR_CITY",      "Vendor City",        "Vendor"),
        ("MACRO_MVGR",       "Macro MVGR",         "MVGR"),
        ("MAIN_MVGR",        "Main MVGR",          "MVGR"),
        ("M_BUYING_TYPE",    "Buying Type",        "MVGR"),
        ("M_FAB_1",          "Fabric 1",           "Fabric"),
        ("M_FAB_2",          "Fabric 2",           "Fabric"),
        ("IS_ACTIVE",        "Is Active",          "Flags"),
    };

    // ── All metrics: base 13 + R1 (9 avg + 3 growth + 7 ref) + R2 (9 avg + 3 growth + 7 ref) ──
    private static readonly (string Alias, string Label)[] MetricDefs = {
        ("SALE_QTY","Sale Qty"), ("SALE_VAL","Sale Val"), ("SALE_GM","Sale GM"),
        ("LYSM_QTY","LYSM Qty"), ("LYSM_VAL","LYSM Val"), ("LYSM_GM","LYSM GM"),
        ("LYSP_QTY","LYSP Qty"), ("LYSP_VAL","LYSP Val"), ("LYSP_GM","LYSP GM"),
        ("STK_0001_QTY","Stk 0001 Qty"), ("STK_0001_VAL","Stk 0001 Val"),
        ("STK_0006_QTY","Stk 0006 Qty"), ("STK_0006_VAL","Stk 0006 Val"),
        ("AVG_SALE_QTY","Avg Sale Qty"), ("AVG_SALE_VAL","Avg Sale Val"), ("AVG_SALE_GM","Avg Sale GM"),
        ("AVG_LYSM_QTY","Avg LYSM Qty"), ("AVG_LYSM_VAL","Avg LYSM Val"), ("AVG_LYSM_GM","Avg LYSM GM"),
        ("AVG_LYSP_QTY","Avg LYSP Qty"), ("AVG_LYSP_VAL","Avg LYSP Val"), ("AVG_LYSP_GM","Avg LYSP GM"),
        ("WK_GROWTH_QTY","Wk Growth Qty %"), ("WK_GROWTH_VAL","Wk Growth Val %"), ("WK_GROWTH_GM","Wk Growth GM %"),
        ("FINAL_GROWTH_QTY","REF-0 Final Growth"),
        ("REF1_QTY","REF-1 Min RXL"), ("REF2_QTY","REF-2 Min LYSP"),
        ("REF3_QTY","REF-3 L3M Adj"), ("REF4_QTY","REF-4 L5M Fcst"),
        ("REF5_QTY","REF-5 L3M Fcst"), ("REF6_QTY","REF-6 STR Prot"),
        // R2 Averages only (no R2 references — single set of references uses both BW1 + BW2)
        ("AVG_SALE_QTY_R2","R2 Avg Sale Qty"), ("AVG_SALE_VAL_R2","R2 Avg Sale Val"), ("AVG_SALE_GM_R2","R2 Avg Sale GM"),
        ("AVG_LYSM_QTY_R2","R2 Avg LYSM Qty"), ("AVG_LYSM_VAL_R2","R2 Avg LYSM Val"), ("AVG_LYSM_GM_R2","R2 Avg LYSM GM"),
        ("AVG_LYSP_QTY_R2","R2 Avg LYSP Qty"), ("AVG_LYSP_VAL_R2","R2 Avg LYSP Val"), ("AVG_LYSP_GM_R2","R2 Avg LYSP GM"),
    };

    private static readonly string[] AllMetricCols = MetricDefs.Select(m => m.Alias).ToArray();
    private static readonly string[] BaseMetricCols = AllMetricCols.Take(13).ToArray();
    private static readonly HashSet<string> R2OnlyCols = new(
        MetricDefs.Where(m => m.Alias.EndsWith("_R2")).Select(m => m.Alias), StringComparer.OrdinalIgnoreCase);
    private static readonly HashSet<string> ValidGroupCols =
        new(GroupableColumns.Select(g => g.Col), StringComparer.OrdinalIgnoreCase);

    private static readonly (string Key, string Label, string[] Metrics)[] MetricGroups = {
        ("SALE", "Current Sale", new[] { "SALE_QTY", "SALE_VAL", "SALE_GM" }),
        ("LYSM", "LYSM",        new[] { "LYSM_QTY", "LYSM_VAL", "LYSM_GM" }),
        ("LYSP", "LYSP",        new[] { "LYSP_QTY", "LYSP_VAL", "LYSP_GM" }),
        ("STK",  "Stock",       new[] { "STK_0001_QTY", "STK_0001_VAL", "STK_0006_QTY", "STK_0006_VAL" }),
        ("AVG_SALE","R1 Avg Sale",  new[] { "AVG_SALE_QTY", "AVG_SALE_VAL", "AVG_SALE_GM" }),
        ("AVG_LYSM","R1 Avg LYSM",  new[] { "AVG_LYSM_QTY", "AVG_LYSM_VAL", "AVG_LYSM_GM" }),
        ("AVG_LYSP","R1 Avg LYSP",  new[] { "AVG_LYSP_QTY", "AVG_LYSP_VAL", "AVG_LYSP_GM" }),
        ("REFERENCES","References", new[] { "FINAL_GROWTH_QTY", "REF1_QTY", "REF2_QTY", "REF3_QTY", "REF4_QTY", "REF5_QTY", "REF6_QTY" }),
        ("AVG_SALE_R2","R2 Avg Sale", new[] { "AVG_SALE_QTY_R2", "AVG_SALE_VAL_R2", "AVG_SALE_GM_R2" }),
        ("AVG_LYSM_R2","R2 Avg LYSM", new[] { "AVG_LYSM_QTY_R2", "AVG_LYSM_VAL_R2", "AVG_LYSM_GM_R2" }),
        ("AVG_LYSP_R2","R2 Avg LYSP", new[] { "AVG_LYSP_QTY_R2", "AVG_LYSP_VAL_R2", "AVG_LYSP_GM_R2" }),
    };
    private static readonly HashSet<string> TimeColumns =
        new(new[] { "FY_WEEK", "WK_ST_DT", "WK_END_DT" }, StringComparer.OrdinalIgnoreCase);

    // Table header groups: base (no ref) vs R1 only vs R1+R2
    private static readonly (int Span, string Label, string Color)[] BaseTableHeaders = {
        (3, "Current Sale", "#059669"), (3, "LYSM", "#0d4f3c"),
        (3, "LYSP", "#7c4a00"), (4, "Stock", "#1a3a5c"),
    };
    private static readonly (int Span, string Label, string Color)[] R1TableHeaders = {
        (3, "Current Sale", "#059669"), (3, "LYSM", "#0d4f3c"),
        (3, "LYSP", "#7c4a00"), (4, "Stock", "#1a3a5c"),
        (3, "R1 Avg Sale", "#d97706"), (3, "R1 Avg LYSM", "#b45309"),
        (3, "R1 Avg LYSP", "#92400e"), (7, "References", "#7c3aed"),
    };
    private static readonly (int Span, string Label, string Color)[] R1R2TableHeaders = {
        (3, "Current Sale", "#059669"), (3, "LYSM", "#0d4f3c"),
        (3, "LYSP", "#7c4a00"), (4, "Stock", "#1a3a5c"),
        (3, "R1 Avg Sale", "#d97706"), (3, "R1 Avg LYSM", "#b45309"),
        (3, "R1 Avg LYSP", "#92400e"), (7, "References", "#7c3aed"),
        (3, "R2 Avg Sale", "#0ea5e9"), (3, "R2 Avg LYSM", "#0284c7"),
        (3, "R2 Avg LYSP", "#075985"),
    };

    public SalePlanOutputController(IConfiguration config)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
    }

    public IActionResult Info() => View();

    private static List<string> ParseCsv(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv)) return new();
        return csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                  .Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static string ToInClause(List<string> vals)
        => string.Join(",", vals.Select(v => $"'{Esc(v)}'"));

    // ── Resolve N weeks before the base week (searches across ALL FYs by date) ──
    private static async Task<List<(string Fy, string Week)>> ResolveLastNWeeksAsync(
        SnowflakeDbConnection conn, string fy, string baseWeek, int n)
    {
        var allWeeks = new List<(string Fy, string Week)>();
        await using var cmd = conn.CreateCommand();
        // Order by actual start date across ALL fiscal years
        cmd.CommandText = "SELECT FY, FY_WEEK, MIN(FY_WEEK_ST_DT) AS ST FROM STORE_CALENDAR GROUP BY FY, FY_WEEK ORDER BY ST";
        cmd.CommandTimeout = 15;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var f = r.IsDBNull(0) ? null : r.GetString(0)?.Trim();
            var w = r.IsDBNull(1) ? null : r.GetString(1)?.Trim();
            if (!string.IsNullOrEmpty(f) && !string.IsNullOrEmpty(w))
                allWeeks.Add((f, w));
        }
        if (allWeeks.Count == 0) return new();

        // Find base week by FY + FY_WEEK
        int baseIdx = allWeeks.FindIndex(x =>
            x.Fy.Equals(fy, StringComparison.OrdinalIgnoreCase) &&
            x.Week.Equals(baseWeek.Trim(), StringComparison.OrdinalIgnoreCase));
        if (baseIdx <= 0) return new();

        var refWeeks = new List<(string Fy, string Week)>();
        for (int i = Math.Max(0, baseIdx - n); i < baseIdx; i++)
            refWeeks.Add(allWeeks[i]);
        return refWeeks;
    }

    // ── Build CTE: base + optional ref weeks → vw_plan as final table ──
    // Uses UNION ALL so a week can be both display ('B') and reference ('R')
    // Ref weeks may come from a different FY (cross-FY lookback)
    // refGroupDims: non-time dimensions for ref_avg grouping (e.g. STORE_CODE, MAJ_CAT)
    private static string BuildDataCte(string fy, List<string> fyWeeks, List<string> stores,
        List<(string Fy, string Week)>? refWeeks = null, List<string>? refGroupDims = null,
        List<(string Fy, string Week)>? refWeeks2 = null,
        int ref6Days = 120)
    {
        bool hasRef = refWeeks != null && refWeeks.Count > 0;
        bool hasRef2 = refWeeks2 != null && refWeeks2.Count > 0;
        int refN = hasRef ? refWeeks!.Count : 1;
        int refN2 = hasRef2 ? refWeeks2!.Count : 1;

        var stFilter = stores.Count > 0
            ? $" AND sc.ST_CD IN ({ToInClause(stores)})"
            : "";

        var baseWeekFilter = fyWeeks.Count == 1
            ? $"FY_WEEK='{Esc(fyWeeks[0])}'"
            : $"FY_WEEK IN ({ToInClause(fyWeeks)})";

        var sb = new StringBuilder(4096);

        if (hasRef || hasRef2)
        {
            sb.Append($@"WITH store_cal AS (
  SELECT ST_CD,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,BGT_MNTH_DATE,LY_SAME_DATE,'B' AS WK_TYPE FROM STORE_CALENDAR sc
  WHERE FY='{Esc(fy)}' AND {baseWeekFilter}{stFilter}");

            if (hasRef)
            {
                // Build (FY, FY_WEEK) IN filter — supports cross-FY ref weeks
                var refPairs = string.Join(",", refWeeks!.Select(r => $"('{Esc(r.Fy)}','{Esc(r.Week)}')"));
                sb.Append($@"
  UNION ALL
  SELECT ST_CD,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,BGT_MNTH_DATE,LY_SAME_DATE,'R' AS WK_TYPE FROM STORE_CALENDAR sc
  WHERE (FY,FY_WEEK) IN ({refPairs}){stFilter}");
            }

            if (hasRef2)
            {
                var refPairs2 = string.Join(",", refWeeks2!.Select(r => $"('{Esc(r.Fy)}','{Esc(r.Week)}')"));
                sb.Append($@"
  UNION ALL
  SELECT ST_CD,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,BGT_MNTH_DATE,LY_SAME_DATE,'R2' AS WK_TYPE FROM STORE_CALENDAR sc
  WHERE (FY,FY_WEEK) IN ({refPairs2}){stFilter}");
            }

            sb.Append(@"
),");
        }
        else
        {
            sb.Append($@"WITH store_cal AS (
  SELECT ST_CD,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,BGT_MNTH_DATE,LY_SAME_DATE,'B' AS WK_TYPE FROM STORE_CALENDAR sc
  WHERE FY='{Esc(fy)}' AND {baseWeekFilter}{stFilter}
),");
        }

        sb.Append($@"
week_stores AS (SELECT DISTINCT ST_CD FROM store_cal),
current_sale AS (
  SELECT sc.ST_CD AS STORE_CODE,s.MATNR AS ARTICLE_NUMBER,sc.FY_WEEK,sc.WK_TYPE,
    SUM(s.FKIMG) AS SALE_QTY,SUM(s.NETWR) AS SALE_VAL,
    SUM(NVL(s.NET_VAL,0)-NVL(s.KWERT_VPRS,0)) AS SALE_GM
  FROM store_cal sc JOIN ET_SALES_DATA s ON s.WERKS=sc.ST_CD AND s.SALES_DATE::DATE=sc.BGT_MNTH_DATE
  GROUP BY sc.ST_CD,s.MATNR,sc.FY_WEEK,sc.WK_TYPE
),
lysm_sale AS (
  SELECT sc.ST_CD AS STORE_CODE,s.MATNR AS ARTICLE_NUMBER,sc.FY_WEEK,sc.WK_TYPE,
    SUM(s.FKIMG) AS LYSM_QTY,SUM(s.NETWR) AS LYSM_VAL,
    SUM(NVL(s.NET_VAL,0)-NVL(s.KWERT_VPRS,0)) AS LYSM_GM
  FROM store_cal sc JOIN ET_SALES_DATA s
    ON s.WERKS=sc.ST_CD AND s.SALES_DATE::DATE=DATEADD('year',-1,sc.BGT_MNTH_DATE)
  GROUP BY sc.ST_CD,s.MATNR,sc.FY_WEEK,sc.WK_TYPE
),
lysp_sale AS (
  SELECT sc.ST_CD AS STORE_CODE,s.MATNR AS ARTICLE_NUMBER,sc.FY_WEEK,sc.WK_TYPE,
    SUM(s.FKIMG) AS LYSP_QTY,SUM(s.NETWR) AS LYSP_VAL,
    SUM(NVL(s.NET_VAL,0)-NVL(s.KWERT_VPRS,0)) AS LYSP_GM
  FROM store_cal sc JOIN ET_SALES_DATA s
    ON s.WERKS=sc.ST_CD AND s.SALES_DATE::DATE=sc.LY_SAME_DATE
  WHERE sc.LY_SAME_DATE IS NOT NULL GROUP BY sc.ST_CD,s.MATNR,sc.FY_WEEK,sc.WK_TYPE
),
stock_0001 AS (
  SELECT WERKS,MATNR,SUM(LABST) AS QTY,SUM(LABST_DMBTR) AS VAL FROM ET_STOCK_DATA
  WHERE LGORT='0001' AND STOCK_DATE=(SELECT MAX(STOCK_DATE) FROM ET_STOCK_DATA WHERE LGORT='0001')
    AND WERKS IN (SELECT ST_CD FROM week_stores)
  GROUP BY WERKS,MATNR
),
stock_0006 AS (
  SELECT WERKS,MATNR,SUM(LABST) AS QTY,SUM(LABST_DMBTR) AS VAL FROM ET_STOCK_DATA
  WHERE LGORT='0006' AND STOCK_DATE=(SELECT MAX(STOCK_DATE) FROM ET_STOCK_DATA WHERE LGORT='0006')
    AND WERKS IN (SELECT ST_CD FROM week_stores)
  GROUP BY WERKS,MATNR
),
cal_weeks AS (SELECT FY_WEEK,MIN(FY_WEEK_ST_DT) AS WK_ST_DT,MAX(FY_WEEK_END_DT) AS WK_END_DT FROM store_cal WHERE WK_TYPE='B' GROUP BY FY_WEEK),
base_keys AS (
  SELECT STORE_CODE,ARTICLE_NUMBER,FY_WEEK,WK_TYPE FROM current_sale
  UNION SELECT STORE_CODE,ARTICLE_NUMBER,FY_WEEK,WK_TYPE FROM lysm_sale
  UNION SELECT STORE_CODE,ARTICLE_NUMBER,FY_WEEK,WK_TYPE FROM lysp_sale
  UNION SELECT WERKS,MATNR,w.FY_WEEK,'B' FROM stock_0001 CROSS JOIN cal_weeks w
  UNION SELECT WERKS,MATNR,w.FY_WEEK,'B' FROM stock_0006 CROSS JOIN cal_weeks w
),
vw_data AS (
  SELECT bk.STORE_CODE,bk.ARTICLE_NUMBER,'{Esc(fy)}' AS FY,bk.FY_WEEK,bk.WK_TYPE,cw.WK_ST_DT,cw.WK_END_DT,
    p.MAJ_CAT,p.SUB_DIV,p.DIV,p.SEG,p.COLOR,p.SIZE_1,p.MC_CODE,p.MC_DESC,
    p.REG_SEG,p.ART_TYPE,p.SSN,p.VENDOR_NAME,p.VENDOR_CITY,
    p.MACRO_MVGR,p.MAIN_MVGR,p.M_BUYING_TYPE,p.M_FAB_1,p.M_FAB_2,p.IS_ACTIVE,
    NVL(cs.SALE_QTY,0) AS SALE_QTY,NVL(cs.SALE_VAL,0) AS SALE_VAL,NVL(cs.SALE_GM,0) AS SALE_GM,
    NVL(lm.LYSM_QTY,0) AS LYSM_QTY,NVL(lm.LYSM_VAL,0) AS LYSM_VAL,NVL(lm.LYSM_GM,0) AS LYSM_GM,
    NVL(lp.LYSP_QTY,0) AS LYSP_QTY,NVL(lp.LYSP_VAL,0) AS LYSP_VAL,NVL(lp.LYSP_GM,0) AS LYSP_GM,
    NVL(s1.QTY,0) AS STK_0001_QTY,NVL(s1.VAL,0) AS STK_0001_VAL,
    NVL(s6.QTY,0) AS STK_0006_QTY,NVL(s6.VAL,0) AS STK_0006_VAL
  FROM base_keys bk
  LEFT JOIN cal_weeks cw ON cw.FY_WEEK=bk.FY_WEEK
  LEFT JOIN DIM_PRODUCT p ON p.MATNR=bk.ARTICLE_NUMBER
  LEFT JOIN current_sale cs ON cs.STORE_CODE=bk.STORE_CODE AND cs.ARTICLE_NUMBER=bk.ARTICLE_NUMBER AND cs.FY_WEEK=bk.FY_WEEK AND cs.WK_TYPE=bk.WK_TYPE
  LEFT JOIN lysm_sale lm ON lm.STORE_CODE=bk.STORE_CODE AND lm.ARTICLE_NUMBER=bk.ARTICLE_NUMBER AND lm.FY_WEEK=bk.FY_WEEK AND lm.WK_TYPE=bk.WK_TYPE
  LEFT JOIN lysp_sale lp ON lp.STORE_CODE=bk.STORE_CODE AND lp.ARTICLE_NUMBER=bk.ARTICLE_NUMBER AND lp.FY_WEEK=bk.FY_WEEK AND lp.WK_TYPE=bk.WK_TYPE
  LEFT JOIN stock_0001 s1 ON s1.WERKS=bk.STORE_CODE AND s1.MATNR=bk.ARTICLE_NUMBER
  LEFT JOIN stock_0006 s6 ON s6.WERKS=bk.STORE_CODE AND s6.MATNR=bk.ARTICLE_NUMBER
  WHERE (NVL(cs.SALE_QTY,0)+NVL(cs.SALE_VAL,0)+NVL(lm.LYSM_QTY,0)+NVL(lm.LYSM_VAL,0)
    +NVL(lp.LYSP_QTY,0)+NVL(lp.LYSP_VAL,0)+NVL(s1.QTY,0)+NVL(s1.VAL,0)
    +NVL(s6.QTY,0)+NVL(s6.VAL,0))>0
),
ref_l3m AS (
  SELECT s.WERKS AS STORE_CODE,p.MAJ_CAT,
    CASE WHEN SUM(CASE WHEN s.SALES_DATE BETWEEN DATEADD('month',-6,CURRENT_DATE) AND DATEADD('month',-3,CURRENT_DATE) THEN s.FKIMG ELSE 0 END)>0
    THEN (SUM(CASE WHEN s.SALES_DATE>DATEADD('month',-3,CURRENT_DATE) THEN s.FKIMG ELSE 0 END)*1.0/
          NULLIF(SUM(CASE WHEN s.SALES_DATE BETWEEN DATEADD('month',-6,CURRENT_DATE) AND DATEADD('month',-3,CURRENT_DATE) THEN s.FKIMG ELSE 0 END),0)-1)*100
    ELSE 0 END AS L3M_GR_PCT
  FROM ET_SALES_DATA s JOIN DIM_PRODUCT p ON p.MATNR=s.MATNR
  WHERE s.WERKS IN (SELECT ST_CD FROM week_stores) AND s.SALES_DATE>=DATEADD('month',-6,CURRENT_DATE)
  GROUP BY s.WERKS,p.MAJ_CAT
),
ref_l5m_base AS (
  SELECT s.WERKS AS STORE_CODE,p.MAJ_CAT,SUM(s.FKIMG)/NULLIF(COUNT(DISTINCT MONTH(s.SALES_DATE)),0) AS BASE_L5M_QTY
  FROM ET_SALES_DATA s JOIN DIM_PRODUCT p ON p.MATNR=s.MATNR
  WHERE s.WERKS IN (SELECT ST_CD FROM week_stores) AND MONTH(s.SALES_DATE) IN (6,7,8,9,10) AND s.SALES_DATE>=DATEADD('year',-1,CURRENT_DATE)
  GROUP BY s.WERKS,p.MAJ_CAT
),
ref_l3m_base AS (
  SELECT s.WERKS AS STORE_CODE,p.MAJ_CAT,SUM(s.FKIMG)/NULLIF(COUNT(DISTINCT MONTH(s.SALES_DATE)),0) AS BASE_L3M_QTY
  FROM ET_SALES_DATA s JOIN DIM_PRODUCT p ON p.MATNR=s.MATNR
  WHERE s.WERKS IN (SELECT ST_CD FROM week_stores) AND MONTH(s.SALES_DATE) IN (10,11,12) AND s.SALES_DATE>=DATEADD('year',-1,CURRENT_DATE)
  GROUP BY s.WERKS,p.MAJ_CAT
),
ref_lysp_maj AS (
  SELECT STORE_CODE,MAJ_CAT,
    CASE WHEN SUM(LYSP_QTY)>0 THEN (SUM(SALE_QTY)*1.0/NULLIF(SUM(LYSP_QTY),0)-1)*100 ELSE 0 END AS LYSP_MAJ_GR_PCT
  FROM vw_data WHERE WK_TYPE='B' GROUP BY STORE_CODE,MAJ_CAT
),
ref_str AS (
  SELECT s.WERKS AS STORE_CODE,p.MAJ_CAT,SUM(s.FKIMG) AS STR_NDAY_QTY
  FROM ET_SALES_DATA s JOIN DIM_PRODUCT p ON p.MATNR=s.MATNR
  WHERE s.WERKS IN (SELECT ST_CD FROM week_stores) AND s.SALES_DATE>=DATEADD('day',-{ref6Days},CURRENT_DATE)
  GROUP BY s.WERKS,p.MAJ_CAT
)");

        // Common ref CTE joins (store+category level)
        var refJoins = @"
  LEFT JOIN ref_l3m rl3 ON rl3.STORE_CODE=v.STORE_CODE AND rl3.MAJ_CAT IS NOT DISTINCT FROM v.MAJ_CAT
  LEFT JOIN ref_l5m_base rl5 ON rl5.STORE_CODE=v.STORE_CODE AND rl5.MAJ_CAT IS NOT DISTINCT FROM v.MAJ_CAT
  LEFT JOIN ref_l3m_base rl3b ON rl3b.STORE_CODE=v.STORE_CODE AND rl3b.MAJ_CAT IS NOT DISTINCT FROM v.MAJ_CAT
  LEFT JOIN ref_lysp_maj rlm ON rlm.STORE_CODE=v.STORE_CODE AND rlm.MAJ_CAT IS NOT DISTINCT FROM v.MAJ_CAT
  LEFT JOIN ref_str rst ON rst.STORE_CODE=v.STORE_CODE AND rst.MAJ_CAT IS NOT DISTINCT FROM v.MAJ_CAT";
        var refCols = @",NVL(rl3.L3M_GR_PCT,0) AS L3M_GR_PCT,NVL(rl5.BASE_L5M_QTY,0) AS BASE_L5M_QTY,
    NVL(rl3b.BASE_L3M_QTY,0) AS BASE_L3M_QTY,NVL(rlm.LYSP_MAJ_GR_PCT,0) AS LYSP_MAJ_GR_PCT,NVL(rst.STR_NDAY_QTY,0) AS STR_NDAY_QTY";

        // R2 AVG zero defaults (used in both hasRef and !hasRef branches when no R2)
        var r2AvgZeros = @",0 AS AVG_SALE_QTY_R2,0 AS AVG_SALE_VAL_R2,0 AS AVG_SALE_GM_R2,
    0 AS AVG_LYSM_QTY_R2,0 AS AVG_LYSM_VAL_R2,0 AS AVG_LYSM_GM_R2,
    0 AS AVG_LYSP_QTY_R2,0 AS AVG_LYSP_VAL_R2,0 AS AVG_LYSP_GM_R2";

        if (hasRef)
        {
            var rgd = refGroupDims != null && refGroupDims.Count > 0
                ? refGroupDims : new List<string> { "STORE_CODE" };
            var rgdSql = string.Join(",", rgd);
            var joinCond = string.Join(" AND ", rgd.Select(d => $"v.{d} IS NOT DISTINCT FROM ra.{d}"));

            sb.Append($@",
ref_avg AS (
  SELECT {rgdSql},
    SUM(SALE_QTY)/{refN}.0 AS AVG_SALE_QTY,SUM(SALE_VAL)/{refN}.0 AS AVG_SALE_VAL,SUM(SALE_GM)/{refN}.0 AS AVG_SALE_GM,
    SUM(LYSM_QTY)/{refN}.0 AS AVG_LYSM_QTY,SUM(LYSM_VAL)/{refN}.0 AS AVG_LYSM_VAL,SUM(LYSM_GM)/{refN}.0 AS AVG_LYSM_GM,
    SUM(LYSP_QTY)/{refN}.0 AS AVG_LYSP_QTY,SUM(LYSP_VAL)/{refN}.0 AS AVG_LYSP_VAL,SUM(LYSP_GM)/{refN}.0 AS AVG_LYSP_GM
  FROM vw_data WHERE WK_TYPE='R'
  GROUP BY {rgdSql}
)");

            if (hasRef2)
            {
                var joinCond2 = string.Join(" AND ", rgd.Select(d => $"v.{d} IS NOT DISTINCT FROM ra2.{d}"));
                sb.Append($@",
ref_avg2 AS (
  SELECT {rgdSql},
    SUM(SALE_QTY)/{refN2}.0 AS AVG_SALE_QTY_R2,SUM(SALE_VAL)/{refN2}.0 AS AVG_SALE_VAL_R2,SUM(SALE_GM)/{refN2}.0 AS AVG_SALE_GM_R2,
    SUM(LYSM_QTY)/{refN2}.0 AS AVG_LYSM_QTY_R2,SUM(LYSM_VAL)/{refN2}.0 AS AVG_LYSM_VAL_R2,SUM(LYSM_GM)/{refN2}.0 AS AVG_LYSM_GM_R2,
    SUM(LYSP_QTY)/{refN2}.0 AS AVG_LYSP_QTY_R2,SUM(LYSP_VAL)/{refN2}.0 AS AVG_LYSP_VAL_R2,SUM(LYSP_GM)/{refN2}.0 AS AVG_LYSP_GM_R2
  FROM vw_data WHERE WK_TYPE='R2'
  GROUP BY {rgdSql}
)");
            }

            var r2AvgSelect = hasRef2
                ? @",NVL(ra2.AVG_SALE_QTY_R2,0) AS AVG_SALE_QTY_R2,NVL(ra2.AVG_SALE_VAL_R2,0) AS AVG_SALE_VAL_R2,NVL(ra2.AVG_SALE_GM_R2,0) AS AVG_SALE_GM_R2,
    NVL(ra2.AVG_LYSM_QTY_R2,0) AS AVG_LYSM_QTY_R2,NVL(ra2.AVG_LYSM_VAL_R2,0) AS AVG_LYSM_VAL_R2,NVL(ra2.AVG_LYSM_GM_R2,0) AS AVG_LYSM_GM_R2,
    NVL(ra2.AVG_LYSP_QTY_R2,0) AS AVG_LYSP_QTY_R2,NVL(ra2.AVG_LYSP_VAL_R2,0) AS AVG_LYSP_VAL_R2,NVL(ra2.AVG_LYSP_GM_R2,0) AS AVG_LYSP_GM_R2"
                : r2AvgZeros;

            var r2JoinSql = hasRef2
                ? "\n  LEFT JOIN ref_avg2 ra2 ON " + string.Join(" AND ", rgd.Select(d => $"v.{d} IS NOT DISTINCT FROM ra2.{d}"))
                : "";

            sb.Append($@",
vw_plan AS (
  SELECT v.*,
    NVL(ra.AVG_SALE_QTY,0) AS AVG_SALE_QTY,NVL(ra.AVG_SALE_VAL,0) AS AVG_SALE_VAL,NVL(ra.AVG_SALE_GM,0) AS AVG_SALE_GM,
    NVL(ra.AVG_LYSM_QTY,0) AS AVG_LYSM_QTY,NVL(ra.AVG_LYSM_VAL,0) AS AVG_LYSM_VAL,NVL(ra.AVG_LYSM_GM,0) AS AVG_LYSM_GM,
    NVL(ra.AVG_LYSP_QTY,0) AS AVG_LYSP_QTY,NVL(ra.AVG_LYSP_VAL,0) AS AVG_LYSP_VAL,NVL(ra.AVG_LYSP_GM,0) AS AVG_LYSP_GM
    {r2AvgSelect}
    {refCols}
  FROM vw_data v
  LEFT JOIN ref_avg ra ON {joinCond}
  {r2JoinSql}
  {refJoins}
  WHERE v.WK_TYPE='B'
) ");
        }
        else if (hasRef2)
        {
            // Only R2, no R1
            var rgd = refGroupDims != null && refGroupDims.Count > 0
                ? refGroupDims : new List<string> { "STORE_CODE" };
            var rgdSql = string.Join(",", rgd);
            var joinCond2 = string.Join(" AND ", rgd.Select(d => $"v.{d} IS NOT DISTINCT FROM ra2.{d}"));

            sb.Append($@",
ref_avg2 AS (
  SELECT {rgdSql},
    SUM(SALE_QTY)/{refN2}.0 AS AVG_SALE_QTY_R2,SUM(SALE_VAL)/{refN2}.0 AS AVG_SALE_VAL_R2,SUM(SALE_GM)/{refN2}.0 AS AVG_SALE_GM_R2,
    SUM(LYSM_QTY)/{refN2}.0 AS AVG_LYSM_QTY_R2,SUM(LYSM_VAL)/{refN2}.0 AS AVG_LYSM_VAL_R2,SUM(LYSM_GM)/{refN2}.0 AS AVG_LYSM_GM_R2,
    SUM(LYSP_QTY)/{refN2}.0 AS AVG_LYSP_QTY_R2,SUM(LYSP_VAL)/{refN2}.0 AS AVG_LYSP_VAL_R2,SUM(LYSP_GM)/{refN2}.0 AS AVG_LYSP_GM_R2
  FROM vw_data WHERE WK_TYPE='R2'
  GROUP BY {rgdSql}
),
vw_plan AS (
  SELECT v.*,
    0 AS AVG_SALE_QTY,0 AS AVG_SALE_VAL,0 AS AVG_SALE_GM,
    0 AS AVG_LYSM_QTY,0 AS AVG_LYSM_VAL,0 AS AVG_LYSM_GM,
    0 AS AVG_LYSP_QTY,0 AS AVG_LYSP_VAL,0 AS AVG_LYSP_GM,
    NVL(ra2.AVG_SALE_QTY_R2,0) AS AVG_SALE_QTY_R2,NVL(ra2.AVG_SALE_VAL_R2,0) AS AVG_SALE_VAL_R2,NVL(ra2.AVG_SALE_GM_R2,0) AS AVG_SALE_GM_R2,
    NVL(ra2.AVG_LYSM_QTY_R2,0) AS AVG_LYSM_QTY_R2,NVL(ra2.AVG_LYSM_VAL_R2,0) AS AVG_LYSM_VAL_R2,NVL(ra2.AVG_LYSM_GM_R2,0) AS AVG_LYSM_GM_R2,
    NVL(ra2.AVG_LYSP_QTY_R2,0) AS AVG_LYSP_QTY_R2,NVL(ra2.AVG_LYSP_VAL_R2,0) AS AVG_LYSP_VAL_R2,NVL(ra2.AVG_LYSP_GM_R2,0) AS AVG_LYSP_GM_R2
    {refCols}
  FROM vw_data v
  LEFT JOIN ref_avg2 ra2 ON {joinCond2}
  {refJoins}
  WHERE v.WK_TYPE='B'
) ");
        }
        else
        {
            sb.Append($@",
vw_plan AS (
  SELECT v.*,
    0 AS AVG_SALE_QTY,0 AS AVG_SALE_VAL,0 AS AVG_SALE_GM,
    0 AS AVG_LYSM_QTY,0 AS AVG_LYSM_VAL,0 AS AVG_LYSM_GM,
    0 AS AVG_LYSP_QTY,0 AS AVG_LYSP_VAL,0 AS AVG_LYSP_GM
    {r2AvgZeros}
    {refCols}
  FROM vw_data v
  {refJoins}
) ");
        }

        return sb.ToString();
    }

    [HttpGet]
    public async Task<IActionResult> Index(
        string? fy, string? fyWeek, string? st, string? majCat,
        string? groupBy,
        string? refMode, string? baseWeek, string? refWeeks,
        string? refMode2, string? baseWeek2, string? refWeeks2,
        decimal ref1Pct = 50, decimal ref2Pct = 110, decimal ref3Cap = 50,
        decimal ref4Up = 35, decimal ref4Dn = 20, decimal ref5Up = 35, decimal ref5Dn = 20, int ref6Days = 120,
        string sortCol = "SALE_QTY", string sortDir = "DESC",
        int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // ── Static dropdowns ──
        ViewBag.FyList = await SnowflakeCrudHelper.DistinctAsync(conn, "STORE_CALENDAR", "FY");
        ViewBag.FyWeeks = await SnowflakeCrudHelper.DistinctAsync(conn, "STORE_CALENDAR", "FY_WEEK");
        ViewBag.AllMajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "DIM_PRODUCT", "MAJ_CAT");

        var allStores = new List<(string Code, string Name)>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT ST_CD,NVL(ST_NM,'') FROM STORE_PLANT_MASTER WHERE ST_CD IS NOT NULL ORDER BY ST_CD";
            cmd.CommandTimeout = 30;
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
                allStores.Add((r.GetString(0), r.GetString(1)));
        }
        ViewBag.AllStores = allStores;

        ViewBag.GroupableColumns = GroupableColumns;
        ViewBag.MetricDefs = MetricDefs;
        ViewBag.MetricGroups = MetricGroups;
        ViewBag.LatestDates = await GetLatestDatesAsync(conn);

        // ── Parse multi-value selections ──
        var selectedWeeks = ParseCsv(fyWeek);
        var selectedStores = ParseCsv(st);
        var selectedMajCats = ParseCsv(majCat);
        ViewBag.SelectedWeeks = new HashSet<string>(selectedWeeks, StringComparer.OrdinalIgnoreCase);
        ViewBag.SelectedStores = new HashSet<string>(selectedStores, StringComparer.OrdinalIgnoreCase);
        ViewBag.SelectedMajCats = new HashSet<string>(selectedMajCats, StringComparer.OrdinalIgnoreCase);

        var selected = ParseGroupBy(groupBy);
        if (selected.Count == 0) selected = new List<string> { "STORE_CODE", "MAJ_CAT" };
        ViewBag.GroupBy = string.Join(",", selected);
        ViewBag.SelectedGroups = selected;

        // ── Ref period ──
        var rm = refMode ?? "off";
        ViewBag.RefMode = rm;
        ViewBag.BaseWeek = baseWeek;
        ViewBag.RefWeeksParam = refWeeks;
        var selRefWeeks = rm == "custom" ? ParseCsv(refWeeks) : new List<string>();
        ViewBag.SelectedRefWeeks = new HashSet<string>(selRefWeeks, StringComparer.OrdinalIgnoreCase);

        // ── Resolve ref weeks (cross-FY) ──
        List<(string Fy, string Week)> resolvedRefWeeks = new();
        if (!string.IsNullOrEmpty(fy))
        {
            if ((rm == "last1" || rm == "last3") && !string.IsNullOrEmpty(baseWeek))
            {
                int n = rm == "last1" ? 1 : 3;
                resolvedRefWeeks = await ResolveLastNWeeksAsync(conn, fy, baseWeek, n);
            }
            else if (rm == "custom" && selRefWeeks.Count > 0)
            {
                resolvedRefWeeks = selRefWeeks.Select(w => (fy!, w)).ToList();
            }
        }
        // Display-friendly list for the view
        ViewBag.ResolvedRefWeeks = resolvedRefWeeks.Select(r =>
            r.Fy.Equals(fy, StringComparison.OrdinalIgnoreCase) ? r.Week : $"{r.Fy}:{r.Week}").ToList();
        bool hasRef = resolvedRefWeeks.Count > 0;
        ViewBag.HasRef = hasRef;

        // ── R2 Ref Period ──
        var rm2 = refMode2 ?? "off";
        ViewBag.RefMode2 = rm2;
        ViewBag.BaseWeek2 = baseWeek2;
        ViewBag.RefWeeksParam2 = refWeeks2;
        var selRefWeeks2 = rm2 == "custom" ? ParseCsv(refWeeks2) : new List<string>();
        ViewBag.SelectedRefWeeks2 = new HashSet<string>(selRefWeeks2, StringComparer.OrdinalIgnoreCase);

        List<(string Fy, string Week)> resolvedRefWeeks2 = new();
        if (!string.IsNullOrEmpty(fy))
        {
            if ((rm2 == "last1" || rm2 == "last3") && !string.IsNullOrEmpty(baseWeek2))
            {
                int n2 = rm2 == "last1" ? 1 : 3;
                resolvedRefWeeks2 = await ResolveLastNWeeksAsync(conn, fy, baseWeek2, n2);
            }
            else if (rm2 == "custom" && selRefWeeks2.Count > 0)
            {
                resolvedRefWeeks2 = selRefWeeks2.Select(w => (fy!, w)).ToList();
            }
        }
        ViewBag.ResolvedRefWeeks2 = resolvedRefWeeks2.Select(r =>
            r.Fy.Equals(fy, StringComparison.OrdinalIgnoreCase) ? r.Week : $"{r.Fy}:{r.Week}").ToList();
        bool hasRef2 = resolvedRefWeeks2.Count > 0;
        ViewBag.HasRef2 = hasRef2;

        // ── Active metrics (base only, or base + R1, or base + R1 + R2) ──
        var activeMetricColsList = BaseMetricCols.ToList();
        if (hasRef)
            activeMetricColsList.AddRange(AllMetricCols.Where(m => !BaseMetricCols.Contains(m) && !R2OnlyCols.Contains(m)));
        if (hasRef2)
            activeMetricColsList.AddRange(R2OnlyCols);
        var activeMetricCols = activeMetricColsList.ToArray();
        // Hide all WK_GROWTH variants from display; REF columns + FINAL_GROWTH_QTY are visible
        var hiddenFromDisplay = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            "WK_GROWTH_QTY", "WK_GROWTH_VAL", "WK_GROWTH_GM",
            "WK_GROWTH_QTY_R2", "WK_GROWTH_VAL_R2", "WK_GROWTH_GM_R2"
        };
        var displayMetrics = activeMetricCols.Where(m => !hiddenFromDisplay.Contains(m)).ToArray();
        ViewBag.DisplayCols = selected.Concat(displayMetrics).ToArray();
        ViewBag.TableHeaders = hasRef && hasRef2 ? R1R2TableHeaders : hasRef ? R1TableHeaders : BaseTableHeaders;

        // ── Persist filter params for links ──
        var rp = new RefParams(ref1Pct, ref2Pct, ref3Cap, ref4Up, ref4Dn, ref5Up, ref5Dn, ref6Days);
        ViewBag.R1 = ref1Pct; ViewBag.R2 = ref2Pct; ViewBag.R3 = ref3Cap;
        ViewBag.R4u = ref4Up; ViewBag.R4d = ref4Dn; ViewBag.R5u = ref5Up; ViewBag.R5d = ref5Dn; ViewBag.R6d = ref6Days;
        ViewBag.Fy = fy; ViewBag.FyWeek = fyWeek;
        ViewBag.St = st; ViewBag.MajCat = majCat;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = sortDir;
        ViewBag.Page = page; ViewBag.PageSize = pageSize;

        if (string.IsNullOrEmpty(fy) || selectedWeeks.Count == 0)
        {
            ViewBag.Rows = new List<Dictionary<string, object?>>();
            ViewBag.TotalCount = 0;
            return View();
        }

        // Non-time dims for ref_avg grouping (exclude FY_WEEK, WK_ST_DT, WK_END_DT, FY)
        var refGroupDims = selected
            .Where(c => !TimeColumns.Contains(c) && !c.Equals("FY", StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (refGroupDims.Count == 0) refGroupDims = new List<string> { "STORE_CODE" };

        var cte = BuildDataCte(fy, selectedWeeks, selectedStores,
            hasRef ? resolvedRefWeeks : null, (hasRef || hasRef2) ? refGroupDims : null,
            hasRef2 ? resolvedRefWeeks2 : null, ref6Days);

        var dw = "1=1";
        if (selectedMajCats.Count == 1)
            dw += $" AND MAJ_CAT='{Esc(selectedMajCats[0])}'";
        else if (selectedMajCats.Count > 1)
            dw += $" AND MAJ_CAT IN ({ToInClause(selectedMajCats)})";

        var groupBySql = string.Join(", ", selected);
        var selectDims = string.Join(", ", selected);
        // AVG columns use MAX (fixed per group, same for every week)
        var selectMetrics = string.Join(", ", activeMetricCols.Select(m => MetricSelectExpr(m, rp)));

        var validSort = new HashSet<string>(selected.Concat(activeMetricCols), StringComparer.OrdinalIgnoreCase);
        if (!validSort.Contains(sortCol)) sortCol = activeMetricCols[0];
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        // ── Query 1: Count + KPIs ──
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"{cte}SELECT
                (SELECT COUNT(*) FROM (SELECT 1 FROM vw_plan WHERE {dw} GROUP BY {groupBySql})),
                COUNT(DISTINCT STORE_CODE),COUNT(DISTINCT MAJ_CAT),
                NVL(SUM(SALE_QTY),0),NVL(SUM(SALE_VAL),0),NVL(SUM(SALE_GM),0),
                NVL(SUM(LYSP_QTY),0),NVL(SUM(LYSP_VAL),0),NVL(SUM(LYSP_GM),0)
                FROM vw_plan WHERE {dw}";
            cmd.CommandTimeout = 60;
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalCount = SnowflakeCrudHelper.Int(r, 0);
                ViewBag.TotalStores = SnowflakeCrudHelper.Int(r, 1);
                ViewBag.TotalCats = SnowflakeCrudHelper.Int(r, 2);
                ViewBag.TotalSaleQty = SnowflakeCrudHelper.Dec(r, 3);
                ViewBag.TotalSaleVal = SnowflakeCrudHelper.Dec(r, 4);
                ViewBag.TotalSaleGm = SnowflakeCrudHelper.Dec(r, 5);
                ViewBag.TotalLyspQty = SnowflakeCrudHelper.Dec(r, 6);
                ViewBag.TotalLyspVal = SnowflakeCrudHelper.Dec(r, 7);
                ViewBag.TotalLyspGm = SnowflakeCrudHelper.Dec(r, 8);
            }
        }

        // ── Query 2: Data (paginated) ──
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"{cte}SELECT {selectDims},{selectMetrics}
                FROM vw_plan WHERE {dw}
                GROUP BY {groupBySql}
                ORDER BY {sortCol} {dir}
                LIMIT {pageSize} OFFSET {offset}";
            cmd.CommandTimeout = 60;
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++)
                    row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
                rows.Add(row);
            }
        }
        ViewBag.Rows = rows;
        return View();
    }

    [HttpGet]
    public async Task ExportCsv(string fy, string? fyWeek, string? st, string? majCat, string? groupBy,
        string? refMode, string? baseWeek, string? refWeeks,
        string? refMode2, string? baseWeek2, string? refWeeks2,
        decimal ref1Pct = 50, decimal ref2Pct = 110, decimal ref3Cap = 50,
        decimal ref4Up = 35, decimal ref4Dn = 20, decimal ref5Up = 35, decimal ref5Dn = 20, int ref6Days = 120)
    {
        var selected = ParseGroupBy(groupBy);
        if (selected.Count == 0) selected = new List<string> { "STORE_CODE", "MAJ_CAT" };

        var selectedWeeks = ParseCsv(fyWeek);
        var selectedStores = ParseCsv(st);
        var selectedMajCats = ParseCsv(majCat);
        if (selectedWeeks.Count == 0) return;

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // Resolve R1 ref (cross-FY)
        var rm = refMode ?? "off";
        List<(string Fy, string Week)> resolvedRef = new();
        if ((rm == "last1" || rm == "last3") && !string.IsNullOrEmpty(baseWeek))
            resolvedRef = await ResolveLastNWeeksAsync(conn, fy, baseWeek, rm == "last1" ? 1 : 3);
        else if (rm == "custom")
            resolvedRef = ParseCsv(refWeeks).Select(w => (fy, w)).ToList();
        bool hasRef = resolvedRef.Count > 0;

        // Resolve R2 ref (cross-FY)
        var rm2 = refMode2 ?? "off";
        List<(string Fy, string Week)> resolvedRef2 = new();
        if ((rm2 == "last1" || rm2 == "last3") && !string.IsNullOrEmpty(baseWeek2))
            resolvedRef2 = await ResolveLastNWeeksAsync(conn, fy, baseWeek2, rm2 == "last1" ? 1 : 3);
        else if (rm2 == "custom")
            resolvedRef2 = ParseCsv(refWeeks2).Select(w => (fy, w)).ToList();
        bool hasRef2 = resolvedRef2.Count > 0;

        var activeMetricColsList = BaseMetricCols.ToList();
        if (hasRef)
            activeMetricColsList.AddRange(AllMetricCols.Where(m => !BaseMetricCols.Contains(m) && !R2OnlyCols.Contains(m)));
        if (hasRef2)
            activeMetricColsList.AddRange(R2OnlyCols);
        var activeMetricCols = activeMetricColsList.ToArray();

        var refGroupDims = selected
            .Where(c => !TimeColumns.Contains(c) && !c.Equals("FY", StringComparison.OrdinalIgnoreCase))
            .ToList();
        if (refGroupDims.Count == 0) refGroupDims = new List<string> { "STORE_CODE" };

        var cte = BuildDataCte(fy, selectedWeeks, selectedStores,
            hasRef ? resolvedRef : null, (hasRef || hasRef2) ? refGroupDims : null,
            hasRef2 ? resolvedRef2 : null);
        var dw = "1=1";
        if (selectedMajCats.Count == 1)
            dw += $" AND MAJ_CAT='{Esc(selectedMajCats[0])}'";
        else if (selectedMajCats.Count > 1)
            dw += $" AND MAJ_CAT IN ({ToInClause(selectedMajCats)})";

        var groupBySql = string.Join(", ", selected);
        var selectDims = string.Join(", ", selected);
        var rp = new RefParams(ref1Pct, ref2Pct, ref3Cap, ref4Up, ref4Dn, ref5Up, ref5Dn, ref6Days);
        var selectMetrics = string.Join(", ", activeMetricCols.Select(m => MetricSelectExpr(m, rp)));

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SalePlan_{fy}.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);

        await SnowflakeCrudHelper.StreamCsvAsync(conn,
            $"{cte}SELECT {selectDims},{selectMetrics} FROM vw_plan WHERE {dw} GROUP BY {groupBySql} ORDER BY {selected[0]}",
            writer,
            string.Join(",", selected.Concat(activeMetricCols)),
            timeout: 300);
    }

    // ── Pivot CSV Export ──
    [HttpGet]
    public async Task ExportPivotCsv(string fy, string? fyWeek, string? st, string? majCat,
        string? groupBy, string? metricGroups,
        string? refMode, string? baseWeek, string? refWeeks,
        string? refMode2, string? baseWeek2, string? refWeeks2,
        decimal ref1Pct = 50, decimal ref2Pct = 110, decimal ref3Cap = 50,
        decimal ref4Up = 35, decimal ref4Dn = 20, decimal ref5Up = 35, decimal ref5Dn = 20, int ref6Days = 120)
    {
        var selected = ParseGroupBy(groupBy);
        if (selected.Count == 0) selected = new List<string> { "STORE_CODE", "MAJ_CAT" };

        var selectedWeeks = ParseCsv(fyWeek);
        var selectedStores = ParseCsv(st);
        var selectedMajCats = ParseCsv(majCat);
        if (selectedWeeks.Count == 0) return;

        var selGroups = ParseMetricGroupKeys(metricGroups);
        var pivotMetrics = MetricGroups
            .Where(g => selGroups.Contains(g.Key, StringComparer.OrdinalIgnoreCase))
            .SelectMany(g => g.Metrics).ToArray();
        if (pivotMetrics.Length == 0) return;

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // Resolve R1 ref (cross-FY)
        var rm = refMode ?? "off";
        List<(string Fy, string Week)> resolvedRef = new();
        if ((rm == "last1" || rm == "last3") && !string.IsNullOrEmpty(baseWeek))
            resolvedRef = await ResolveLastNWeeksAsync(conn, fy, baseWeek, rm == "last1" ? 1 : 3);
        else if (rm == "custom")
            resolvedRef = ParseCsv(refWeeks).Select(w => (fy, w)).ToList();
        bool hasRef = resolvedRef.Count > 0;

        // Resolve R2 ref (cross-FY)
        var rm2 = refMode2 ?? "off";
        List<(string Fy, string Week)> resolvedRef2 = new();
        if ((rm2 == "last1" || rm2 == "last3") && !string.IsNullOrEmpty(baseWeek2))
            resolvedRef2 = await ResolveLastNWeeksAsync(conn, fy, baseWeek2, rm2 == "last1" ? 1 : 3);
        else if (rm2 == "custom")
            resolvedRef2 = ParseCsv(refWeeks2).Select(w => (fy, w)).ToList();
        bool hasRef2 = resolvedRef2.Count > 0;

        // If pivot includes ref avg metrics but ref is off, filter them out
        if (!hasRef)
            pivotMetrics = pivotMetrics.Where(m => !m.StartsWith("AVG_") || m.EndsWith("_R2")).ToArray();
        if (!hasRef2)
            pivotMetrics = pivotMetrics.Where(m => !R2OnlyCols.Contains(m)).ToArray();
        if (pivotMetrics.Length == 0) return;

        // Split metrics: weekly (pivoted per week) vs flat (AVG — appended once at end)
        var weeklyMetrics = pivotMetrics.Where(m => !m.StartsWith("AVG_")).ToArray();
        var flatMetrics = pivotMetrics.Where(m => m.StartsWith("AVG_")).ToArray();
        // Need at least weekly or flat metrics
        if (weeklyMetrics.Length == 0 && flatMetrics.Length == 0) return;
        // Combine all for the SQL SELECT
        var allQueryMetrics = weeklyMetrics.Concat(flatMetrics).ToArray();

        var rowDims = selected.Where(c => !TimeColumns.Contains(c)).ToList();
        if (rowDims.Count == 0) rowDims = new List<string> { "STORE_CODE" };

        var refGroupDims = rowDims.Where(c => !c.Equals("FY", StringComparison.OrdinalIgnoreCase)).ToList();
        if (refGroupDims.Count == 0) refGroupDims = new List<string> { "STORE_CODE" };

        var cte = BuildDataCte(fy, selectedWeeks, selectedStores,
            hasRef ? resolvedRef : null, (hasRef || hasRef2) ? refGroupDims : null,
            hasRef2 ? resolvedRef2 : null);
        var dw = "1=1";
        if (selectedMajCats.Count == 1) dw += $" AND MAJ_CAT='{Esc(selectedMajCats[0])}'";
        else if (selectedMajCats.Count > 1) dw += $" AND MAJ_CAT IN ({ToInClause(selectedMajCats)})";

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SalePlan_Pivot_{fy}.csv");

        // Step 1: sorted distinct weeks
        var weeks = new List<string>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"{cte}SELECT DISTINCT FY_WEEK FROM vw_plan WHERE {dw} ORDER BY TO_NUMBER(REGEXP_SUBSTR(FY_WEEK,'\\\\d+'))";
            cmd.CommandTimeout = 120;
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) weeks.Add(r.GetValue(0)?.ToString() ?? "");
        }
        if (weeks.Count == 0) return;

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536, leaveOpen: true);

        // Step 2: Header — weekly metrics pivoted, then flat AVG columns once
        var hdr = new StringBuilder();
        hdr.Append(string.Join(",", rowDims));
        foreach (var w in weeks)
            foreach (var m in weeklyMetrics)
                hdr.Append($",{w}_{MetricLabel(m)}");
        foreach (var m in flatMetrics)
            hdr.Append($",{MetricLabel(m)}");
        await writer.WriteLineAsync(hdr.ToString());

        // Step 3: Data query
        var groupBySql = string.Join(",", rowDims.Append("FY_WEEK"));
        var selectDims = string.Join(",", rowDims) + ",FY_WEEK";
        var rp = new RefParams(ref1Pct, ref2Pct, ref3Cap, ref4Up, ref4Dn, ref5Up, ref5Dn, ref6Days);
        var selectMet = string.Join(",", allQueryMetrics.Select(m => MetricSelectExpr(m, rp)));

        await using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = $"{cte}SELECT {selectDims},{selectMet} FROM vw_plan WHERE {dw} GROUP BY {groupBySql} ORDER BY {string.Join(",", rowDims)},TO_NUMBER(REGEXP_SUBSTR(FY_WEEK,'\\\\d+'))";
        cmd2.CommandTimeout = 600;
        await using var reader = await cmd2.ExecuteReaderAsync();

        string? prevKey = null;
        string[]? idVals = null;
        var weekData = new Dictionary<string, decimal[]>(); // weekly metric values per week
        decimal[] flatVals = new decimal[flatMetrics.Length]; // AVG values (same across weeks, take from first row)
        int cnt = 0;

        while (await reader.ReadAsync())
        {
            var keyParts = new string[rowDims.Count];
            for (int i = 0; i < rowDims.Count; i++)
                keyParts[i] = reader[rowDims[i]]?.ToString() ?? "";
            var key = string.Join("|", keyParts);
            var fw = reader["FY_WEEK"]?.ToString() ?? "";

            if (key != prevKey)
            {
                if (prevKey != null && idVals != null)
                {
                    WritePivotRow(writer, idVals, weeks, weekData, weeklyMetrics, flatVals);
                    if (++cnt % 5000 == 0) await writer.FlushAsync();
                }
                idVals = keyParts;
                weekData.Clear();
                // Read flat AVG values from the first week row of this group
                for (int i = 0; i < flatMetrics.Length; i++)
                {
                    var v = reader[flatMetrics[i]];
                    flatVals[i] = v == null || v == DBNull.Value ? 0m : Convert.ToDecimal(v);
                }
                prevKey = key;
            }
            // Read weekly metric values
            var vals = new decimal[weeklyMetrics.Length];
            for (int i = 0; i < weeklyMetrics.Length; i++)
            {
                var v = reader[weeklyMetrics[i]];
                vals[i] = v == null || v == DBNull.Value ? 0m : Convert.ToDecimal(v);
            }
            weekData[fw] = vals;
        }
        if (prevKey != null && idVals != null)
            WritePivotRow(writer, idVals, weeks, weekData, weeklyMetrics, flatVals);
        await writer.FlushAsync();
    }

    private static void WritePivotRow(StreamWriter w, string[] ids, List<string> weeks,
        Dictionary<string, decimal[]> data, string[] weeklyMetrics, decimal[] flatVals)
    {
        var sb = new StringBuilder(2048);
        sb.Append(string.Join(",", ids.Select(Qcsv)));
        // Weekly metrics pivoted per week
        foreach (var wk in weeks)
        {
            var has = data.TryGetValue(wk, out var v);
            for (int i = 0; i < weeklyMetrics.Length; i++)
                sb.Append(',').Append(has ? v![i] : 0);
        }
        // Flat AVG columns once at the end
        for (int i = 0; i < flatVals.Length; i++)
            sb.Append(',').Append(flatVals[i]);
        w.WriteLine(sb.ToString());
    }

    private static string Qcsv(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return s.Contains(',') || s.Contains('"') || s.Contains('\n')
            ? "\"" + s.Replace("\"", "\"\"") + "\"" : s;
    }

    private static string MetricLabel(string alias)
        => MetricDefs.FirstOrDefault(m => m.Alias == alias).Label ?? alias;

    private static List<string> ParseMetricGroupKeys(string? csv)
    {
        if (string.IsNullOrWhiteSpace(csv))
            return MetricGroups.Select(g => g.Key).ToList();
        return csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(k => MetricGroups.Any(g => g.Key.Equals(k, StringComparison.OrdinalIgnoreCase)))
            .Distinct(StringComparer.OrdinalIgnoreCase).ToList();
    }

    private static List<string> ParseGroupBy(string? groupBy)
    {
        if (string.IsNullOrWhiteSpace(groupBy)) return new();
        return groupBy.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                      .Where(g => ValidGroupCols.Contains(g))
                      .Distinct(StringComparer.OrdinalIgnoreCase)
                      .ToList();
    }

    private static string Esc(string s) => s.Replace("'", "''");

    // Ref params: all configurable from UI inputs
    private record RefParams(decimal Ref1Pct = 50, decimal Ref2Pct = 110, decimal Ref3Cap = 50,
        decimal Ref4Up = 35, decimal Ref4Dn = 20, decimal Ref5Up = 35, decimal Ref5Dn = 20, int Ref6Days = 120);

    // Growth % = (week LYSP / Avg LYSP - 1) * 100  (Excel: =WK_LYSP/AVG_LYSP-1)
    private static string MetricSelectExpr(string m, RefParams rp) => m switch
    {
        "WK_GROWTH_QTY" => "CASE WHEN MAX(NVL(AVG_LYSP_QTY,0))>0 THEN ROUND((SUM(NVL(LYSP_QTY,0))/MAX(NVL(AVG_LYSP_QTY,0))-1)*100,2) ELSE 0 END AS WK_GROWTH_QTY",
        "WK_GROWTH_VAL" => "CASE WHEN MAX(NVL(AVG_LYSP_VAL,0))>0 THEN ROUND((SUM(NVL(LYSP_VAL,0))/MAX(NVL(AVG_LYSP_VAL,0))-1)*100,2) ELSE 0 END AS WK_GROWTH_VAL",
        "WK_GROWTH_GM"  => "CASE WHEN MAX(NVL(AVG_LYSP_GM,0))>0 THEN ROUND((SUM(NVL(LYSP_GM,0))/MAX(NVL(AVG_LYSP_GM,0))-1)*100,2) ELSE 0 END AS WK_GROWTH_GM",
        // Final Growth = AVG_LYSP * (1 + WK_GROWTH/100)
        "FINAL_GROWTH_QTY" => "CASE WHEN MAX(NVL(AVG_LYSP_QTY,0))>0 THEN ROUND(MAX(NVL(AVG_LYSP_QTY,0))*(1+(SUM(NVL(LYSP_QTY,0))/MAX(NVL(AVG_LYSP_QTY,0))-1)),2) ELSE 0 END AS FINAL_GROWTH_QTY",
        // REF-1: Min RXL Growth — placeholder (RXL data not available yet)
        "REF1_QTY" => "0 AS REF1_QTY",
        // REF-2: Min LYSP Qty = LYSP * growth_multiplier
        "REF2_QTY" => $"ROUND(SUM(NVL(LYSP_QTY,0))*{rp.Ref2Pct}/100.0,2) AS REF2_QTY",
        // REF-3: L3M Growth Adj = LYSP * (1 + capped(BW1 GR%))  where GR% = AVG_SALE_VAL / LYSP_VAL - 1
        "REF3_QTY" => $"CASE WHEN SUM(NVL(LYSP_VAL,0))>0 THEN ROUND(SUM(NVL(LYSP_QTY,0))*(1+LEAST({rp.Ref3Cap}/100.0,GREATEST(-{rp.Ref3Cap}/100.0,MAX(NVL(AVG_SALE_VAL,0))/SUM(NVL(LYSP_VAL,0))-1))),2) ELSE 0 END AS REF3_QTY",
        // REF-4: Old Store L5M = BASE_L5M * (1 + capped(BW2 GR%))  where GR% = AVG_SALE_VAL_R2 / LYSP_VAL - 1
        "REF4_QTY" => $"ROUND(MAX(NVL(BASE_L5M_QTY,0))*(1+LEAST({rp.Ref4Up}/100.0,GREATEST(-{rp.Ref4Dn}/100.0,CASE WHEN SUM(NVL(LYSP_VAL,0))>0 THEN MAX(NVL(AVG_SALE_VAL_R2,0))/SUM(NVL(LYSP_VAL,0))-1 ELSE 0 END))),2) AS REF4_QTY",
        // REF-5: Old Store L3M = BASE_L3M * (1 + capped(BW1 GR%))  where GR% = AVG_SALE_VAL / LYSP_VAL - 1
        "REF5_QTY" => $"ROUND(MAX(NVL(BASE_L3M_QTY,0))*(1+LEAST({rp.Ref5Up}/100.0,GREATEST(-{rp.Ref5Dn}/100.0,CASE WHEN SUM(NVL(LYSP_VAL,0))>0 THEN MAX(NVL(AVG_SALE_VAL,0))/SUM(NVL(LYSP_VAL,0))-1 ELSE 0 END))),2) AS REF5_QTY",
        // REF-6: STR Protection = N-day sales
        "REF6_QTY" => "MAX(NVL(STR_NDAY_QTY,0)) AS REF6_QTY",
        // R2 AVG columns handled by the wildcard below (StartsWith("AVG_"))
        _ when m.StartsWith("STK_") || m.StartsWith("AVG_") => $"MAX(NVL({m},0)) AS {m}",
        _ => $"SUM(NVL({m},0)) AS {m}",
    };

    private static async Task<Dictionary<string, string>> GetLatestDatesAsync(SnowflakeDbConnection conn)
    {
        var dates = new Dictionary<string, string>();
        var queries = new (string Key, string Sql)[]
        {
            ("ET_SALES_DATA",   "SELECT MAX(SALES_DATE)::DATE FROM ET_SALES_DATA"),
            ("ET_STOCK_DATA",   "SELECT MAX(STOCK_DATE)::DATE FROM ET_STOCK_DATA"),
            ("STORE_CALENDAR",  "SELECT MAX(BGT_MNTH_DATE)::DATE FROM STORE_CALENDAR"),
            ("DIM_PRODUCT",     "SELECT COUNT(*) FROM DIM_PRODUCT"),
        };
        foreach (var (key, sql) in queries)
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.CommandTimeout = 30;
                var val = await cmd.ExecuteScalarAsync();
                dates[key] = val == null || val == DBNull.Value ? "N/A" : val.ToString()!;
            }
            catch { dates[key] = "Error"; }
        }
        return dates;
    }
}
