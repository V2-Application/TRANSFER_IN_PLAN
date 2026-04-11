using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class PlanDashboardController : Controller
{
    private readonly ILogger<PlanDashboardController> _logger;
    private readonly string _connStr;

    public PlanDashboardController(ILogger<PlanDashboardController> logger, IConfiguration config)
    {
        _logger = logger;
        _connStr = config.GetConnectionString("PlanningDatabase")!;
    }

    public async Task<IActionResult> Index()
    {
        var d = new PipelineDashboard();

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();

        // ── Plan 1: Sale Budget ──
        await LoadPlanStatus(conn, d.Plan1, "SALE_BUDGET_PLAN",
            "SELECT COUNT(1), ISNULL(SUM(BGT_SALE_VAL),0), ISNULL(SUM(BGT_SALE_QTY),0), COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT MAJOR_CATEGORY), COUNT(DISTINCT PLAN_MONTH), MAX(RUN_ID), MAX(CREATED_DT), ISNULL(SUM(LYSP_SALE_VAL),0), ISNULL(SUM(BGT_GM_VAL),0) FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK)",
            r => { d.Plan1.Val1 = r.GetDecimal(1); d.Plan1.Qty1 = r.GetDecimal(2); d.Plan1.Stores = r.GetInt32(3); d.Plan1.Categories = r.GetInt32(4); d.Plan1.Months = r.GetInt32(5); d.Plan1.RunId = r.IsDBNull(6) ? null : r.GetString(6); d.Plan1.LastRun = r.IsDBNull(7) ? null : r.GetDateTime(7); d.Plan1.LyspVal = r.GetDecimal(8); d.Plan1.GmVal = r.GetDecimal(9); });

        // ── Plan 2: Fixture & Density ──
        await LoadPlanStatus(conn, d.Plan2, "FIXTURE_DENSITY_PLAN",
            "SELECT COUNT(1), ISNULL(SUM(BGT_DISP_VAL),0), ISNULL(SUM(BGT_DISP_QTY),0), COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT MAJOR_CATEGORY), COUNT(DISTINCT PLAN_MONTH), MAX(RUN_ID), MAX(CREATED_DT), ISNULL(AVG(ACC_DENSITY),0), ISNULL(SUM(FIX_COUNT),0) FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK)",
            r => { d.Plan2.Val1 = r.GetDecimal(1); d.Plan2.Qty1 = r.GetDecimal(2); d.Plan2.Stores = r.GetInt32(3); d.Plan2.Categories = r.GetInt32(4); d.Plan2.Months = r.GetInt32(5); d.Plan2.RunId = r.IsDBNull(6) ? null : r.GetString(6); d.Plan2.LastRun = r.IsDBNull(7) ? null : r.GetDateTime(7); d.Plan2.LyspVal = r.GetDecimal(8); d.Plan2.GmVal = r.GetDecimal(9); });

        // ── Plan 3: Weekly Disagg ──
        d.Plan3.Rows = await CountAsync(conn, "QTY_SALE_QTY");
        d.Plan3.Qty1 = await CountAsync(conn, "QTY_DISP_QTY");
        d.Plan3.Categories = await CountAsync(conn, "WEEK_CALENDAR");
        try
        {
            await using var cmd3 = conn.CreateCommand();
            cmd3.CommandText = "SELECT TOP 1 RUN_ID, CREATED_DT FROM dbo.WEEKLY_DISAGG_LOG WITH (NOLOCK) ORDER BY CREATED_DT DESC";
            await using var r3 = await cmd3.ExecuteReaderAsync();
            if (await r3.ReadAsync()) { d.Plan3.RunId = r3.IsDBNull(0) ? null : r3.GetString(0); d.Plan3.LastRun = r3.IsDBNull(1) ? null : r3.GetDateTime(1); }
        }
        catch { }

        // ── Plan 4: TRF ──
        await LoadPlanStatus(conn, d.Plan4, "TRF_IN_PLAN",
            "SELECT COUNT(1), ISNULL(SUM(TRF_IN_STK_Q),0), 0, COUNT(DISTINCT ST_CD), COUNT(DISTINCT MAJ_CAT), 0, '', MAX(CREATED_DT), ISNULL(SUM(ST_CL_SHORT_Q),0), ISNULL(SUM(ST_CL_EXCESS_Q),0) FROM dbo.TRF_IN_PLAN WITH (NOLOCK)",
            r => { d.Plan4.Val1 = r.GetDecimal(1); d.Plan4.Stores = r.GetInt32(3); d.Plan4.Categories = r.GetInt32(4); d.Plan4.LastRun = r.IsDBNull(7) ? null : r.GetDateTime(7); d.Plan4.LyspVal = r.GetDecimal(8); d.Plan4.GmVal = r.GetDecimal(9); });

        // ── Plan 5: PP ──
        await LoadPlanStatus(conn, d.Plan5, "PURCHASE_PLAN",
            "SELECT COUNT(1), ISNULL(SUM(BGT_PUR_Q_INIT),0), 0, COUNT(DISTINCT RDC_CD), COUNT(DISTINCT MAJ_CAT), 0, '', MAX(CREATED_DT), ISNULL(SUM(DC_STK_SHORT_Q),0), ISNULL(SUM(DC_STK_EXCESS_Q),0) FROM dbo.PURCHASE_PLAN WITH (NOLOCK)",
            r => { d.Plan5.Val1 = r.GetDecimal(1); d.Plan5.Stores = r.GetInt32(3); d.Plan5.Categories = r.GetInt32(4); d.Plan5.LastRun = r.IsDBNull(7) ? null : r.GetDateTime(7); d.Plan5.LyspVal = r.GetDecimal(8); d.Plan5.GmVal = r.GetDecimal(9); });

        // ── Plan 6: Sub TRF ──
        d.Plan6.Rows = await CountAsync(conn, "SUB_LEVEL_TRF_PLAN");
        d.Plan6.LastRun = await MaxDateAsync(conn, "SUB_LEVEL_TRF_PLAN", "CREATED_DT");

        // ── Plan 7: Sub PP ──
        d.Plan7.Rows = await CountAsync(conn, "SUB_LEVEL_PP_PLAN");
        d.Plan7.LastRun = await MaxDateAsync(conn, "SUB_LEVEL_PP_PLAN", "CREATED_DT");

        // ── Staging status ──
        d.StagingSaleActual = await CountAsync(conn, "STG_SF_SALE_ACTUAL");
        d.StagingForecasts = await CountAsync(conn, "STG_SF_DEMAND_FORECAST");
        d.StagingContPct = await CountAsync(conn, "STG_SF_CONT_PCT");
        d.StagingDimStore = await CountAsync(conn, "STG_SF_DIM_STORE");
        d.StagingDimArticle = await CountAsync(conn, "STG_SF_DIM_ARTICLE");

        // ── Division breakdown for chart (from Plan 1) ──
        try
        {
            await using var cmdDiv = conn.CreateCommand();
            cmdDiv.CommandText = "SELECT TOP 10 ISNULL(DIVISION,'Other'), SUM(BGT_SALE_VAL), SUM(LYSP_SALE_VAL) FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) GROUP BY DIVISION ORDER BY SUM(BGT_SALE_VAL) DESC";
            await using var rDiv = await cmdDiv.ExecuteReaderAsync();
            while (await rDiv.ReadAsync())
                d.DivisionData.Add(new DivRow { Name = rDiv.GetString(0), BgtVal = rDiv.GetDecimal(1), LyspVal = rDiv.GetDecimal(2) });
        }
        catch { }

        return View(d);
    }

    private async Task LoadPlanStatus(SqlConnection conn, PlanStatus ps, string table, string sql, Action<SqlDataReader> map)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 30;
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync()) { ps.Rows = r.GetInt32(0); map(r); }
        }
        catch { }
    }

    private async Task<int> CountAsync(SqlConnection conn, string table)
    {
        try { await using var cmd = conn.CreateCommand(); cmd.CommandText = $"SELECT COUNT(1) FROM dbo.[{table}] WITH (NOLOCK)"; return (int)(await cmd.ExecuteScalarAsync() ?? 0); }
        catch { return 0; }
    }

    private async Task<DateTime?> MaxDateAsync(SqlConnection conn, string table, string col)
    {
        try { await using var cmd = conn.CreateCommand(); cmd.CommandText = $"SELECT MAX([{col}]) FROM dbo.[{table}] WITH (NOLOCK)"; var v = await cmd.ExecuteScalarAsync(); return v is DateTime dt ? dt : null; }
        catch { return null; }
    }
}
