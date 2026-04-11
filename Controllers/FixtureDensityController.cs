using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class FixtureDensityController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly FixtureDensityJobService _jobService;
    private readonly ILogger<FixtureDensityController> _logger;
    private readonly string _connStr;

    public FixtureDensityController(PlanningDbContext context, FixtureDensityJobService jobService, ILogger<FixtureDensityController> logger, IConfiguration config)
    {
        _context = context; _jobService = jobService; _logger = logger;
        _connStr = config.GetConnectionString("PlanningDatabase")!;
    }

    public async Task<IActionResult> Execute()
    {
        ViewBag.FixtureRows = await CountAsync("FIXTURE_DENSITY_PLAN");
        ViewBag.BudgetRows = await CountAsync("SALE_BUDGET_PLAN");
        ViewBag.StagingReady = await CountAsync("STG_SF_DIM_STORE") > 0;
        ViewBag.LatestRun = await ScalarAsync("SELECT TOP 1 RUN_ID FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) ORDER BY CREATED_DT DESC");
        ViewBag.AvailableMonths = await ListAsync("SELECT DISTINCT FORMAT(PLAN_MONTH, 'yyyy-MM') FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) ORDER BY 1");
        return View();
    }

    [HttpPost]
    public IActionResult StartRun(string[] targetMonths, string algoMethod)
    {
        if (targetMonths == null || targetMonths.Length == 0) return Json(new { success = false, message = "Select target months." });
        if (_jobService.IsRunning) return Json(new { success = false, message = "Already running." });
        var started = _jobService.TryStartRun(targetMonths.ToList(), algoMethod ?? "STANDARD");
        return Json(new { success = started });
    }

    [HttpGet] public IActionResult JobStatus() => Json(_jobService.GetStatus());

    public async Task<IActionResult> Output(string? month, string? division, string? store, int page = 1, int pageSize = 100)
    {
        var vm = new FixtureDensityOutputViewModel { CurrentPage = page, PageSize = pageSize };
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(month)) { where.Append(" AND PLAN_MONTH = @m"); parms.Add(new("@m", month + "-01")); }
        if (!string.IsNullOrEmpty(division)) { where.Append(" AND DIVISION = @d"); parms.Add(new("@d", division)); }
        if (!string.IsNullOrEmpty(store)) { where.Append(" AND STORE_CODE = @s"); parms.Add(new("@s", store)); }

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1), ISNULL(SUM(BGT_DISP_QTY),0), ISNULL(SUM(BGT_DISP_VAL),0), ISNULL(AVG(ACC_DENSITY),0), COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT MAJOR_CATEGORY), MAX(RUN_ID) FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            { vm.TotalRows = rdr.GetInt32(0); vm.TotalDispQty = rdr.GetDecimal(1); vm.TotalDispVal = rdr.GetDecimal(2); vm.AvgDensity = rdr.GetDecimal(3); vm.StoreCount = rdr.GetInt32(4); vm.CategoryCount = rdr.GetInt32(5); vm.LatestRunId = rdr.IsDBNull(6) ? null : rdr.GetString(6); }
        }

        int offset = (page - 1) * pageSize;
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT ID, RUN_ID, STORE_CODE, STORE_NAME, STATE, ZONE, REGION, STORE_SIZE_SQFT, SIZE_CATEGORY,
                MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT, PLAN_MONTH,
                BGT_DISP_QTY, BGT_DISP_VAL, ACC_DENSITY, FIX_COUNT, AREA_SQFT,
                SALE_BGT_VAL, CL_STK_QTY, GP_PSF, SALES_PSF, ALGO_METHOD
                FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) {where}
                ORDER BY BGT_DISP_VAL DESC OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
            parms.ForEach(p => cmd.Parameters.Add(new SqlParameter(p.ParameterName, p.Value)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
                vm.Rows.Add(new FixtureDensityPlan
                {
                    Id = rdr.GetInt32(0), RunId = S(rdr, 1), StoreCode = S(rdr, 2), StoreName = S(rdr, 3),
                    State = S(rdr, 4), Zone = S(rdr, 5), Region = S(rdr, 6),
                    StoreSizeSqft = D(rdr, 7), SizeCategory = S(rdr, 8),
                    MajorCategory = S(rdr, 9), Division = S(rdr, 10), Subdivision = S(rdr, 11), Segment = S(rdr, 12),
                    PlanMonth = rdr.IsDBNull(13) ? null : rdr.GetDateTime(13),
                    BgtDispQty = D(rdr, 14), BgtDispVal = D(rdr, 15), AccDensity = D(rdr, 16),
                    FixCount = D(rdr, 17), AreaSqft = D(rdr, 18),
                    SaleBgtVal = D(rdr, 19), ClStkQty = D(rdr, 20),
                    GpPsf = D(rdr, 21), SalesPsf = D(rdr, 22), AlgoMethod = S(rdr, 23)
                });
        }

        ViewBag.Months = await ListAsync("SELECT DISTINCT FORMAT(PLAN_MONTH, 'yyyy-MM') FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) ORDER BY 1");
        ViewBag.Divisions = await ListAsync("SELECT DISTINCT DIVISION FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) WHERE DIVISION IS NOT NULL ORDER BY 1");
        ViewBag.SelMonth = month; ViewBag.SelDivision = division; ViewBag.SelStore = store;
        return View(vm);
    }

    [HttpPost]
    public async Task<IActionResult> Reset()
    {
        await using var conn = new SqlConnection(_connStr); await conn.OpenAsync();
        await using var cmd = conn.CreateCommand(); cmd.CommandText = "TRUNCATE TABLE dbo.FIXTURE_DENSITY_PLAN"; await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Fixture & Density Plan reset."; return RedirectToAction("Execute");
    }

    private async Task<int> CountAsync(string t) { try { await using var c = new SqlConnection(_connStr); await c.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT COUNT(1) FROM dbo.[{t}] WITH (NOLOCK)"; return (int)(await cmd.ExecuteScalarAsync() ?? 0); } catch { return 0; } }
    private async Task<string?> ScalarAsync(string sql) { try { await using var c = new SqlConnection(_connStr); await c.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = sql; return (await cmd.ExecuteScalarAsync())?.ToString(); } catch { return null; } }
    private async Task<List<string>> ListAsync(string sql) { var l = new List<string>(); try { await using var c = new SqlConnection(_connStr); await c.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = sql; await using var r = await cmd.ExecuteReaderAsync(); while (await r.ReadAsync()) l.Add(r.GetString(0)); } catch { } return l; }
    private static string? S(SqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);
    private static decimal? D(SqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetDecimal(i);
}
