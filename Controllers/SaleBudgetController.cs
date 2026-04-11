using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class SaleBudgetController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly SnowflakeService _sfService;
    private readonly SaleBudgetJobService _jobService;
    private readonly ILogger<SaleBudgetController> _logger;
    private readonly string _connStr;

    public SaleBudgetController(
        PlanningDbContext context,
        SnowflakeService sfService,
        SaleBudgetJobService jobService,
        ILogger<SaleBudgetController> logger,
        IConfiguration config)
    {
        _context = context;
        _sfService = sfService;
        _jobService = jobService;
        _logger = logger;
        _connStr = config.GetConnectionString("PlanningDatabase")!;
    }

    // ────────────────────────────────────────────────────────
    //  EXECUTE PAGE
    // ────────────────────────────────────────────────────────
    public async Task<IActionResult> Execute()
    {
        ViewBag.StagingStatus = await _sfService.GetStagingStatusAsync();
        ViewBag.BudgetRows = await GetRowCountAsync("SALE_BUDGET_PLAN");
        ViewBag.LatestRun = await GetLatestRunAsync();
        return View();
    }

    // ────────────────────────────────────────────────────────
    //  FETCH SNOWFLAKE DATA (AJAX)
    // ────────────────────────────────────────────────────────
    [HttpPost]
    public async Task<IActionResult> FetchSnowflakeData()
    {
        try
        {
            var r1 = await _sfService.FetchDimStoreAsync();
            var r2 = await _sfService.FetchDimArticleAsync();
            var r3 = await _sfService.FetchSaleActualsAsync();
            var r4 = await _sfService.FetchDemandForecastsAsync();
            var r5 = await _sfService.FetchContributionPctsAsync();
            return Json(new
            {
                success = true,
                message = $"Fetched: {r1} stores, {r2} categories, {r3:N0} sale actuals, {r4:N0} forecasts, {r5:N0} contribution rows"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FetchSnowflakeData failed");
            return Json(new { success = false, message = ex.InnerException?.Message ?? ex.Message });
        }
    }

    // ────────────────────────────────────────────────────────
    //  START BUDGET RUN (AJAX)
    // ────────────────────────────────────────────────────────
    [HttpPost]
    public IActionResult StartBudgetRun(string[] targetMonths, string algoMethod, decimal? growthOverride)
    {
        if (targetMonths == null || targetMonths.Length == 0)
            return Json(new { success = false, message = "Select at least one target month." });

        if (_jobService.IsRunning)
            return Json(new { success = false, message = "A budget run is already in progress." });

        var started = _jobService.TryStartBudgetRun(
            targetMonths.ToList(), algoMethod ?? "HYBRID", growthOverride);

        return Json(new { success = started, message = started ? "Budget run started." : "Could not start." });
    }

    // ────────────────────────────────────────────────────────
    //  JOB STATUS (polled every 5s)
    // ────────────────────────────────────────────────────────
    [HttpGet]
    public IActionResult JobStatus() => Json(_jobService.GetStatus());

    // ────────────────────────────────────────────────────────
    //  STAGING STATUS (AJAX)
    // ────────────────────────────────────────────────────────
    [HttpGet]
    public async Task<IActionResult> StagingStatus()
        => Json(await _sfService.GetStagingStatusAsync());

    // ────────────────────────────────────────────────────────
    //  OUTPUT PAGE
    // ────────────────────────────────────────────────────────
    public async Task<IActionResult> Output(
        string? month, string? division, string? store, string? majCat,
        string? algoMethod, int page = 1, int pageSize = 100)
    {
        var connStr = _connStr;
        var vm = new SaleBudgetOutputViewModel { CurrentPage = page, PageSize = pageSize };

        // Build WHERE clauses — plain (for KPI/count) and aliased (for JOIN query)
        var where = new StringBuilder("WHERE 1=1");
        var whereB = new StringBuilder("WHERE 1=1");
        var parms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(month))
        {
            where.Append(" AND PLAN_MONTH = @month");
            whereB.Append(" AND b.PLAN_MONTH = @month");
            parms.Add(new SqlParameter("@month", month + "-01"));
        }
        if (!string.IsNullOrEmpty(division))
        {
            where.Append(" AND DIVISION = @div");
            whereB.Append(" AND b.DIVISION = @div");
            parms.Add(new SqlParameter("@div", division));
        }
        if (!string.IsNullOrEmpty(store))
        {
            where.Append(" AND STORE_CODE = @store");
            whereB.Append(" AND b.STORE_CODE = @store");
            parms.Add(new SqlParameter("@store", store));
        }
        if (!string.IsNullOrEmpty(majCat))
        {
            where.Append(" AND MAJOR_CATEGORY = @majCat");
            whereB.Append(" AND b.MAJOR_CATEGORY = @majCat");
            parms.Add(new SqlParameter("@majCat", majCat));
        }
        if (!string.IsNullOrEmpty(algoMethod))
        {
            where.Append(" AND ALGO_METHOD = @algo");
            whereB.Append(" AND b.ALGO_METHOD = @algo");
            parms.Add(new SqlParameter("@algo", algoMethod));
        }

        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT COUNT(1),
                       ISNULL(SUM(BGT_SALE_VAL), 0), ISNULL(SUM(BGT_SALE_QTY), 0),
                       ISNULL(SUM(LYSP_SALE_VAL), 0),
                       COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT MAJOR_CATEGORY),
                       COUNT(DISTINCT PLAN_MONTH), MAX(RUN_ID)
                FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(CloneParam(p)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                vm.TotalRows = rdr.GetInt32(0);
                vm.TotalBgtSaleVal = rdr.IsDBNull(1) ? 0 : rdr.GetDecimal(1);
                vm.TotalBgtSaleQty = rdr.IsDBNull(2) ? 0 : rdr.GetDecimal(2);
                vm.TotalLyspSaleVal = rdr.IsDBNull(3) ? 0 : rdr.GetDecimal(3);
                vm.StoreCount = rdr.GetInt32(4);
                vm.CategoryCount = rdr.GetInt32(5);
                vm.MonthCount = rdr.GetInt32(6);
                vm.LatestRunId = rdr.IsDBNull(7) ? null : rdr.GetString(7);
            }
        }
        vm.YoyGrowthPct = vm.TotalLyspSaleVal > 0
            ? (vm.TotalBgtSaleVal - vm.TotalLyspSaleVal) / vm.TotalLyspSaleVal * 100
            : 0;

        // Division chart
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT ISNULL(DIVISION, 'Other'),
                       SUM(BGT_SALE_VAL), SUM(LYSP_SALE_VAL), COUNT(DISTINCT MAJOR_CATEGORY)
                FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) {where}
                GROUP BY DIVISION ORDER BY SUM(BGT_SALE_VAL) DESC";
            parms.ForEach(p => cmd.Parameters.Add(CloneParam(p)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                var bgt = rdr.IsDBNull(1) ? 0m : rdr.GetDecimal(1);
                var lysp = rdr.IsDBNull(2) ? 0m : rdr.GetDecimal(2);
                vm.DivisionChart.Add(new DivisionSummary
                {
                    Division = rdr.GetString(0),
                    BgtSaleVal = bgt,
                    LyspSaleVal = lysp,
                    GrowthPct = lysp > 0 ? (bgt - lysp) / lysp * 100 : 0,
                    CategoryCount = rdr.GetInt32(3)
                });
            }
        }

        // Monthly chart
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT FORMAT(PLAN_MONTH, 'MMM-yy'), SUM(BGT_SALE_VAL), SUM(BGT_SALE_QTY), SUM(LYSP_SALE_VAL)
                FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) {where}
                GROUP BY PLAN_MONTH ORDER BY PLAN_MONTH";
            parms.ForEach(p => cmd.Parameters.Add(CloneParam(p)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                vm.MonthlyChart.Add(new MonthlySummary
                {
                    Month = rdr.GetString(0),
                    BgtSaleVal = rdr.IsDBNull(1) ? 0 : rdr.GetDecimal(1),
                    BgtSaleQty = rdr.IsDBNull(2) ? 0 : rdr.GetDecimal(2),
                    LyspSaleVal = rdr.IsDBNull(3) ? 0 : rdr.GetDecimal(3)
                });
            }
        }

        // Paginated data
        int offset = (page - 1) * pageSize;
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT b.ID, b.RUN_ID, b.STORE_CODE, b.STORE_NAME, b.STATE, b.ZONE, b.REGION, b.SIZE_CATEGORY, b.OLD_NEW,
                       b.MAJOR_CATEGORY, b.DIVISION, b.SUBDIVISION, b.SEGMENT, b.PLAN_MONTH,
                       b.LYSP_SALE_QTY, b.LYSP_SALE_VAL, b.LYSP_GM_VAL,
                       b.GROWTH_RATE_COMBINED, b.FILL_RATE_ADJ, b.FESTIVAL_ADJ,
                       b.ML_FORECAST_QTY, b.ML_FORECAST_MAPE, b.ML_BEST_METHOD,
                       b.BGT_SALE_QTY, b.BGT_SALE_VAL, b.BGT_GM_VAL, b.AVG_SELLING_PRICE,
                       b.ALGO_METHOD, b.STORE_CONT_PCT,
                       ISNULL(a.SALE_QTY, 0) AS ACTUAL_SALE_QTY,
                       ISNULL(a.SALE_VAL, 0) AS ACTUAL_SALE_VAL
                FROM dbo.SALE_BUDGET_PLAN b WITH (NOLOCK)
                LEFT JOIN dbo.STG_SF_SALE_ACTUAL a WITH (NOLOCK)
                    ON a.STORE_CODE = b.STORE_CODE
                    AND a.MAJOR_CATEGORY = b.MAJOR_CATEGORY
                    AND a.SALE_MONTH = b.PLAN_MONTH
                {whereB}
                ORDER BY b.BGT_SALE_VAL DESC
                OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
            parms.ForEach(p => cmd.Parameters.Add(CloneParam(p)));
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
            {
                vm.Rows.Add(new SaleBudgetPlan
                {
                    Id = rdr.GetInt32(0),
                    RunId = Str(rdr, 1), StoreCode = Str(rdr, 2), StoreName = Str(rdr, 3),
                    State = Str(rdr, 4), Zone = Str(rdr, 5), Region = Str(rdr, 6),
                    SizeCategory = Str(rdr, 7), OldNew = Str(rdr, 8),
                    MajorCategory = Str(rdr, 9), Division = Str(rdr, 10),
                    Subdivision = Str(rdr, 11), Segment = Str(rdr, 12),
                    PlanMonth = rdr.IsDBNull(13) ? null : rdr.GetDateTime(13),
                    LyspSaleQty = Dec(rdr, 14), LyspSaleVal = Dec(rdr, 15), LyspGmVal = Dec(rdr, 16),
                    GrowthRateCombined = Dec(rdr, 17), FillRateAdj = Dec(rdr, 18), FestivalAdj = Dec(rdr, 19),
                    MlForecastQty = Dec(rdr, 20), MlForecastMape = Dec(rdr, 21), MlBestMethod = Str(rdr, 22),
                    BgtSaleQty = Dec(rdr, 23), BgtSaleVal = Dec(rdr, 24), BgtGmVal = Dec(rdr, 25),
                    AvgSellingPrice = Dec(rdr, 26), AlgoMethod = Str(rdr, 27), StoreContPct = Dec(rdr, 28),
                    ActualSaleQty = Dec(rdr, 29), ActualSaleVal = Dec(rdr, 30)
                });
            }
        }

        // Dropdowns for filters
        ViewBag.Months = await GetDistinctAsync("FORMAT(PLAN_MONTH, 'yyyy-MM')");
        ViewBag.Divisions = await GetDistinctAsync("DIVISION");
        ViewBag.Stores = await GetDistinctAsync("STORE_CODE");
        ViewBag.MajCats = await GetDistinctAsync("MAJOR_CATEGORY");

        // Preserve filter selections
        ViewBag.SelMonth = month; ViewBag.SelDivision = division;
        ViewBag.SelStore = store; ViewBag.SelMajCat = majCat;
        ViewBag.SelAlgo = algoMethod;

        return View(vm);
    }

    // ────────────────────────────────────────────────────────
    //  EXPORT CSV
    // ────────────────────────────────────────────────────────
    [HttpGet]
    public async Task ExportCsv(string? month, string? division, string? store, string? majCat)
    {
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=SaleBudgetPlan.csv");

        var where = new StringBuilder("WHERE 1=1");
        if (!string.IsNullOrEmpty(month)) where.Append($" AND PLAN_MONTH = '{month}-01'");
        if (!string.IsNullOrEmpty(division)) where.Append($" AND DIVISION = '{division}'");
        if (!string.IsNullOrEmpty(store)) where.Append($" AND STORE_CODE = '{store}'");
        if (!string.IsNullOrEmpty(majCat)) where.Append($" AND MAJOR_CATEGORY = '{majCat}'");

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("STORE_CODE,STORE_NAME,STATE,ZONE,REGION,MAJOR_CATEGORY,DIVISION,SEGMENT,PLAN_MONTH,LYSP_QTY,LYSP_VAL,GROWTH%,BGT_QTY,BGT_VAL,BGT_GM,ASP,ALGO,ML_QTY,ML_MAPE,CONT%");

        var connStr = _connStr;
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
            SELECT STORE_CODE, STORE_NAME, STATE, ZONE, REGION,
                   MAJOR_CATEGORY, DIVISION, SEGMENT,
                   FORMAT(PLAN_MONTH, 'yyyy-MM'), LYSP_SALE_QTY, LYSP_SALE_VAL,
                   GROWTH_RATE_COMBINED, BGT_SALE_QTY, BGT_SALE_VAL, BGT_GM_VAL,
                   AVG_SELLING_PRICE, ALGO_METHOD, ML_FORECAST_QTY, ML_FORECAST_MAPE, STORE_CONT_PCT
            FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) {where}
            ORDER BY BGT_SALE_VAL DESC";
        cmd.CommandTimeout = 300;

        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = rdr.IsDBNull(i) ? "" : rdr.GetValue(i).ToString() ?? "";
                if (val.Contains(',') || val.Contains('"'))
                    sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else
                    sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    // ────────────────────────────────────────────────────────
    //  DOWNLOAD STAGING TABLE AS CSV
    // ────────────────────────────────────────────────────────
    [HttpGet]
    public async Task DownloadStaging(string tableName)
    {
        // Whitelist allowed staging tables
        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "STG_SF_SALE_ACTUAL", "STG_SF_DEMAND_FORECAST", "STG_SF_CONT_PCT",
            "STG_SF_DIM_STORE", "STG_SF_DIM_ARTICLE"
        };
        if (string.IsNullOrWhiteSpace(tableName) || !allowed.Contains(tableName))
        {
            Response.StatusCode = 400;
            return;
        }

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename={tableName}.csv");

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT * FROM dbo.[{tableName}] WITH (NOLOCK)";
        cmd.CommandTimeout = 300;

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await using var rdr = await cmd.ExecuteReaderAsync();

        // Header row
        var headers = new List<string>();
        for (int i = 0; i < rdr.FieldCount; i++) headers.Add(rdr.GetName(i));
        await writer.WriteLineAsync(string.Join(",", headers));

        // Data rows
        while (await rdr.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = rdr.IsDBNull(i) ? "" : rdr.GetValue(i).ToString() ?? "";
                if (val.Contains(',') || val.Contains('"'))
                    sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else
                    sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    // ────────────────────────────────────────────────────────
    //  RESET
    // ────────────────────────────────────────────────────────
    [HttpPost]
    public async Task<IActionResult> ResetBudget()
    {
        var connStr = _connStr;
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE dbo.SALE_BUDGET_PLAN";
        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Sale Budget Plan data has been reset.";
        return RedirectToAction("Execute");
    }

    // ── Helpers ─────────────────────────────────────────────
    private async Task<int> GetRowCountAsync(string table)
    {
        try
        {
            var connStr = _connStr;
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(1) FROM dbo.[{table}] WITH (NOLOCK)";
            return (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }
        catch { return 0; }
    }

    private async Task<string?> GetLatestRunAsync()
    {
        try
        {
            var connStr = _connStr;
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT TOP 1 RUN_ID FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) ORDER BY CREATED_DT DESC";
            return (await cmd.ExecuteScalarAsync())?.ToString();
        }
        catch { return null; }
    }

    private async Task<List<string>> GetDistinctAsync(string column)
    {
        var list = new List<string>();
        try
        {
            var connStr = _connStr;
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT DISTINCT {column} FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) WHERE {column} IS NOT NULL ORDER BY 1";
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync()) list.Add(rdr.GetString(0));
        }
        catch { }
        return list;
    }

    private static string? Str(SqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);
    private static decimal? Dec(SqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetDecimal(i);
    private static SqlParameter CloneParam(SqlParameter p)
        => new(p.ParameterName, p.SqlDbType) { Value = p.Value };
}
