using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class PlanController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly PlanService _planService;
    private readonly PlanJobService _jobService;
    private readonly ILogger<PlanController> _logger;

    public PlanController(PlanningDbContext context, PlanService planService, PlanJobService jobService, ILogger<PlanController> logger)
    {
        _context = context;
        _planService = planService;
        _jobService = jobService;
        _logger = logger;
    }

    public async Task<IActionResult> Execute()
    {
        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        ViewBag.TotalStores = await _context.StoreMasters.Select(x => x.StCd).Distinct().CountAsync();
        ViewBag.TotalCategories = await _context.BinCapacities.Select(x => x.MajCat).Distinct().CountAsync();
        ViewBag.TrfInRows = await _context.TrfInPlans.CountAsync();
        ViewBag.PpRows = await _context.PurchasePlans.CountAsync();
        ViewBag.StoreCodes = await _context.StoreMasters.Select(x => x.StCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCats = await _context.ProductHierarchies.Select(x => x.MajCatNm).Where(x => x != null && x != "NA").Distinct().OrderBy(x => x).ToListAsync();
        return View(new SpExecutionParams());
    }

    [HttpGet]
    public async Task<IActionResult> Progress()
    {
        // Use raw SQL with NOLOCK to avoid blocking the running SP
        var connStr = _context.Database.GetConnectionString()!;
        int trfRows = 0, ppRows = 0, storesProcessed = 0, totalStores = 0;
        using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr))
        {
            await conn.OpenAsync();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT COUNT(*) FROM dbo.TRF_IN_PLAN WITH (NOLOCK);
                SELECT COUNT(DISTINCT ST_CD) FROM dbo.TRF_IN_PLAN WITH (NOLOCK);
                SELECT COUNT(*) FROM dbo.PURCHASE_PLAN WITH (NOLOCK);
                SELECT COUNT(DISTINCT [ST CD]) FROM dbo.MASTER_ST_MASTER WITH (NOLOCK);";
            using var reader = await cmd.ExecuteReaderAsync();
            if (await reader.ReadAsync()) trfRows = reader.GetInt32(0);
            await reader.NextResultAsync();
            if (await reader.ReadAsync()) storesProcessed = reader.GetInt32(0);
            await reader.NextResultAsync();
            if (await reader.ReadAsync()) ppRows = reader.GetInt32(0);
            await reader.NextResultAsync();
            if (await reader.ReadAsync()) totalStores = reader.GetInt32(0);
        }
        return Json(new { trfRows, ppRows, storesProcessed, totalStores });
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public IActionResult StartFullRun(int startWeekId, int endWeekId)
    {
        if (_jobService.IsRunning)
            return Json(new { success = false, message = "A job is already running." });

        var started = _jobService.TryStartFullRun(startWeekId, endWeekId);
        return Json(new { success = started, message = started ? "Full run started!" : "Could not start job." });
    }

    [HttpGet]
    public IActionResult JobStatus()
    {
        return Json(_jobService.GetStatus());
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Execute(SpExecutionParams parameters)
    {
        try
        {
            var (rowsInserted, executionTime) = await _planService.ExecutePlanGeneration(
                parameters.StartWeekId,
                parameters.EndWeekId,
                parameters.StoreCode,
                parameters.MajCat,
                parameters.CoverDaysCm1,
                parameters.CoverDaysCm2);

            TempData["SuccessMessage"] = $"SP executed successfully! {rowsInserted} rows inserted at {executionTime:yyyy-MM-dd HH:mm:ss}";
            _logger.LogInformation("TrfInPlan SP executed: Start={Start} End={End} Rows={Rows}", parameters.StartWeekId, parameters.EndWeekId, rowsInserted);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing TrfInPlan SP");
            TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
        }

        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(parameters);
    }

    [HttpGet]
    public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? majCat, string? stCd, int page = 1, int pageSize = 100)
    {
        var query = _context.TrfInPlans.AsQueryable();
        if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
        if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
        if (!string.IsNullOrEmpty(majCat)) { var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => cats.Contains(x.MajCat)); }
        if (!string.IsNullOrEmpty(stCd)) { var stores = stCd.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => stores.Contains(x.StCd)); }

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.FyYear = fyYear;
        ViewBag.FyWeek = fyWeek;
        ViewBag.MajCat = majCat;
        ViewBag.StCd = stCd;
        ViewBag.Categories = await _context.TrfInPlans.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.StoreCodes = await _context.TrfInPlans.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();

        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        _logger.LogInformation("TrfInPlan Output: {Count} rows returned", data.Count);
        return View(data);
    }

    // ──────────────────────────────────────────────────────────────
    // STREAMING CSV EXPORT (handles 13M+ rows without loading into memory)
    // ──────────────────────────────────────────────────────────────

    // Full metric columns for flat CSV
    private static readonly string[] MetricCols = {
        "S_GRT_STK_Q","W_GRT_STK_Q","BGT_DISP_CL_Q","BGT_DISP_CL_OPT",
        "CM1_SALE_COVER_DAY","CM2_SALE_COVER_DAY","COVER_SALE_QTY",
        "BGT_ST_CL_MBQ","BGT_DISP_CL_OPT_MBQ",
        "BGT_TTL_CF_OP_STK_Q","NT_ACT_Q","NET_BGT_CF_STK_Q",
        "CM_BGT_SALE_Q","CM1_BGT_SALE_Q","CM2_BGT_SALE_Q",
        "TRF_IN_STK_Q","TRF_IN_OPT_CNT","TRF_IN_OPT_MBQ","DC_MBQ",
        "BGT_TTL_CF_CL_STK_Q","BGT_NT_ACT_Q","NET_ST_CL_STK_Q",
        "ST_CL_EXCESS_Q","ST_CL_SHORT_Q"
    };

    // All 24 metrics in pivot CSV (same as flat CSV)
    private static readonly string[] PivotMetricCols = {
        "S_GRT_STK_Q","W_GRT_STK_Q","BGT_DISP_CL_Q","BGT_DISP_CL_OPT",
        "CM1_SALE_COVER_DAY","CM2_SALE_COVER_DAY","COVER_SALE_QTY",
        "BGT_ST_CL_MBQ","BGT_DISP_CL_OPT_MBQ",
        "BGT_TTL_CF_OP_STK_Q","NT_ACT_Q","NET_BGT_CF_STK_Q",
        "CM_BGT_SALE_Q","CM1_BGT_SALE_Q","CM2_BGT_SALE_Q",
        "TRF_IN_STK_Q","TRF_IN_OPT_CNT","TRF_IN_OPT_MBQ","DC_MBQ",
        "BGT_TTL_CF_CL_STK_Q","BGT_NT_ACT_Q","NET_ST_CL_STK_Q",
        "ST_CL_EXCESS_Q","ST_CL_SHORT_Q"
    };

    // Week-1-only columns in pivot (static data — skipped for weeks 2+)
    private static readonly HashSet<string> PivotWeek1Only = new() {
        "S_GRT_STK_Q","W_GRT_STK_Q","BGT_TTL_CF_OP_STK_Q"
    };

    private static readonly string[] IdCols = {
        "ST_CD","ST_NM","RDC_CD","RDC_NM","HUB_CD","HUB_NM","AREA","MAJ_CAT",
        "SEG","DIV","SUB_DIV","MAJ_CAT_NM","SSN"
    };

    // All columns we need (no CREATED_DT, CREATED_BY, WK_ST_DT, WK_END_DT bloat)
    private static readonly string AllSelectCols =
        "[ID],[FY_YEAR],[FY_WEEK],[WEEK_ID]," +
        string.Join(",", IdCols.Select(c => $"[{c}]")) + "," +
        string.Join(",", MetricCols.Select(c => $"[{c}]"));

    private (string sql, List<Microsoft.Data.SqlClient.SqlParameter> prms) BuildFilterSql(
        int? fyYear, int? fyWeek, string? majCat, string? stCd, string orderBy)
    {
        var where = new List<string>();
        var prms = new List<Microsoft.Data.SqlClient.SqlParameter>();
        if (fyYear.HasValue) { where.Add("[FY_YEAR]=@fy"); prms.Add(new("@fy", fyYear.Value)); }
        if (fyWeek.HasValue) { where.Add("[FY_WEEK]=@fw"); prms.Add(new("@fw", fyWeek.Value)); }
        if (!string.IsNullOrEmpty(majCat))
        {
            var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (cats.Length == 1) { where.Add("[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
            else
            {
                var inList = string.Join(",", cats.Select((c, i) => $"@mc{i}"));
                where.Add($"[MAJ_CAT] IN ({inList})");
                for (int i = 0; i < cats.Length; i++) prms.Add(new($"@mc{i}", cats[i].Trim()));
            }
        }
        if (!string.IsNullOrEmpty(stCd))
        {
            var stores = stCd.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (stores.Length == 1) { where.Add("[ST_CD]=@st"); prms.Add(new("@st", stCd)); }
            else
            {
                var inList = string.Join(",", stores.Select((s, i) => $"@st{i}"));
                where.Add($"[ST_CD] IN ({inList})");
                for (int i = 0; i < stores.Length; i++) prms.Add(new($"@st{i}", stores[i].Trim()));
            }
        }
        var w = where.Count > 0 ? " WHERE " + string.Join(" AND ", where) : "";
        return ($"SELECT {AllSelectCols} FROM dbo.TRF_IN_PLAN WITH (NOLOCK){w} ORDER BY {orderBy}", prms);
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
    {
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_{fyYear}_{fyWeek}.csv");

        var (sql, prms) = BuildFilterSql(fyYear, fyWeek, majCat, stCd, "[ID]");
        var connStr = _context.Database.GetConnectionString()!;

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        // Header
        await writer.WriteLineAsync("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Area,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,"
            + string.Join(",", MetricCols));

        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        cmd.Parameters.AddRange(prms.ToArray());

        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            var sb = new StringBuilder(512);
            sb.Append(reader["ID"]);
            foreach (var col in IdCols) sb.Append(',').Append(Q(reader[col]?.ToString()));
            sb.Append(',').Append(reader["FY_YEAR"]);
            sb.Append(',').Append(reader["FY_WEEK"]);
            foreach (var col in MetricCols)
            {
                var v = reader[col];
                sb.Append(',').Append(v == DBNull.Value ? "0" : v);
            }
            await writer.WriteLineAsync(sb.ToString());
            count++;
            if (count % 100000 == 0) await writer.FlushAsync();
        }
        await writer.FlushAsync();
        _logger.LogInformation("TrfInPlan ExportCsv: {Count:N0} rows streamed", count);
        return new EmptyResult();
    }

    [HttpGet]
    public async Task<IActionResult> ExportPivotCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
    {
        // Pivot export works with or without filters

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_Pivot_{fyYear}_{fyWeek}.csv");

        var connStr = _context.Database.GetConnectionString()!;

        // Step 1: Get distinct weeks
        var weeks = new List<int>();
        await using (var conn1 = new Microsoft.Data.SqlClient.SqlConnection(connStr))
        {
            await conn1.OpenAsync();
            await using var cmd1 = conn1.CreateCommand();
            var (baseSql, _) = BuildFilterSql(fyYear, fyWeek, majCat, stCd, "");
            var whereIdx = baseSql.IndexOf(" WHERE ");
            var whereClause = whereIdx >= 0 ? baseSql.Substring(whereIdx) : "";
            var orderIdx = whereClause.IndexOf(" ORDER BY ");
            if (orderIdx >= 0) whereClause = whereClause.Substring(0, orderIdx);

            cmd1.CommandText = $"SELECT DISTINCT ISNULL([FY_WEEK],0) AS W FROM dbo.TRF_IN_PLAN WITH (NOLOCK){whereClause} ORDER BY W";
            cmd1.CommandTimeout = 120;
            var (_, prms0) = BuildFilterSql(fyYear, fyWeek, majCat, stCd, "");
            cmd1.Parameters.AddRange(prms0.ToArray());
            await using var r1 = await cmd1.ExecuteReaderAsync();
            while (await r1.ReadAsync()) weeks.Add(r1.GetInt32(0));
        }

        var firstWeek = weeks.Count > 0 ? weeks[0] : 0;

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        // Step 2: Write header (reduced metrics, skip week1-only cols for weeks 2+)
        var header = new StringBuilder();
        header.Append(string.Join(",", IdCols));
        foreach (var w in weeks)
            foreach (var m in PivotMetricCols)
                if (w == firstWeek || !PivotWeek1Only.Contains(m))
                    header.Append($",WK-{w}_{m}");
        await writer.WriteLineAsync(header.ToString());

        // Step 3: Stream rows
        var (sql, prms2) = BuildFilterSql(fyYear, fyWeek, majCat, stCd, "[ST_CD],[MAJ_CAT],[WEEK_ID]");

        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        cmd.Parameters.AddRange(prms2.ToArray());

        // Build index map: PivotMetricCols -> position in MetricCols
        var pivotIdx = PivotMetricCols.Select(p => Array.IndexOf(MetricCols, p)).ToArray();

        await using var reader = await cmd.ExecuteReaderAsync();

        string? prevKey = null;
        string[]? idValues = null;
        var weekData = new Dictionary<int, decimal[]>();
        int pivotCount = 0;

        while (await reader.ReadAsync())
        {
            var stCdVal = reader["ST_CD"]?.ToString() ?? "";
            var majCatVal = reader["MAJ_CAT"]?.ToString() ?? "";
            var currentKey = stCdVal + "|" + majCatVal;
            var week = reader["FY_WEEK"] == DBNull.Value ? 0 : Convert.ToInt32(reader["FY_WEEK"]);

            if (currentKey != prevKey)
            {
                if (prevKey != null && idValues != null)
                {
                    await WritePivotRow(writer, idValues, weeks, firstWeek, weekData);
                    pivotCount++;
                    if (pivotCount % 10000 == 0) await writer.FlushAsync();
                }
                idValues = new string[IdCols.Length];
                for (int i = 0; i < IdCols.Length; i++)
                    idValues[i] = reader[IdCols[i]]?.ToString() ?? "NA";
                weekData.Clear();
                prevKey = currentKey;
            }

            // Collect only pivot metric values
            var vals = new decimal[PivotMetricCols.Length];
            for (int i = 0; i < PivotMetricCols.Length; i++)
            {
                var v = reader[PivotMetricCols[i]];
                vals[i] = v == DBNull.Value ? 0m : Convert.ToDecimal(v);
            }
            weekData[week] = vals;
        }

        if (prevKey != null && idValues != null)
            await WritePivotRow(writer, idValues, weeks, firstWeek, weekData);

        await writer.FlushAsync();
        _logger.LogInformation("TrfInPlan ExportPivotCsv: {Count:N0} pivot rows, {Metrics} metrics x {Weeks} weeks", pivotCount + 1, PivotMetricCols.Length, weeks.Count);
        return new EmptyResult();
    }

    private static async Task WritePivotRow(StreamWriter writer, string[] idValues, List<int> weeks, int firstWeek, Dictionary<int, decimal[]> weekData)
    {
        var sb = new StringBuilder(2048);
        sb.Append(string.Join(",", idValues.Select(v => Q(v))));
        foreach (var w in weeks)
        {
            var hasData = weekData.TryGetValue(w, out var vals);
            for (int i = 0; i < PivotMetricCols.Length; i++)
            {
                if (w != firstWeek && PivotWeek1Only.Contains(PivotMetricCols[i])) continue;
                sb.Append(',').Append(hasData ? vals![i] : 0);
            }
        }
        await writer.WriteLineAsync(sb.ToString());
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> ResetTrf()
    {
        try
        {
            await _context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE [dbo].[TRF_IN_PLAN]");
            _logger.LogInformation("TRF_IN_PLAN truncated");
            TempData["SuccessMessage"] = "Transfer In Plan data cleared successfully.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ResetTrf error");
            TempData["ErrorMessage"] = $"Error: {ex.InnerException?.Message ?? ex.Message}";
        }
        return RedirectToAction(nameof(Execute));
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "NA";
        if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        return s;
    }
    }

