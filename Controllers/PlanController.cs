using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class PlanController : Controller
{
    private readonly string _sfConnStr;
    private readonly PlanService _planService;
    private readonly PlanJobService _jobService;
    private readonly ILogger<PlanController> _logger;

    public PlanController(IConfiguration config, PlanService planService, PlanJobService jobService, ILogger<PlanController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _planService = planService;
        _jobService = jobService;
        _logger = logger;
    }

    public async Task<IActionResult> Execute()
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // Load week calendars
        var weekCals = new List<WeekCalendar>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT FROM WEEK_CALENDAR ORDER BY WEEK_ID";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
                weekCals.Add(new WeekCalendar
                {
                    WeekId = SnowflakeCrudHelper.Int(r, 0),
                    WeekSeq = SnowflakeCrudHelper.Int(r, 1),
                    FyWeek = SnowflakeCrudHelper.Int(r, 2),
                    FyYear = SnowflakeCrudHelper.Int(r, 3),
                    CalYear = SnowflakeCrudHelper.Int(r, 4),
                    YearWeek = SnowflakeCrudHelper.StrNull(r, 5),
                    WkStDt = SnowflakeCrudHelper.DateNull(r, 6),
                    WkEndDt = SnowflakeCrudHelper.DateNull(r, 7)
                });
        }
        ViewBag.WeekCalendars = weekCals;

        ViewBag.TotalStores = Convert.ToInt32(await SnowflakeCrudHelper.ScalarAsync(conn, "SELECT COUNT(DISTINCT ST_CD) FROM MASTER_ST_MASTER") ?? 0);
        ViewBag.TotalCategories = Convert.ToInt32(await SnowflakeCrudHelper.ScalarAsync(conn, "SELECT COUNT(DISTINCT MAJ_CAT) FROM MASTER_BIN_CAPACITY") ?? 0);
        ViewBag.TrfInRows = await SnowflakeCrudHelper.CountAsync(conn, "TRF_IN_PLAN");
        ViewBag.PpRows = await SnowflakeCrudHelper.CountAsync(conn, "PURCHASE_PLAN");
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_ST_MASTER", "ST_CD");
        ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_PRODUCT_HIERARCHY", "MAJ_CAT_NM", "MAJ_CAT_NM != 'NA'");

        return View(new SpExecutionParams());
    }

    [HttpGet]
    public async Task<IActionResult> Progress()
    {
        int trfRows = 0, ppRows = 0, storesProcessed = 0, totalStores = 0;
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM TRF_IN_PLAN";
            trfRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
        }
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(DISTINCT ST_CD) FROM TRF_IN_PLAN";
            storesProcessed = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
        }
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM PURCHASE_PLAN";
            ppRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
        }
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(DISTINCT ST_CD) FROM MASTER_ST_MASTER";
            totalStores = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
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

        // Reload week calendars for the view
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var weekCals = new List<WeekCalendar>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT FROM WEEK_CALENDAR ORDER BY WEEK_ID";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
                weekCals.Add(new WeekCalendar
                {
                    WeekId = SnowflakeCrudHelper.Int(r, 0),
                    WeekSeq = SnowflakeCrudHelper.Int(r, 1),
                    FyWeek = SnowflakeCrudHelper.Int(r, 2),
                    FyYear = SnowflakeCrudHelper.Int(r, 3),
                    CalYear = SnowflakeCrudHelper.Int(r, 4),
                    YearWeek = SnowflakeCrudHelper.StrNull(r, 5),
                    WkStDt = SnowflakeCrudHelper.DateNull(r, 6),
                    WkEndDt = SnowflakeCrudHelper.DateNull(r, 7)
                });
        }
        ViewBag.WeekCalendars = weekCals;

        return View(parameters);
    }

    [HttpGet]
    public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? majCat, string? stCd, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = BuildSnowflakeFilter(fyYear, fyWeek, majCat, stCd);

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, "TRF_IN_PLAN", where, parms);
        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.FyYear = fyYear;
        ViewBag.FyWeek = fyWeek;
        ViewBag.MajCat = majCat;
        ViewBag.StCd = stCd;
        ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, "TRF_IN_PLAN", "MAJ_CAT");
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "TRF_IN_PLAN", "ST_CD");

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, "TRF_IN_PLAN", AllSelectCols, where, parms,
            "ST_CD, MAJ_CAT", page, pageSize, MapTrfInPlan);

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

    // All columns we need (Snowflake — no brackets, no dbo prefix)
    private static readonly string AllSelectCols =
        "ID, FY_YEAR, FY_WEEK, WEEK_ID, " +
        string.Join(", ", IdCols) + ", " +
        string.Join(", ", MetricCols);

    private (string? where, List<SnowflakeDbParameter> parms) BuildSnowflakeFilter(
        int? fyYear, int? fyWeek, string? majCat, string? stCd)
    {
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (fyYear.HasValue) { idx++; conditions.Add("FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        if (!string.IsNullOrEmpty(majCat))
        {
            var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (cats.Length == 1) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), cats[0].Trim())); }
            else
            {
                var inList = string.Join(",", cats.Select(c => { idx++; parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), c.Trim())); return "?"; }));
                conditions.Add($"MAJ_CAT IN ({inList})");
            }
        }
        if (!string.IsNullOrEmpty(stCd))
        {
            var stores = stCd.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (stores.Length == 1) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stores[0].Trim())); }
            else
            {
                var inList = string.Join(",", stores.Select(s => { idx++; parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), s.Trim())); return "?"; }));
                conditions.Add($"ST_CD IN ({inList})");
            }
        }
        return (conditions.Count > 0 ? string.Join(" AND ", conditions) : null, parms);
    }

    private (string sql, string whereClause) BuildFullQuerySql(int? fyYear, int? fyWeek, string? majCat, string? stCd, string orderBy)
    {
        var (where, _) = BuildSnowflakeFilter(fyYear, fyWeek, majCat, stCd);
        var whereStr = string.IsNullOrEmpty(where) ? "" : " WHERE " + where;
        return ($"SELECT {AllSelectCols} FROM TRF_IN_PLAN{whereStr} ORDER BY {orderBy}", whereStr);
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
    {
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_{fyYear}_{fyWeek}.csv");

        var (where, parms) = BuildSnowflakeFilter(fyYear, fyWeek, majCat, stCd);
        var whereStr = string.IsNullOrEmpty(where) ? "" : " WHERE " + where;
        var sql = $"SELECT {AllSelectCols} FROM TRF_IN_PLAN{whereStr} ORDER BY ID";

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Area,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,"
            + string.Join(",", MetricCols));

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

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
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_Pivot_{fyYear}_{fyWeek}.csv");

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // Step 1: Get distinct weeks
        var weeks = new List<int>();
        var (where, parms) = BuildSnowflakeFilter(fyYear, fyWeek, majCat, stCd);
        var whereStr = string.IsNullOrEmpty(where) ? "" : " WHERE " + where;

        await using (var cmd1 = conn.CreateCommand())
        {
            cmd1.CommandText = $"SELECT DISTINCT NVL(FY_WEEK, 0) AS W FROM TRF_IN_PLAN{whereStr} ORDER BY W";
            cmd1.CommandTimeout = 120;
            foreach (var p in parms) cmd1.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));
            await using var r1 = await cmd1.ExecuteReaderAsync();
            while (await r1.ReadAsync()) weeks.Add(Convert.ToInt32(r1.GetValue(0)));
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
        var (where2, parms2) = BuildSnowflakeFilter(fyYear, fyWeek, majCat, stCd);
        var whereStr2 = string.IsNullOrEmpty(where2) ? "" : " WHERE " + where2;
        var sql = $"SELECT {AllSelectCols} FROM TRF_IN_PLAN{whereStr2} ORDER BY ST_CD, MAJ_CAT, WEEK_ID";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 600;
        foreach (var p in parms2) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

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
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.ExecAsync(conn, "DELETE FROM TRF_IN_PLAN");
            _logger.LogInformation("TRF_IN_PLAN cleared");
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

    private static TrfInPlan MapTrfInPlan(IDataReader r)
    {
        return new TrfInPlan
        {
            Id = SnowflakeCrudHelper.Int(r, 0),
            FyYear = SnowflakeCrudHelper.IntNull(r, 1),
            FyWeek = SnowflakeCrudHelper.IntNull(r, 2),
            WeekId = SnowflakeCrudHelper.IntNull(r, 3),
            StCd = SnowflakeCrudHelper.StrNull(r, 4),
            StNm = SnowflakeCrudHelper.StrNull(r, 5),
            RdcCd = SnowflakeCrudHelper.StrNull(r, 6),
            RdcNm = SnowflakeCrudHelper.StrNull(r, 7),
            HubCd = SnowflakeCrudHelper.StrNull(r, 8),
            HubNm = SnowflakeCrudHelper.StrNull(r, 9),
            Area = SnowflakeCrudHelper.StrNull(r, 10),
            MajCat = SnowflakeCrudHelper.StrNull(r, 11),
            Seg = SnowflakeCrudHelper.StrNull(r, 12),
            Div = SnowflakeCrudHelper.StrNull(r, 13),
            SubDiv = SnowflakeCrudHelper.StrNull(r, 14),
            MajCatNm = SnowflakeCrudHelper.StrNull(r, 15),
            Ssn = SnowflakeCrudHelper.StrNull(r, 16),
            SGrtStkQ = SnowflakeCrudHelper.DecNull(r, 17),
            WGrtStkQ = SnowflakeCrudHelper.DecNull(r, 18),
            BgtDispClQ = SnowflakeCrudHelper.DecNull(r, 19),
            BgtDispClOpt = SnowflakeCrudHelper.DecNull(r, 20),
            Cm1SaleCoverDay = SnowflakeCrudHelper.DecNull(r, 21),
            Cm2SaleCoverDay = SnowflakeCrudHelper.DecNull(r, 22),
            CoverSaleQty = SnowflakeCrudHelper.DecNull(r, 23),
            BgtStClMbq = SnowflakeCrudHelper.DecNull(r, 24),
            BgtDispClOptMbq = SnowflakeCrudHelper.DecNull(r, 25),
            BgtTtlCfOpStkQ = SnowflakeCrudHelper.DecNull(r, 26),
            NtActQ = SnowflakeCrudHelper.DecNull(r, 27),
            NetBgtCfStkQ = SnowflakeCrudHelper.DecNull(r, 28),
            CmBgtSaleQ = SnowflakeCrudHelper.DecNull(r, 29),
            Cm1BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 30),
            Cm2BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 31),
            TrfInStkQ = SnowflakeCrudHelper.DecNull(r, 32),
            TrfInOptCnt = SnowflakeCrudHelper.DecNull(r, 33),
            TrfInOptMbq = SnowflakeCrudHelper.DecNull(r, 34),
            DcMbq = SnowflakeCrudHelper.DecNull(r, 35),
            BgtTtlCfClStkQ = SnowflakeCrudHelper.DecNull(r, 36),
            BgtNtActQ = SnowflakeCrudHelper.DecNull(r, 37),
            NetStClStkQ = SnowflakeCrudHelper.DecNull(r, 38),
            StClExcessQ = SnowflakeCrudHelper.DecNull(r, 39),
            StClShortQ = SnowflakeCrudHelper.DecNull(r, 40)
        };
    }
}
