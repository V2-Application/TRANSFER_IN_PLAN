using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubLevelController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<SubLevelController> _logger;
    private readonly SubLevelJobService _jobService;

    public SubLevelController(PlanningDbContext context, ILogger<SubLevelController> logger, SubLevelJobService jobService)
    {
        _context = context;
        _logger = logger;
        _jobService = jobService;
    }

    // ── Level metadata ──────────────────────────────────────────
    private static (string table, string col, string label)? GetLevelInfo(string? level) => level?.ToLower() switch
    {
        "mvgr" => ("ST_MAJ_CAT_MACRO_MVGR_PLAN", "DISP_MVGR_MATRIX", "Macro MVGR"),
        "sz"   => ("ST_MAJ_CAT_SZ_PLAN", "SZ", "Size"),
        "seg"  => ("ST_MAJ_CAT_SEG_PLAN", "SEG", "Segment"),
        "vnd"  => ("ST_MAJ_CAT_VND_PLAN", "M_VND_CD", "Vendor"),
        _ => null
    };

    private static readonly string[] LevelKeys = { "mvgr", "sz", "seg", "vnd" };

    // ═════════════════════════════════════════════════════════════
    // EXECUTE / STATUS PAGE
    // ═════════════════════════════════════════════════════════════
    public async Task<IActionResult> Execute()
    {
        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        ViewBag.MajCats = await _context.BinCapacities.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.StoreCodes = await _context.StoreMasters.Select(x => x.StCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        return View();
    }

    // ── Background Full Run ─────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public IActionResult StartFullRun(string[] levels, int startWeekId, int endWeekId, string? storeCode = null, string? majCat = null)
    {
        if (_jobService.IsRunning)
            return Json(new { success = false, message = "A sub-level job is already running." });

        if (levels == null || levels.Length == 0)
            levels = new[] { "MVGR", "SZ", "SEG", "VND" };

        var started = _jobService.TryStartFullRun(levels, startWeekId, endWeekId, storeCode, majCat);
        return Json(new { success = started, message = started ? "Sub-level full run started!" : "Could not start job." });
    }

    [HttpGet]
    public IActionResult SubJobStatus() => Json(_jobService.GetStatus());

    [HttpGet]
    public async Task<IActionResult> ContStatus()
    {
        var connStr = _context.Database.GetConnectionString()!;
        var result = new List<object>();
        // All tables the sub-level algorithm needs, grouped by category
        var tables = new[] {
            // Contribution tables (CONT_PCT per ST_CD + MAJ_CAT + sub-value)
            ("Contribution", "ST_MAJ_CAT_MACRO_MVGR_PLAN", "MVGR Contribution", "Sale/Display × CONT% for MVGR level"),
            ("Contribution", "ST_MAJ_CAT_SZ_PLAN",         "Size Contribution", "Sale/Display × CONT% for Size level"),
            ("Contribution", "ST_MAJ_CAT_SEG_PLAN",        "Segment Contribution", "Sale/Display × CONT% for Segment level"),
            ("Contribution", "ST_MAJ_CAT_VND_PLAN",        "Vendor Contribution", "Sale/Display × CONT% for Vendor level"),
            // Sub-level store stock (opening stock per sub-value)
            ("Sub Store Stk", "SUB_ST_STK_MVGR", "St Stk MVGR", "Store opening stock for MVGR TRF"),
            ("Sub Store Stk", "SUB_ST_STK_SZ",   "St Stk Size", "Store opening stock for Size TRF"),
            ("Sub Store Stk", "SUB_ST_STK_SEG",  "St Stk Segment", "Store opening stock for Segment TRF"),
            ("Sub Store Stk", "SUB_ST_STK_VND",  "St Stk Vendor", "Store opening stock for Vendor TRF"),
            // Sub-level DC stock (opening DC stock per sub-value)
            ("Sub DC Stk", "SUB_DC_STK_MVGR", "DC Stk MVGR", "DC opening stock for MVGR PP"),
            ("Sub DC Stk", "SUB_DC_STK_SZ",   "DC Stk Size", "DC opening stock for Size PP"),
            ("Sub DC Stk", "SUB_DC_STK_SEG",  "DC Stk Segment", "DC opening stock for Segment PP"),
            ("Sub DC Stk", "SUB_DC_STK_VND",  "DC Stk Vendor", "DC opening stock for Vendor PP"),
            // Reference tables (shared with main plan)
            ("Reference", "QTY_SALE_QTY",             "Sale Qty",           "Weekly sale forecast × CONT% → sub-level sale"),
            ("Reference", "QTY_DISP_QTY",             "Display Qty",        "Weekly display qty × CONT% → sub-level display"),
            ("Reference", "MASTER_BIN_CAPACITY",       "Bin Capacity",       "MBQ + BIN_CAP per MAJ_CAT for TRF chain"),
            ("Reference", "MASTER_ST_MASTER",          "Store Master",       "ST_CD → RDC mapping for PP aggregation"),
            ("Reference", "WEEK_CALENDAR",             "Week Calendar",      "Week ID → FY_YEAR/FY_WEEK mapping"),
            ("Reference", "MASTER_PRODUCT_HIERARCHY",  "Product Hierarchy",  "MAJ_CAT → SSN for shrinkage calculation"),
        };
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        foreach (var (cat, tbl, label, logic) in tables)
        {
            int cnt = 0;
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(*) FROM [dbo].[{tbl}] WITH (NOLOCK)";
                cmd.CommandTimeout = 15;
                cnt = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            }
            catch { cnt = -1; }
            result.Add(new { category = cat, table = tbl, label, logic, rows = cnt });
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> PlanStatus()
    {
        var connStr = _context.Database.GetConnectionString()!;
        var result = new List<object>();
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        foreach (var (tbl, plan) in new[] { ("TRF_IN_PLAN", "Transfer In Plan"), ("PURCHASE_PLAN", "Purchase Plan") })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM [dbo].[{tbl}] WITH (NOLOCK)";
            cmd.CommandTimeout = 30;
            var cnt = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            result.Add(new { plan, rows = cnt });
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> SubOutputStatus()
    {
        var connStr = _context.Database.GetConnectionString()!;
        var result = new List<object>();
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        foreach (var (tbl, label) in new[] { ("SUB_LEVEL_TRF_PLAN", "Sub-Level TRF"), ("SUB_LEVEL_PP_PLAN", "Sub-Level PP") })
        {
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(*), MAX([CREATED_DT]) FROM [dbo].[{tbl}] WITH (NOLOCK)";
                cmd.CommandTimeout = 30;
                using var r = await cmd.ExecuteReaderAsync();
                if (await r.ReadAsync())
                    result.Add(new { table = label, rows = r.GetInt32(0), lastRun = r.IsDBNull(1) ? "" : r.GetDateTime(1).ToString("dd-MMM HH:mm") });
            }
            catch { result.Add(new { table = label, rows = 0, lastRun = "" }); }
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> SubOutputStatusByLevel()
    {
        var connStr = _context.Database.GetConnectionString()!;
        var result = new List<object>();
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        foreach (var lv in new[] { "MVGR", "SZ", "SEG", "VND" })
        {
            int trfRows = 0, ppRows = 0; string? lastRun = null;
            try
            {
                using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT COUNT(*), MAX([CREATED_DT]) FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{lv}'"; cmd.CommandTimeout = 30; using var r = await cmd.ExecuteReaderAsync(); if (await r.ReadAsync()) { trfRows = r.GetInt32(0); if (!r.IsDBNull(1)) lastRun = r.GetDateTime(1).ToString("dd-MMM HH:mm"); } }
                using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT COUNT(*) FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{lv}'"; cmd.CommandTimeout = 30; ppRows = (int)(await cmd.ExecuteScalarAsync() ?? 0); }
            }
            catch { }
            result.Add(new { level = lv, trfRows, ppRows, lastRun = lastRun ?? "" });
        }
        return Json(result);
    }

    // ═════════════════════════════════════════════════════════════
    // RESET — CLEAR SUB-LEVEL DATA
    // ═════════════════════════════════════════════════════════════
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> ResetLevel(string? level)
    {
        try
        {
            if (string.IsNullOrEmpty(level) || level == "all")
            {
                await _context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE [dbo].[SUB_LEVEL_TRF_PLAN]");
                await _context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE [dbo].[SUB_LEVEL_PP_PLAN]");
                _logger.LogInformation("All sub-level data truncated");
                TempData["SuccessMessage"] = "All sub-level plan data cleared successfully.";
            }
            else
            {
                var lv = level.ToUpper();
                await _context.Database.ExecuteSqlRawAsync($"DELETE FROM [dbo].[SUB_LEVEL_TRF_PLAN] WHERE [LEVEL]='{lv}'");
                await _context.Database.ExecuteSqlRawAsync($"DELETE FROM [dbo].[SUB_LEVEL_PP_PLAN] WHERE [LEVEL]='{lv}'");
                _logger.LogInformation("Sub-level data cleared for level {Level}", lv);
                TempData["SuccessMessage"] = $"Sub-level data cleared for {lv}.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ResetLevel error");
            TempData["ErrorMessage"] = $"Error: {ex.InnerException?.Message ?? ex.Message}";
        }
        return RedirectToAction(nameof(Execute));
    }

    // ═════════════════════════════════════════════════════════════
    // EXECUTE — GENERATE SUB-LEVEL DATA
    // ═════════════════════════════════════════════════════════════
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> RunGenerate(string[] levels, int startWeekId, int endWeekId, string? storeCode = null, string? majCat = null)
    {
        if (levels == null || levels.Length == 0)
        {
            TempData["ErrorMessage"] = "Select at least one level.";
            return RedirectToAction(nameof(Execute));
        }
        if (startWeekId <= 0 || endWeekId <= 0 || endWeekId < startWeekId)
        {
            TempData["ErrorMessage"] = "Invalid Start/End Week.";
            return RedirectToAction(nameof(Execute));
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            foreach (var level in levels)
            {
                var levelKey = level.ToUpper();
                _logger.LogInformation("SubLevel generating TRF [{Level}] weeks {Start}-{End} store={Store} cat={Cat}", levelKey, startWeekId, endWeekId, storeCode ?? "ALL", majCat ?? "ALL");

                // Call TRF SP
                var trfParams = new[] {
                    new SqlParameter("@p0", levelKey),
                    new SqlParameter("@p1", startWeekId),
                    new SqlParameter("@p2", endWeekId),
                    new SqlParameter("@p3", System.Data.SqlDbType.NVarChar, 50) { Value = string.IsNullOrEmpty(storeCode) ? (object)DBNull.Value : storeCode },
                    new SqlParameter("@p4", System.Data.SqlDbType.NVarChar, 100) { Value = string.IsNullOrEmpty(majCat) ? (object)DBNull.Value : majCat }
                };
                await _context.Database.ExecuteSqlRawAsync(
                    "EXEC SP_GENERATE_SUB_LEVEL_TRF @Level=@p0, @StartWeekID=@p1, @EndWeekID=@p2, @StoreCode=@p3, @MajCat=@p4",
                    trfParams);

                _logger.LogInformation("SubLevel generating PP [{Level}]", levelKey);

                // Call PP SP
                await _context.Database.ExecuteSqlRawAsync(
                    "EXEC SP_GENERATE_SUB_LEVEL_PP @Level=@p0, @StartWeekID=@p1, @EndWeekID=@p2",
                    levelKey, startWeekId, endWeekId);

                _logger.LogInformation("SubLevel [{Level}] done", levelKey);
            }

            sw.Stop();
            var filterMsg = "";
            if (!string.IsNullOrEmpty(storeCode)) filterMsg += $" Store={storeCode}";
            if (!string.IsNullOrEmpty(majCat)) filterMsg += $" Cat={majCat}";
            TempData["SuccessMessage"] = $"Sub-level plans generated in {sw.Elapsed.TotalSeconds:N1}s for levels: {string.Join(", ", levels.Select(l => l.ToUpper()))}. Weeks {startWeekId}-{endWeekId}.{filterMsg}";
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "SubLevel RunGenerate error");
            TempData["ErrorMessage"] = $"Error ({sw.Elapsed.TotalSeconds:N1}s): {ex.InnerException?.Message ?? ex.Message}";
        }

        return RedirectToAction(nameof(Execute));
    }

    // ═════════════════════════════════════════════════════════════
    // TRANSFER IN — SUB-LEVEL OUTPUT (reads from materialized table)
    // ═════════════════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> TrfOutput(string level = "mvgr", string? stCd = null, string? majCat = null,
        int? fyYear = null, int? fyWeek = null, int page = 1, int pageSize = 100)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (_, subCol, label) = info.Value;
        var levelKey = level.ToUpper();

        ViewBag.Level = level; ViewBag.LevelLabel = label; ViewBag.SubColName = subCol;
        ViewBag.StCd = stCd; ViewBag.MajCat = majCat; ViewBag.FyYear = fyYear; ViewBag.FyWeek = fyWeek;
        ViewBag.Page = page; ViewBag.PageSize = pageSize; ViewBag.LevelKeys = LevelKeys;

        var connStr = _context.Database.GetConnectionString()!;
        var clauses = new List<string> { "[LEVEL]=@lv" };
        var prms = new List<SqlParameter> { new("@lv", levelKey) };
        if (!string.IsNullOrEmpty(stCd)) { clauses.Add("[ST_CD]=@st"); prms.Add(new("@st", stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { clauses.Add("[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
        if (fyYear.HasValue) { clauses.Add("[FY_YEAR]=@fy"); prms.Add(new("@fy", fyYear.Value)); }
        if (fyWeek.HasValue) { clauses.Add("[FY_WEEK]=@fw"); prms.Add(new("@fw", fyWeek.Value)); }
        var where = "WHERE " + string.Join(" AND ", clauses);

        // Load dropdowns from materialized table
        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        ViewBag.StoreCodes = new List<string>();
        ViewBag.Categories = new List<string>();
        try
        {
            using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT DISTINCT [ST_CD] FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{levelKey}' ORDER BY [ST_CD]"; cmd.CommandTimeout = 30; using var r = await cmd.ExecuteReaderAsync(); var list = new List<string>(); while (await r.ReadAsync()) list.Add(r.GetString(0)); ViewBag.StoreCodes = list; }
            using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT DISTINCT [MAJ_CAT] FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{levelKey}' ORDER BY [MAJ_CAT]"; cmd.CommandTimeout = 30; using var r = await cmd.ExecuteReaderAsync(); var list = new List<string>(); while (await r.ReadAsync()) list.Add(r.GetString(0)); ViewBag.Categories = list; }
        }
        catch { /* table may not exist yet */ }

        int totalCount = 0;
        var data = new List<SubLevelTrfRow>();

        try
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) {where}";
                cmd.CommandTimeout = 60;
                foreach (var p in prms) cmd.Parameters.Add(CloneParam(p));
                totalCount = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            }
            ViewBag.TotalCount = totalCount;

            if (totalCount > 0)
            {
                using var cmd2 = conn.CreateCommand();
                cmd2.CommandText = $@"SELECT [ST_CD],[MAJ_CAT],[SUB_VALUE],[CONT_PCT],[FY_YEAR],[FY_WEEK],
                    [BGT_DISP_CL_Q],[CM_BGT_SALE_Q],[CM1_BGT_SALE_Q],[CM2_BGT_SALE_Q],[COVER_SALE_QTY],
                    [TRF_IN_STK_Q],[DC_MBQ],[BGT_TTL_CF_OP_STK_Q],[BGT_TTL_CF_CL_STK_Q],[BGT_ST_CL_MBQ],[ST_CL_EXCESS_Q],[ST_CL_SHORT_Q]
                FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) {where}
                ORDER BY [ST_CD],[MAJ_CAT],[SUB_VALUE],[FY_WEEK]
                OFFSET {(page - 1) * pageSize} ROWS FETCH NEXT {pageSize} ROWS ONLY";
                cmd2.CommandTimeout = 60;
                foreach (var p in prms) cmd2.Parameters.Add(CloneParam(p));
                using var reader = await cmd2.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    data.Add(new SubLevelTrfRow { StCd = reader.GetString(0), MajCat = reader.GetString(1), SubLevel = reader.GetString(2), ContPct = reader.GetDecimal(3), FyYear = reader.IsDBNull(4)?0:reader.GetInt32(4), FyWeek = reader.IsDBNull(5)?0:reader.GetInt32(5), BgtDispClQ = reader.GetDecimal(6), CmBgtSaleQ = reader.GetDecimal(7), Cm1BgtSaleQ = reader.GetDecimal(8), Cm2BgtSaleQ = reader.GetDecimal(9), CoverSaleQty = reader.GetDecimal(10), TrfInStkQ = reader.GetDecimal(11), DcMbq = reader.GetDecimal(12), BgtTtlCfOpStkQ = reader.GetDecimal(13), BgtTtlCfClStkQ = reader.GetDecimal(14), BgtStClMbq = reader.GetDecimal(15), StClExcessQ = reader.GetDecimal(16), StClShortQ = reader.GetDecimal(17) });
            }
        }
        catch { ViewBag.TotalCount = 0; ViewBag.NotGenerated = true; }

        return View(data);
    }

    // ═════════════════════════════════════════════════════════════
    // PURCHASE PLAN — SUB-LEVEL OUTPUT (reads from materialized table)
    // ═════════════════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> PpOutput(string level = "mvgr", string? rdcCd = null, string? majCat = null,
        int? fyYear = null, int? fyWeek = null, int page = 1, int pageSize = 100)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (_, subCol, label) = info.Value;
        var levelKey = level.ToUpper();

        ViewBag.Level = level; ViewBag.LevelLabel = label; ViewBag.SubColName = subCol;
        ViewBag.RdcCd = rdcCd; ViewBag.MajCat = majCat; ViewBag.FyYear = fyYear; ViewBag.FyWeek = fyWeek;
        ViewBag.Page = page; ViewBag.PageSize = pageSize; ViewBag.LevelKeys = LevelKeys;

        var connStr = _context.Database.GetConnectionString()!;
        var clauses = new List<string> { "[LEVEL]=@lv" };
        var prms = new List<SqlParameter> { new("@lv", levelKey) };
        if (!string.IsNullOrEmpty(rdcCd)) { clauses.Add("[RDC_CD]=@rdc"); prms.Add(new("@rdc", rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { clauses.Add("[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
        if (fyYear.HasValue) { clauses.Add("[FY_YEAR]=@fy"); prms.Add(new("@fy", fyYear.Value)); }
        if (fyWeek.HasValue) { clauses.Add("[FY_WEEK]=@fw"); prms.Add(new("@fw", fyWeek.Value)); }
        var where = "WHERE " + string.Join(" AND ", clauses);

        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();

        ViewBag.RdcCodes = new List<string>();
        ViewBag.Categories = new List<string>();
        try
        {
            using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT DISTINCT [RDC_CD] FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{levelKey}' ORDER BY [RDC_CD]"; cmd.CommandTimeout = 30; using var r = await cmd.ExecuteReaderAsync(); var list = new List<string>(); while (await r.ReadAsync()) list.Add(r.GetString(0)); ViewBag.RdcCodes = list; }
            using (var cmd = conn.CreateCommand()) { cmd.CommandText = $"SELECT DISTINCT [MAJ_CAT] FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{levelKey}' ORDER BY [MAJ_CAT]"; cmd.CommandTimeout = 30; using var r = await cmd.ExecuteReaderAsync(); var list = new List<string>(); while (await r.ReadAsync()) list.Add(r.GetString(0)); ViewBag.Categories = list; }
        }
        catch { }

        int totalCount = 0;
        var data = new List<SubLevelPpRow>();

        try
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) {where}";
                cmd.CommandTimeout = 60;
                foreach (var p in prms) cmd.Parameters.Add(CloneParam(p));
                totalCount = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            }
            ViewBag.TotalCount = totalCount;

            if (totalCount > 0)
            {
                using var cmd2 = conn.CreateCommand();
                cmd2.CommandText = $@"SELECT [RDC_CD],[MAJ_CAT],[SUB_VALUE],[CONT_PCT],[FY_YEAR],[FY_WEEK],
                    [BGT_DISP_CL_Q],[CW_BGT_SALE_Q],[CW1_BGT_SALE_Q],[CW2_BGT_SALE_Q],[CW3_BGT_SALE_Q],[CW4_BGT_SALE_Q],
                    [BGT_PUR_Q_INIT],[BGT_DC_CL_STK_Q],[BGT_DC_CL_MBQ],[BGT_DC_MBQ_SALE],[DC_STK_EXCESS_Q],[DC_STK_SHORT_Q]
                FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) {where}
                ORDER BY [RDC_CD],[MAJ_CAT],[SUB_VALUE],[FY_WEEK]
                OFFSET {(page - 1) * pageSize} ROWS FETCH NEXT {pageSize} ROWS ONLY";
                cmd2.CommandTimeout = 60;
                foreach (var p in prms) cmd2.Parameters.Add(CloneParam(p));
                using var reader = await cmd2.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    data.Add(new SubLevelPpRow { RdcCd = reader.GetString(0), MajCat = reader.GetString(1), SubLevel = reader.GetString(2), ContPct = reader.GetDecimal(3), FyYear = reader.IsDBNull(4)?0:reader.GetInt32(4), FyWeek = reader.IsDBNull(5)?0:reader.GetInt32(5), BgtDispClQ = reader.GetDecimal(6), CwBgtSaleQ = reader.GetDecimal(7), Cw1BgtSaleQ = reader.GetDecimal(8), Cw2BgtSaleQ = reader.GetDecimal(9), Cw3BgtSaleQ = reader.GetDecimal(10), Cw4BgtSaleQ = reader.GetDecimal(11), BgtPurQInit = reader.GetDecimal(12), BgtDcClStkQ = reader.GetDecimal(13), BgtDcClMbq = reader.GetDecimal(14), BgtDcMbqSale = reader.GetDecimal(15), DcStkExcessQ = reader.GetDecimal(16), DcStkShortQ = reader.GetDecimal(17) });
            }
        }
        catch { ViewBag.TotalCount = 0; ViewBag.NotGenerated = true; }

        return View(data);
    }

    // ═════════════════════════════════════════════════════════════
    // CSV EXPORTS
    // ═════════════════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> ExportTrfCsv(string level = "mvgr", string? stCd = null, string? majCat = null,
        int? fyYear = null, int? fyWeek = null)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (contTable, subCol, label) = info.Value;

        if (string.IsNullOrEmpty(stCd) && string.IsNullOrEmpty(majCat) && !fyYear.HasValue && !fyWeek.HasValue)
            return BadRequest("At least one filter required.");

        var connStr = _context.Database.GetConnectionString()!;
        var (where, prms) = BuildTrfWhere(stCd, majCat, fyYear, fyWeek, subCol);

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_{label.Replace(" ","")}_{fyYear}_{fyWeek}.csv");

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync($"ST_CD,MAJ_CAT,{subCol},CONT_PCT,FY_YEAR,FY_WEEK,BGT_DISP_CL_Q,CM_BGT_SALE_Q,CM1_BGT_SALE_Q,CM2_BGT_SALE_Q,COVER_SALE_QTY,TRF_IN_STK_Q,DC_MBQ,BGT_TTL_CF_OP_STK_Q,BGT_TTL_CF_CL_STK_Q,BGT_ST_CL_MBQ,ST_CL_EXCESS_Q,ST_CL_SHORT_Q");

        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"SELECT
            t.[ST_CD], t.[MAJ_CAT], c.[{subCol}], c.[CONT_PCT],
            t.[FY_YEAR], t.[FY_WEEK],
            ISNULL(t.[BGT_DISP_CL_Q],0)*c.[CONT_PCT], ISNULL(t.[CM_BGT_SALE_Q],0)*c.[CONT_PCT],
            ISNULL(t.[CM1_BGT_SALE_Q],0)*c.[CONT_PCT], ISNULL(t.[CM2_BGT_SALE_Q],0)*c.[CONT_PCT],
            ISNULL(t.[COVER_SALE_QTY],0)*c.[CONT_PCT],
            ISNULL(t.[TRF_IN_STK_Q],0), ISNULL(t.[DC_MBQ],0),
            ISNULL(t.[BGT_TTL_CF_OP_STK_Q],0), ISNULL(t.[BGT_TTL_CF_CL_STK_Q],0),
            ISNULL(t.[BGT_ST_CL_MBQ],0), ISNULL(t.[ST_CL_EXCESS_Q],0), ISNULL(t.[ST_CL_SHORT_Q],0)
        FROM [dbo].[TRF_IN_PLAN] t WITH (NOLOCK)
        INNER JOIN [dbo].[{contTable}] c ON c.[ST_CD]=t.[ST_CD] AND c.[MAJ_CAT_CD]=t.[MAJ_CAT]
        {where} ORDER BY t.[ST_CD],t.[MAJ_CAT],c.[{subCol}],t.[FY_WEEK]";
        cmd.CommandTimeout = 600;
        foreach (var p in prms) cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            var sb = new StringBuilder(256);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(reader.IsDBNull(i) ? "0" : reader.GetValue(i));
            }
            await writer.WriteLineAsync(sb.ToString());
            count++;
            if (count % 50000 == 0) await writer.FlushAsync();
        }
        await writer.FlushAsync();
        _logger.LogInformation("SubLevel ExportTrfCsv [{Level}]: {Count:N0} rows", level, count);
        return new EmptyResult();
    }

    [HttpGet]
    public async Task<IActionResult> ExportPpCsv(string level = "mvgr", string? rdcCd = null, string? majCat = null,
        int? fyYear = null, int? fyWeek = null)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (contTable, subCol, label) = info.Value;

        if (string.IsNullOrEmpty(rdcCd) && string.IsNullOrEmpty(majCat) && !fyYear.HasValue && !fyWeek.HasValue)
            return BadRequest("At least one filter required.");

        var connStr = _context.Database.GetConnectionString()!;
        var (where, prms) = BuildPpWhere(rdcCd, majCat, fyYear, fyWeek, subCol);

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=PP_{label.Replace(" ","")}_{fyYear}_{fyWeek}.csv");

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync($"RDC_CD,MAJ_CAT,{subCol},CONT_PCT,FY_YEAR,FY_WEEK,BGT_DISP_CL_Q,CW_BGT_SALE_Q,CW1_BGT_SALE_Q,CW2_BGT_SALE_Q,CW3_BGT_SALE_Q,CW4_BGT_SALE_Q,BGT_PUR_Q_INIT,BGT_DC_CL_STK_Q,BGT_DC_CL_MBQ,BGT_DC_MBQ_SALE,DC_STK_EXCESS_Q,DC_STK_SHORT_Q");

        var cteSql = $@"WITH RdcCont AS (
            SELECT sm.[RDC_CD], c.[MAJ_CAT_CD], c.[{subCol}], AVG(c.[CONT_PCT]) AS [CONT_PCT]
            FROM [dbo].[{contTable}] c
            INNER JOIN [dbo].[MASTER_ST_MASTER] sm ON sm.[ST CD] = c.[ST_CD]
            GROUP BY sm.[RDC_CD], c.[MAJ_CAT_CD], c.[{subCol}]
        )";

        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"{cteSql}
        SELECT p.[RDC_CD], p.[MAJ_CAT], rc.[{subCol}], rc.[CONT_PCT],
            p.[FY_YEAR], p.[FY_WEEK],
            ISNULL(p.[BGT_DISP_CL_Q],0)*rc.[CONT_PCT], ISNULL(p.[CW_BGT_SALE_Q],0)*rc.[CONT_PCT],
            ISNULL(p.[CW1_BGT_SALE_Q],0)*rc.[CONT_PCT], ISNULL(p.[CW2_BGT_SALE_Q],0)*rc.[CONT_PCT],
            ISNULL(p.[CW3_BGT_SALE_Q],0)*rc.[CONT_PCT], ISNULL(p.[CW4_BGT_SALE_Q],0)*rc.[CONT_PCT],
            ISNULL(p.[BGT_PUR_Q_INIT],0), ISNULL(p.[BGT_DC_CL_STK_Q],0),
            ISNULL(p.[BGT_DC_CL_MBQ],0), ISNULL(p.[BGT_DC_MBQ_SALE],0),
            ISNULL(p.[DC_STK_EXCESS_Q],0), ISNULL(p.[DC_STK_SHORT_Q],0)
        FROM [dbo].[PURCHASE_PLAN] p WITH (NOLOCK)
        INNER JOIN RdcCont rc ON rc.[RDC_CD]=p.[RDC_CD] AND rc.[MAJ_CAT_CD]=p.[MAJ_CAT]
        {where} ORDER BY p.[RDC_CD],p.[MAJ_CAT],rc.[{subCol}],p.[FY_WEEK]";
        cmd.CommandTimeout = 600;
        foreach (var p in prms) cmd.Parameters.Add(p);

        await using var reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync())
        {
            var sb = new StringBuilder(256);
            for (int i = 0; i < reader.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(reader.IsDBNull(i) ? "0" : reader.GetValue(i));
            }
            await writer.WriteLineAsync(sb.ToString());
            count++;
            if (count % 50000 == 0) await writer.FlushAsync();
        }
        await writer.FlushAsync();
        _logger.LogInformation("SubLevel ExportPpCsv [{Level}]: {Count:N0} rows", level, count);
        return new EmptyResult();
    }

    // ═════════════════════════════════════════════════════════════
    // PIVOT CSV EXPORTS (one row per ST_CD+MAJ_CAT+SUB_VALUE, weeks as columns)
    // ═════════════════════════════════════════════════════════════
    private static readonly string[] TrfPivotMetrics = {
        "CM_BGT_SALE_Q","BGT_DISP_CL_Q","TRF_IN_STK_Q","DC_MBQ",
        "BGT_TTL_CF_OP_STK_Q","BGT_TTL_CF_CL_STK_Q","BGT_ST_CL_MBQ","ST_CL_EXCESS_Q","ST_CL_SHORT_Q"
    };
    private static readonly string[] PpPivotMetrics = {
        "CW_BGT_SALE_Q","BGT_DISP_CL_Q","BGT_PUR_Q_INIT","BGT_DC_CL_STK_Q",
        "BGT_DC_CL_MBQ","BGT_DC_MBQ_SALE","DC_STK_EXCESS_Q","DC_STK_SHORT_Q"
    };

    [HttpGet]
    public async Task<IActionResult> ExportTrfPivotCsv(string level = "mvgr", string? stCd = null, string? majCat = null)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (_, subCol, label) = info.Value;
        var levelKey = level.ToUpper();
        var metricCount = TrfPivotMetrics.Length;

        var connStr = _context.Database.GetConnectionString()!;
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SubTRF_Pivot_{label.Replace(" ", "")}_{DateTime.Now:yyyyMMdd}.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        try
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            var clauses = new List<string> { "[LEVEL]=@lv", "[FY_WEEK] IS NOT NULL" };
            var prms = new List<SqlParameter> { new("@lv", levelKey) };
            if (!string.IsNullOrEmpty(stCd)) { clauses.Add("[ST_CD]=@st"); prms.Add(new("@st", stCd)); }
            if (!string.IsNullOrEmpty(majCat)) { clauses.Add("[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
            var where = "WHERE " + string.Join(" AND ", clauses);

            var weeks = new List<int>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT DISTINCT ISNULL([FY_WEEK],0) FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) {where} ORDER BY 1";
                cmd.CommandTimeout = 60;
                foreach (var p in prms) cmd.Parameters.Add(CloneParam(p));
                using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) weeks.Add(r.GetInt32(0));
            }

            var hdr = new StringBuilder();
            hdr.Append($"ST_CD,MAJ_CAT,{subCol},CONT_PCT");
            foreach (var w in weeks)
                foreach (var m in TrfPivotMetrics)
                    hdr.Append($",WK{w}_{m}");
            await writer.WriteLineAsync(hdr.ToString());

            using var cmd2 = conn.CreateCommand();
            cmd2.CommandText = $@"SELECT ISNULL([ST_CD],'NA'),ISNULL([MAJ_CAT],'NA'),ISNULL([SUB_VALUE],'NA'),ISNULL([CONT_PCT],0),ISNULL([FY_WEEK],0),
                {string.Join(",", TrfPivotMetrics.Select(m => $"ISNULL([{m}],0)"))}
                FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) {where}
                ORDER BY [ST_CD],[MAJ_CAT],[SUB_VALUE],[FY_WEEK]";
            cmd2.CommandTimeout = 600;
            foreach (var p in prms) cmd2.Parameters.Add(CloneParam(p));

            using var reader = await cmd2.ExecuteReaderAsync();
            string? prevKey = null; string[]? idVals = null;
            var weekData = new Dictionary<int, decimal[]>();
            int count = 0;

            while (await reader.ReadAsync())
            {
                var key = $"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}";
                var week = reader.GetInt32(4);

                if (key != prevKey)
                {
                    if (prevKey != null) { WritePivotLine(writer, idVals!, weeks, weekData, metricCount); count++; if (count % 10000 == 0) await writer.FlushAsync(); }
                    idVals = new[] { Q(reader.GetString(0)), Q(reader.GetString(1)), Q(reader.GetString(2)), reader.GetDecimal(3).ToString() };
                    weekData.Clear();
                    prevKey = key;
                }
                var vals = new decimal[metricCount];
                for (int i = 0; i < metricCount; i++) vals[i] = reader.GetDecimal(5 + i);
                weekData[week] = vals;
            }
            if (prevKey != null) WritePivotLine(writer, idVals!, weeks, weekData, metricCount);
            await writer.FlushAsync();
            _logger.LogInformation("SubLevel ExportTrfPivotCsv [{Level}]: {Count:N0} pivot rows x {Weeks} weeks", level, count + 1, weeks.Count);
        }
        catch (Exception ex) { _logger.LogError(ex, "ExportTrfPivotCsv failed"); }
        return new EmptyResult();
    }

    [HttpGet]
    public async Task<IActionResult> ExportPpPivotCsv(string level = "mvgr", string? rdcCd = null, string? majCat = null)
    {
        var info = GetLevelInfo(level);
        if (info == null) return BadRequest("Invalid level.");
        var (_, subCol, label) = info.Value;
        var levelKey = level.ToUpper();
        var metricCount = PpPivotMetrics.Length;

        var connStr = _context.Database.GetConnectionString()!;
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SubPP_Pivot_{label.Replace(" ", "")}_{DateTime.Now:yyyyMMdd}.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        try
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            var clauses = new List<string> { "[LEVEL]=@lv", "[FY_WEEK] IS NOT NULL" };
            var prms = new List<SqlParameter> { new("@lv", levelKey) };
            if (!string.IsNullOrEmpty(rdcCd)) { clauses.Add("[RDC_CD]=@rdc"); prms.Add(new("@rdc", rdcCd)); }
            if (!string.IsNullOrEmpty(majCat)) { clauses.Add("[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
            var where = "WHERE " + string.Join(" AND ", clauses);

            var weeks = new List<int>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT DISTINCT ISNULL([FY_WEEK],0) FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) {where} ORDER BY 1";
                cmd.CommandTimeout = 60;
                foreach (var p in prms) cmd.Parameters.Add(CloneParam(p));
                using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) weeks.Add(r.GetInt32(0));
            }

            var hdr = new StringBuilder();
            hdr.Append($"RDC_CD,MAJ_CAT,{subCol},CONT_PCT");
            foreach (var w in weeks)
                foreach (var m in PpPivotMetrics)
                    hdr.Append($",WK{w}_{m}");
            await writer.WriteLineAsync(hdr.ToString());

            using var cmd2 = conn.CreateCommand();
            cmd2.CommandText = $@"SELECT ISNULL([RDC_CD],'NA'),ISNULL([MAJ_CAT],'NA'),ISNULL([SUB_VALUE],'NA'),ISNULL([CONT_PCT],0),ISNULL([FY_WEEK],0),
                {string.Join(",", PpPivotMetrics.Select(m => $"ISNULL([{m}],0)"))}
                FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) {where}
                ORDER BY [RDC_CD],[MAJ_CAT],[SUB_VALUE],[FY_WEEK]";
            cmd2.CommandTimeout = 600;
            foreach (var p in prms) cmd2.Parameters.Add(CloneParam(p));

            using var reader = await cmd2.ExecuteReaderAsync();
            string? prevKey = null; string[]? idVals = null;
            var weekData = new Dictionary<int, decimal[]>();
            int count = 0;

            while (await reader.ReadAsync())
            {
                var key = $"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}";
                var week = reader.GetInt32(4);

                if (key != prevKey)
                {
                    if (prevKey != null) { WritePivotLine(writer, idVals!, weeks, weekData, metricCount); count++; if (count % 10000 == 0) await writer.FlushAsync(); }
                    idVals = new[] { Q(reader.GetString(0)), Q(reader.GetString(1)), Q(reader.GetString(2)), reader.GetDecimal(3).ToString() };
                    weekData.Clear();
                    prevKey = key;
                }
                var vals = new decimal[metricCount];
                for (int i = 0; i < metricCount; i++) vals[i] = reader.GetDecimal(5 + i);
                weekData[week] = vals;
            }
            if (prevKey != null) WritePivotLine(writer, idVals!, weeks, weekData, metricCount);
            await writer.FlushAsync();
            _logger.LogInformation("SubLevel ExportPpPivotCsv [{Level}]: {Count:N0} pivot rows x {Weeks} weeks", level, count + 1, weeks.Count);
        }
        catch (Exception ex) { _logger.LogError(ex, "ExportPpPivotCsv failed"); }
        return new EmptyResult();
    }

    private static void WritePivotLine(StreamWriter writer, string[] idVals, List<int> weeks, Dictionary<int, decimal[]> weekData, int metricCount)
    {
        var sb = new StringBuilder(2048);
        sb.Append(string.Join(",", idVals));
        foreach (var w in weeks)
        {
            if (weekData.TryGetValue(w, out var vals))
                for (int i = 0; i < metricCount; i++) sb.Append(',').Append(vals[i]);
            else
                for (int i = 0; i < metricCount; i++) sb.Append(",0");
        }
        writer.WriteLine(sb.ToString());
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "NA";
        if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        return s;
    }

    // ── WHERE clause builders ───────────────────────────────────
    private (string where, List<SqlParameter> prms) BuildTrfWhere(
        string? stCd, string? majCat, int? fyYear, int? fyWeek, string subCol)
    {
        var clauses = new List<string>();
        var prms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(stCd)) { clauses.Add("t.[ST_CD]=@st"); prms.Add(new("@st", stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { clauses.Add("t.[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
        if (fyYear.HasValue) { clauses.Add("t.[FY_YEAR]=@fy"); prms.Add(new("@fy", fyYear.Value)); }
        if (fyWeek.HasValue) { clauses.Add("t.[FY_WEEK]=@fw"); prms.Add(new("@fw", fyWeek.Value)); }
        var w = clauses.Count > 0 ? "WHERE " + string.Join(" AND ", clauses) : "";
        return (w, prms);
    }

    private (string where, List<SqlParameter> prms) BuildPpWhere(
        string? rdcCd, string? majCat, int? fyYear, int? fyWeek, string subCol)
    {
        var clauses = new List<string>();
        var prms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(rdcCd)) { clauses.Add("p.[RDC_CD]=@rdc"); prms.Add(new("@rdc", rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { clauses.Add("p.[MAJ_CAT]=@mc"); prms.Add(new("@mc", majCat)); }
        if (fyYear.HasValue) { clauses.Add("p.[FY_YEAR]=@fy"); prms.Add(new("@fy", fyYear.Value)); }
        if (fyWeek.HasValue) { clauses.Add("p.[FY_WEEK]=@fw"); prms.Add(new("@fw", fyWeek.Value)); }
        var w = clauses.Count > 0 ? "WHERE " + string.Join(" AND ", clauses) : "";
        return (w, prms);
    }

    private static SqlParameter CloneParam(SqlParameter src) => new(src.ParameterName, src.Value);
}
