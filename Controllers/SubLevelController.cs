using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubLevelController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<SubLevelController> _logger;
    private readonly SubLevelJobService _jobService;

    public SubLevelController(IConfiguration config, ILogger<SubLevelController> logger, SubLevelJobService jobService)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
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
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        ViewBag.WeekCalendars = await LoadWeekCalendars(conn);
        ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_BIN_CAPACITY", "MAJ_CAT");
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_ST_MASTER", "ST_CD");

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
        var result = new List<object>();
        var tables = new[] {
            // Contribution tables
            ("Contribution", "ST_MAJ_CAT_MACRO_MVGR_PLAN", "MVGR Contribution", "Sale/Display x CONT% for MVGR level"),
            ("Contribution", "ST_MAJ_CAT_SZ_PLAN",         "Size Contribution", "Sale/Display x CONT% for Size level"),
            ("Contribution", "ST_MAJ_CAT_SEG_PLAN",        "Segment Contribution", "Sale/Display x CONT% for Segment level"),
            ("Contribution", "ST_MAJ_CAT_VND_PLAN",        "Vendor Contribution", "Sale/Display x CONT% for Vendor level"),
            // Sub-level store stock
            ("Sub Store Stk", "SUB_ST_STK_MVGR", "St Stk MVGR", "Store opening stock for MVGR TRF"),
            ("Sub Store Stk", "SUB_ST_STK_SZ",   "St Stk Size", "Store opening stock for Size TRF"),
            ("Sub Store Stk", "SUB_ST_STK_SEG",  "St Stk Segment", "Store opening stock for Segment TRF"),
            ("Sub Store Stk", "SUB_ST_STK_VND",  "St Stk Vendor", "Store opening stock for Vendor TRF"),
            // Sub-level DC stock
            ("Sub DC Stk", "SUB_DC_STK_MVGR", "DC Stk MVGR", "DC opening stock for MVGR PP"),
            ("Sub DC Stk", "SUB_DC_STK_SZ",   "DC Stk Size", "DC opening stock for Size PP"),
            ("Sub DC Stk", "SUB_DC_STK_SEG",  "DC Stk Segment", "DC opening stock for Segment PP"),
            ("Sub DC Stk", "SUB_DC_STK_VND",  "DC Stk Vendor", "DC opening stock for Vendor PP"),
            // Reference tables
            ("Reference", "QTY_SALE_QTY",             "Sale Qty",           "Weekly sale forecast x CONT% -> sub-level sale"),
            ("Reference", "QTY_DISP_QTY",             "Display Qty",        "Weekly display qty x CONT% -> sub-level display"),
            ("Reference", "MASTER_BIN_CAPACITY",       "Bin Capacity",       "MBQ + BIN_CAP per MAJ_CAT for TRF chain"),
            ("Reference", "MASTER_ST_MASTER",          "Store Master",       "ST_CD -> RDC mapping for PP aggregation"),
            ("Reference", "WEEK_CALENDAR",             "Week Calendar",      "Week ID -> FY_YEAR/FY_WEEK mapping"),
            ("Reference", "MASTER_PRODUCT_HIERARCHY",  "Product Hierarchy",  "MAJ_CAT -> SSN for shrinkage calculation"),
        };

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        foreach (var (cat, tbl, label, logic) in tables)
        {
            int cnt = 0;
            try
            {
                cnt = await SnowflakeCrudHelper.CountAsync(conn, tbl);
            }
            catch { cnt = -1; }
            result.Add(new { category = cat, table = tbl, label, logic, rows = cnt });
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> PlanStatus()
    {
        var result = new List<object>();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        foreach (var (tbl, plan) in new[] { ("TRF_IN_PLAN", "Transfer In Plan"), ("PURCHASE_PLAN", "Purchase Plan") })
        {
            var cnt = await SnowflakeCrudHelper.CountAsync(conn, tbl);
            result.Add(new { plan, rows = cnt });
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> SubOutputStatus()
    {
        var result = new List<object>();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        foreach (var (tbl, label) in new[] { ("SUB_LEVEL_TRF_PLAN", "Sub-Level TRF"), ("SUB_LEVEL_PP_PLAN", "Sub-Level PP") })
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(*), MAX(CREATED_DT) FROM {tbl}";
                cmd.CommandTimeout = 30;
                await using var r = await cmd.ExecuteReaderAsync();
                if (await r.ReadAsync())
                    result.Add(new { table = label, rows = SnowflakeCrudHelper.Int(r, 0), lastRun = r.IsDBNull(1) ? "" : Convert.ToDateTime(r.GetValue(1)).ToString("dd-MMM HH:mm") });
            }
            catch { result.Add(new { table = label, rows = 0, lastRun = "" }); }
        }
        return Json(result);
    }

    [HttpGet]
    public async Task<IActionResult> SubOutputStatusByLevel()
    {
        var result = new List<object>();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        foreach (var lv in new[] { "MVGR", "SZ", "SEG", "VND" })
        {
            int trfRows = 0, ppRows = 0; string? lastRun = null;
            try
            {
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT COUNT(*), MAX(CREATED_DT) FROM SUB_LEVEL_TRF_PLAN WHERE LEVEL = '{lv}'";
                    cmd.CommandTimeout = 30;
                    await using var r = await cmd.ExecuteReaderAsync();
                    if (await r.ReadAsync())
                    {
                        trfRows = SnowflakeCrudHelper.Int(r, 0);
                        if (!r.IsDBNull(1)) lastRun = Convert.ToDateTime(r.GetValue(1)).ToString("dd-MMM HH:mm");
                    }
                }
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT COUNT(*) FROM SUB_LEVEL_PP_PLAN WHERE LEVEL = '{lv}'";
                    cmd.CommandTimeout = 30;
                    ppRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
                }
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
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            if (string.IsNullOrEmpty(level) || level == "all")
            {
                await SnowflakeCrudHelper.ExecAsync(conn, "DELETE FROM SUB_LEVEL_TRF_PLAN");
                await SnowflakeCrudHelper.ExecAsync(conn, "DELETE FROM SUB_LEVEL_PP_PLAN");
                _logger.LogInformation("All sub-level data cleared");
                TempData["SuccessMessage"] = "All sub-level plan data cleared successfully.";
            }
            else
            {
                var lv = level.ToUpper();
                await SnowflakeCrudHelper.ExecAsync(conn, $"DELETE FROM SUB_LEVEL_TRF_PLAN WHERE LEVEL = '{lv}'");
                await SnowflakeCrudHelper.ExecAsync(conn, $"DELETE FROM SUB_LEVEL_PP_PLAN WHERE LEVEL = '{lv}'");
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
    // EXECUTE — GENERATE SUB-LEVEL DATA (sync, single-level)
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
            var scParam = string.IsNullOrEmpty(storeCode) ? "NULL" : $"'{storeCode}'";
            var mcParam = string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'";

            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            foreach (var level in levels)
            {
                var levelKey = level.ToUpper();
                _logger.LogInformation("SubLevel generating TRF [{Level}] weeks {Start}-{End} store={Store} cat={Cat}", levelKey, startWeekId, endWeekId, storeCode ?? "ALL", majCat ?? "ALL");

                // Call TRF SP on Snowflake
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"CALL SF_SP_GENERATE_SUB_LEVEL_TRF('{levelKey}', {startWeekId}, {endWeekId}, {scParam}, {mcParam})";
                    cmd.CommandTimeout = 3600;
                    await cmd.ExecuteNonQueryAsync();
                }

                _logger.LogInformation("SubLevel generating PP [{Level}]", levelKey);

                // Call PP SP on Snowflake
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"CALL SF_SP_GENERATE_SUB_LEVEL_PP('{levelKey}', {startWeekId}, {endWeekId}, NULL, {mcParam})";
                    cmd.CommandTimeout = 3600;
                    await cmd.ExecuteNonQueryAsync();
                }

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
    // TRANSFER IN — SUB-LEVEL OUTPUT
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

        var (where, parms) = BuildTrfFilter(levelKey, stCd, majCat, fyYear, fyWeek);

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        // Load dropdowns
        ViewBag.StoreCodes = new List<string>();
        ViewBag.Categories = new List<string>();
        try
        {
            ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "SUB_LEVEL_TRF_PLAN", "ST_CD", $"LEVEL = '{levelKey}'");
            ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, "SUB_LEVEL_TRF_PLAN", "MAJ_CAT", $"LEVEL = '{levelKey}'");
        }
        catch { /* table may not exist yet */ }

        int totalCount = 0;
        var data = new List<SubLevelTrfRow>();

        try
        {
            totalCount = await SnowflakeCrudHelper.CountAsync(conn, "SUB_LEVEL_TRF_PLAN", where, parms);
            ViewBag.TotalCount = totalCount;

            if (totalCount > 0)
            {
                data = await SnowflakeCrudHelper.PagedQueryAsync(conn, "SUB_LEVEL_TRF_PLAN",
                    "ST_CD, MAJ_CAT, SUB_VALUE, CONT_PCT, FY_YEAR, FY_WEEK, " +
                    "BGT_DISP_CL_Q, CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q, COVER_SALE_QTY, " +
                    "TRF_IN_STK_Q, DC_MBQ, BGT_TTL_CF_OP_STK_Q, BGT_TTL_CF_CL_STK_Q, BGT_ST_CL_MBQ, ST_CL_EXCESS_Q, ST_CL_SHORT_Q",
                    where, parms, "ST_CD, MAJ_CAT, SUB_VALUE, FY_WEEK", page, pageSize,
                    r => new SubLevelTrfRow
                    {
                        StCd = SnowflakeCrudHelper.Str(r, 0),
                        MajCat = SnowflakeCrudHelper.Str(r, 1),
                        SubLevel = SnowflakeCrudHelper.Str(r, 2),
                        ContPct = SnowflakeCrudHelper.Dec(r, 3),
                        FyYear = SnowflakeCrudHelper.Int(r, 4),
                        FyWeek = SnowflakeCrudHelper.Int(r, 5),
                        BgtDispClQ = SnowflakeCrudHelper.Dec(r, 6),
                        CmBgtSaleQ = SnowflakeCrudHelper.Dec(r, 7),
                        Cm1BgtSaleQ = SnowflakeCrudHelper.Dec(r, 8),
                        Cm2BgtSaleQ = SnowflakeCrudHelper.Dec(r, 9),
                        CoverSaleQty = SnowflakeCrudHelper.Dec(r, 10),
                        TrfInStkQ = SnowflakeCrudHelper.Dec(r, 11),
                        DcMbq = SnowflakeCrudHelper.Dec(r, 12),
                        BgtTtlCfOpStkQ = SnowflakeCrudHelper.Dec(r, 13),
                        BgtTtlCfClStkQ = SnowflakeCrudHelper.Dec(r, 14),
                        BgtStClMbq = SnowflakeCrudHelper.Dec(r, 15),
                        StClExcessQ = SnowflakeCrudHelper.Dec(r, 16),
                        StClShortQ = SnowflakeCrudHelper.Dec(r, 17)
                    });
            }
        }
        catch { ViewBag.TotalCount = 0; ViewBag.NotGenerated = true; }

        return View(data);
    }

    // ═════════════════════════════════════════════════════════════
    // PURCHASE PLAN — SUB-LEVEL OUTPUT
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

        var (where, parms) = BuildPpFilter(levelKey, rdcCd, majCat, fyYear, fyWeek);

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        ViewBag.RdcCodes = new List<string>();
        ViewBag.Categories = new List<string>();
        try
        {
            ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "SUB_LEVEL_PP_PLAN", "RDC_CD", $"LEVEL = '{levelKey}'");
            ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, "SUB_LEVEL_PP_PLAN", "MAJ_CAT", $"LEVEL = '{levelKey}'");
        }
        catch { }

        int totalCount = 0;
        var data = new List<SubLevelPpRow>();

        try
        {
            totalCount = await SnowflakeCrudHelper.CountAsync(conn, "SUB_LEVEL_PP_PLAN", where, parms);
            ViewBag.TotalCount = totalCount;

            if (totalCount > 0)
            {
                data = await SnowflakeCrudHelper.PagedQueryAsync(conn, "SUB_LEVEL_PP_PLAN",
                    "RDC_CD, MAJ_CAT, SUB_VALUE, CONT_PCT, FY_YEAR, FY_WEEK, " +
                    "BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, " +
                    "BGT_PUR_Q_INIT, BGT_DC_CL_STK_Q, BGT_DC_CL_MBQ, BGT_DC_MBQ_SALE, DC_STK_EXCESS_Q, DC_STK_SHORT_Q",
                    where, parms, "RDC_CD, MAJ_CAT, SUB_VALUE, FY_WEEK", page, pageSize,
                    r => new SubLevelPpRow
                    {
                        RdcCd = SnowflakeCrudHelper.Str(r, 0),
                        MajCat = SnowflakeCrudHelper.Str(r, 1),
                        SubLevel = SnowflakeCrudHelper.Str(r, 2),
                        ContPct = SnowflakeCrudHelper.Dec(r, 3),
                        FyYear = SnowflakeCrudHelper.Int(r, 4),
                        FyWeek = SnowflakeCrudHelper.Int(r, 5),
                        BgtDispClQ = SnowflakeCrudHelper.Dec(r, 6),
                        CwBgtSaleQ = SnowflakeCrudHelper.Dec(r, 7),
                        Cw1BgtSaleQ = SnowflakeCrudHelper.Dec(r, 8),
                        Cw2BgtSaleQ = SnowflakeCrudHelper.Dec(r, 9),
                        Cw3BgtSaleQ = SnowflakeCrudHelper.Dec(r, 10),
                        Cw4BgtSaleQ = SnowflakeCrudHelper.Dec(r, 11),
                        BgtPurQInit = SnowflakeCrudHelper.Dec(r, 12),
                        BgtDcClStkQ = SnowflakeCrudHelper.Dec(r, 13),
                        BgtDcClMbq = SnowflakeCrudHelper.Dec(r, 14),
                        BgtDcMbqSale = SnowflakeCrudHelper.Dec(r, 15),
                        DcStkExcessQ = SnowflakeCrudHelper.Dec(r, 16),
                        DcStkShortQ = SnowflakeCrudHelper.Dec(r, 17)
                    });
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

        var (where, parms) = BuildTrfJoinWhere(stCd, majCat, fyYear, fyWeek);

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=TrfInPlan_{label.Replace(" ", "")}_{fyYear}_{fyWeek}.csv");

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync($"ST_CD,MAJ_CAT,{subCol},CONT_PCT,FY_YEAR,FY_WEEK,BGT_DISP_CL_Q,CM_BGT_SALE_Q,CM1_BGT_SALE_Q,CM2_BGT_SALE_Q,COVER_SALE_QTY,TRF_IN_STK_Q,DC_MBQ,BGT_TTL_CF_OP_STK_Q,BGT_TTL_CF_CL_STK_Q,BGT_ST_CL_MBQ,ST_CL_EXCESS_Q,ST_CL_SHORT_Q");

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"SELECT
            t.ST_CD, t.MAJ_CAT, c.{subCol}, c.CONT_PCT,
            t.FY_YEAR, t.FY_WEEK,
            NVL(t.BGT_DISP_CL_Q,0)*c.CONT_PCT, NVL(t.CM_BGT_SALE_Q,0)*c.CONT_PCT,
            NVL(t.CM1_BGT_SALE_Q,0)*c.CONT_PCT, NVL(t.CM2_BGT_SALE_Q,0)*c.CONT_PCT,
            NVL(t.COVER_SALE_QTY,0)*c.CONT_PCT,
            NVL(t.TRF_IN_STK_Q,0), NVL(t.DC_MBQ,0),
            NVL(t.BGT_TTL_CF_OP_STK_Q,0), NVL(t.BGT_TTL_CF_CL_STK_Q,0),
            NVL(t.BGT_ST_CL_MBQ,0), NVL(t.ST_CL_EXCESS_Q,0), NVL(t.ST_CL_SHORT_Q,0)
        FROM TRF_IN_PLAN t
        INNER JOIN {contTable} c ON c.ST_CD = t.ST_CD AND c.MAJ_CAT_CD = t.MAJ_CAT
        {where} ORDER BY t.ST_CD, t.MAJ_CAT, c.{subCol}, t.FY_WEEK";
        cmd.CommandTimeout = 600;
        foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

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

        var (where, parms) = BuildPpJoinWhere(rdcCd, majCat, fyYear, fyWeek);

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=PP_{label.Replace(" ", "")}_{fyYear}_{fyWeek}.csv");

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync($"RDC_CD,MAJ_CAT,{subCol},CONT_PCT,FY_YEAR,FY_WEEK,BGT_DISP_CL_Q,CW_BGT_SALE_Q,CW1_BGT_SALE_Q,CW2_BGT_SALE_Q,CW3_BGT_SALE_Q,CW4_BGT_SALE_Q,BGT_PUR_Q_INIT,BGT_DC_CL_STK_Q,BGT_DC_CL_MBQ,BGT_DC_MBQ_SALE,DC_STK_EXCESS_Q,DC_STK_SHORT_Q");

        var cteSql = $@"WITH RdcCont AS (
            SELECT sm.RDC_CD, c.MAJ_CAT_CD, c.{subCol}, AVG(c.CONT_PCT) AS CONT_PCT
            FROM {contTable} c
            INNER JOIN MASTER_ST_MASTER sm ON sm.ST_CD = c.ST_CD
            GROUP BY sm.RDC_CD, c.MAJ_CAT_CD, c.{subCol}
        )";

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"{cteSql}
        SELECT p.RDC_CD, p.MAJ_CAT, rc.{subCol}, rc.CONT_PCT,
            p.FY_YEAR, p.FY_WEEK,
            NVL(p.BGT_DISP_CL_Q,0)*rc.CONT_PCT, NVL(p.CW_BGT_SALE_Q,0)*rc.CONT_PCT,
            NVL(p.CW1_BGT_SALE_Q,0)*rc.CONT_PCT, NVL(p.CW2_BGT_SALE_Q,0)*rc.CONT_PCT,
            NVL(p.CW3_BGT_SALE_Q,0)*rc.CONT_PCT, NVL(p.CW4_BGT_SALE_Q,0)*rc.CONT_PCT,
            NVL(p.BGT_PUR_Q_INIT,0), NVL(p.BGT_DC_CL_STK_Q,0),
            NVL(p.BGT_DC_CL_MBQ,0), NVL(p.BGT_DC_MBQ_SALE,0),
            NVL(p.DC_STK_EXCESS_Q,0), NVL(p.DC_STK_SHORT_Q,0)
        FROM PURCHASE_PLAN p
        INNER JOIN RdcCont rc ON rc.RDC_CD = p.RDC_CD AND rc.MAJ_CAT_CD = p.MAJ_CAT
        {where} ORDER BY p.RDC_CD, p.MAJ_CAT, rc.{subCol}, p.FY_WEEK";
        cmd.CommandTimeout = 600;
        foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

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
    // PIVOT CSV EXPORTS
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

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SubTRF_Pivot_{label.Replace(" ", "")}_{DateTime.Now:yyyyMMdd}.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var (where, parms) = BuildSubLevelTrfPivotFilter(levelKey, stCd, majCat);

            var weeks = new List<int>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT DISTINCT NVL(FY_WEEK, 0) FROM SUB_LEVEL_TRF_PLAN WHERE {where} ORDER BY 1";
                cmd.CommandTimeout = 60;
                foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));
                await using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) weeks.Add(Convert.ToInt32(r.GetValue(0)));
            }

            var hdr = new StringBuilder();
            hdr.Append($"ST_CD,MAJ_CAT,{subCol},CONT_PCT");
            foreach (var w in weeks)
                foreach (var m in TrfPivotMetrics)
                    hdr.Append($",WK{w}_{m}");
            await writer.WriteLineAsync(hdr.ToString());

            await using var cmd2 = conn.CreateCommand();
            cmd2.CommandText = $@"SELECT NVL(ST_CD,'NA'), NVL(MAJ_CAT,'NA'), NVL(SUB_VALUE,'NA'), NVL(CONT_PCT,0), NVL(FY_WEEK,0),
                {string.Join(",", TrfPivotMetrics.Select(m => $"NVL({m},0)"))}
                FROM SUB_LEVEL_TRF_PLAN WHERE {where}
                ORDER BY ST_CD, MAJ_CAT, SUB_VALUE, FY_WEEK";
            cmd2.CommandTimeout = 600;
            var (_, parms2) = BuildSubLevelTrfPivotFilter(levelKey, stCd, majCat);
            foreach (var p in parms2) cmd2.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

            await using var reader = await cmd2.ExecuteReaderAsync();
            string? prevKey = null; string[]? idVals = null;
            var weekData = new Dictionary<int, decimal[]>();
            int count = 0;

            while (await reader.ReadAsync())
            {
                var key = $"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}";
                var week = Convert.ToInt32(reader.GetValue(4));

                if (key != prevKey)
                {
                    if (prevKey != null) { WritePivotLine(writer, idVals!, weeks, weekData, metricCount); count++; if (count % 10000 == 0) await writer.FlushAsync(); }
                    idVals = new[] { Q(reader.GetString(0)), Q(reader.GetString(1)), Q(reader.GetString(2)), Convert.ToDecimal(reader.GetValue(3)).ToString() };
                    weekData.Clear();
                    prevKey = key;
                }
                var vals = new decimal[metricCount];
                for (int i = 0; i < metricCount; i++) vals[i] = Convert.ToDecimal(reader.GetValue(5 + i));
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

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=SubPP_Pivot_{label.Replace(" ", "")}_{DateTime.Now:yyyyMMdd}.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var (where, parms) = BuildSubLevelPpPivotFilter(levelKey, rdcCd, majCat);

            var weeks = new List<int>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"SELECT DISTINCT NVL(FY_WEEK, 0) FROM SUB_LEVEL_PP_PLAN WHERE {where} ORDER BY 1";
                cmd.CommandTimeout = 60;
                foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));
                await using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) weeks.Add(Convert.ToInt32(r.GetValue(0)));
            }

            var hdr = new StringBuilder();
            hdr.Append($"RDC_CD,MAJ_CAT,{subCol},CONT_PCT");
            foreach (var w in weeks)
                foreach (var m in PpPivotMetrics)
                    hdr.Append($",WK{w}_{m}");
            await writer.WriteLineAsync(hdr.ToString());

            await using var cmd2 = conn.CreateCommand();
            cmd2.CommandText = $@"SELECT NVL(RDC_CD,'NA'), NVL(MAJ_CAT,'NA'), NVL(SUB_VALUE,'NA'), NVL(CONT_PCT,0), NVL(FY_WEEK,0),
                {string.Join(",", PpPivotMetrics.Select(m => $"NVL({m},0)"))}
                FROM SUB_LEVEL_PP_PLAN WHERE {where}
                ORDER BY RDC_CD, MAJ_CAT, SUB_VALUE, FY_WEEK";
            cmd2.CommandTimeout = 600;
            var (_, parms2) = BuildSubLevelPpPivotFilter(levelKey, rdcCd, majCat);
            foreach (var p in parms2) cmd2.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

            await using var reader = await cmd2.ExecuteReaderAsync();
            string? prevKey = null; string[]? idVals = null;
            var weekData = new Dictionary<int, decimal[]>();
            int count = 0;

            while (await reader.ReadAsync())
            {
                var key = $"{reader.GetString(0)}|{reader.GetString(1)}|{reader.GetString(2)}";
                var week = Convert.ToInt32(reader.GetValue(4));

                if (key != prevKey)
                {
                    if (prevKey != null) { WritePivotLine(writer, idVals!, weeks, weekData, metricCount); count++; if (count % 10000 == 0) await writer.FlushAsync(); }
                    idVals = new[] { Q(reader.GetString(0)), Q(reader.GetString(1)), Q(reader.GetString(2)), Convert.ToDecimal(reader.GetValue(3)).ToString() };
                    weekData.Clear();
                    prevKey = key;
                }
                var vals = new decimal[metricCount];
                for (int i = 0; i < metricCount; i++) vals[i] = Convert.ToDecimal(reader.GetValue(5 + i));
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

    // ── WHERE clause builders (Snowflake parameterized) ─────────

    private (string where, List<SnowflakeDbParameter> parms) BuildTrfFilter(
        string levelKey, string? stCd, string? majCat, int? fyYear, int? fyWeek)
    {
        var conditions = new List<string> { "LEVEL = ?" };
        var parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", levelKey) };
        int idx = 1;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        if (fyYear.HasValue) { idx++; conditions.Add("FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        return (string.Join(" AND ", conditions), parms);
    }

    private (string where, List<SnowflakeDbParameter> parms) BuildPpFilter(
        string levelKey, string? rdcCd, string? majCat, int? fyYear, int? fyWeek)
    {
        var conditions = new List<string> { "LEVEL = ?" };
        var parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", levelKey) };
        int idx = 1;
        if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        if (fyYear.HasValue) { idx++; conditions.Add("FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        return (string.Join(" AND ", conditions), parms);
    }

    private (string where, List<SnowflakeDbParameter> parms) BuildTrfJoinWhere(
        string? stCd, string? majCat, int? fyYear, int? fyWeek)
    {
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("t.ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("t.MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        if (fyYear.HasValue) { idx++; conditions.Add("t.FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("t.FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        return (conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "", parms);
    }

    private (string where, List<SnowflakeDbParameter> parms) BuildPpJoinWhere(
        string? rdcCd, string? majCat, int? fyYear, int? fyWeek)
    {
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("p.RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("p.MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        if (fyYear.HasValue) { idx++; conditions.Add("p.FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("p.FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        return (conditions.Count > 0 ? "WHERE " + string.Join(" AND ", conditions) : "", parms);
    }

    private (string where, List<SnowflakeDbParameter> parms) BuildSubLevelTrfPivotFilter(string levelKey, string? stCd, string? majCat)
    {
        var conditions = new List<string> { "LEVEL = ?", "FY_WEEK IS NOT NULL" };
        var parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", levelKey) };
        int idx = 1;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        return (string.Join(" AND ", conditions), parms);
    }

    private (string where, List<SnowflakeDbParameter> parms) BuildSubLevelPpPivotFilter(string levelKey, string? rdcCd, string? majCat)
    {
        var conditions = new List<string> { "LEVEL = ?", "FY_WEEK IS NOT NULL" };
        var parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", levelKey) };
        int idx = 1;
        if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        return (string.Join(" AND ", conditions), parms);
    }

    private static async Task<List<WeekCalendar>> LoadWeekCalendars(SnowflakeDbConnection conn)
    {
        var list = new List<WeekCalendar>();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT FROM WEEK_CALENDAR ORDER BY WEEK_ID";
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
            list.Add(new WeekCalendar
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
        return list;
    }
}
