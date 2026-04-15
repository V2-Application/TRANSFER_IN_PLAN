using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class PurchasePlanController : Controller
{
    private readonly string _sfConnStr;
    private readonly PlanService _planService;
    private readonly ILogger<PurchasePlanController> _logger;

    public PurchasePlanController(IConfiguration config, PlanService planService, ILogger<PurchasePlanController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _planService = planService;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> Execute()
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        ViewBag.WeekCalendars = await LoadWeekCalendars(conn);
        ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_ST_MASTER", "RDC_CD");
        ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_PRODUCT_HIERARCHY", "MAJ_CAT_NM", "MAJ_CAT_NM != 'NA'");

        return View(new PurchasePlanExecutionParams());
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Execute(PurchasePlanExecutionParams model)
    {
        if (!ModelState.IsValid)
        {
            await using var conn0 = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            ViewBag.WeekCalendars = await LoadWeekCalendars(conn0);
            ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn0, "MASTER_ST_MASTER", "RDC_CD");
            ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn0, "MASTER_PRODUCT_HIERARCHY", "MAJ_CAT_NM", "MAJ_CAT_NM != 'NA'");
            return View(model);
        }
        try
        {
            var (rowsInserted, executionTime) = await _planService.ExecutePurchasePlanAsync(
                model.StartWeekId, model.EndWeekId, model.RdcCode, model.MajCat);

            _logger.LogInformation("PurchasePlan SP executed: StartWeek={Start} EndWeek={End} Rows={Rows}", model.StartWeekId, model.EndWeekId, rowsInserted);
            TempData["SuccessMessage"] = $"Purchase Plan executed successfully! {rowsInserted} rows at {executionTime:yyyy-MM-dd HH:mm:ss}";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing PurchasePlan SP");
            TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
        }
        return RedirectToAction(nameof(Execute));
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> ResetPp()
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.ExecAsync(conn, "DELETE FROM PURCHASE_PLAN");
            _logger.LogInformation("PURCHASE_PLAN cleared");
            TempData["SuccessMessage"] = "Purchase Plan data cleared successfully.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ResetPp error");
            TempData["ErrorMessage"] = $"Error: {ex.InnerException?.Message ?? ex.Message}";
        }
        return RedirectToAction(nameof(Execute));
    }

    [HttpGet]
    public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? rdcCd, string? majCat, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = BuildFilter(fyYear, fyWeek, rdcCd, majCat);

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, "PURCHASE_PLAN", where, parms);
        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.FyYear = fyYear;
        ViewBag.FyWeek = fyWeek;
        ViewBag.RdcCd = rdcCd;
        ViewBag.MajCat = majCat;
        ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, "PURCHASE_PLAN", "MAJ_CAT");
        ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "PURCHASE_PLAN", "RDC_CD");

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, "PURCHASE_PLAN", AllSelectCols, where, parms,
            "RDC_CD, MAJ_CAT", page, pageSize, MapPurchasePlan);

        _logger.LogInformation("PurchasePlan Output: {Count} rows returned", data.Count);
        return View(data);
    }

    // ── CSV EXPORT ────────────────────────────────────────────────

    [HttpGet]
    public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
    {
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=PurchasePlan_{fyYear}_{fyWeek}.csv");

        var (where, parms) = BuildFilter(fyYear, fyWeek, rdcCd, majCat);
        var whereStr = string.IsNullOrEmpty(where) ? "" : " WHERE " + where;
        var sql = $"SELECT {AllSelectCols} FROM PURCHASE_PLAN{whereStr} ORDER BY RDC_CD, MAJ_CAT";

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);
        await writer.WriteLineAsync("RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek," +
            "DcStkQ,GrtStkQ,SGrtStkQ,WGrtStkQ,BinCapDcTeam,BinCap,BgtDispClQ," +
            "CwBgtSaleQ,Cw1BgtSaleQ,Cw2BgtSaleQ,Cw3BgtSaleQ,Cw4BgtSaleQ,Cw5BgtSaleQ," +
            "BgtStOpMbq,NetStOpStkQ,BgtDcOpStkQ,PpNtActQ,BgtCfStkQ," +
            "TtlStk,OpStk,NtActStk,GrtConsPct,GrtConsQ,DelPendQ," +
            "PpNetBgtCfStkQ,CwTrfOutQ,Cw1TrfOutQ,Cw2TrfOutQ,Cw3TrfOutQ,Cw4TrfOutQ,TtlTrfOutQ," +
            "BgtStClMbq,NetBgtStClStkQ,NetSsnlClStkQ," +
            "BgtDcMbqSale,BgtDcClMbq,BgtDcClStkQ,BgtPurQInit," +
            "PosPORaised,NegPORaised,BgtCoClStkQ," +
            "DcStkExcessQ,DcStkShortQ,StStkExcessQ,StStkShortQ," +
            "CoStkExcessQ,CoStkShortQ,FreshBinReq,GrtBinReq");

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
            // RDC_CD, RDC_NM, MAJ_CAT, SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN
            for (int i = 0; i < 8; i++)
            {
                if (i > 0) sb.Append(',');
                sb.Append(Q(reader.IsDBNull(i) ? "NA" : reader.GetValue(i)?.ToString()));
            }
            // FY_YEAR, FY_WEEK
            sb.Append(',').Append(reader.IsDBNull(8) ? 0 : reader.GetValue(8));
            sb.Append(',').Append(reader.IsDBNull(9) ? 0 : reader.GetValue(9));
            // All metric columns (indices 10+)
            for (int i = 10; i < reader.FieldCount; i++)
            {
                sb.Append(',').Append(reader.IsDBNull(i) ? "0" : reader.GetValue(i));
            }
            await writer.WriteLineAsync(sb.ToString());
            count++;
            if (count % 100000 == 0) await writer.FlushAsync();
        }
        await writer.FlushAsync();
        _logger.LogInformation("PurchasePlan ExportCsv: {Count:N0} rows streamed", count);
        return new EmptyResult();
    }

    [HttpGet]
    public async Task<IActionResult> ExportPivotCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
    {
        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename=PurchasePlan_Pivot_{fyYear}_{fyWeek}.csv");

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = BuildFilter(fyYear, fyWeek, rdcCd, majCat);
        var whereStr = string.IsNullOrEmpty(where) ? "" : " WHERE " + where;

        // Step 1: Get distinct weeks
        var weeks = new List<int>();
        await using (var cmd1 = conn.CreateCommand())
        {
            cmd1.CommandText = $"SELECT DISTINCT NVL(FY_WEEK, 0) AS W FROM PURCHASE_PLAN{whereStr} ORDER BY W";
            cmd1.CommandTimeout = 120;
            foreach (var p in parms) cmd1.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));
            await using var r1 = await cmd1.ExecuteReaderAsync();
            while (await r1.ReadAsync()) weeks.Add(Convert.ToInt32(r1.GetValue(0)));
        }

        if (weeks.Count == 0)
        {
            await using var writer0 = new StreamWriter(Response.Body, Encoding.UTF8);
            await writer0.WriteLineAsync("No data");
            await writer0.FlushAsync();
            return new EmptyResult();
        }

        var firstWeek = weeks[0];

        // Metrics only for Week 1 (opening week)
        var week1Only = new HashSet<string> { "DC_STK_Q", "GRT_STK_Q", "S_GRT_STK_Q", "W_GRT_STK_Q", "BIN_CAP_DC_TEAM", "BIN_CAP" };

        // All metrics per week
        var metricLabels = new[] {
            "DC_STK_Q","GRT_STK_Q","S_GRT_STK_Q","W_GRT_STK_Q","BIN_CAP_DC_TEAM","BIN_CAP",
            "BGT_DISP_CL_Q","CW_BGT_SALE_Q","CW1_BGT_SALE_Q","CW2_BGT_SALE_Q","CW3_BGT_SALE_Q","CW4_BGT_SALE_Q","CW5_BGT_SALE_Q",
            "BGT_ST_OP_MBQ","NET_ST_OP_STK_Q","BGT_DC_OP_STK_Q","PP_NT_ACT_Q","BGT_CF_STK_Q",
            "TTL_STK","OP_STK","NT_ACT_STK","GRT_CONS_PCT","GRT_CONS_Q","DEL_PEND_Q",
            "PP_NET_BGT_CF_STK_Q","CW_TRF_OUT_Q","CW1_TRF_OUT_Q","CW2_TRF_OUT_Q","CW3_TRF_OUT_Q","CW4_TRF_OUT_Q","TTL_TRF_OUT_Q",
            "BGT_ST_CL_MBQ","NET_BGT_ST_CL_STK_Q","NET_SSNL_CL_STK_Q",
            "BGT_DC_MBQ_SALE","BGT_DC_CL_MBQ","BGT_DC_CL_STK_Q","BGT_PUR_Q_INIT",
            "POS_PO_RAISED","NEG_PO_RAISED","BGT_CO_CL_STK_Q",
            "DC_STK_EXCESS_Q","DC_STK_SHORT_Q","ST_STK_EXCESS_Q","ST_STK_SHORT_Q",
            "CO_STK_EXCESS_Q","CO_STK_SHORT_Q","FRESH_BIN_REQ","GRT_BIN_REQ"
        };

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, 65536);

        // Header
        var hdr = new StringBuilder();
        hdr.Append("RDC_CD,RDC_NM,MAJ_CAT,SEG,DIV,SUB_DIV,MAJ_CAT_NM,SSN");
        foreach (var w in weeks)
            foreach (var m in metricLabels)
                if (w == firstWeek || !week1Only.Contains(m))
                    hdr.Append($",WK-{w}_{m}");
        await writer.WriteLineAsync(hdr.ToString());

        // Step 2: Stream rows grouped by RDC + MAJ_CAT
        var sql = $"SELECT {AllSelectCols} FROM PURCHASE_PLAN{whereStr} ORDER BY RDC_CD, MAJ_CAT, FY_WEEK";
        var (_, parms2) = BuildFilter(fyYear, fyWeek, rdcCd, majCat);
        await using var cmd2 = conn.CreateCommand();
        cmd2.CommandText = sql;
        cmd2.CommandTimeout = 600;
        foreach (var p in parms2) cmd2.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

        await using var reader = await cmd2.ExecuteReaderAsync();

        // Build a map of metric column name -> index in reader for quick lookup
        var metricIdxMap = new Dictionary<string, int>();
        for (int c = 0; c < reader.FieldCount; c++)
            metricIdxMap[reader.GetName(c).ToUpper()] = c;

        string? prevKey = null;
        string[]? idValues = null;
        var weekData = new Dictionary<int, decimal[]>();
        int pivotCount = 0;

        while (await reader.ReadAsync())
        {
            var rdcVal = reader["RDC_CD"]?.ToString() ?? "";
            var majCatVal = reader["MAJ_CAT"]?.ToString() ?? "";
            var currentKey = rdcVal + "|" + majCatVal;
            var week = reader["FY_WEEK"] == DBNull.Value ? 0 : Convert.ToInt32(reader["FY_WEEK"]);

            if (currentKey != prevKey)
            {
                if (prevKey != null && idValues != null)
                {
                    WritePpPivotRow(writer, idValues, weeks, firstWeek, weekData, metricLabels, week1Only);
                    pivotCount++;
                    if (pivotCount % 10000 == 0) await writer.FlushAsync();
                }
                idValues = new[] {
                    Q(rdcVal), Q(reader["RDC_NM"]?.ToString()), Q(majCatVal),
                    Q(reader["SEG"]?.ToString() ?? "NA"), Q(reader["DIV"]?.ToString() ?? "NA"),
                    Q(reader["SUB_DIV"]?.ToString() ?? "NA"), Q(reader["MAJ_CAT_NM"]?.ToString() ?? "NA"),
                    Q(reader["SSN"]?.ToString() ?? "NA")
                };
                weekData.Clear();
                prevKey = currentKey;
            }

            var vals = new decimal[metricLabels.Length];
            for (int i = 0; i < metricLabels.Length; i++)
            {
                if (metricIdxMap.TryGetValue(metricLabels[i], out var colIdx))
                    vals[i] = reader.IsDBNull(colIdx) ? 0m : Convert.ToDecimal(reader.GetValue(colIdx));
            }
            weekData[week] = vals;
        }

        if (prevKey != null && idValues != null)
            WritePpPivotRow(writer, idValues, weeks, firstWeek, weekData, metricLabels, week1Only);

        await writer.FlushAsync();
        _logger.LogInformation("PurchasePlan ExportPivotCsv: {Count:N0} pivot rows, {Metrics} metrics x {Weeks} weeks", pivotCount + 1, metricLabels.Length, weeks.Count);
        return new EmptyResult();
    }

    private static void WritePpPivotRow(StreamWriter writer, string[] idValues, List<int> weeks, int firstWeek,
        Dictionary<int, decimal[]> weekData, string[] metricLabels, HashSet<string> week1Only)
    {
        var sb = new StringBuilder(2048);
        sb.Append(string.Join(",", idValues));
        foreach (var w in weeks)
        {
            var hasData = weekData.TryGetValue(w, out var vals);
            for (int i = 0; i < metricLabels.Length; i++)
            {
                if (w != firstWeek && week1Only.Contains(metricLabels[i])) continue;
                sb.Append(',').Append(hasData ? vals![i] : 0);
            }
        }
        writer.WriteLine(sb.ToString());
    }

    // ── Helpers ───────────────────────────────────────────────────

    private static readonly string AllSelectCols =
        "RDC_CD, RDC_NM, MAJ_CAT, SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN, FY_YEAR, FY_WEEK, " +
        "DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q, BIN_CAP_DC_TEAM, BIN_CAP, " +
        "BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, CW5_BGT_SALE_Q, " +
        "BGT_ST_OP_MBQ, NET_ST_OP_STK_Q, BGT_DC_OP_STK_Q, PP_NT_ACT_Q, BGT_CF_STK_Q, " +
        "TTL_STK, OP_STK, NT_ACT_STK, GRT_CONS_PCT, GRT_CONS_Q, DEL_PEND_Q, " +
        "PP_NET_BGT_CF_STK_Q, CW_TRF_OUT_Q, CW1_TRF_OUT_Q, CW2_TRF_OUT_Q, CW3_TRF_OUT_Q, CW4_TRF_OUT_Q, TTL_TRF_OUT_Q, " +
        "BGT_ST_CL_MBQ, NET_BGT_ST_CL_STK_Q, NET_SSNL_CL_STK_Q, " +
        "BGT_DC_MBQ_SALE, BGT_DC_CL_MBQ, BGT_DC_CL_STK_Q, BGT_PUR_Q_INIT, " +
        "POS_PO_RAISED, NEG_PO_RAISED, BGT_CO_CL_STK_Q, " +
        "DC_STK_EXCESS_Q, DC_STK_SHORT_Q, ST_STK_EXCESS_Q, ST_STK_SHORT_Q, " +
        "CO_STK_EXCESS_Q, CO_STK_SHORT_Q, FRESH_BIN_REQ, GRT_BIN_REQ";

    private (string? where, List<SnowflakeDbParameter> parms) BuildFilter(
        int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
    {
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (fyYear.HasValue) { idx++; conditions.Add("FY_YEAR = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyYear.Value, DbType.Int32)); }
        if (fyWeek.HasValue) { idx++; conditions.Add("FY_WEEK = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), fyWeek.Value, DbType.Int32)); }
        if (!string.IsNullOrEmpty(rdcCd))
        {
            var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (rdcs.Length == 1) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcs[0].Trim())); }
            else
            {
                var inList = string.Join(",", rdcs.Select(r => { idx++; parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), r.Trim())); return "?"; }));
                conditions.Add($"RDC_CD IN ({inList})");
            }
        }
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
        return (conditions.Count > 0 ? string.Join(" AND ", conditions) : null, parms);
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        return s;
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

    private static PurchasePlan MapPurchasePlan(IDataReader r)
    {
        return new PurchasePlan
        {
            RdcCd = SnowflakeCrudHelper.StrNull(r, 0),
            RdcNm = SnowflakeCrudHelper.StrNull(r, 1),
            MajCat = SnowflakeCrudHelper.StrNull(r, 2),
            Seg = SnowflakeCrudHelper.StrNull(r, 3),
            Div = SnowflakeCrudHelper.StrNull(r, 4),
            SubDiv = SnowflakeCrudHelper.StrNull(r, 5),
            MajCatNm = SnowflakeCrudHelper.StrNull(r, 6),
            Ssn = SnowflakeCrudHelper.StrNull(r, 7),
            FyYear = SnowflakeCrudHelper.IntNull(r, 8),
            FyWeek = SnowflakeCrudHelper.IntNull(r, 9),
            DcStkQ = SnowflakeCrudHelper.DecNull(r, 10),
            GrtStkQ = SnowflakeCrudHelper.DecNull(r, 11),
            SGrtStkQ = SnowflakeCrudHelper.DecNull(r, 12),
            WGrtStkQ = SnowflakeCrudHelper.DecNull(r, 13),
            BinCapDcTeam = SnowflakeCrudHelper.DecNull(r, 14),
            BinCap = SnowflakeCrudHelper.DecNull(r, 15),
            BgtDispClQ = SnowflakeCrudHelper.DecNull(r, 16),
            CwBgtSaleQ = SnowflakeCrudHelper.DecNull(r, 17),
            Cw1BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 18),
            Cw2BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 19),
            Cw3BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 20),
            Cw4BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 21),
            Cw5BgtSaleQ = SnowflakeCrudHelper.DecNull(r, 22),
            BgtStOpMbq = SnowflakeCrudHelper.DecNull(r, 23),
            NetStOpStkQ = SnowflakeCrudHelper.DecNull(r, 24),
            BgtDcOpStkQ = SnowflakeCrudHelper.DecNull(r, 25),
            PpNtActQ = SnowflakeCrudHelper.DecNull(r, 26),
            BgtCfStkQ = SnowflakeCrudHelper.DecNull(r, 27),
            TtlStk = SnowflakeCrudHelper.DecNull(r, 28),
            OpStk = SnowflakeCrudHelper.DecNull(r, 29),
            NtActStk = SnowflakeCrudHelper.DecNull(r, 30),
            GrtConsPct = SnowflakeCrudHelper.DecNull(r, 31),
            GrtConsQ = SnowflakeCrudHelper.DecNull(r, 32),
            DelPendQ = SnowflakeCrudHelper.DecNull(r, 33),
            PpNetBgtCfStkQ = SnowflakeCrudHelper.DecNull(r, 34),
            CwTrfOutQ = SnowflakeCrudHelper.DecNull(r, 35),
            Cw1TrfOutQ = SnowflakeCrudHelper.DecNull(r, 36),
            Cw2TrfOutQ = SnowflakeCrudHelper.DecNull(r, 37),
            Cw3TrfOutQ = SnowflakeCrudHelper.DecNull(r, 38),
            Cw4TrfOutQ = SnowflakeCrudHelper.DecNull(r, 39),
            TtlTrfOutQ = SnowflakeCrudHelper.DecNull(r, 40),
            BgtStClMbq = SnowflakeCrudHelper.DecNull(r, 41),
            NetBgtStClStkQ = SnowflakeCrudHelper.DecNull(r, 42),
            NetSsnlClStkQ = SnowflakeCrudHelper.DecNull(r, 43),
            BgtDcMbqSale = SnowflakeCrudHelper.DecNull(r, 44),
            BgtDcClMbq = SnowflakeCrudHelper.DecNull(r, 45),
            BgtDcClStkQ = SnowflakeCrudHelper.DecNull(r, 46),
            BgtPurQInit = SnowflakeCrudHelper.DecNull(r, 47),
            PosPORaised = SnowflakeCrudHelper.DecNull(r, 48),
            NegPORaised = SnowflakeCrudHelper.DecNull(r, 49),
            BgtCoClStkQ = SnowflakeCrudHelper.DecNull(r, 50),
            DcStkExcessQ = SnowflakeCrudHelper.DecNull(r, 51),
            DcStkShortQ = SnowflakeCrudHelper.DecNull(r, 52),
            StStkExcessQ = SnowflakeCrudHelper.DecNull(r, 53),
            StStkShortQ = SnowflakeCrudHelper.DecNull(r, 54),
            CoStkExcessQ = SnowflakeCrudHelper.DecNull(r, 55),
            CoStkShortQ = SnowflakeCrudHelper.DecNull(r, 56),
            FreshBinReq = SnowflakeCrudHelper.DecNull(r, 57),
            GrtBinReq = SnowflakeCrudHelper.DecNull(r, 58)
        };
    }
}
