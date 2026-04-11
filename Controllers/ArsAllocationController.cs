using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsAllocationController : Controller
{
    private readonly ArsAllocationJobService _jobService;
    private readonly string _connStr;

    public ArsAllocationController(ArsAllocationJobService jobService, IConfiguration config)
    {
        _jobService = jobService;
        _connStr = config.GetConnectionString("DataV2Database")!;
    }

    // ── EXECUTE PAGE ──
    public async Task<IActionResult> Execute()
    {
        ViewBag.OutputRows = await CountAsync("ARS_ALLOCATION_OUTPUT");
        ViewBag.StockRows = await CountWithDateAsync("ET_STOCK_DATA", "stock_Date");
        ViewBag.MsaRows = await CountWithDateAsync("VIEW_ET_MSA_STOCK_N_1", "MSA_Stock_Date");
        ViewBag.DisplayRows = await CountAsync("ARS_ST_MJ_DISPLAY_MASTER");
        ViewBag.AutoSaleRows = await CountAsync("ARS_ST_MJ_AUTO_SALE");
        ViewBag.ArtSaleRows = await CountAsync("ARS_ST_ART_AUTO_SALE");
        ViewBag.HoldDaysRows = await CountAsync("ARS_HOLD_DAYS_MASTER");
        ViewBag.StMasterRows = await CountAsync("ARS_ST_MASTER");
        ViewBag.LatestRun = await GetLatestRunAsync();
        return View();
    }

    // ── START RUN ──
    [HttpPost]
    public IActionResult StartRun()
    {
        var started = _jobService.TryStartRun();
        return Json(new { success = started, message = started ? "Allocation run started." : "Already running." });
    }

    // ── JOB STATUS ──
    [HttpGet]
    public IActionResult JobStatus() => Json(_jobService.GetStatus());

    // ── RESET ──
    [HttpPost]
    public async Task<IActionResult> Reset()
    {
        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "TRUNCATE TABLE dbo.ARS_ALLOCATION_OUTPUT";
        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Allocation output cleared.";
        return RedirectToAction("Execute");
    }

    // ── OUTPUT PAGE ──
    public async Task<IActionResult> Output(string? st, string? mj, string? artClass,
        string sortCol = "ST", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(st)) { where.Append(" AND ST = @st"); parms.Add(new SqlParameter("@st", st)); }
        if (!string.IsNullOrEmpty(mj)) { where.Append(" AND MJ = @mj"); parms.Add(new SqlParameter("@mj", mj)); }
        if (!string.IsNullOrEmpty(artClass)) { where.Append(" AND ART_CLASS = @cls"); parms.Add(new SqlParameter("@cls", artClass)); }

        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "ST", "MJ", "GEN-ART", "ART_CLASS", "TTL_ST_STK_Q", "ART_ALC_Q", "ART_HOLD_Q", "ST_MJ_MBQ", "ST_ART_REQ" };
        if (!validCols.Contains(sortCol)) sortCol = "ST";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT COUNT(1), COUNT(DISTINCT ST), COUNT(DISTINCT [GEN-ART]),
                ISNULL(SUM(ART_ALC_Q), 0), ISNULL(SUM(ART_HOLD_Q), 0),
                SUM(CASE WHEN ART_CLASS='L-ART' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ART_CLASS='MIX-ART' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ART_CLASS='OLD-ART' THEN 1 ELSE 0 END),
                SUM(CASE WHEN ART_CLASS='NEW-L' THEN 1 ELSE 0 END)
                FROM dbo.ARS_ALLOCATION_OUTPUT WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalCount = r.GetInt32(0); ViewBag.TotalStores = r.GetInt32(1);
                ViewBag.TotalArticles = r.GetInt32(2);
                ViewBag.TotalAlcQty = r.GetDecimal(3); ViewBag.TotalHoldQty = r.GetDecimal(4);
                ViewBag.LCount = r.GetInt32(5); ViewBag.MixCount = r.GetInt32(6);
                ViewBag.OldCount = r.GetInt32(7); ViewBag.NewLCount = r.GetInt32(8);
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT ST, ST_NM, MJ, [GEN-ART], ART_CLASS,
                TTL_ST_STK_Q, MSA_QTY, TTL_ALC_DAYS, HOLD_DAYS,
                ACC_DENSITY, ST_MJ_DISP_Q, ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
                ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ,
                ART_ALC_Q, ART_HOLD_Q, REM_MSA
                FROM dbo.ARS_ALLOCATION_OUTPUT WITH (NOLOCK) {where}
                ORDER BY [{sortCol}] {dir}
                OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++)
                    row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
                rows.Add(row);
            }
        }

        ViewBag.Rows = rows; ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = dir;
        ViewBag.St = st; ViewBag.Mj = mj; ViewBag.ArtClass = artClass;
        ViewBag.Stores = await GetDistinctAsync("ST");
        ViewBag.MajCats = await GetDistinctAsync("MJ");
        ViewBag.ArtClasses = await GetDistinctAsync("ART_CLASS");
        return View();
    }

    // ── CSV EXPORT ──
    public async Task ExportCsv(string? st, string? mj, string? artClass)
    {
        var where = new StringBuilder("WHERE 1=1");
        if (!string.IsNullOrEmpty(st)) where.Append($" AND ST = '{st}'");
        if (!string.IsNullOrEmpty(mj)) where.Append($" AND MJ = '{mj}'");
        if (!string.IsNullOrEmpty(artClass)) where.Append($" AND ART_CLASS = '{artClass}'");

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=ARS_ALLOCATION_OUTPUT.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("ST,ST_NM,MJ,GEN-ART,ART_CLASS,TTL_ST_STK_Q,MSA_QTY,TTL_ALC_DAYS,HOLD_DAYS,ACC_DENSITY,ST_MJ_MBQ,ART_MBQ,ART_HOLD_MBQ,ST_MJ_REQ,ST_ART_REQ,ART_ALC_Q,ART_HOLD_Q,REM_MSA");

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ST,ST_NM,MJ,[GEN-ART],ART_CLASS,TTL_ST_STK_Q,MSA_QTY,TTL_ALC_DAYS,HOLD_DAYS,ACC_DENSITY,ST_MJ_MBQ,ART_MBQ,ART_HOLD_MBQ,ST_MJ_REQ,ST_ART_REQ,ART_ALC_Q,ART_HOLD_Q,REM_MSA FROM dbo.ARS_ALLOCATION_OUTPUT WITH (NOLOCK) {where} ORDER BY ST,MJ";
        cmd.CommandTimeout = 300;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < r.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = r.IsDBNull(i) ? "" : r.GetValue(i).ToString() ?? "";
                if (val.Contains(',') || val.Contains('"')) sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    // ── Helpers ──
    private async Task<int> CountAsync(string table)
    {
        try
        {
            await using var conn = new SqlConnection(_connStr);
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
            await using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT TOP 1 RUN_ID FROM dbo.ARS_RUN_LOG WITH (NOLOCK) ORDER BY STARTED_DT DESC";
            return (await cmd.ExecuteScalarAsync())?.ToString();
        }
        catch { return null; }
    }

    private async Task<int> CountWithDateAsync(string table, string dateColumn)
    {
        try
        {
            await using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
            SELECT COUNT(1)
            FROM dbo.[{table}] WITH (NOLOCK)
            WHERE [{dateColumn}] = (
                SELECT MAX([{dateColumn}])
                FROM dbo.[{table}] WITH (NOLOCK)
            )";

            return (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }
        catch
        {
            return 0;
        }
    }
    private async Task<List<string>> GetDistinctAsync(string col)
    {
        var list = new List<string>();
        try
        {
            await using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT DISTINCT [{col}] FROM dbo.ARS_ALLOCATION_OUTPUT WITH (NOLOCK) WHERE [{col}] IS NOT NULL ORDER BY 1";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) list.Add(r.GetString(0));
        }
        catch { }
        return list;
    }

    private static SqlParameter Clone(SqlParameter p) => new(p.ParameterName, p.SqlDbType) { Value = p.Value };
}
