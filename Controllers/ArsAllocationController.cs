using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsAllocationController : Controller
{
    private readonly ArsAllocationJobService _jobService;
    private readonly string _sfConnStr;

    public ArsAllocationController(ArsAllocationJobService jobService, IConfiguration config)
    {
        _jobService = jobService;
        _sfConnStr = config.GetConnectionString("Snowflake")!;
    }

    // ── EXECUTE PAGE ──
    public async Task<IActionResult> Execute()
    {
        ViewBag.OutputRows = await CountAsync("ARS_ALLOCATION_OUTPUT");
        ViewBag.StockRows = await CountWithDateAsync("ET_STOCK_DATA", "STOCK_DATE");
        ViewBag.MsaRows = await CountWithDateAsync("ET_MSA_STOCK", "MSA_STOCK_DATE");
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
        await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM ARS_ALLOCATION_OUTPUT";
        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Allocation output cleared.";
        return RedirectToAction("Execute");
    }

    // ── OUTPUT PAGE ──
    public async Task<IActionResult> Output(string? st, string? mj, string? artClass,
        string sortCol = "ST", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<(string name, object val)>();
        int pIdx = 0;
        if (!string.IsNullOrEmpty(st)) { pIdx++; where.Append($" AND ST = :p{pIdx}"); parms.Add(($"p{pIdx}", st)); }
        if (!string.IsNullOrEmpty(mj)) { pIdx++; where.Append($" AND MJ = :p{pIdx}"); parms.Add(($"p{pIdx}", mj)); }
        if (!string.IsNullOrEmpty(artClass)) { pIdx++; where.Append($" AND ART_CLASS = :p{pIdx}"); parms.Add(($"p{pIdx}", artClass)); }

        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "ST", "MJ", "GEN_ART", "ART_CLASS", "TTL_ST_STK_Q", "MSA_QTY", "ART_ALC_Q", "ART_HOLD_Q", "ST_MJ_MBQ", "ST_ART_REQ", "TAGGED_RDC", "ART_AUTO_SALE_PD", "RUN_ID", "CREATED_DT", "REM_MSA" };
        if (!validCols.Contains(sortCol)) sortCol = "ST";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
        await conn.OpenAsync();

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT COUNT(1), COUNT(DISTINCT ST), COUNT(DISTINCT GEN_ART),
                NVL(SUM(ART_ALC_Q), 0), NVL(SUM(ART_HOLD_Q), 0),
                NVL(SUM(CASE WHEN ART_CLASS='L-ART' THEN 1 ELSE 0 END), 0),
                NVL(SUM(CASE WHEN ART_CLASS='MIX-ART' THEN 1 ELSE 0 END), 0),
                NVL(SUM(CASE WHEN ART_CLASS='OLD-ART' THEN 1 ELSE 0 END), 0),
                NVL(SUM(CASE WHEN ART_CLASS='NEW-L' THEN 1 ELSE 0 END), 0)
                FROM ARS_ALLOCATION_OUTPUT {where}";
            AddParams(cmd, parms);
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalCount = ToInt(r, 0);
                ViewBag.TotalStores = ToInt(r, 1);
                ViewBag.TotalArticles = ToInt(r, 2);
                ViewBag.TotalAlcQty = ToDec(r, 3);
                ViewBag.TotalHoldQty = ToDec(r, 4);
                ViewBag.LCount = ToInt(r, 5);
                ViewBag.MixCount = ToInt(r, 6);
                ViewBag.OldCount = ToInt(r, 7);
                ViewBag.NewLCount = ToInt(r, 8);
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT
                RUN_ID, ST, ST_NM, TAGGED_RDC, HUB_CD, MJ, GEN_ART, CLR, SZ, ART_CLASS,
                TTL_ST_STK_Q, STK_0001, STK_0002, STK_0004, STK_0006,
                STK_HUB_INTRA, STK_HUB_PRD, STK_INTRA, STK_PRD, MSA_QTY,
                TTL_ALC_DAYS, HOLD_DAYS, CM_PD_SALE_Q, NM_PD_SALE_Q, ART_AUTO_SALE_PD,
                ACC_DENSITY, ST_MJ_DISP_Q, ST_MJ_MBQ, ART_MBQ, ART_HOLD_MBQ,
                VAR_ART_MBQ, VAR_ART_HOLD_MBQ,
                ST_MJ_REQ, ST_ART_REQ, ST_ART_HOLD_REQ, ST_VAR_ART_REQ, ST_VAR_ART_HOLD_REQ,
                ART_ALC_Q, ART_HOLD_Q, REM_MSA, CREATED_DT
                FROM ARS_ALLOCATION_OUTPUT {where}
                ORDER BY {sortCol} {dir}
                LIMIT {pageSize} OFFSET {offset}";
            AddParams(cmd, parms);
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
        if (!string.IsNullOrEmpty(st)) where.Append($" AND ST = '{Esc(st)}'");
        if (!string.IsNullOrEmpty(mj)) where.Append($" AND MJ = '{Esc(mj)}'");
        if (!string.IsNullOrEmpty(artClass)) where.Append($" AND ART_CLASS = '{Esc(artClass)}'");

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=ARS_ALLOCATION_OUTPUT.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("RUN_ID,ST,ST_NM,TAGGED_RDC,HUB_CD,MJ,GEN_ART,CLR,SZ,ART_CLASS,TTL_ST_STK_Q,STK_0001,STK_0002,STK_0004,STK_0006,STK_HUB_INTRA,STK_HUB_PRD,STK_INTRA,STK_PRD,MSA_QTY,TTL_ALC_DAYS,HOLD_DAYS,CM_PD_SALE_Q,NM_PD_SALE_Q,ART_AUTO_SALE_PD,ACC_DENSITY,ST_MJ_DISP_Q,ST_MJ_MBQ,ART_MBQ,ART_HOLD_MBQ,VAR_ART_MBQ,VAR_ART_HOLD_MBQ,ST_MJ_REQ,ST_ART_REQ,ST_ART_HOLD_REQ,ST_VAR_ART_REQ,ST_VAR_ART_HOLD_REQ,ART_ALC_Q,ART_HOLD_Q,REM_MSA,CREATED_DT");

        await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"SELECT RUN_ID,ST,ST_NM,TAGGED_RDC,HUB_CD,MJ,GEN_ART,CLR,SZ,ART_CLASS,
            TTL_ST_STK_Q,STK_0001,STK_0002,STK_0004,STK_0006,STK_HUB_INTRA,STK_HUB_PRD,STK_INTRA,STK_PRD,MSA_QTY,
            TTL_ALC_DAYS,HOLD_DAYS,CM_PD_SALE_Q,NM_PD_SALE_Q,ART_AUTO_SALE_PD,
            ACC_DENSITY,ST_MJ_DISP_Q,ST_MJ_MBQ,ART_MBQ,ART_HOLD_MBQ,VAR_ART_MBQ,VAR_ART_HOLD_MBQ,
            ST_MJ_REQ,ST_ART_REQ,ST_ART_HOLD_REQ,ST_VAR_ART_REQ,ST_VAR_ART_HOLD_REQ,
            ART_ALC_Q,ART_HOLD_Q,REM_MSA,CREATED_DT
            FROM ARS_ALLOCATION_OUTPUT {where} ORDER BY ST,MJ";
        cmd.CommandTimeout = 300;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < r.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = r.IsDBNull(i) ? "" : r.GetValue(i)?.ToString() ?? "";
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
            await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(1) FROM {table}";
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result ?? 0);
        }
        catch { return 0; }
    }

    private async Task<int> CountWithDateAsync(string table, string dateColumn)
    {
        try
        {
            await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(1) FROM {table} WHERE {dateColumn} = CURRENT_DATE() - 1";
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result ?? 0);
        }
        catch { return 0; }
    }

    private async Task<string?> GetLatestRunAsync()
    {
        try
        {
            await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT RUN_ID FROM ARS_RUN_LOG ORDER BY STARTED_DT DESC LIMIT 1";
            return (await cmd.ExecuteScalarAsync())?.ToString();
        }
        catch { return null; }
    }

    private async Task<List<string>> GetDistinctAsync(string col)
    {
        var list = new List<string>();
        try
        {
            await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
            await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT DISTINCT {col} FROM ARS_ALLOCATION_OUTPUT WHERE {col} IS NOT NULL ORDER BY 1";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) list.Add(r.GetString(0));
        }
        catch { }
        return list;
    }

    private static void AddParams(IDbCommand cmd, List<(string name, object val)> parms)
    {
        foreach (var (name, val) in parms)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = val;
            cmd.Parameters.Add(p);
        }
    }

    private static int ToInt(IDataReader r, int i) => r.IsDBNull(i) ? 0 : Convert.ToInt32(r.GetValue(i));
    private static decimal ToDec(IDataReader r, int i) => r.IsDBNull(i) ? 0m : Convert.ToDecimal(r.GetValue(i));
    private static string Esc(string s) => s.Replace("'", "''");
}
