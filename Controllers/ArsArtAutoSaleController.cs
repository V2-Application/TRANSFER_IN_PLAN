using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsArtAutoSaleController : Controller
{
    private readonly string _sfConnStr;

    public ArsArtAutoSaleController(IConfiguration config)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
    }

    // ── Helpers ─────────────────────────────────────────────
    private SnowflakeDbConnection CreateConn()
    {
        var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _sfConnStr;
        return conn;
    }

    private static string? Str(IDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);
    private static decimal? Dec(IDataReader r, int i) => r.IsDBNull(i) ? null : Convert.ToDecimal(r.GetValue(i));

    private static ArsStArtAutoSale MapRow(IDataReader r) => new()
    {
        Id = Convert.ToInt32(r.GetValue(0)),
        St = Str(r, 1) ?? "",
        GenArt = Str(r, 2) ?? "",
        Clr = Str(r, 3) ?? "",
        Mj = Str(r, 4),
        CmRemDays = Dec(r, 5),
        NmDays = Dec(r, 6),
        CmAutoSaleQ = Dec(r, 7),
        NmAutoSaleQ = Dec(r, 8),
        ArtTag = Str(r, 9)
    };

    private static IDbDataParameter MakeParam(IDbCommand cmd, string name, object? value)
    {
        var p = cmd.CreateParameter();
        p.ParameterName = name;
        p.Value = value ?? DBNull.Value;
        return p;
    }

    // ── Index ───────────────────────────────────────────────
    public async Task<IActionResult> Index(string? st, string? genArt, string? clr, string? mj, int page = 1, int pageSize = 100)
    {
        await using var conn = CreateConn();
        await conn.OpenAsync();

        // Build WHERE clause
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<(string name, object? val)>();
        int pi = 1;
        if (!string.IsNullOrEmpty(st)) { where.Append($" AND ST = ?"); parms.Add(($"{pi++}", st)); }
        if (!string.IsNullOrEmpty(genArt)) { where.Append($" AND GEN_ART = ?"); parms.Add(($"{pi++}", genArt)); }
        if (!string.IsNullOrEmpty(clr)) { where.Append($" AND CLR = ?"); parms.Add(($"{pi++}", clr)); }
        if (!string.IsNullOrEmpty(mj)) { where.Append($" AND MJ = ?"); parms.Add(($"{pi++}", mj)); }

        void AddParams(IDbCommand cmd)
        {
            foreach (var (name, val) in parms)
                cmd.Parameters.Add(MakeParam(cmd, name, val));
        }

        // Filtered count
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1) FROM ARS_ST_ART_AUTO_SALE {where}";
            AddParams(cmd);
            ViewBag.TotalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        // Total row count (unfiltered)
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(1) FROM ARS_ST_ART_AUTO_SALE";
            ViewBag.TotalRows = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        // Dropdown lists
        ViewBag.Stores = await GetDistinctListAsync(conn, "ST");
        ViewBag.Articles = await GetDistinctListAsync(conn, "GEN_ART", 500);
        ViewBag.Colors = await GetDistinctListAsync(conn, "CLR");
        ViewBag.MajCats = await GetDistinctListAsync(conn, "MJ");

        // Analytic counts
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                SELECT COUNT(DISTINCT ST), COUNT(DISTINCT GEN_ART),
                       COUNT(DISTINCT CLR), COUNT(DISTINCT NULLIF(MJ, ''))
                FROM ARS_ST_ART_AUTO_SALE";
            await using var rdr = await cmd.ExecuteReaderAsync();
            if (await rdr.ReadAsync())
            {
                ViewBag.TotalStores = Convert.ToInt32(rdr.GetValue(0));
                ViewBag.TotalArticles = Convert.ToInt32(rdr.GetValue(1));
                ViewBag.TotalColors = Convert.ToInt32(rdr.GetValue(2));
                ViewBag.TotalMajCats = Convert.ToInt32(rdr.GetValue(3));
            }
        }

        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.St = st; ViewBag.GenArt = genArt; ViewBag.Clr = clr; ViewBag.Mj = mj;

        // Paginated data
        int offset = (page - 1) * pageSize;
        var data = new List<ArsStArtAutoSale>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT ID, ST, GEN_ART, CLR, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q, ART_TAG
                FROM ARS_ST_ART_AUTO_SALE
                {where}
                ORDER BY ST, MJ, GEN_ART, CLR
                LIMIT {pageSize} OFFSET {offset}";
            AddParams(cmd);
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
                data.Add(MapRow(rdr));
        }

        return View(data);
    }

    // ── Create ──────────────────────────────────────────────
    public IActionResult Create() => View(new ArsStArtAutoSale());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStArtAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);

        await using var conn = CreateConn();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO ARS_ST_ART_AUTO_SALE (ST, GEN_ART, CLR, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q, ART_TAG)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        cmd.Parameters.Add(MakeParam(cmd, "1", model.St));
        cmd.Parameters.Add(MakeParam(cmd, "2", model.GenArt));
        cmd.Parameters.Add(MakeParam(cmd, "3", model.Clr));
        cmd.Parameters.Add(MakeParam(cmd, "4", (object?)model.Mj ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "5", (object?)model.CmRemDays ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "6", (object?)model.NmDays ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "7", (object?)model.CmAutoSaleQ ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "8", (object?)model.NmAutoSaleQ ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "9", (object?)model.ArtTag ?? DBNull.Value));
        await cmd.ExecuteNonQueryAsync();

        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    // ── Edit ────────────────────────────────────────────────
    public async Task<IActionResult> Edit(int id)
    {
        var item = await FindByIdAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsStArtAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);

        await using var conn = CreateConn();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            UPDATE ARS_ST_ART_AUTO_SALE
            SET ST = ?, GEN_ART = ?, CLR = ?, MJ = ?,
                CM_REM_DAYS = ?, NM_DAYS = ?, CM_AUTO_SALE_Q = ?, NM_AUTO_SALE_Q = ?, ART_TAG = ?
            WHERE ID = ?";
        cmd.Parameters.Add(MakeParam(cmd, "1", model.St));
        cmd.Parameters.Add(MakeParam(cmd, "2", model.GenArt));
        cmd.Parameters.Add(MakeParam(cmd, "3", model.Clr));
        cmd.Parameters.Add(MakeParam(cmd, "4", (object?)model.Mj ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "5", (object?)model.CmRemDays ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "6", (object?)model.NmDays ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "7", (object?)model.CmAutoSaleQ ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "8", (object?)model.NmAutoSaleQ ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "9", (object?)model.ArtTag ?? DBNull.Value));
        cmd.Parameters.Add(MakeParam(cmd, "10", model.Id));
        await cmd.ExecuteNonQueryAsync();

        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    // ── Delete ──────────────────────────────────────────────
    public async Task<IActionResult> Delete(int id)
    {
        var item = await FindByIdAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        await using var conn = CreateConn();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM ARS_ST_ART_AUTO_SALE WHERE ID = ?";
        cmd.Parameters.Add(MakeParam(cmd, "1", id));
        await cmd.ExecuteNonQueryAsync();

        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    // ── CSV Export ──────────────────────────────────────────
    public async Task<IActionResult> ExportCsv(string? st, string? genArt, string? clr, string? mj)
    {
        await using var conn = CreateConn();
        await conn.OpenAsync();

        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<(string name, object? val)>();
        int pi = 1;
        if (!string.IsNullOrEmpty(st)) { where.Append(" AND ST = ?"); parms.Add(($"{pi++}", st)); }
        if (!string.IsNullOrEmpty(genArt)) { where.Append(" AND GEN_ART = ?"); parms.Add(($"{pi++}", genArt)); }
        if (!string.IsNullOrEmpty(clr)) { where.Append(" AND CLR = ?"); parms.Add(($"{pi++}", clr)); }
        if (!string.IsNullOrEmpty(mj)) { where.Append(" AND MJ = ?"); parms.Add(($"{pi++}", mj)); }

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $@"
            SELECT ST, GEN_ART, CLR, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q, ART_TAG
            FROM ARS_ST_ART_AUTO_SALE
            {where}
            ORDER BY ST, MJ, GEN_ART, CLR";
        foreach (var (name, val) in parms)
            cmd.Parameters.Add(MakeParam(cmd, name, val));

        var sb = new StringBuilder();
        sb.AppendLine("ST,GEN-ART,CLR,MJ,CM-REM-DAYS,NM-DAYS,CM-AUTO-SALE-Q,NM-AUTO-SALE-Q,ART_TAG,CM_PD_SALE_Q,NM_PD_SALE_Q");

        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            var cmRemDays = Dec(rdr, 4) ?? 0;
            var nmDays = Dec(rdr, 5) ?? 0;
            var cmAutoSaleQ = Dec(rdr, 6) ?? 0;
            var nmAutoSaleQ = Dec(rdr, 7) ?? 0;
            var artTag = Str(rdr, 8) ?? "";
            var cmPdSaleQ = cmRemDays > 0 ? cmAutoSaleQ / cmRemDays : 0m;
            var nmPdSaleQ = nmDays > 0 ? nmAutoSaleQ / nmDays : 0m;

            sb.AppendLine($"{Str(rdr, 0)},{Str(rdr, 1)},{Str(rdr, 2)},{Str(rdr, 3)},{cmRemDays},{nmDays},{cmAutoSaleQ},{nmAutoSaleQ},{artTag},{cmPdSaleQ:F4},{nmPdSaleQ:F4}");
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Art_Auto_Sale.csv");
    }

    // ── Private helpers ─────────────────────────────────────
    private async Task<ArsStArtAutoSale?> FindByIdAsync(int id)
    {
        await using var conn = CreateConn();
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT ID, ST, GEN_ART, CLR, MJ, CM_REM_DAYS, NM_DAYS, CM_AUTO_SALE_Q, NM_AUTO_SALE_Q, ART_TAG
            FROM ARS_ST_ART_AUTO_SALE
            WHERE ID = ?";
        cmd.Parameters.Add(MakeParam(cmd, "1", id));
        await using var rdr = await cmd.ExecuteReaderAsync();
        return await rdr.ReadAsync() ? MapRow(rdr) : null;
    }

    private static async Task<List<string>> GetDistinctListAsync(SnowflakeDbConnection conn, string column, int? limit = null)
    {
        var list = new List<string>();
        await using var cmd = conn.CreateCommand();
        var limitClause = limit.HasValue ? $" LIMIT {limit.Value}" : "";
        cmd.CommandText = $"SELECT DISTINCT {column} FROM ARS_ST_ART_AUTO_SALE WHERE {column} IS NOT NULL ORDER BY {column}{limitClause}";
        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
            list.Add(rdr.GetString(0));
        return list;
    }
}
