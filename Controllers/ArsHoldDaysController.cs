using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsHoldDaysController : Controller
{
    private readonly string _connStr;
    public ArsHoldDaysController(IConfiguration config) =>
        _connStr = config.GetConnectionString("Snowflake")!;

    // ── helpers ──────────────────────────────────────────────
    private SnowflakeDbConnection OpenConn()
    {
        var conn = new SnowflakeDbConnection { ConnectionString = _connStr };
        conn.Open();
        return conn;
    }

    private static ArsHoldDaysMaster ReadRow(IDataReader r) => new()
    {
        Id       = Convert.ToInt32(r["ID"]),
        St       = r["ST"]?.ToString() ?? "",
        Mj       = r["MJ"]?.ToString() ?? "",
        HoldDays = r.IsDBNull(r.GetOrdinal("HOLD_DAYS")) ? null : Convert.ToDecimal(r["HOLD_DAYS"])
    };

    // ── Index ────────────────────────────────────────────────
    public async Task<IActionResult> Index(string? st, string? mj, int page = 1, int pageSize = 100)
    {
        await using var conn = OpenConn();

        // ── filter clause ──
        var where = new List<string>();
        if (!string.IsNullOrEmpty(st)) where.Add("ST = :1");
        if (!string.IsNullOrEmpty(mj)) where.Add("MJ = :2");
        var whereClause = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";

        void AddFilterParams(IDbCommand cmd)
        {
            if (!string.IsNullOrEmpty(st))
            {
                var p = cmd.CreateParameter();
                p.ParameterName = "1";
                p.Value = st;
                cmd.Parameters.Add(p);
            }
            if (!string.IsNullOrEmpty(mj))
            {
                var p = cmd.CreateParameter();
                p.ParameterName = "2";
                p.Value = mj;
                cmd.Parameters.Add(p);
            }
        }

        // ── filtered count ──
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(*) FROM ARS_HOLD_DAYS_MASTER {whereClause}";
            AddFilterParams(cmd);
            ViewBag.TotalCount = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        // ── total rows (unfiltered) ──
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM ARS_HOLD_DAYS_MASTER";
            ViewBag.TotalRows = Convert.ToInt32(await cmd.ExecuteScalarAsync());
        }

        // ── distinct stores ──
        var stores = new List<string>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT ST FROM ARS_HOLD_DAYS_MASTER ORDER BY ST";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) stores.Add(r.GetString(0));
        }

        // ── distinct major cats ──
        var majCats = new List<string>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT DISTINCT MJ FROM ARS_HOLD_DAYS_MASTER ORDER BY MJ";
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) majCats.Add(r.GetString(0));
        }

        ViewBag.Page = page;
        ViewBag.PageSize = pageSize;
        ViewBag.St = st;
        ViewBag.Mj = mj;
        ViewBag.Stores = stores;
        ViewBag.MajCats = majCats;
        ViewBag.TotalStores = stores.Count;
        ViewBag.TotalCats = majCats.Count;

        // ── paginated data ──
        var data = new List<ArsHoldDaysMaster>();
        int offset = (page - 1) * pageSize;
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"
                SELECT ID, ST, MJ, HOLD_DAYS
                FROM ARS_HOLD_DAYS_MASTER
                {whereClause}
                ORDER BY ST, MJ
                LIMIT {pageSize} OFFSET {offset}";
            AddFilterParams(cmd);
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync()) data.Add(ReadRow(r));
        }

        return View(data);
    }

    // ── Create GET ───────────────────────────────────────────
    public IActionResult Create() => View(new ArsHoldDaysMaster());

    // ── Create POST ──────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsHoldDaysMaster model)
    {
        if (!ModelState.IsValid) return View(model);

        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO ARS_HOLD_DAYS_MASTER (ST, MJ, HOLD_DAYS) VALUES (:1, :2, :3)";

        var p1 = cmd.CreateParameter(); p1.ParameterName = "1"; p1.Value = model.St; cmd.Parameters.Add(p1);
        var p2 = cmd.CreateParameter(); p2.ParameterName = "2"; p2.Value = model.Mj; cmd.Parameters.Add(p2);
        var p3 = cmd.CreateParameter(); p3.ParameterName = "3"; p3.Value = (object?)model.HoldDays ?? DBNull.Value; cmd.Parameters.Add(p3);

        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    // ── Edit GET ─────────────────────────────────────────────
    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ID, ST, MJ, HOLD_DAYS FROM ARS_HOLD_DAYS_MASTER WHERE ID = :1";
        var p = cmd.CreateParameter(); p.ParameterName = "1"; p.Value = id; cmd.Parameters.Add(p);

        await using var r = await cmd.ExecuteReaderAsync();
        if (!await r.ReadAsync()) return NotFound();
        return View(ReadRow(r));
    }

    // ── Edit POST ────────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsHoldDaysMaster model)
    {
        if (!ModelState.IsValid) return View(model);

        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "UPDATE ARS_HOLD_DAYS_MASTER SET ST = :1, MJ = :2, HOLD_DAYS = :3 WHERE ID = :4";

        var p1 = cmd.CreateParameter(); p1.ParameterName = "1"; p1.Value = model.St; cmd.Parameters.Add(p1);
        var p2 = cmd.CreateParameter(); p2.ParameterName = "2"; p2.Value = model.Mj; cmd.Parameters.Add(p2);
        var p3 = cmd.CreateParameter(); p3.ParameterName = "3"; p3.Value = (object?)model.HoldDays ?? DBNull.Value; cmd.Parameters.Add(p3);
        var p4 = cmd.CreateParameter(); p4.ParameterName = "4"; p4.Value = model.Id; cmd.Parameters.Add(p4);

        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    // ── Delete GET ───────────────────────────────────────────
    public async Task<IActionResult> Delete(int id)
    {
        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ID, ST, MJ, HOLD_DAYS FROM ARS_HOLD_DAYS_MASTER WHERE ID = :1";
        var p = cmd.CreateParameter(); p.ParameterName = "1"; p.Value = id; cmd.Parameters.Add(p);

        await using var r = await cmd.ExecuteReaderAsync();
        if (!await r.ReadAsync()) return NotFound();
        return View(ReadRow(r));
    }

    // ── Delete POST ──────────────────────────────────────────
    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DELETE FROM ARS_HOLD_DAYS_MASTER WHERE ID = :1";
        var p = cmd.CreateParameter(); p.ParameterName = "1"; p.Value = id; cmd.Parameters.Add(p);

        await cmd.ExecuteNonQueryAsync();
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    // ── CSV Export ───────────────────────────────────────────
    public async Task<IActionResult> ExportCsv(string? st, string? mj)
    {
        await using var conn = OpenConn();
        await using var cmd = conn.CreateCommand();

        var where = new List<string>();
        if (!string.IsNullOrEmpty(st)) where.Add("ST = :1");
        if (!string.IsNullOrEmpty(mj)) where.Add("MJ = :2");
        var whereClause = where.Count > 0 ? "WHERE " + string.Join(" AND ", where) : "";

        cmd.CommandText = $"SELECT ST, MJ, HOLD_DAYS FROM ARS_HOLD_DAYS_MASTER {whereClause} ORDER BY ST, MJ";

        if (!string.IsNullOrEmpty(st))
        {
            var p = cmd.CreateParameter(); p.ParameterName = "1"; p.Value = st; cmd.Parameters.Add(p);
        }
        if (!string.IsNullOrEmpty(mj))
        {
            var p = cmd.CreateParameter(); p.ParameterName = "2"; p.Value = mj; cmd.Parameters.Add(p);
        }

        var sb = new StringBuilder();
        sb.AppendLine("ST,MJ,HOLD_DAYS");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var s = r["ST"]?.ToString() ?? "";
            var m = r["MJ"]?.ToString() ?? "";
            var h = r.IsDBNull(r.GetOrdinal("HOLD_DAYS")) ? "" : Convert.ToDecimal(r["HOLD_DAYS"]).ToString();
            sb.AppendLine($"{s},{m},{h}");
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Hold_Days.csv");
    }
}
