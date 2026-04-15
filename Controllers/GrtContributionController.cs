using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class GrtContributionController : Controller
{
    private readonly string _sfConnStr;
    public GrtContributionController(IConfiguration config) =>
        _sfConnStr = config.GetConnectionString("Snowflake")!;

    private const string TABLE = "MASTER_GRT_CONTRIBUTION";

    // Week column names for Snowflake (WK_1 .. WK_48)
    private static readonly string[] WkCols = Enumerable.Range(1, 48).Select(i => $"WK_{i}").ToArray();

    // All columns: ID, SSN, WK_1..WK_48  (50 total)
    private static readonly string COLS = "ID, SSN, " + string.Join(", ", WkCols);

    // Insert columns (no ID)
    private static readonly string[] InsertCols = new[] { "SSN" }.Concat(WkCols).ToArray();

    private static GrtContribution ReadRow(IDataReader r)
    {
        var m = new GrtContribution
        {
            Id  = SnowflakeCrudHelper.Int(r, 0),
            Ssn = SnowflakeCrudHelper.Str(r, 1)
        };
        // Weeks 1-48 at ordinals 2..49
        for (int w = 1; w <= 48; w++)
        {
            var prop = typeof(GrtContribution).GetProperty($"Wk{w}");
            if (prop != null) prop.SetValue(m, SnowflakeCrudHelper.Dec(r, 1 + w));
        }
        return m;
    }

    private static object?[] BuildValues(GrtContribution model)
    {
        var vals = new List<object?> { model.Ssn };
        for (int w = 1; w <= 48; w++)
        {
            var prop = typeof(GrtContribution).GetProperty($"Wk{w}");
            vals.Add(prop?.GetValue(model) ?? 0m);
        }
        return vals.ToArray();
    }

    // ── Index ────────────────────────────────────────────────
    public async Task<IActionResult> Index(string? ssn, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "SSN", ssn }
        });

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms);
        ViewBag.TotalRows  = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
        ViewBag.Ssns       = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SSN");
        ViewBag.TotalSsns  = ((List<string>)ViewBag.Ssns).Count;
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.Ssn = ssn;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms,
            "SSN", page, pageSize, ReadRow);
        return View(data);
    }

    // ── Create GET ───────────────────────────────────────────
    public IActionResult Create() => View(new GrtContribution());

    // ── Create POST ──────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(GrtContribution model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.InsertAsync(conn, TABLE, InsertCols, BuildValues(model));
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    // ── Edit GET ─────────────────────────────────────────────
    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    // ── Edit POST ────────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(GrtContribution model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.UpdateAsync(conn, TABLE, InsertCols, BuildValues(model), model.Id);
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    // ── Delete GET ───────────────────────────────────────────
    public async Task<IActionResult> Delete(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    // ── Delete POST ──────────────────────────────────────────
    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.DeleteAsync(conn, TABLE, id);
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    // ── CSV Export ───────────────────────────────────────────
    public async Task<IActionResult> ExportCsv(string? ssn)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "SSN", ssn }
        });

        var dataCols = "SSN, " + string.Join(", ", WkCols);
        var sql = $"SELECT {dataCols} FROM {TABLE}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)} ORDER BY SSN";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

        var sb = new StringBuilder();
        sb.AppendLine("Ssn," + string.Join(",", Enumerable.Range(1, 48).Select(w => $"Wk{w}")));
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            sb.Append(Q(SnowflakeCrudHelper.Str(r, 0)));
            for (int w = 0; w < 48; w++)
            {
                sb.Append(',');
                sb.Append(SnowflakeCrudHelper.Dec(r, 1 + w));
            }
            sb.AppendLine();
        }
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "GrtContribution.csv");
    }

    private static string Q(string? s) => string.IsNullOrEmpty(s) ? "" : "\"" + s.Replace("\"", "\"\"") + "\"";
}
