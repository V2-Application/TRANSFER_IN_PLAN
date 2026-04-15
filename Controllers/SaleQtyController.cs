using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SaleQtyController : Controller
{
    private readonly string _sfConnStr;
    public SaleQtyController(IConfiguration config) =>
        _sfConnStr = config.GetConnectionString("Snowflake")!;

    private const string TABLE = "QTY_SALE_QTY";

    // Week column names for Snowflake (WK_1 .. WK_48)
    private static readonly string[] WkCols = Enumerable.Range(1, 48).Select(i => $"WK_{i}").ToArray();

    // All columns: ID, ST_CD, MAJ_CAT, WK_1..WK_48, COL_2  (51 total)
    private static readonly string COLS = "ID, ST_CD, MAJ_CAT, " + string.Join(", ", WkCols) + ", COL_2";

    // Insert columns (no ID)
    private static readonly string[] InsertCols = new[] { "ST_CD", "MAJ_CAT" }.Concat(WkCols).Append("COL_2").ToArray();

    private static SaleQty ReadRow(IDataReader r)
    {
        var m = new SaleQty
        {
            Id    = SnowflakeCrudHelper.Int(r, 0),
            StCd  = SnowflakeCrudHelper.Str(r, 1),
            MajCat = SnowflakeCrudHelper.Str(r, 2)
        };
        // Weeks 1-48 at ordinals 3..50
        for (int w = 1; w <= 48; w++)
        {
            var prop = typeof(SaleQty).GetProperty($"Wk{w}");
            if (prop != null) prop.SetValue(m, SnowflakeCrudHelper.DecNull(r, 2 + w));
        }
        m.Col2 = SnowflakeCrudHelper.DecNull(r, 51);
        return m;
    }

    private static object?[] BuildValues(SaleQty model)
    {
        var vals = new List<object?> { model.StCd, model.MajCat };
        for (int w = 1; w <= 48; w++)
        {
            var prop = typeof(SaleQty).GetProperty($"Wk{w}");
            vals.Add(prop?.GetValue(model));
        }
        vals.Add(model.Col2);
        return vals.ToArray();
    }

    // ── Index ────────────────────────────────────────────────
    public async Task<IActionResult> Index(string? stCd, string? majCat, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "ST_CD", stCd }, { "MAJ_CAT", majCat }
        });

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms);
        ViewBag.TotalRows  = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD");
        ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
        ViewBag.TotalStores = ((List<string>)ViewBag.StoreCodes).Count;
        ViewBag.TotalCats   = ((List<string>)ViewBag.Categories).Count;
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCat = majCat;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms,
            "ST_CD, MAJ_CAT", page, pageSize, ReadRow);
        return View(data);
    }

    // ── Create GET ───────────────────────────────────────────
    public IActionResult Create() => View(new SaleQty());

    // ── Create POST ──────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(SaleQty model)
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
    public async Task<IActionResult> Edit(SaleQty model)
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
    public async Task<IActionResult> ExportCsv(string? stCd, string? majCat)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "ST_CD", stCd }, { "MAJ_CAT", majCat }
        });

        var dataCols = "ST_CD, MAJ_CAT, " + string.Join(", ", WkCols) + ", COL_2";
        var sql = $"SELECT {dataCols} FROM {TABLE}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)} ORDER BY ST_CD, MAJ_CAT";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

        var sb = new StringBuilder();
        sb.AppendLine("StCd,MajCat," + string.Join(",", Enumerable.Range(1, 48).Select(w => $"Wk{w}")) + ",Col2");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            sb.Append(Q(SnowflakeCrudHelper.Str(r, 0)));
            sb.Append(',');
            sb.Append(Q(SnowflakeCrudHelper.Str(r, 1)));
            for (int w = 0; w < 48; w++)
            {
                sb.Append(',');
                sb.Append(SnowflakeCrudHelper.DecNull(r, 2 + w)?.ToString() ?? "");
            }
            sb.Append(',');
            sb.Append(SnowflakeCrudHelper.DecNull(r, 50)?.ToString() ?? "");
            sb.AppendLine();
        }
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "SaleQty.csv");
    }

    private static string Q(string? s) => string.IsNullOrEmpty(s) ? "" : "\"" + s.Replace("\"", "\"\"") + "\"";
}
