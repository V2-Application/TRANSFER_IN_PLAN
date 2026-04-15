using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubStStkSzController : Controller
{
    private readonly string _sfConnStr;
    public SubStStkSzController(IConfiguration config) =>
        _sfConnStr = config.GetConnectionString("Snowflake")!;

    private const string TABLE = "SUB_ST_STK_SZ";
    private const string COLS = "ID, ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY, DATE";
    private static readonly string[] InsertCols = { "ST_CD", "MAJ_CAT", "SUB_VALUE", "STK_QTY", "DATE" };

    private static SubStStkSz ReadRow(IDataReader r) => new()
    {
        Id       = SnowflakeCrudHelper.Int(r, 0),
        StCd     = SnowflakeCrudHelper.Str(r, 1),
        MajCat   = SnowflakeCrudHelper.Str(r, 2),
        SubValue = SnowflakeCrudHelper.Str(r, 3),
        StkQty   = SnowflakeCrudHelper.Dec(r, 4),
        Date     = SnowflakeCrudHelper.DateNull(r, 5)
    };

    // ── Index ────────────────────────────────────────────────
    public async Task<IActionResult> Index(string? stCd, string? majCat, string? subValue, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "ST_CD", stCd }, { "MAJ_CAT", majCat }, { "SUB_VALUE", subValue }
        });

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms);
        ViewBag.TotalRows  = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD");
        ViewBag.MajCats    = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
        ViewBag.SubValues  = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SUB_VALUE");
        ViewBag.TotalStores = ((List<string>)ViewBag.StoreCodes).Count;
        ViewBag.TotalCats   = ((List<string>)ViewBag.MajCats).Count;
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCat = majCat; ViewBag.SubValue = subValue;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms,
            "ST_CD, MAJ_CAT", page, pageSize, ReadRow);
        return View(data);
    }

    // ── Create GET ───────────────────────────────────────────
    public IActionResult Create() => View(new SubStStkSz());

    // ── Create POST ──────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(SubStStkSz model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.InsertAsync(conn, TABLE, InsertCols, new object?[]
        {
            model.StCd, model.MajCat, model.SubValue, model.StkQty, model.Date
        });
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
    public async Task<IActionResult> Edit(SubStStkSz model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.UpdateAsync(conn, TABLE, InsertCols, new object?[]
        {
            model.StCd, model.MajCat, model.SubValue, model.StkQty, model.Date
        }, model.Id);
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
        var sql = $"SELECT ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY, DATE FROM {TABLE}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)} ORDER BY ST_CD, MAJ_CAT";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,MAJ_CAT,SUB_VALUE,STK_QTY,DATE");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            sb.AppendLine($"{SnowflakeCrudHelper.Str(r, 0)},{SnowflakeCrudHelper.Str(r, 1)},{SnowflakeCrudHelper.Str(r, 2)},{SnowflakeCrudHelper.Dec(r, 3)},{SnowflakeCrudHelper.DateNull(r, 4)?.ToString("yyyy-MM-dd") ?? ""}");
        }
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreStock_Sz.csv");
    }
}
