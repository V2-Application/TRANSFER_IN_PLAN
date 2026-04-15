using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubDcStkSzController : Controller
{
    private readonly string _sfConnStr;
    public SubDcStkSzController(IConfiguration config) =>
        _sfConnStr = config.GetConnectionString("Snowflake")!;

    private const string TABLE = "SUB_DC_STK_SZ";
    private const string COLS = "ID, RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, W_GRT_STK_Q, DATE";
    private static readonly string[] InsertCols = { "RDC_CD", "MAJ_CAT", "SUB_VALUE", "DC_STK_Q", "GRT_STK_Q", "W_GRT_STK_Q", "DATE" };

    private static SubDcStkSz ReadRow(IDataReader r) => new()
    {
        Id       = SnowflakeCrudHelper.Int(r, 0),
        RdcCd    = SnowflakeCrudHelper.Str(r, 1),
        MajCat   = SnowflakeCrudHelper.Str(r, 2),
        SubValue = SnowflakeCrudHelper.Str(r, 3),
        DcStkQ   = SnowflakeCrudHelper.Dec(r, 4),
        GrtStkQ  = SnowflakeCrudHelper.Dec(r, 5),
        WGrtStkQ = SnowflakeCrudHelper.Dec(r, 6),
        Date     = SnowflakeCrudHelper.DateNull(r, 7)
    };

    // ── Index ────────────────────────────────────────────────
    public async Task<IActionResult> Index(string? rdcCd, string? majCat, string? subValue, int page = 1, int pageSize = 100)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "RDC_CD", rdcCd }, { "MAJ_CAT", majCat }, { "SUB_VALUE", subValue }
        });

        ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms);
        ViewBag.TotalRows  = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
        ViewBag.RdcCodes   = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "RDC_CD");
        ViewBag.MajCats    = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
        ViewBag.SubValues  = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SUB_VALUE");
        ViewBag.TotalRdcs  = ((List<string>)ViewBag.RdcCodes).Count;
        ViewBag.TotalCats  = ((List<string>)ViewBag.MajCats).Count;
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.RdcCd = rdcCd; ViewBag.MajCat = majCat; ViewBag.SubValue = subValue;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms,
            "RDC_CD, MAJ_CAT", page, pageSize, ReadRow);
        return View(data);
    }

    // ── Create GET ───────────────────────────────────────────
    public IActionResult Create() => View(new SubDcStkSz());

    // ── Create POST ──────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(SubDcStkSz model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.InsertAsync(conn, TABLE, InsertCols, new object?[]
        {
            model.RdcCd, model.MajCat, model.SubValue, model.DcStkQ, model.GrtStkQ, model.WGrtStkQ, model.Date
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
    public async Task<IActionResult> Edit(SubDcStkSz model)
    {
        if (!ModelState.IsValid) return View(model);
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        await SnowflakeCrudHelper.UpdateAsync(conn, TABLE, InsertCols, new object?[]
        {
            model.RdcCd, model.MajCat, model.SubValue, model.DcStkQ, model.GrtStkQ, model.WGrtStkQ, model.Date
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
    public async Task<IActionResult> ExportCsv(string? rdcCd, string? majCat)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var (where, parms) = SnowflakeCrudHelper.BuildFilter(new Dictionary<string, string?>
        {
            { "RDC_CD", rdcCd }, { "MAJ_CAT", majCat }
        });
        var sql = $"SELECT RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, W_GRT_STK_Q, DATE FROM {TABLE}{(string.IsNullOrEmpty(where) ? "" : " WHERE " + where)} ORDER BY RDC_CD, MAJ_CAT";

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        if (parms != null) foreach (var p in parms) cmd.Parameters.Add(SnowflakeCrudHelper.CloneParam(p));

        var sb = new StringBuilder();
        sb.AppendLine("RDC_CD,MAJ_CAT,SUB_VALUE,DC_STK_Q,GRT_STK_Q,W_GRT_STK_Q,DATE");
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            sb.AppendLine($"{Q(SnowflakeCrudHelper.Str(r, 0))},{Q(SnowflakeCrudHelper.Str(r, 1))},{Q(SnowflakeCrudHelper.Str(r, 2))},{SnowflakeCrudHelper.Dec(r, 3)},{SnowflakeCrudHelper.Dec(r, 4)},{SnowflakeCrudHelper.Dec(r, 5)},{SnowflakeCrudHelper.DateNull(r, 6)?.ToString("yyyy-MM-dd") ?? ""}");
        }
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "SubDcStkSz.csv");
    }

    private static string Q(string? s) => string.IsNullOrEmpty(s) ? "" : "\"" + s.Replace("\"", "\"\"") + "\"";
}
