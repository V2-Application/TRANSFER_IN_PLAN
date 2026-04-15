using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DcStockController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<DcStockController> _logger;

    public DcStockController(IConfiguration config, ILogger<DcStockController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static DcStock ReadRow(IDataReader r) => new()
    {
        Id       = SnowflakeCrudHelper.Int(r, 0),
        RdcCd    = SnowflakeCrudHelper.StrNull(r, 1),
        Rdc      = SnowflakeCrudHelper.StrNull(r, 2),
        MajCat   = SnowflakeCrudHelper.StrNull(r, 3),
        DcStkQ   = SnowflakeCrudHelper.DecNull(r, 4),
        GrtStkQ  = SnowflakeCrudHelper.DecNull(r, 5),
        WGrtStkQ = SnowflakeCrudHelper.DecNull(r, 6),
        Date     = SnowflakeCrudHelper.DateNull(r, 7)
    };

    private const string TABLE = "QTY_MSA_AND_GRT";
    private const string COLS = "ID, RDC_CD, RDC, MAJ_CAT, DC_STK_Q, GRT_STK_Q, W_GRT_STK_Q, DATE";

    [HttpGet]
    public async Task<IActionResult> Index(string? rdcCd, string? majCat, int page = 1, int pageSize = 100)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var conditions = new List<string>();
            var parms = new List<SnowflakeDbParameter>();
            int idx = 0;
            if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
            if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
            string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

            ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms.Count > 0 ? parms : null);
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "RDC_CD");
            ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
            ViewBag.RdcCd = rdcCd; ViewBag.MajCat = majCat;
            ViewBag.TotalRows = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
            ViewBag.TotalRdcs = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "RDC_CD")).Count;
            ViewBag.TotalCats = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT")).Count;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "RDC_CD, MAJ_CAT", page, pageSize, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading DcStock");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<DcStock>());
        }
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(string? rdcCd, string? majCat)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "RDC_CD, MAJ_CAT", 1, 100000, ReadRow);
        _logger.LogInformation("DcStock ExportCsv: {Count} rows exported", data.Count);

        var sb = new StringBuilder();
        sb.AppendLine("Id,RdcCd,Rdc,MajCat,DcStkQ,GrtStkQ,WGrtStkQ,Date");
        foreach (var r in data)
        {
            sb.AppendLine(string.Join(",",
                r.Id, Q(r.RdcCd), Q(r.Rdc), Q(r.MajCat),
                r.DcStkQ, r.GrtStkQ, r.WGrtStkQ,
                r.Date?.ToString("yyyy-MM-dd")));
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "DcStock.csv");
    }

    [HttpGet]
    public async Task<IActionResult> Create()
    {
        await LoadDropdowns();
        return View();
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(DcStock model)
    {
        if (!ModelState.IsValid) { await LoadDropdowns(); return View(model); }
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "RDC_CD", "RDC", "MAJ_CAT", "DC_STK_Q", "GRT_STK_Q", "W_GRT_STK_Q", "DATE" },
                new object?[] { model.RdcCd, model.Rdc, model.MajCat, model.DcStkQ, model.GrtStkQ, model.WGrtStkQ, model.Date });
            _logger.LogInformation("DcStock created: RdcCd={RdcCd} MajCat={MajCat}", model.RdcCd, model.MajCat);
            TempData["SuccessMessage"] = "DC Stock record created.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating DcStock");
            ModelState.AddModelError("", "Error: " + ex.Message);
            await LoadDropdowns();
            return View(model);
        }
    }

    [HttpGet]
    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        if (item == null) return NotFound();
        await LoadDropdowns();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, DcStock model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "RDC_CD", "RDC", "MAJ_CAT", "DC_STK_Q", "GRT_STK_Q", "W_GRT_STK_Q", "DATE" },
                new object?[] { model.RdcCd, model.Rdc, model.MajCat, model.DcStkQ, model.GrtStkQ, model.WGrtStkQ, model.Date }, id);
            _logger.LogInformation("DcStock updated: Id={Id}", id);
            TempData["SuccessMessage"] = "DC Stock record updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating DcStock");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    [HttpGet]
    public async Task<IActionResult> Delete(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.DeleteAsync(conn, TABLE, id);
            _logger.LogInformation("DcStock deleted: Id={Id}", id);
            TempData["SuccessMessage"] = "DC Stock record deleted.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting DcStock");
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private async Task LoadDropdowns()
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_ST_MASTER", "RDC_CD");
        ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_BIN_CAPACITY", "MAJ_CAT");
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
