using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreStockController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<StoreStockController> _logger;

    public StoreStockController(IConfiguration config, ILogger<StoreStockController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static StoreStock ReadRow(IDataReader r) => new()
    {
        Id     = SnowflakeCrudHelper.Int(r, 0),
        StCd   = SnowflakeCrudHelper.StrNull(r, 1),
        MajCat = SnowflakeCrudHelper.StrNull(r, 2),
        StkQty = SnowflakeCrudHelper.DecNull(r, 3),
        Date   = SnowflakeCrudHelper.DateNull(r, 4)
    };

    private const string TABLE = "QTY_ST_STK_Q";
    private const string COLS = "ID, ST_CD, MAJ_CAT, STK_QTY, DATE";

    [HttpGet]
    public async Task<IActionResult> Index(string? stCd, string? majCat, int page = 1, int pageSize = 100)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var conditions = new List<string>();
            var parms = new List<SnowflakeDbParameter>();
            int idx = 0;
            if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
            if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
            string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

            ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms.Count > 0 ? parms : null);
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
            ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD");
            ViewBag.StCd = stCd; ViewBag.MajCat = majCat;
            ViewBag.TotalRows = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
            ViewBag.TotalStores = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD")).Count;
            ViewBag.TotalCats = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT")).Count;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "ST_CD, MAJ_CAT", page, pageSize, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading StoreStock");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<StoreStock>());
        }
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(string? stCd, string? majCat)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(majCat)) { idx++; conditions.Add("MAJ_CAT = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCat)); }
        string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "ST_CD, MAJ_CAT", 1, 100000, ReadRow);
        _logger.LogInformation("StoreStock ExportCsv: {Count} rows", data.Count);
        var sb = new StringBuilder();
        sb.AppendLine("Id,StCd,MajCat,StkQty,Date");
        foreach (var r in data)
            sb.AppendLine(string.Join(",", r.Id, Q(r.StCd), Q(r.MajCat), r.StkQty, r.Date?.ToString("yyyy-MM-dd")));
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreStock.csv");
    }

    [HttpGet]
    public async Task<IActionResult> Create()
    {
        await LoadDropdowns();
        return View();
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(StoreStock model)
    {
        if (!ModelState.IsValid) { await LoadDropdowns(); return View(model); }
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "ST_CD", "MAJ_CAT", "STK_QTY", "DATE" },
                new object?[] { model.StCd, model.MajCat, model.StkQty, model.Date });
            TempData["SuccessMessage"] = "Created.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating StoreStock");
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
    public async Task<IActionResult> Edit(int id, StoreStock model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "ST_CD", "MAJ_CAT", "STK_QTY", "DATE" },
                new object?[] { model.StCd, model.MajCat, model.StkQty, model.Date }, id);
            TempData["SuccessMessage"] = "Updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating StoreStock");
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
            TempData["SuccessMessage"] = "Deleted.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting StoreStock");
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private async Task LoadDropdowns()
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_ST_MASTER", "ST_CD");
        ViewBag.MajCats = await SnowflakeCrudHelper.DistinctAsync(conn, "MASTER_BIN_CAPACITY", "MAJ_CAT");
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
