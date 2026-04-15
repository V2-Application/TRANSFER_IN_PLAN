using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ProductHierarchyController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<ProductHierarchyController> _logger;

    public ProductHierarchyController(IConfiguration config, ILogger<ProductHierarchyController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static ProductHierarchy ReadRow(IDataReader r) => new()
    {
        Id       = SnowflakeCrudHelper.Int(r, 0),
        Seg      = SnowflakeCrudHelper.Str(r, 1),
        Div      = SnowflakeCrudHelper.Str(r, 2),
        SubDiv   = SnowflakeCrudHelper.Str(r, 3),
        MajCatNm = SnowflakeCrudHelper.Str(r, 4),
        Ssn      = SnowflakeCrudHelper.Str(r, 5)
    };

    private const string TABLE = "MASTER_PRODUCT_HIERARCHY";
    private const string COLS = "ID, SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN";

    [HttpGet]
    public async Task<IActionResult> Index(string? seg, string? div, string? subDiv, string? majCatNm)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var conditions = new List<string>();
            var parms = new List<SnowflakeDbParameter>();
            int idx = 0;
            if (!string.IsNullOrEmpty(seg)) { idx++; conditions.Add("SEG = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), seg)); }
            if (!string.IsNullOrEmpty(div)) { idx++; conditions.Add("DIV = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), div)); }
            if (!string.IsNullOrEmpty(subDiv)) { idx++; conditions.Add("SUB_DIV = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), subDiv)); }
            if (!string.IsNullOrEmpty(majCatNm)) { idx++; conditions.Add("MAJ_CAT_NM = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCatNm)); }
            string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

            ViewBag.Segs = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SEG");
            ViewBag.Divs = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "DIV");
            ViewBag.SubDivs = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SUB_DIV");
            ViewBag.MajCatNms = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT_NM");
            ViewBag.Seg = seg; ViewBag.Div = div; ViewBag.SubDiv = subDiv; ViewBag.MajCatNm = majCatNm;

            // Analytics
            ViewBag.TotalRows = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
            ViewBag.TotalSegs = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "SEG")).Count;
            ViewBag.TotalDivs = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "DIV")).Count;
            ViewBag.TotalMajCats = (await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT_NM")).Count;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "SEG, DIV, MAJ_CAT_NM", 1, 100000, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading ProductHierarchy");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<ProductHierarchy>());
        }
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(string? seg, string? div, string? subDiv, string? majCatNm)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(seg)) { idx++; conditions.Add("SEG = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), seg)); }
        if (!string.IsNullOrEmpty(div)) { idx++; conditions.Add("DIV = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), div)); }
        if (!string.IsNullOrEmpty(subDiv)) { idx++; conditions.Add("SUB_DIV = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), subDiv)); }
        if (!string.IsNullOrEmpty(majCatNm)) { idx++; conditions.Add("MAJ_CAT_NM = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCatNm)); }
        string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "SEG, DIV, MAJ_CAT_NM", 1, 100000, ReadRow);
        _logger.LogInformation("ProductHierarchy ExportCsv: {Count} rows", data.Count);
        var sb = new StringBuilder();
        sb.AppendLine("Seg,Div,SubDiv,MajCatNm,Ssn");
        foreach (var r in data)
            sb.AppendLine(string.Join(",", Q(r.Seg), Q(r.Div), Q(r.SubDiv), Q(r.MajCatNm), Q(r.Ssn)));
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ProductHierarchy.csv");
    }

    [HttpGet]
    public IActionResult Create() => View();

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ProductHierarchy model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "SEG", "DIV", "SUB_DIV", "MAJ_CAT_NM", "SSN" },
                new object?[] { model.Seg, model.Div, model.SubDiv, model.MajCatNm, model.Ssn });
            _logger.LogInformation("ProductHierarchy created: {Seg}/{MajCatNm}", model.Seg, model.MajCatNm);
            TempData["SuccessMessage"] = "Created.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ProductHierarchy");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    [HttpGet]
    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, ProductHierarchy model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "SEG", "DIV", "SUB_DIV", "MAJ_CAT_NM", "SSN" },
                new object?[] { model.Seg, model.Div, model.SubDiv, model.MajCatNm, model.Ssn }, id);
            TempData["SuccessMessage"] = "Updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating ProductHierarchy");
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
            _logger.LogError(ex, "Error deleting ProductHierarchy");
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
