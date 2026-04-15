using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ContMacroMvgrController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<ContMacroMvgrController> _logger;

    public ContMacroMvgrController(IConfiguration config, ILogger<ContMacroMvgrController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static ContMacroMvgr ReadRow(IDataReader r) => new()
    {
        Id             = SnowflakeCrudHelper.Int(r, 0),
        StCd           = SnowflakeCrudHelper.Str(r, 1),
        MajCatCd       = SnowflakeCrudHelper.Str(r, 2),
        DispMvgrMatrix = SnowflakeCrudHelper.Str(r, 3),
        ContPct        = SnowflakeCrudHelper.Dec(r, 4)
    };

    private const string TABLE = "ST_MAJ_CAT_MACRO_MVGR_PLAN";
    private const string COLS = "ID, ST_CD, MAJ_CAT_CD, DISP_MVGR_MATRIX, CONT_PCT";

    public async Task<IActionResult> Index(string? stCd, string? majCatCd, string? dispMvgrMatrix, int page = 1, int pageSize = 100)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var conditions = new List<string>();
            var parms = new List<SnowflakeDbParameter>();
            int idx = 0;
            if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
            if (!string.IsNullOrEmpty(majCatCd)) { idx++; conditions.Add("MAJ_CAT_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCatCd)); }
            if (!string.IsNullOrEmpty(dispMvgrMatrix)) { idx++; conditions.Add("DISP_MVGR_MATRIX = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), dispMvgrMatrix)); }
            string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

            ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms.Count > 0 ? parms : null);
            ViewBag.TotalRows = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.StCd = stCd; ViewBag.MajCatCd = majCatCd; ViewBag.DispMvgrMatrix = dispMvgrMatrix;
            ViewBag.StoreCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD");
            ViewBag.MajCatCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT_CD");
            ViewBag.MvgrValues = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "DISP_MVGR_MATRIX");
            ViewBag.TotalStores = ((List<string>)ViewBag.StoreCodes).Count;
            ViewBag.TotalCats = ((List<string>)ViewBag.MajCatCodes).Count;
            ViewBag.TotalLevels = ((List<string>)ViewBag.MvgrValues).Count;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "ST_CD, MAJ_CAT_CD", page, pageSize, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading ContMacroMvgr");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<ContMacroMvgr>());
        }
    }

    public IActionResult Create() => View(new ContMacroMvgr());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ContMacroMvgr model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "ST_CD", "MAJ_CAT_CD", "DISP_MVGR_MATRIX", "CONT_PCT" },
                new object?[] { model.StCd, model.MajCatCd, model.DispMvgrMatrix, model.ContPct });
            TempData["SuccessMessage"] = "Record added.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating ContMacroMvgr");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ContMacroMvgr model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "ST_CD", "MAJ_CAT_CD", "DISP_MVGR_MATRIX", "CONT_PCT" },
                new object?[] { model.StCd, model.MajCatCd, model.DispMvgrMatrix, model.ContPct }, model.Id);
            TempData["SuccessMessage"] = "Record updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating ContMacroMvgr");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

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
            TempData["SuccessMessage"] = "Record deleted.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting ContMacroMvgr");
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? stCd, string? majCatCd, string? dispMvgrMatrix)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(majCatCd)) { idx++; conditions.Add("MAJ_CAT_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), majCatCd)); }
        if (!string.IsNullOrEmpty(dispMvgrMatrix)) { idx++; conditions.Add("DISP_MVGR_MATRIX = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), dispMvgrMatrix)); }
        string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms.Count > 0 ? parms : null, "ST_CD, MAJ_CAT_CD", 1, 100000, ReadRow);
        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,MAJ_CAT_CD,DISP_MVGR_MATRIX,CONT%");
        foreach (var r in data) sb.AppendLine($"{r.StCd},{r.MajCatCd},{r.DispMvgrMatrix},{r.ContPct}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ContMacroMvgr.csv");
    }
}
