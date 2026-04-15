using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class BinCapacityController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<BinCapacityController> _logger;

    public BinCapacityController(IConfiguration config, ILogger<BinCapacityController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static BinCapacity ReadRow(IDataReader r) => new()
    {
        Id           = SnowflakeCrudHelper.Int(r, 0),
        MajCat       = SnowflakeCrudHelper.StrNull(r, 1),
        BinCapDcTeam = SnowflakeCrudHelper.DecNull(r, 2),
        BinCap       = SnowflakeCrudHelper.DecNull(r, 3)
    };

    private const string TABLE = "MASTER_BIN_CAPACITY";
    private const string COLS = "ID, MAJ_CAT, BIN_CAP_DC_TEAM, BIN_CAP";

    [HttpGet]
    public async Task<IActionResult> Index(string? majCat)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            string? where = null;
            List<SnowflakeDbParameter>? parms = null;
            if (!string.IsNullOrEmpty(majCat))
            {
                where = "MAJ_CAT = ?";
                parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", majCat) };
            }

            ViewBag.Categories = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "MAJ_CAT");
            ViewBag.MajCat = majCat;

            // Analytics from ALL rows
            var all = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, null, null, "MAJ_CAT", 1, 100000, ReadRow);
            ViewBag.TotalRows = all.Count;
            ViewBag.TotalCategories = all.Select(x => x.MajCat).Distinct().Count();
            ViewBag.AvgBinCap = all.Any() ? all.Average(x => x.BinCap ?? 0) : 0;
            ViewBag.AvgDcTeam = all.Any() ? all.Average(x => x.BinCapDcTeam ?? 0) : 0;
            ViewBag.ChartLabels = all.OrderBy(x => x.MajCat).Select(x => x.MajCat ?? "NA").ToList();
            ViewBag.ChartBinCap = all.OrderBy(x => x.MajCat).Select(x => x.BinCap ?? 0).ToList();
            ViewBag.ChartDcTeam = all.OrderBy(x => x.MajCat).Select(x => x.BinCapDcTeam ?? 0).ToList();

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "MAJ_CAT", 1, 100000, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading BinCapacity");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<BinCapacity>());
        }
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(string? majCat)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        string? where = null;
        List<SnowflakeDbParameter>? parms = null;
        if (!string.IsNullOrEmpty(majCat))
        {
            where = "MAJ_CAT = ?";
            parms = new List<SnowflakeDbParameter> { SnowflakeCrudHelper.Param("1", majCat) };
        }
        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "MAJ_CAT", 1, 100000, ReadRow);
        _logger.LogInformation("BinCapacity ExportCsv: {Count} rows", data.Count);
        var sb = new StringBuilder();
        sb.AppendLine("Id,MajCat,BinCapDcTeam,BinCap");
        foreach (var r in data)
            sb.AppendLine(string.Join(",", r.Id, Q(r.MajCat), r.BinCapDcTeam, r.BinCap));
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "BinCapacity.csv");
    }

    [HttpGet]
    public IActionResult Create() => View();

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(BinCapacity model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "MAJ_CAT", "BIN_CAP_DC_TEAM", "BIN_CAP" },
                new object?[] { model.MajCat, model.BinCapDcTeam, model.BinCap });
            TempData["SuccessMessage"] = "Created.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating BinCapacity");
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
    public async Task<IActionResult> Edit(int id, BinCapacity model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "MAJ_CAT", "BIN_CAP_DC_TEAM", "BIN_CAP" },
                new object?[] { model.MajCat, model.BinCapDcTeam, model.BinCap }, id);
            TempData["SuccessMessage"] = "Updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating BinCapacity");
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
            _logger.LogError(ex, "Error deleting BinCapacity");
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
