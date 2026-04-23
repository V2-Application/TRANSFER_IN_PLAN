using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SalePlanController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<SalePlanController> _logger;

    public SalePlanController(IConfiguration config, ILogger<SalePlanController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private const string TABLE = "STORE_CALENDAR";
    private const string COLS = "ID, ST_CD, BGT_MNTH_DATE, YEAR, FY, MONTH, YEAR_WEEK, FY_WEEK, FY_WEEK_ST_DT, FY_WEEK_END_DT, DAY, LY_SAME_DATE";

    private static StoreCalendar ReadRow(IDataReader r) => new()
    {
        Id = SnowflakeCrudHelper.Int(r, 0),
        StCd = SnowflakeCrudHelper.StrNull(r, 1),
        BgtMnthDate = SnowflakeCrudHelper.DateNull(r, 2),
        Year = SnowflakeCrudHelper.StrNull(r, 3),
        Fy = SnowflakeCrudHelper.StrNull(r, 4),
        Month = SnowflakeCrudHelper.StrNull(r, 5),
        YearWeek = SnowflakeCrudHelper.StrNull(r, 6),
        FyWeek = SnowflakeCrudHelper.StrNull(r, 7),
        FyWeekStDt = SnowflakeCrudHelper.DateNull(r, 8),
        FyWeekEndDt = SnowflakeCrudHelper.DateNull(r, 9),
        Day = SnowflakeCrudHelper.StrNull(r, 10),
        LySameDate = SnowflakeCrudHelper.DateNull(r, 11)
    };

    public async Task<IActionResult> Index(string? st, string? fy, string? fyWeek, int page = 1, int pageSize = 100)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            var filters = new Dictionary<string, string?> { ["ST_CD"] = st, ["FY"] = fy, ["FY_WEEK"] = fyWeek };
            var (where, parms) = SnowflakeCrudHelper.BuildFilter(filters);

            ViewBag.TotalRows = await SnowflakeCrudHelper.CountAsync(conn, TABLE);
            ViewBag.TotalCount = await SnowflakeCrudHelper.CountAsync(conn, TABLE, where, parms);
            ViewBag.Stores = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "ST_CD");
            ViewBag.FyList = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "FY");
            ViewBag.FyWeeks = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "FY_WEEK");
            ViewBag.TotalStores = ((List<string>)ViewBag.Stores).Count;
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.St = st; ViewBag.Fy = fy; ViewBag.FyWeek = fyWeek;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "ST_CD, BGT_MNTH_DATE", page, pageSize, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading StoreCalendar");
            ViewBag.ErrorMessage = "Error: " + ex.Message;
            return View(new List<StoreCalendar>());
        }
    }

    public IActionResult Create() => View(new StoreCalendar());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(StoreCalendar model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "ST_CD","BGT_MNTH_DATE","YEAR","FY","MONTH","YEAR_WEEK","FY_WEEK","FY_WEEK_ST_DT","FY_WEEK_END_DT","DAY","LY_SAME_DATE" },
                new object?[] { model.StCd, model.BgtMnthDate, model.Year, model.Fy, model.Month, model.YearWeek, model.FyWeek, model.FyWeekStDt, model.FyWeekEndDt, model.Day, model.LySameDate });
            TempData["SuccessMessage"] = "Record created.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating StoreCalendar");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id.Value, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, StoreCalendar model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "ST_CD","BGT_MNTH_DATE","YEAR","FY","MONTH","YEAR_WEEK","FY_WEEK","FY_WEEK_ST_DT","FY_WEEK_END_DT","DAY","LY_SAME_DATE" },
                new object?[] { model.StCd, model.BgtMnthDate, model.Year, model.Fy, model.Month, model.YearWeek, model.FyWeek, model.FyWeekStDt, model.FyWeekEndDt, model.Day, model.LySameDate }, id);
            TempData["SuccessMessage"] = "Record updated.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating StoreCalendar");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id.Value, ReadRow);
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
            _logger.LogError(ex, "Error deleting StoreCalendar");
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? fy, string? fyWeek)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var where = new StringBuilder();
        if (!string.IsNullOrEmpty(st)) where.Append($" AND ST_CD = '{st}'");
        if (!string.IsNullOrEmpty(fy)) where.Append($" AND FY = '{fy}'");
        if (!string.IsNullOrEmpty(fyWeek)) where.Append($" AND FY_WEEK = '{fyWeek}'");
        var filter = where.Length > 0 ? "WHERE " + where.ToString()[5..] : "";

        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,BGT_MNTH_DATE,YEAR,FY,MONTH,YEAR_WEEK,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,DAY,LY_SAME_DATE");
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ST_CD,BGT_MNTH_DATE,YEAR,FY,MONTH,YEAR_WEEK,FY_WEEK,FY_WEEK_ST_DT,FY_WEEK_END_DT,DAY,LY_SAME_DATE FROM {TABLE} {filter} ORDER BY ST_CD,BGT_MNTH_DATE";
        cmd.CommandTimeout = 300;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            sb.AppendLine($"{SnowflakeCrudHelper.Str(r,0)},{SnowflakeCrudHelper.DateNull(r,1)?.ToString("yyyy-MM-dd")},{SnowflakeCrudHelper.Str(r,2)},{SnowflakeCrudHelper.Str(r,3)},{SnowflakeCrudHelper.Str(r,4)},{SnowflakeCrudHelper.Str(r,5)},{SnowflakeCrudHelper.Str(r,6)},{SnowflakeCrudHelper.DateNull(r,7)?.ToString("yyyy-MM-dd")},{SnowflakeCrudHelper.DateNull(r,8)?.ToString("yyyy-MM-dd")},{SnowflakeCrudHelper.Str(r,9)},{SnowflakeCrudHelper.DateNull(r,10)?.ToString("yyyy-MM-dd")}");
        }
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreCalendar_Data.csv");
    }
}
