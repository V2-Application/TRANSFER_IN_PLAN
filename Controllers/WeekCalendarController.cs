using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class WeekCalendarController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<WeekCalendarController> _logger;

    public WeekCalendarController(IConfiguration config, ILogger<WeekCalendarController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static WeekCalendar ReadRow(IDataReader r) => new()
    {
        WeekId  = SnowflakeCrudHelper.Int(r, 0),
        WeekSeq = SnowflakeCrudHelper.Int(r, 1),
        FyWeek  = SnowflakeCrudHelper.Int(r, 2),
        FyYear  = SnowflakeCrudHelper.Int(r, 3),
        CalYear = SnowflakeCrudHelper.Int(r, 4),
        YearWeek = SnowflakeCrudHelper.StrNull(r, 5),
        WkStDt  = SnowflakeCrudHelper.DateNull(r, 6),
        WkEndDt = SnowflakeCrudHelper.DateNull(r, 7)
    };

    private const string TABLE = "WEEK_CALENDAR";
    private const string COLS = "WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT";

    // ── Custom find by WEEK_ID (not auto-increment ID) ───────────
    private async Task<WeekCalendar?> FindByWeekIdAsync(SnowflakeDbConnection conn, int weekId)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {COLS} FROM {TABLE} WHERE WEEK_ID = ?";
        cmd.Parameters.Add(SnowflakeCrudHelper.Param("1", weekId, DbType.Int32));
        await using var r = await cmd.ExecuteReaderAsync();
        return await r.ReadAsync() ? ReadRow(r) : null;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            _logger.LogInformation("Loading WeekCalendar list.");
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, null, null, "WEEK_ID", 1, 100000, ReadRow);
            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading WeekCalendar list.");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<WeekCalendar>());
        }
    }

    public IActionResult Create() => View();

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(WeekCalendar model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "WEEK_ID", "WEEK_SEQ", "FY_WEEK", "FY_YEAR", "CAL_YEAR", "YEAR_WEEK", "WK_ST_DT", "WK_END_DT" },
                new object?[] { model.WeekId, model.WeekSeq, model.FyWeek, model.FyYear, model.CalYear, model.YearWeek, model.WkStDt, model.WkEndDt });
            _logger.LogInformation("WeekCalendar created: WeekId={WeekId}", model.WeekId);
            TempData["SuccessMessage"] = $"Week {model.YearWeek} created successfully.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating WeekCalendar WeekId={WeekId}", model.WeekId);
            ModelState.AddModelError("", "Error saving record: " + ex.Message);
            return View(model);
        }
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await FindByWeekIdAsync(conn, id.Value);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, WeekCalendar model)
    {
        if (id != model.WeekId) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"UPDATE {TABLE} SET WEEK_SEQ=?, FY_WEEK=?, FY_YEAR=?, CAL_YEAR=?, YEAR_WEEK=?, WK_ST_DT=?, WK_END_DT=? WHERE WEEK_ID=?";
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("1", model.WeekSeq, DbType.Int32));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("2", model.FyWeek, DbType.Int32));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("3", model.FyYear, DbType.Int32));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("4", model.CalYear, DbType.Int32));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("5", model.YearWeek));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("6", model.WkStDt, DbType.Date));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("7", model.WkEndDt, DbType.Date));
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("8", model.WeekId, DbType.Int32));
            await cmd.ExecuteNonQueryAsync();
            _logger.LogInformation("WeekCalendar updated: WeekId={WeekId}", model.WeekId);
            TempData["SuccessMessage"] = $"Week {model.YearWeek} updated successfully.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating WeekCalendar WeekId={WeekId}", model.WeekId);
            ModelState.AddModelError("", "Error updating record: " + ex.Message);
            return View(model);
        }
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await FindByWeekIdAsync(conn, id.Value);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DELETE FROM {TABLE} WHERE WEEK_ID=?";
            cmd.Parameters.Add(SnowflakeCrudHelper.Param("1", id, DbType.Int32));
            await cmd.ExecuteNonQueryAsync();
            _logger.LogInformation("WeekCalendar deleted: WeekId={WeekId}", id);
            TempData["SuccessMessage"] = "Week calendar record deleted successfully.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting WeekCalendar WeekId={WeekId}", id);
            TempData["ErrorMessage"] = "Error deleting record: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }
}
