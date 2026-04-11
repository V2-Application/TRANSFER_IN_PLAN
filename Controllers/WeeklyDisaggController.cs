using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class WeeklyDisaggController : Controller
{
    private readonly WeeklyDisaggService _jobService;
    private readonly ILogger<WeeklyDisaggController> _logger;
    private readonly string _connStr;

    public WeeklyDisaggController(WeeklyDisaggService jobService, ILogger<WeeklyDisaggController> logger, IConfiguration config)
    {
        _jobService = jobService; _logger = logger;
        _connStr = config.GetConnectionString("PlanningDatabase")!;
    }

    public async Task<IActionResult> Execute()
    {
        var vm = new WeeklyDisaggViewModel
        {
            SaleBudgetRows = await CountAsync("SALE_BUDGET_PLAN"),
            FixturePlanRows = await CountAsync("FIXTURE_DENSITY_PLAN"),
            SaleQtyRows = await CountAsync("QTY_SALE_QTY"),
            DispQtyRows = await CountAsync("QTY_DISP_QTY"),
            WeekCalendarRows = await CountAsync("WEEK_CALENDAR"),
            AvailableMonths = await ListAsync("SELECT DISTINCT FORMAT(PLAN_MONTH, 'yyyy-MM') FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) ORDER BY 1"),
        };

        // Recent runs
        try
        {
            await using var conn = new SqlConnection(_connStr); await conn.OpenAsync();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT TOP 10 ID, RUN_ID, SOURCE_TABLE, TARGET_TABLE, ROWS_WRITTEN, MONTHS_PROCESSED, WEEKS_PER_MONTH, METHOD, CREATED_DT FROM dbo.WEEKLY_DISAGG_LOG WITH (NOLOCK) ORDER BY CREATED_DT DESC";
            await using var rdr = await cmd.ExecuteReaderAsync();
            while (await rdr.ReadAsync())
                vm.RecentRuns.Add(new WeeklyDisaggLog
                {
                    Id = rdr.GetInt32(0), RunId = rdr.IsDBNull(1) ? null : rdr.GetString(1),
                    SourceTable = rdr.IsDBNull(2) ? null : rdr.GetString(2), TargetTable = rdr.IsDBNull(3) ? null : rdr.GetString(3),
                    RowsWritten = rdr.GetInt32(4), MonthsProcessed = rdr.GetInt32(5),
                    WeeksPerMonth = rdr.GetInt32(6), Method = rdr.IsDBNull(7) ? null : rdr.GetString(7),
                    CreatedDt = rdr.IsDBNull(8) ? null : rdr.GetDateTime(8)
                });
        }
        catch { }

        return View(vm);
    }

    [HttpPost]
    public IActionResult StartDisagg(string method)
    {
        if (_jobService.IsRunning) return Json(new { success = false, message = "Already running." });
        var started = _jobService.TryStartDisagg(method ?? "EQUAL_SPLIT");
        return Json(new { success = started });
    }

    [HttpGet] public IActionResult JobStatus() => Json(_jobService.GetStatus());

    private async Task<int> CountAsync(string t) { try { await using var c = new SqlConnection(_connStr); await c.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = $"SELECT COUNT(1) FROM dbo.[{t}] WITH (NOLOCK)"; return (int)(await cmd.ExecuteScalarAsync() ?? 0); } catch { return 0; } }
    private async Task<List<string>> ListAsync(string sql) { var l = new List<string>(); try { await using var c = new SqlConnection(_connStr); await c.OpenAsync(); await using var cmd = c.CreateCommand(); cmd.CommandText = sql; await using var r = await cmd.ExecuteReaderAsync(); while (await r.ReadAsync()) l.Add(r.GetString(0)); } catch { } return l; }
}
