using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class PlanController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly PlanService _planService;
    private readonly ILogger<PlanController> _logger;

    public PlanController(PlanningDbContext context, PlanService planService, ILogger<PlanController> logger)
    {
        _context = context;
        _planService = planService;
        _logger = logger;
    }

    public async Task<IActionResult> Execute()
    {
        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(new SpExecutionParams());
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Execute(SpExecutionParams parameters)
    {
        try
        {
            var (rowsInserted, executionTime) = await _planService.ExecutePlanGeneration(
                parameters.StartWeekId,
                parameters.EndWeekId,
                parameters.StoreCode,
                parameters.MajCat,
                parameters.CoverDaysCm1,
                parameters.CoverDaysCm2);

            ViewBag.SuccessMessage = $"SP executed successfully! {rowsInserted} rows inserted. Executed at: {executionTime:yyyy-MM-dd HH:mm:ss}";
            ViewBag.RowsInserted = rowsInserted;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error executing plan: {ex.Message}");
            ViewBag.ErrorMessage = $"Error executing plan: {ex.Message}";
        }

        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(parameters);
    }

    public async Task<IActionResult> Output(int? startWeekId, int? endWeekId, string? storeCode, string? majCat)
    {
        try
        {
            var query = _context.TrfInPlans.AsQueryable();

            if (startWeekId.HasValue)
                query = query.Where(p => p.WeekId >= startWeekId);

            if (endWeekId.HasValue)
                query = query.Where(p => p.WeekId <= endWeekId);

            if (!string.IsNullOrEmpty(storeCode))
                query = query.Where(p => p.StCd == storeCode);

            if (!string.IsNullOrEmpty(majCat))
                query = query.Where(p => p.MajCat == majCat);

            var plans = await query.OrderBy(p => p.StCd).ThenBy(p => p.MajCat).ThenBy(p => p.WeekId).ToListAsync();

            ViewBag.StoreList = await _context.TrfInPlans.Select(p => p.StCd).Distinct().OrderBy(s => s).ToListAsync();
            ViewBag.CategoryList = await _context.TrfInPlans.Select(p => p.MajCat).Distinct().OrderBy(c => c).ToListAsync();
            ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();

            return View(plans);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error loading plan output: {ex.Message}");
            ViewBag.ErrorMessage = $"Error loading plan output: {ex.Message}";
            return View(new List<TrfInPlan>());
        }
    }

    [HttpPost]
    public async Task<IActionResult> ExportExcel(int? startWeekId, int? endWeekId, string? storeCode, string? majCat)
    {
        try
        {
            var excelBytes = await _planService.ExportToExcel(startWeekId, endWeekId, storeCode, majCat);
            var fileName = $"TrfInPlan_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
            return File(excelBytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", fileName);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error exporting to Excel: {ex.Message}");
            return BadRequest($"Error exporting to Excel: {ex.Message}");
        }
    }
}
