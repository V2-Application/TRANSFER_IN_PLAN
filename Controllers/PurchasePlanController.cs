using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using TRANSFER_IN_PLAN.Services;

namespace TRANSFER_IN_PLAN.Controllers;

public class PurchasePlanController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly PlanService _planService;
    private readonly ILogger<PurchasePlanController> _logger;

    public PurchasePlanController(PlanningDbContext context, PlanService planService, ILogger<PurchasePlanController> logger)
    {
        _context = context;
        _planService = planService;
        _logger = logger;
    }

    public async Task<IActionResult> Execute()
    {
        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(new PurchasePlanExecutionParams());
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Execute(PurchasePlanExecutionParams parameters)
    {
        try
        {
            var (rowsInserted, executionTime) = await _planService.ExecutePurchasePlanAsync(
                parameters.StartWeekId,
                parameters.EndWeekId,
                parameters.RdcCode,
                parameters.MajCat);

            ViewBag.SuccessMessage = $"SP executed successfully! {rowsInserted} rows inserted. Executed at: {executionTime:yyyy-MM-dd HH:mm:ss}";
            ViewBag.RowsInserted = rowsInserted;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error executing purchase plan: {ex.Message}");
            ViewBag.ErrorMessage = $"Error executing purchase plan: {ex.Message}";
        }

        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(parameters);
    }

    public async Task<IActionResult> Output(int? startWeekId, int? endWeekId, string? rdcCode, string? majCat)
    {
        try
        {
            var query = _context.PurchasePlans.AsQueryable();

            if (startWeekId.HasValue)
                query = query.Where(p => p.WeekId >= startWeekId);

            if (endWeekId.HasValue)
                query = query.Where(p => p.WeekId <= endWeekId);

            if (!string.IsNullOrEmpty(rdcCode))
                query = query.Where(p => p.RdcCd == rdcCode);

            if (!string.IsNullOrEmpty(majCat))
                query = query.Where(p => p.MajCat == majCat);

            var plans = await query.OrderBy(p => p.RdcCd).ThenBy(p => p.MajCat).ThenBy(p => p.WeekId).ToListAsync();

            ViewBag.RdcList = await _context.PurchasePlans.Select(p => p.RdcCd).Distinct().OrderBy(r => r).ToListAsync();
            ViewBag.CategoryList = await _context.PurchasePlans.Select(p => p.MajCat).Distinct().OrderBy(c => c).ToListAsync();
            ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();

            return View(plans);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error loading purchase plan output: {ex.Message}");
            ViewBag.ErrorMessage = $"Error loading purchase plan output: {ex.Message}";
            return View(new List<PurchasePlan>());
        }
    }

    [HttpPost]
    public async Task<IActionResult> ExportExcel(int? startWeekId, int? endWeekId, string? rdcCode, string? majCat)
    {
        try
        {
            var excelBytes = await _planService.ExportPurchasePlanToExcel(startWeekId, endWeekId, rdcCode, majCat);
            var fileName = $"PurchasePlan_{DateTime.Now:yyyyMMdd_HHmmss}.xlsx";
            return File(excelBytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", fileName);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error exporting to Excel: {ex.Message}");
            return BadRequest($"Error exporting to Excel: {ex.Message}");
        }
    }
}
