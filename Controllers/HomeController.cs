using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class HomeController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<HomeController> _logger;

    public HomeController(PlanningDbContext context, ILogger<HomeController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            _logger.LogInformation("Loading dashboard data");

            var vm = new DashboardViewModel();

            // Total active stores
            vm.TotalStores = await _context.StoreMasters
                .Where(s => s.Status == "Active")
                .CountAsync();

            // Total distinct categories from TrfInPlan
            vm.TotalCategories = await _context.TrfInPlans
                .Select(t => t.MajCat)
                .Distinct()
                .CountAsync();

            // Total plan rows
            vm.TotalPlanRows = await _context.TrfInPlans.CountAsync();

            // Last execution date
            vm.LastExecutionDate = await _context.TrfInPlans
                .Where(t => t.CreatedDt != null)
                .OrderByDescending(t => t.CreatedDt)
                .Select(t => t.CreatedDt)
                .FirstOrDefaultAsync();

            // Category summary - sum TrfInStkQ grouped by MajCat
            vm.CategorySummary = await _context.TrfInPlans
                .Where(t => t.MajCat != null && t.TrfInStkQ != null)
                .GroupBy(t => t.MajCat)
                .Select(g => new DashboardViewModel.CategorySummary
                {
                    MajCat = g.Key,
                    TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                    RowCount = g.Count()
                })
                .OrderByDescending(x => x.TotalTrfInQty)
                .Take(10)
                .ToListAsync();

            // Weekly summary - sum TrfInStkQ grouped by FyWeek/FyYear
            vm.WeeklySummary = await _context.TrfInPlans
                .Where(t => t.FyWeek != null && t.TrfInStkQ != null)
                .GroupBy(t => new { t.FyYear, t.FyWeek })
                .Select(g => new DashboardViewModel.WeeklySummary
                {
                    FyYear = g.Key.FyYear ?? 0,
                    FyWeek = g.Key.FyWeek ?? 0,
                    TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                    RowCount = g.Count()
                })
                .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek)
                .Take(48)
                .ToListAsync();

            // Top 10 short stores (highest shortage)
            vm.TopShortStores = await _context.TrfInPlans
                .Where(t => t.StClShortQ != null && t.StClShortQ > 0)
                .GroupBy(t => new { t.StCd, t.StNm, t.MajCat })
                .Select(g => new DashboardViewModel.StoreMetric
                {
                    StCd = g.Key.StCd,
                    StNm = g.Key.StNm,
                    MajCat = g.Key.MajCat,
                    Quantity = g.Sum(x => x.StClShortQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10)
                .ToListAsync();

            // Top 10 excess stores (highest excess)
            vm.TopExcessStores = await _context.TrfInPlans
                .Where(t => t.StClExcessQ != null && t.StClExcessQ > 0)
                .GroupBy(t => new { t.StCd, t.StNm, t.MajCat })
                .Select(g => new DashboardViewModel.StoreMetric
                {
                    StCd = g.Key.StCd,
                    StNm = g.Key.StNm,
                    MajCat = g.Key.MajCat,
                    Quantity = g.Sum(x => x.StClExcessQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10)
                .ToListAsync();

            _logger.LogInformation("Dashboard loaded: {Stores} stores, {PlanRows} plan rows", vm.TotalStores, vm.TotalPlanRows);
            return View(vm);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading dashboard");
            return View(new DashboardViewModel());
        }
    }

    public IActionResult Privacy()
    {
        return View();
    }
}
