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
            _logger.LogInformation("Loading full analytics dashboard");
            var vm = new DashboardViewModel();

            // ── KPI Counters ──────────────────────────────────────
            vm.TotalStores = await _context.StoreMasters.CountAsync();

            vm.TotalCategories = await _context.StoreStocks
                .Where(s => s.MajCat != null)
                .Select(s => s.MajCat).Distinct().CountAsync();
            if (vm.TotalCategories == 0)
                vm.TotalCategories = await _context.TrfInPlans
                    .Where(t => t.MajCat != null)
                    .Select(t => t.MajCat).Distinct().CountAsync();

            vm.TotalPlanRows = await _context.TrfInPlans.CountAsync();
            vm.TotalPurchasePlanRows = await _context.PurchasePlans.CountAsync();

            vm.LastExecutionDate = await _context.TrfInPlans
                .Where(t => t.CreatedDt != null)
                .OrderByDescending(t => t.CreatedDt)
                .Select(t => t.CreatedDt).FirstOrDefaultAsync();

            // ── Aggregate Totals ──────────────────────────────────
            vm.TotalTrfInQty = await _context.TrfInPlans
                .Where(t => t.TrfInStkQ != null)
                .SumAsync(t => t.TrfInStkQ ?? 0);

            vm.TotalPurchaseQty = await _context.PurchasePlans
                .Where(p => p.BgtPurQInit != null)
                .SumAsync(p => p.BgtPurQInit ?? 0);

            vm.TotalDcStkShortQ = await _context.PurchasePlans
                .Where(p => p.DcStkShortQ != null)
                .SumAsync(p => p.DcStkShortQ ?? 0);

            vm.TotalDcStkExcessQ = await _context.PurchasePlans
                .Where(p => p.DcStkExcessQ != null)
                .SumAsync(p => p.DcStkExcessQ ?? 0);

            vm.TotalStStkShortQ = await _context.TrfInPlans
                .Where(t => t.StClShortQ != null)
                .SumAsync(t => t.StClShortQ ?? 0);

            vm.TotalStStkExcessQ = await _context.TrfInPlans
                .Where(t => t.StClExcessQ != null)
                .SumAsync(t => t.StClExcessQ ?? 0);

            // ── TrfInPlan: Category Summary ───────────────────────
            vm.CategorySummary = await _context.TrfInPlans
                .Where(t => t.MajCat != null)
                .GroupBy(t => t.MajCat)
                .Select(g => new CategorySummary
                {
                    MajCat       = g.Key,
                    TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                    TotalShortQ   = g.Sum(x => x.StClShortQ ?? 0),
                    TotalExcessQ  = g.Sum(x => x.StClExcessQ ?? 0),
                    RowCount      = g.Count()
                })
                .OrderByDescending(x => x.TotalTrfInQty)
                .Take(15).ToListAsync();

            // ── TrfInPlan: Weekly Trend ───────────────────────────
            vm.WeeklySummary = await _context.TrfInPlans
                .Where(t => t.FyWeek != null)
                .GroupBy(t => new { t.FyYear, t.FyWeek })
                .Select(g => new WeeklySummary
                {
                    FyYear        = g.Key.FyYear ?? 0,
                    FyWeek        = g.Key.FyWeek ?? 0,
                    TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                    RowCount      = g.Count()
                })
                .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek)
                .Take(48).ToListAsync();

            // ── TrfInPlan: Top Short Stores ───────────────────────
            vm.TopShortStores = await _context.TrfInPlans
                .Where(t => t.StClShortQ > 0)
                .GroupBy(t => new { t.StCd, t.StNm, t.MajCat })
                .Select(g => new StoreMetric
                {
                    StCd     = g.Key.StCd,
                    StNm     = g.Key.StNm,
                    MajCat   = g.Key.MajCat,
                    Quantity = g.Sum(x => x.StClShortQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10).ToListAsync();

            // ── TrfInPlan: Top Excess Stores ──────────────────────
            vm.TopExcessStores = await _context.TrfInPlans
                .Where(t => t.StClExcessQ > 0)
                .GroupBy(t => new { t.StCd, t.StNm, t.MajCat })
                .Select(g => new StoreMetric
                {
                    StCd     = g.Key.StCd,
                    StNm     = g.Key.StNm,
                    MajCat   = g.Key.MajCat,
                    Quantity = g.Sum(x => x.StClExcessQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10).ToListAsync();

            // ── TrfInPlan: RDC Summary ────────────────────────────
            vm.RdcSummary = await _context.TrfInPlans
                .Where(t => t.RdcCd != null)
                .GroupBy(t => new { t.RdcCd, t.RdcNm })
                .Select(g => new RdcSummary
                {
                    RdcCd        = g.Key.RdcCd,
                    RdcNm        = g.Key.RdcNm,
                    TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                    StoreCount   = g.Select(x => x.StCd).Distinct().Count()
                })
                .OrderByDescending(x => x.TotalTrfInQty)
                .Take(15).ToListAsync();

            // ── PurchasePlan: Category Summary ────────────────────
            vm.PpCategorySummary = await _context.PurchasePlans
                .Where(p => p.MajCat != null)
                .GroupBy(p => p.MajCat)
                .Select(g => new PpCategorySummary
                {
                    MajCat        = g.Key,
                    BgtPurQ       = g.Sum(x => x.BgtPurQInit ?? 0),
                    DcStkShortQ   = g.Sum(x => x.DcStkShortQ ?? 0),
                    DcStkExcessQ  = g.Sum(x => x.DcStkExcessQ ?? 0),
                    StStkShortQ   = g.Sum(x => x.StStkShortQ ?? 0),
                    StStkExcessQ  = g.Sum(x => x.StStkExcessQ ?? 0),
                    RowCount      = g.Count()
                })
                .OrderByDescending(x => x.BgtPurQ)
                .Take(15).ToListAsync();

            // ── PurchasePlan: Weekly Trend ────────────────────────
            vm.PpWeeklySummary = await _context.PurchasePlans
                .Where(p => p.FyWeek != null)
                .GroupBy(p => new { p.FyYear, p.FyWeek })
                .Select(g => new PpWeeklySummary
                {
                    FyYear       = g.Key.FyYear ?? 0,
                    FyWeek       = g.Key.FyWeek ?? 0,
                    BgtPurQ      = g.Sum(x => x.BgtPurQInit ?? 0),
                    DcStkShortQ  = g.Sum(x => x.DcStkShortQ ?? 0),
                    DcStkExcessQ = g.Sum(x => x.DcStkExcessQ ?? 0)
                })
                .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek)
                .Take(48).ToListAsync();

            // ── PurchasePlan: Top DC Short Categories ─────────────
            vm.TopDcShortCategories = await _context.PurchasePlans
                .Where(p => p.DcStkShortQ > 0 && p.RdcCd != null)
                .GroupBy(p => new { p.RdcCd, p.RdcNm, p.MajCat })
                .Select(g => new DcStockMetric
                {
                    RdcCd    = g.Key.RdcCd,
                    RdcNm    = g.Key.RdcNm,
                    MajCat   = g.Key.MajCat,
                    Quantity = g.Sum(x => x.DcStkShortQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10).ToListAsync();

            // ── PurchasePlan: Top DC Excess Categories ────────────
            vm.TopDcExcessCategories = await _context.PurchasePlans
                .Where(p => p.DcStkExcessQ > 0 && p.RdcCd != null)
                .GroupBy(p => new { p.RdcCd, p.RdcNm, p.MajCat })
                .Select(g => new DcStockMetric
                {
                    RdcCd    = g.Key.RdcCd,
                    RdcNm    = g.Key.RdcNm,
                    MajCat   = g.Key.MajCat,
                    Quantity = g.Sum(x => x.DcStkExcessQ ?? 0)
                })
                .OrderByDescending(x => x.Quantity)
                .Take(10).ToListAsync();

            _logger.LogInformation("Dashboard loaded: Stores={S} TrfInRows={T} PpRows={P}",
                vm.TotalStores, vm.TotalPlanRows, vm.TotalPurchasePlanRows);
            return View(vm);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading analytics dashboard");
            return View(new DashboardViewModel());
        }
    }

    public IActionResult Privacy() => View();
}
