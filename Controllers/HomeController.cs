using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class HomeController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<HomeController> _logger;
        public HomeController(PlanningDbContext context, ILogger<HomeController> logger) { _context = context; _logger = logger; }

        public async Task<IActionResult> Index()
        {
            var vm = new DashboardViewModel();
            try
            {
                vm.TotalStores = await _context.TrfInPlans.Select(x => x.StCd).Distinct().CountAsync();
                vm.TotalCategories = await _context.TrfInPlans.Select(x => x.MajCat).Distinct().CountAsync();
                vm.TotalPlanRows = await _context.TrfInPlans.CountAsync();
                vm.TotalPurchasePlanRows = await _context.PurchasePlans.CountAsync();
                vm.TotalTrfInQty = await _context.TrfInPlans.SumAsync(x => (decimal)(x.TrfInStkQ ?? 0));
                vm.TotalPurchaseQty = await _context.PurchasePlans.SumAsync(x => (decimal)(x.BgtPurQInit ?? 0));
                vm.TotalDcStkShortQ = await _context.PurchasePlans.SumAsync(x => (decimal)(x.DcStkShortQ ?? 0));
                vm.TotalDcStkExcessQ = await _context.PurchasePlans.SumAsync(x => (decimal)(x.DcStkExcessQ ?? 0));
                vm.TotalStStkShortQ = await _context.PurchasePlans.SumAsync(x => (decimal)(x.StStkShortQ ?? 0));
                vm.TotalStStkExcessQ = await _context.PurchasePlans.SumAsync(x => (decimal)(x.StStkExcessQ ?? 0));
                vm.LastExecutionDate = await _context.TrfInPlans.MaxAsync(x => x.CreatedDt);

                vm.CategorySummary = await _context.TrfInPlans
                    .GroupBy(x => x.MajCat)
                    .Select(g => new CategorySummary
                    {
                        MajCat = g.Key,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        TotalShortQ = g.Sum(x => x.StClShortQ ?? 0),
                        TotalExcessQ = g.Sum(x => x.StClExcessQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderByDescending(x => x.TotalTrfInQty).Take(15).ToListAsync();

                vm.WeeklySummary = await _context.TrfInPlans
                    .GroupBy(x => new { x.FyYear, x.FyWeek })
                    .Select(g => new WeeklySummary
                    {
                        FyYear = g.Key.FyYear ?? 0,
                        FyWeek = g.Key.FyWeek ?? 0,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek).ToListAsync();

                vm.TopShortStores = await _context.TrfInPlans
                    .Where(x => x.StClShortQ > 0)
                    .GroupBy(x => new { x.StCd, x.StNm, x.MajCat })
                    .Select(g => new StoreMetric
                    {
                        StCd = g.Key.StCd,
                        StNm = g.Key.StNm,
                        MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.StClShortQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                vm.TopExcessStores = await _context.TrfInPlans
                    .Where(x => x.StClExcessQ > 0)
                    .GroupBy(x => new { x.StCd, x.StNm, x.MajCat })
                    .Select(g => new StoreMetric
                    {
                        StCd = g.Key.StCd,
                        StNm = g.Key.StNm,
                        MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.StClExcessQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                vm.RdcSummary = await _context.TrfInPlans
                    .GroupBy(x => new { x.RdcCd, x.RdcNm })
                    .Select(g => new RdcSummary
                    {
                        RdcCd = g.Key.RdcCd,
                        RdcNm = g.Key.RdcNm,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        StoreCount = g.Select(x => x.StCd).Distinct().Count()
                    })
                    .OrderByDescending(x => x.TotalTrfInQty).Take(10).ToListAsync();

                vm.PpCategorySummary = await _context.PurchasePlans
                    .GroupBy(x => x.MajCat)
                    .Select(g => new PpCategorySummary
                    {
                        MajCat = g.Key,
                        BgtPurQ = g.Sum(x => x.BgtPurQInit ?? 0),
                        DcStkShortQ = g.Sum(x => x.DcStkShortQ ?? 0),
                        DcStkExcessQ = g.Sum(x => x.DcStkExcessQ ?? 0),
                        StStkShortQ = g.Sum(x => x.StStkShortQ ?? 0),
                        StStkExcessQ = g.Sum(x => x.StStkExcessQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderByDescending(x => x.BgtPurQ).Take(15).ToListAsync();

                vm.PpWeeklySummary = await _context.PurchasePlans
                    .GroupBy(x => new { x.FyYear, x.FyWeek })
                    .Select(g => new PpWeeklySummary
                    {
                        FyYear = g.Key.FyYear ?? 0,
                        FyWeek = g.Key.FyWeek ?? 0,
                        BgtPurQ = g.Sum(x => x.BgtPurQInit ?? 0),
                        DcStkShortQ = g.Sum(x => x.DcStkShortQ ?? 0),
                        DcStkExcessQ = g.Sum(x => x.DcStkExcessQ ?? 0)
                    })
                    .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek).ToListAsync();

                vm.TopDcShortCategories = await _context.PurchasePlans
                    .Where(x => x.DcStkShortQ > 0)
                    .GroupBy(x => new { x.RdcCd, x.RdcNm, x.MajCat })
                    .Select(g => new DcStockMetric
                    {
                        RdcCd = g.Key.RdcCd,
                        RdcNm = g.Key.RdcNm,
                        MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.DcStkShortQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                vm.TopDcExcessCategories = await _context.PurchasePlans
                    .Where(x => x.DcStkExcessQ > 0)
                    .GroupBy(x => new { x.RdcCd, x.RdcNm, x.MajCat })
                    .Select(g => new DcStockMetric
                    {
                        RdcCd = g.Key.RdcCd,
                        RdcNm = g.Key.RdcNm,
                        MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.DcStkExcessQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                _logger.LogInformation("Dashboard loaded: Stores={Stores} Categories={Cat} TrfInRows={TrfIn} PPRows={PP}",
                    vm.TotalStores, vm.TotalCategories, vm.TotalPlanRows, vm.TotalPurchasePlanRows);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading dashboard data");
                ViewBag.ErrorMessage = "Unable to load dashboard data: " + ex.Message;
            }
            return View(vm);
        }

        [HttpGet]
        public async Task<IActionResult> ExportTrfInCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
        {
            var query = _context.TrfInPlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("Dashboard TrfIn ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Area,MajCat,Ssn,WeekId,WkStDt,WkEndDt,FyYear,FyWeek,SGrtStkQ,WGrtStkQ,BgtDispClQ,TrfInStkQ,StClShortQ,StClExcessQ,CreatedDt,CreatedBy");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", r.Id, Q(r.StCd), Q(r.StNm), Q(r.RdcCd), Q(r.RdcNm), Q(r.HubCd), Q(r.HubNm), Q(r.Area), Q(r.MajCat), r.Ssn, r.WeekId, r.WkStDt?.ToString("yyyy-MM-dd"), r.WkEndDt?.ToString("yyyy-MM-dd"), r.FyYear, r.FyWeek, r.SGrtStkQ, r.WGrtStkQ, r.BgtDispClQ, r.TrfInStkQ, r.StClShortQ, r.StClExcessQ, r.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss"), Q(r.CreatedBy)));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "TrfInPlan_Export.csv");
        }

        [HttpGet]
        public async Task<IActionResult> ExportPpCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var query = _context.PurchasePlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("Dashboard PP ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Id,RdcCd,RdcNm,MajCat,Ssn,FyWeek,FyYear,WkStDt,WkEndDt,DcStkQ,GrtStkQ,BgtPurQInit,DcStkShortQ,DcStkExcessQ,StStkShortQ,StStkExcessQ,CreatedDt,CreatedBy");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", r.Id, Q(r.RdcCd), Q(r.RdcNm), Q(r.MajCat), r.Ssn, r.FyWeek, r.FyYear, r.WkStDt?.ToString("yyyy-MM-dd"), r.WkEndDt?.ToString("yyyy-MM-dd"), r.DcStkQ, r.GrtStkQ, r.BgtPurQInit, r.DcStkShortQ, r.DcStkExcessQ, r.StStkShortQ, r.StStkExcessQ, r.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss"), Q(r.CreatedBy)));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "PurchasePlan_Export.csv");
        }

        public IActionResult Privacy() => View();
        private static string Q(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }

    }
}
