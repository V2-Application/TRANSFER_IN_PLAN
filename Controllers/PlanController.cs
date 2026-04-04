using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class PlanController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<PlanController> _logger;

        public PlanController(PlanningDbContext context, ILogger<PlanController> logger)
        {
            _context = context;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Execute()
        {
            var @params = new SpExecutionParams();
            return View(@params);
        }

        [HttpPost]
        public async Task<IActionResult> Execute(SpExecutionParams model)
        {
            if (!ModelState.IsValid) return View(model);
            try
            {
                await _context.Database.ExecuteSqlRawAsync(
                    "EXEC sp_TrfInPlan @FyYear={0}, @FyWeek={1}, @Ssn={2}",
                    model.FyYear, model.FyWeek, model.Ssn);
                _logger.LogInformation("TrfInPlan SP executed: Year={Year} Week={Week} Ssn={Ssn}", model.FyYear, model.FyWeek, model.Ssn);
                TempData["SuccessMessage"] = "Transfer In Plan executed successfully.";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing TrfInPlan SP");
                TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
            }
            return RedirectToAction(nameof(Execute));
        }

        [HttpGet]
        public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? majCat, string? stCd, int page = 1, int pageSize = 100)
        {
            var query = _context.TrfInPlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);

            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page;
            ViewBag.PageSize = pageSize;
            ViewBag.FyYear = fyYear;
            ViewBag.FyWeek = fyWeek;
            ViewBag.MajCat = majCat;
            ViewBag.StCd = stCd;
            ViewBag.Categories = await _context.TrfInPlans.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();

            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            _logger.LogInformation("TrfInPlan Output: {Count} rows returned", data.Count);
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
        {
            var query = _context.TrfInPlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);

            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("TrfInPlan ExportCsv: {Count} rows exported", data.Count);

            var sb = new StringBuilder();
            sb.AppendLine("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Area,MajCat,Ssn,WeekId,WkStDt,WkEndDt,FyYear,FyWeek,SGrtStkQ,WGrtStkQ,BgtDispClQ,BgtDispClOpt,Cm1SaleCoverDay,Cm2SaleCoverDay,CoverSaleQty,BgtStClMbq,BgtDispClOptMbq,BgtTtlCfOpStkQ,NtActQ,NetBgtCfStkQ,CmBgtSaleQ,Cm1BgtSaleQ,Cm2BgtSaleQ,TrfInStkQ,TrfInOptCnt,TrfInOptMbq,DcMbq,BgtTtlCfClStkQ,BgtNtActQ,NetStClStkQ,StClExcessQ,StClShortQ,CreatedDt,CreatedBy");
            foreach (var r in data)
            {
                sb.AppendLine(string.Join(",",
                    r.Id, Q(r.StCd), Q(r.StNm), Q(r.RdcCd), Q(r.RdcNm), Q(r.HubCd), Q(r.HubNm), Q(r.Area), Q(r.MajCat),
                    r.Ssn, r.WeekId, r.WkStDt?.ToString("yyyy-MM-dd"), r.WkEndDt?.ToString("yyyy-MM-dd"),
                    r.FyYear, r.FyWeek, r.SGrtStkQ, r.WGrtStkQ, r.BgtDispClQ, r.BgtDispClOpt,
                    r.Cm1SaleCoverDay, r.Cm2SaleCoverDay, r.CoverSaleQty, r.BgtStClMbq, r.BgtDispClOptMbq,
                    r.BgtTtlCfOpStkQ, r.NtActQ, r.NetBgtCfStkQ, r.CmBgtSaleQ, r.Cm1BgtSaleQ, r.Cm2BgtSaleQ,
                    r.TrfInStkQ, r.TrfInOptCnt, r.TrfInOptMbq, r.DcMbq, r.BgtTtlCfClStkQ, r.BgtNtActQ,
                    r.NetStClStkQ, r.StClExcessQ, r.StClShortQ,
                    r.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss"), Q(r.CreatedBy)));
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return File(bytes, "text/csv", $"TrfInPlan_{fyYear}_{fyWeek}.csv");
        }

        private static string Q(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }
    }
}
