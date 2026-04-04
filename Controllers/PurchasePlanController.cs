using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class PurchasePlanController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<PurchasePlanController> _logger;

        public PurchasePlanController(PlanningDbContext context, ILogger<PurchasePlanController> logger)
        {
            _context = context;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Execute()
        {
            var @params = new PurchasePlanExecutionParams();
            return View(@params);
        }

        [HttpPost]
        public async Task<IActionResult> Execute(PurchasePlanExecutionParams model)
        {
            if (!ModelState.IsValid) return View(model);
            try
            {
                await _context.Database.ExecuteSqlRawAsync(
                    "EXEC sp_PurchasePlan @FyYear={0}, @FyWeek={1}, @Ssn={2}",
                    model.FyYear, model.FyWeek, model.Ssn);
                _logger.LogInformation("PurchasePlan SP executed: Year={Year} Week={Week} Ssn={Ssn}", model.FyYear, model.FyWeek, model.Ssn);
                TempData["SuccessMessage"] = "Purchase Plan executed successfully.";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing PurchasePlan SP");
                TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
            }
            return RedirectToAction(nameof(Execute));
        }

        [HttpGet]
        public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? rdcCd, string? majCat, int page = 1, int pageSize = 100)
        {
            var query = _context.PurchasePlan.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);

            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page;
            ViewBag.PageSize = pageSize;
            ViewBag.FyYear = fyYear;
            ViewBag.FyWeek = fyWeek;
            ViewBag.RdcCd = rdcCd;
            ViewBag.MajCat = majCat;
            ViewBag.Categories = await _context.PurchasePlan.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.RdcCodes = await _context.PurchasePlan.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();

            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            _logger.LogInformation("PurchasePlan Output: {Count} rows returned", data.Count);
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var query = _context.PurchasePlan.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);

            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("PurchasePlan ExportCsv: {Count} rows exported", data.Count);

            var sb = new StringBuilder();
            sb.AppendLine("Id,RdcCd,RdcNm,MajCat,Ssn,WeekId,FyWeek,FyYear,WkStDt,WkEndDt,DcStkQ,GrtStkQ,SGrtStkQ,WGrtStkQ,BinCapDcTeam,BinCap,BgtDispClQ,CwBgtSaleQ,Cw1BgtSaleQ,Cw2BgtSaleQ,Cw3BgtSaleQ,Cw4BgtSaleQ,Cw5BgtSaleQ,BgtStOpMbq,NetStOpStkQ,BgtDcOpStkQ,PpNtActQ,BgtCfStkQ,TtlStk,OpStk,NtActStk,GrtConsPct,GrtConsQ,DelPendQ,PpNetBgtCfStkQ,CwTrfOutQ,Cw1TrfOutQ,TtlTrfOutQ,BgtStClMbq,NetBgtStClStkQ,NetSsnlClStkQ,BgtDcMbqSale,BgtDcClMbq,BgtDcClStkQ,BgtPurQInit,PosPORaised,NegPORaised,BgtCoClStkQ,DcStkShortQ,DcStkExcessQ,StStkShortQ,StStkExcessQ,CoStkExcessQ,CoStkShortQ,FreshBinReq,GrtBinReq,CreatedDt,CreatedBy");
            foreach (var r in data)
            {
                sb.AppendLine(string.Join(",",
                    r.Id, Q(r.RdcCd), Q(r.RdcNm), Q(r.MajCat), r.Ssn, r.WeekId, r.FyWeek, r.FyYear,
                    r.WkStDt?.ToString("yyyy-MM-dd"), r.WkEndDt?.ToString("yyyy-MM-dd"),
                    r.DcStkQ, r.GrtStkQ, r.SGrtStkQ, r.WGrtStkQ, r.BinCapDcTeam, r.BinCap,
                    r.BgtDispClQ, r.CwBgtSaleQ, r.Cw1BgtSaleQ, r.Cw2BgtSaleQ, r.Cw3BgtSaleQ, r.Cw4BgtSaleQ, r.Cw5BgtSaleQ,
                    r.BgtStOpMbq, r.NetStOpStkQ, r.BgtDcOpStkQ, r.PpNtActQ, r.BgtCfStkQ,
                    r.TtlStk, r.OpStk, r.NtActStk, r.GrtConsPct, r.GrtConsQ, r.DelPendQ,
                    r.PpNetBgtCfStkQ, r.CwTrfOutQ, r.Cw1TrfOutQ, r.TtlTrfOutQ,
                    r.BgtStClMbq, r.NetBgtStClStkQ, r.NetSsnlClStkQ,
                    r.BgtDcMbqSale, r.BgtDcClMbq, r.BgtDcClStkQ, r.BgtPurQInit,
                    r.PosPORaised, r.NegPORaised, r.BgtCoClStkQ,
                    r.DcStkShortQ, r.DcStkExcessQ, r.StStkShortQ, r.StStkExcessQ,
                    r.CoStkExcessQ, r.CoStkShortQ, r.FreshBinReq, r.GrtBinReq,
                    r.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss"), Q(r.CreatedBy)));
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return File(bytes, "text/csv", $"PurchasePlan_{fyYear}_{fyWeek}.csv");
        }

        private static string Q(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return "\"" + s.Replace("\"", "\"\""") + "\"";
        }
    }
}
