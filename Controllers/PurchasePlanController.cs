using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
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
            ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
            ViewBag.RdcCodes = await _context.StoreMasters.Select(x => x.RdcCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.MajCats = await _context.ProductHierarchies.Select(x => x.MajCatNm).Where(x => x != null && x != "NA").Distinct().OrderBy(x => x).ToListAsync();
            return View(new PurchasePlanExecutionParams());
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Execute(PurchasePlanExecutionParams model)
        {
            if (!ModelState.IsValid) { ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync(); ViewBag.RdcCodes = await _context.StoreMasters.Select(x => x.RdcCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync(); ViewBag.MajCats = await _context.ProductHierarchies.Select(x => x.MajCatNm).Where(x => x != null && x != "NA").Distinct().OrderBy(x => x).ToListAsync(); return View(model); }
            try
            {
                var pStart = new SqlParameter("@StartWeekId", model.StartWeekId);
                var pEnd = new SqlParameter("@EndWeekId", model.EndWeekId);
                var pRdc = new SqlParameter("@RdcCode", SqlDbType.VarChar) { Value = (object?)model.RdcCode ?? DBNull.Value };
                var pMaj = new SqlParameter("@MajCat", SqlDbType.VarChar) { Value = (object?)model.MajCat ?? DBNull.Value };
                await _context.Database.ExecuteSqlRawAsync(
                    "EXEC SP_GENERATE_PURCHASE_PLAN @StartWeekID=@StartWeekId, @EndWeekID=@EndWeekId, @RdcCode=@RdcCode, @MajCat=@MajCat",
                    pStart, pEnd, pRdc, pMaj);
                _logger.LogInformation("PurchasePlan SP executed: StartWeek={Start} EndWeek={End}", model.StartWeekId, model.EndWeekId);
                TempData["SuccessMessage"] = "Purchase Plan executed successfully.";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing PurchasePlan SP");
                TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
            }
            return RedirectToAction(nameof(Execute));
        }

        [HttpPost, ValidateAntiForgeryToken]
        public async Task<IActionResult> ResetPp()
        {
            try
            {
                await _context.Database.ExecuteSqlRawAsync("TRUNCATE TABLE [dbo].[PURCHASE_PLAN]");
                _logger.LogInformation("PURCHASE_PLAN truncated");
                TempData["SuccessMessage"] = "Purchase Plan data cleared successfully.";
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ResetPp error");
                TempData["ErrorMessage"] = $"Error: {ex.InnerException?.Message ?? ex.Message}";
            }
            return RedirectToAction(nameof(Execute));
        }

        [HttpGet]
        public async Task<IActionResult> Output(int? fyYear, int? fyWeek, string? rdcCd, string? majCat, int page = 1, int pageSize = 100)
        {
            var query = _context.PurchasePlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) { var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => rdcs.Contains(x.RdcCd)); }
            if (!string.IsNullOrEmpty(majCat)) { var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => cats.Contains(x.MajCat)); }

            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page;
            ViewBag.PageSize = pageSize;
            ViewBag.FyYear = fyYear;
            ViewBag.FyWeek = fyWeek;
            ViewBag.RdcCd = rdcCd;
            ViewBag.MajCat = majCat;
            ViewBag.Categories = await _context.PurchasePlans.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.RdcCodes = await _context.PurchasePlans.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();

            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            _logger.LogInformation("PurchasePlan Output: {Count} rows returned", data.Count);
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var query = _context.PurchasePlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) { var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => rdcs.Contains(x.RdcCd)); }
            if (!string.IsNullOrEmpty(majCat)) { var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => cats.Contains(x.MajCat)); }

            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("PurchasePlan ExportCsv: {Count} rows exported", data.Count);

            var sb = new StringBuilder();
            sb.AppendLine("RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,DcStkQ,GrtStkQ,SGrtStkQ,WGrtStkQ,BinCapDcTeam,BinCap,BgtDispClQ,CwBgtSaleQ,Cw1BgtSaleQ,Cw2BgtSaleQ,Cw3BgtSaleQ,Cw4BgtSaleQ,Cw5BgtSaleQ,BgtStOpMbq,NetStOpStkQ,BgtDcOpStkQ,PpNtActQ,BgtCfStkQ,TtlStk,OpStk,NtActStk,GrtConsPct,GrtConsQ,DelPendQ,PpNetBgtCfStkQ,CwTrfOutQ,Cw1TrfOutQ,Cw2TrfOutQ,Cw3TrfOutQ,Cw4TrfOutQ,TtlTrfOutQ,BgtStClMbq,NetBgtStClStkQ,NetSsnlClStkQ,BgtDcMbqSale,BgtDcClMbq,BgtDcClStkQ,BgtPurQInit,PosPORaised,NegPORaised,BgtCoClStkQ,DcStkExcessQ,DcStkShortQ,StStkExcessQ,StStkShortQ,CoStkExcessQ,CoStkShortQ,FreshBinReq,GrtBinReq");
            foreach (var r in data)
            {
                sb.AppendLine(string.Join(",",
                    Q(r.RdcCd), Q(r.RdcNm ?? "NA"), Q(r.MajCat),
                    Q(r.Seg ?? "NA"), Q(r.Div ?? "NA"), Q(r.SubDiv ?? "NA"), Q(r.MajCatNm ?? "NA"), Q(r.Ssn ?? "NA"),
                    r.FyYear, r.FyWeek,
                    r.DcStkQ ?? 0, r.GrtStkQ ?? 0, r.SGrtStkQ ?? 0, r.WGrtStkQ ?? 0, r.BinCapDcTeam ?? 0, r.BinCap ?? 0,
                    r.BgtDispClQ ?? 0, r.CwBgtSaleQ ?? 0, r.Cw1BgtSaleQ ?? 0, r.Cw2BgtSaleQ ?? 0, r.Cw3BgtSaleQ ?? 0, r.Cw4BgtSaleQ ?? 0, r.Cw5BgtSaleQ ?? 0,
                    r.BgtStOpMbq ?? 0, r.NetStOpStkQ ?? 0, r.BgtDcOpStkQ ?? 0, r.PpNtActQ ?? 0, r.BgtCfStkQ ?? 0,
                    r.TtlStk ?? 0, r.OpStk ?? 0, r.NtActStk ?? 0, r.GrtConsPct ?? 0, r.GrtConsQ ?? 0, r.DelPendQ ?? 0,
                    r.PpNetBgtCfStkQ ?? 0, r.CwTrfOutQ ?? 0, r.Cw1TrfOutQ ?? 0, r.Cw2TrfOutQ ?? 0, r.Cw3TrfOutQ ?? 0, r.Cw4TrfOutQ ?? 0, r.TtlTrfOutQ ?? 0,
                    r.BgtStClMbq ?? 0, r.NetBgtStClStkQ ?? 0, r.NetSsnlClStkQ ?? 0,
                    r.BgtDcMbqSale ?? 0, r.BgtDcClMbq ?? 0, r.BgtDcClStkQ ?? 0, r.BgtPurQInit ?? 0,
                    r.PosPORaised ?? 0, r.NegPORaised ?? 0, r.BgtCoClStkQ ?? 0,
                    r.DcStkExcessQ ?? 0, r.DcStkShortQ ?? 0, r.StStkExcessQ ?? 0, r.StStkShortQ ?? 0,
                    r.CoStkExcessQ ?? 0, r.CoStkShortQ ?? 0, r.FreshBinReq ?? 0, r.GrtBinReq ?? 0));
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return File(bytes, "text/csv", $"PurchasePlan_{fyYear}_{fyWeek}.csv");
        }

        [HttpGet]
        public async Task<IActionResult> ExportPivotCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var query = _context.PurchasePlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) { var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => rdcs.Contains(x.RdcCd)); }
            if (!string.IsNullOrEmpty(majCat)) { var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries); query = query.Where(x => cats.Contains(x.MajCat)); }

            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ThenBy(x => x.FyWeek).ToListAsync();
            _logger.LogInformation("PurchasePlan ExportPivotCsv: {Count} rows pivoted", data.Count);

            var weeks = data.Select(x => x.FyWeek ?? 0).Distinct().OrderBy(w => w).ToList();

            // Metrics only for Week 1 (opening week)
            var week1Only = new HashSet<string> { "DC_STK_Q", "GRT_STK_Q", "S_GRT_STK_Q", "W_GRT_STK_Q", "BIN_CAP_DC_TEAM", "BIN_CAP" };
            var firstWeek = weeks.First();

            // All metrics per week (week-first: WK-1 all cols, WK-2 all cols, ...)
            var metrics = new (string Label, Func<PurchasePlan, decimal?>)[] {
                ("DC_STK_Q", r => r.DcStkQ),
                ("GRT_STK_Q", r => r.GrtStkQ),
                ("S_GRT_STK_Q", r => r.SGrtStkQ),
                ("W_GRT_STK_Q", r => r.WGrtStkQ),
                ("BIN_CAP_DC_TEAM", r => r.BinCapDcTeam),
                ("BIN_CAP", r => r.BinCap),
                ("BGT_DISP_CL_Q", r => r.BgtDispClQ),
                ("CW_BGT_SALE_Q", r => r.CwBgtSaleQ),
                ("CW1_BGT_SALE_Q", r => r.Cw1BgtSaleQ),
                ("CW2_BGT_SALE_Q", r => r.Cw2BgtSaleQ),
                ("CW3_BGT_SALE_Q", r => r.Cw3BgtSaleQ),
                ("CW4_BGT_SALE_Q", r => r.Cw4BgtSaleQ),
                ("CW5_BGT_SALE_Q", r => r.Cw5BgtSaleQ),
                ("BGT_ST_OP_MBQ", r => r.BgtStOpMbq),
                ("NET_ST_OP_STK_Q", r => r.NetStOpStkQ),
                ("BGT_DC_OP_STK_Q", r => r.BgtDcOpStkQ),
                ("PP_NT_ACT_Q", r => r.PpNtActQ),
                ("BGT_CF_STK_Q", r => r.BgtCfStkQ),
                ("TTL_STK", r => r.TtlStk),
                ("OP_STK", r => r.OpStk),
                ("NT_ACT_STK", r => r.NtActStk),
                ("GRT_CONS_PCT", r => r.GrtConsPct),
                ("GRT_CONS_Q", r => r.GrtConsQ),
                ("DEL_PEND_Q", r => r.DelPendQ),
                ("PP_NET_BGT_CF_STK_Q", r => r.PpNetBgtCfStkQ),
                ("CW_TRF_OUT_Q", r => r.CwTrfOutQ),
                ("CW1_TRF_OUT_Q", r => r.Cw1TrfOutQ),
                ("CW2_TRF_OUT_Q", r => r.Cw2TrfOutQ),
                ("CW3_TRF_OUT_Q", r => r.Cw3TrfOutQ),
                ("CW4_TRF_OUT_Q", r => r.Cw4TrfOutQ),
                ("TTL_TRF_OUT_Q", r => r.TtlTrfOutQ),
                ("BGT_ST_CL_MBQ", r => r.BgtStClMbq),
                ("NET_BGT_ST_CL_STK_Q", r => r.NetBgtStClStkQ),
                ("NET_SSNL_CL_STK_Q", r => r.NetSsnlClStkQ),
                ("BGT_DC_MBQ_SALE", r => r.BgtDcMbqSale),
                ("BGT_DC_CL_MBQ", r => r.BgtDcClMbq),
                ("BGT_DC_CL_STK_Q", r => r.BgtDcClStkQ),
                ("BGT_PUR_Q_INIT", r => r.BgtPurQInit),
                ("POS_PO_RAISED", r => r.PosPORaised),
                ("NEG_PO_RAISED", r => r.NegPORaised),
                ("BGT_CO_CL_STK_Q", r => r.BgtCoClStkQ),
                ("DC_STK_EXCESS_Q", r => r.DcStkExcessQ),
                ("DC_STK_SHORT_Q", r => r.DcStkShortQ),
                ("ST_STK_EXCESS_Q", r => r.StStkExcessQ),
                ("ST_STK_SHORT_Q", r => r.StStkShortQ),
                ("CO_STK_EXCESS_Q", r => r.CoStkExcessQ),
                ("CO_STK_SHORT_Q", r => r.CoStkShortQ),
                ("FRESH_BIN_REQ", r => r.FreshBinReq),
                ("GRT_BIN_REQ", r => r.GrtBinReq)
            };

            var sb = new StringBuilder();

            // Header: identifiers + WK-n_METRIC (skip week1-only columns for weeks 2+)
            sb.Append("RDC_CD,RDC_NM,MAJ_CAT,SEG,DIV,SUB_DIV,MAJ_CAT_NM,SSN");
            foreach (var w in weeks)
                foreach (var m in metrics)
                    if (w == firstWeek || !week1Only.Contains(m.Label))
                        sb.Append($",WK-{w}_{m.Label}");
            sb.AppendLine();

            // Group by RDC + category
            var groups = data.GroupBy(x => new { x.RdcCd, x.RdcNm, x.MajCat, x.Seg, x.Div, x.SubDiv, x.MajCatNm, x.Ssn });

            foreach (var g in groups)
            {
                var k = g.Key;
                sb.Append(string.Join(",", Q(k.RdcCd), Q(k.RdcNm), Q(k.MajCat),
                    Q(k.Seg ?? "NA"), Q(k.Div ?? "NA"), Q(k.SubDiv ?? "NA"), Q(k.MajCatNm ?? "NA"), Q(k.Ssn ?? "NA")));

                var byWeek = g.GroupBy(x => x.FyWeek ?? 0).ToDictionary(x => x.Key, x => x.First());
                for (int wi = 0; wi < weeks.Count; wi++)
                {
                    var w = weeks[wi];
                    byWeek.TryGetValue(w, out var r);
                    // Chain: WK-2+ OP_STK = previous week NET_SSNL_CL_STK_Q
                    PurchasePlan? prevR = null;
                    if (wi > 0) byWeek.TryGetValue(weeks[wi - 1], out prevR);
                    foreach (var m in metrics)
                    {
                        if (w != firstWeek && week1Only.Contains(m.Label)) continue;
                        decimal val = 0;
                        if (r != null)
                        {
                            // For OP_STK in week 2+, use previous week's NET_SSNL_CL_STK_Q
                            if (m.Label == "OP_STK" && wi > 0 && prevR != null)
                                val = prevR.NetSsnlClStkQ ?? 0;
                            else
                                val = m.Item2(r) ?? 0;
                        }
                        sb.Append($",{val}");
                    }
                }
                sb.AppendLine();
            }

            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", $"PurchasePlan_Pivot_{fyYear}_{fyWeek}.csv");
        }

        private static string Q(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }
    }
}
