using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
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

            TempData["SuccessMessage"] = $"SP executed successfully! {rowsInserted} rows inserted at {executionTime:yyyy-MM-dd HH:mm:ss}";
            _logger.LogInformation("TrfInPlan SP executed: Start={Start} End={End} Rows={Rows}", parameters.StartWeekId, parameters.EndWeekId, rowsInserted);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing TrfInPlan SP");
            TempData["ErrorMessage"] = "Execution failed: " + (ex.InnerException?.Message ?? ex.Message);
        }

        ViewBag.WeekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(parameters);
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
        sb.AppendLine("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Area,MajCat,Ssn,WeekId,WkStDt,WkEndDt,FyYear,FyWeek,SGrtStkQ,WGrtStkQ,BgtDispClQ,TrfInStkQ,StClShortQ,StClExcessQ,CreatedDt,CreatedBy");
        foreach (var r in data)
        {
            sb.AppendLine(string.Join(",",
                r.Id, Q(r.StCd), Q(r.StNm), Q(r.RdcCd), Q(r.RdcNm), Q(r.HubCd), Q(r.HubNm), Q(r.Area), Q(r.MajCat),
                r.Ssn, r.WeekId, r.WkStDt?.ToString("yyyy-MM-dd"), r.WkEndDt?.ToString("yyyy-MM-dd"),
                r.FyYear, r.FyWeek, r.SGrtStkQ, r.WGrtStkQ, r.BgtDispClQ,
                r.TrfInStkQ, r.StClShortQ, r.StClExcessQ,
                r.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss"), Q(r.CreatedBy)));
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", $"TrfInPlan_{fyYear}_{fyWeek}.csv");
    }

    private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }

