using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubDcStkMvgrController : Controller
{
    private readonly PlanningDbContext _context;
    public SubDcStkMvgrController(PlanningDbContext context) => _context = context;

    public async Task<IActionResult> Index(string? rdcCd = null, string? majCat = null, string? subValue = null, int page = 1, int pageSize = 100)
    {
        var query = _context.SubDcStkMvgrs.AsQueryable();
        if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
        if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
        if (!string.IsNullOrEmpty(subValue)) query = query.Where(x => x.SubValue == subValue);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.RdcCd = rdcCd; ViewBag.MajCat = majCat; ViewBag.SubValue = subValue;
        ViewBag.RdcCodes = await _context.SubDcStkMvgrs.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCats = await _context.SubDcStkMvgrs.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.SubValues = await _context.SubDcStkMvgrs.Select(x => x.SubValue).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalRows = await _context.SubDcStkMvgrs.CountAsync();
        ViewBag.TotalRdcs = await _context.SubDcStkMvgrs.Select(x => x.RdcCd).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.SubDcStkMvgrs.Select(x => x.MajCat).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ThenBy(x => x.SubValue)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new SubDcStkMvgr());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(SubDcStkMvgr model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.SubDcStkMvgrs.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.SubDcStkMvgrs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(SubDcStkMvgr model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.SubDcStkMvgrs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.SubDcStkMvgrs.FindAsync(id);
        if (item != null) { _context.SubDcStkMvgrs.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? rdcCd = null, string? majCat = null)
    {
        var query = _context.SubDcStkMvgrs.AsQueryable();
        if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
        if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
        var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("RDC_CD,MAJ_CAT,SUB_VALUE,DC_STK_Q,GRT_STK_Q,W_GRT_STK_Q,DATE");
        foreach (var r in data) sb.AppendLine($"{Q(r.RdcCd)},{Q(r.MajCat)},{Q(r.SubValue)},{r.DcStkQ},{r.GrtStkQ},{r.WGrtStkQ},{r.Date?.ToString("yyyy-MM-dd") ?? ""}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "SubDcStkMvgr.csv");
    }

    private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
}
