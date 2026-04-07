using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SubStStkMvgrController : Controller
{
    private readonly PlanningDbContext _context;
    public SubStStkMvgrController(PlanningDbContext context) => _context = context;

    public async Task<IActionResult> Index(string? stCd, string? majCat, string? subValue, int page = 1, int pageSize = 100)
    {
        var query = _context.SubStStkMvgrs.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
        if (!string.IsNullOrEmpty(subValue)) query = query.Where(x => x.SubValue == subValue);
        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCat = majCat; ViewBag.SubValue = subValue;
        ViewBag.StoreCodes = await _context.SubStStkMvgrs.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCats = await _context.SubStStkMvgrs.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.SubValues = await _context.SubStStkMvgrs.Select(x => x.SubValue).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.SubStStkMvgrs.Select(x => x.StCd).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.SubStStkMvgrs.Select(x => x.MajCat).Distinct().CountAsync();
        return View(await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).Skip((page - 1) * pageSize).Take(pageSize).ToListAsync());
    }
    public IActionResult Create() => View(new SubStStkMvgr());
    [HttpPost, ValidateAntiForgeryToken] public async Task<IActionResult> Create(SubStStkMvgr m) { if (!ModelState.IsValid) return View(m); _context.SubStStkMvgrs.Add(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Added."; return RedirectToAction(nameof(Index)); }
    public async Task<IActionResult> Edit(int id) { var i = await _context.SubStStkMvgrs.FindAsync(id); return i == null ? NotFound() : View(i); }
    [HttpPost, ValidateAntiForgeryToken] public async Task<IActionResult> Edit(SubStStkMvgr m) { if (!ModelState.IsValid) return View(m); _context.Update(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; return RedirectToAction(nameof(Index)); }
    public async Task<IActionResult> Delete(int id) { var i = await _context.SubStStkMvgrs.FindAsync(id); return i == null ? NotFound() : View(i); }
    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken] public async Task<IActionResult> DeleteConfirmed(int id) { var i = await _context.SubStStkMvgrs.FindAsync(id); if (i != null) { _context.SubStStkMvgrs.Remove(i); await _context.SaveChangesAsync(); } TempData["SuccessMessage"] = "Deleted."; return RedirectToAction(nameof(Index)); }
    public async Task<IActionResult> ExportCsv(string? stCd, string? majCat) { var q = _context.SubStStkMvgrs.AsQueryable(); if (!string.IsNullOrEmpty(stCd)) q = q.Where(x => x.StCd == stCd); if (!string.IsNullOrEmpty(majCat)) q = q.Where(x => x.MajCat == majCat); var d = await q.OrderBy(x => x.StCd).ToListAsync(); var sb = new StringBuilder(); sb.AppendLine("ST_CD,MAJ_CAT,SUB_VALUE,STK_QTY,DATE"); foreach (var r in d) sb.AppendLine($"{r.StCd},{r.MajCat},{r.SubValue},{r.StkQty},{r.Date?.ToString("yyyy-MM-dd") ?? ""}"); return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreStock_Mvgr.csv"); }
}
