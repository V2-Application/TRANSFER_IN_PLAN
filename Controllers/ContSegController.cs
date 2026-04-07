using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ContSegController : Controller
{
    private readonly PlanningDbContext _context;
    public ContSegController(PlanningDbContext context) => _context = context;

    public async Task<IActionResult> Index(string? stCd, string? majCatCd, string? seg, int page = 1, int pageSize = 100)
    {
        var query = _context.ContSegs.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(seg)) query = query.Where(x => x.Seg == seg);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCatCd = majCatCd; ViewBag.Seg = seg;
        ViewBag.StoreCodes = await _context.ContSegs.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCatCodes = await _context.ContSegs.Select(x => x.MajCatCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.SegValues = await _context.ContSegs.Select(x => x.Seg).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ContSegs.Select(x => x.StCd).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.ContSegs.Select(x => x.MajCatCd).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ContSeg());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ContSeg model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ContSegs.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ContSegs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ContSeg model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ContSegs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ContSegs.FindAsync(id);
        if (item != null) { _context.ContSegs.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? stCd, string? majCatCd, string? seg)
    {
        var query = _context.ContSegs.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(seg)) query = query.Where(x => x.Seg == seg);
        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,MAJ_CAT_CD,SEG,CONT%");
        foreach (var r in data) sb.AppendLine($"{r.StCd},{r.MajCatCd},{r.Seg},{r.ContPct}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ContSeg.csv");
    }
}
