using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ContSzController : Controller
{
    private readonly PlanningDbContext _context;
    public ContSzController(PlanningDbContext context) => _context = context;

    public async Task<IActionResult> Index(string? stCd, string? majCatCd, string? sz, int page = 1, int pageSize = 100)
    {
        var query = _context.ContSzs.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(sz)) query = query.Where(x => x.Sz == sz);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCatCd = majCatCd; ViewBag.Sz = sz;
        ViewBag.StoreCodes = await _context.ContSzs.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCatCodes = await _context.ContSzs.Select(x => x.MajCatCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.SzValues = await _context.ContSzs.Select(x => x.Sz).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ContSzs.Select(x => x.StCd).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.ContSzs.Select(x => x.MajCatCd).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ContSz());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ContSz model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ContSzs.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ContSzs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ContSz model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ContSzs.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ContSzs.FindAsync(id);
        if (item != null) { _context.ContSzs.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? stCd, string? majCatCd, string? sz)
    {
        var query = _context.ContSzs.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(sz)) query = query.Where(x => x.Sz == sz);
        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,MAJ_CAT_CD,SZ,CONT%");
        foreach (var r in data) sb.AppendLine($"{r.StCd},{r.MajCatCd},{r.Sz},{r.ContPct}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ContSz.csv");
    }
}
