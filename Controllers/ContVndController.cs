using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ContVndController : Controller
{
    private readonly PlanningDbContext _context;
    public ContVndController(PlanningDbContext context) => _context = context;

    public async Task<IActionResult> Index(string? stCd, string? majCatCd, string? mVndCd, int page = 1, int pageSize = 100)
    {
        var query = _context.ContVnds.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(mVndCd)) query = query.Where(x => x.MVndCd == mVndCd);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.StCd = stCd; ViewBag.MajCatCd = majCatCd; ViewBag.MVndCd = mVndCd;
        ViewBag.StoreCodes = await _context.ContVnds.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCatCodes = await _context.ContVnds.Select(x => x.MajCatCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.VndValues = await _context.ContVnds.Select(x => x.MVndCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ContVnds.Select(x => x.StCd).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.ContVnds.Select(x => x.MajCatCd).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ContVnd());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ContVnd model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ContVnds.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ContVnds.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ContVnd model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ContVnds.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ContVnds.FindAsync(id);
        if (item != null) { _context.ContVnds.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? stCd, string? majCatCd, string? mVndCd)
    {
        var query = _context.ContVnds.AsQueryable();
        if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
        if (!string.IsNullOrEmpty(majCatCd)) query = query.Where(x => x.MajCatCd == majCatCd);
        if (!string.IsNullOrEmpty(mVndCd)) query = query.Where(x => x.MVndCd == mVndCd);
        var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCatCd).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,MAJ_CAT_CD,M_VND_CD,CONT%");
        foreach (var r in data) sb.AppendLine($"{r.StCd},{r.MajCatCd},{r.MVndCd},{r.ContPct}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ContVnd.csv");
    }
}
