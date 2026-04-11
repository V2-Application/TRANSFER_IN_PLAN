using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsHoldDaysController : Controller
{
    private readonly DataV2DbContext _context;
    public ArsHoldDaysController(DataV2DbContext context) => _context = context;

    public async Task<IActionResult> Index(string? st, string? mj, int page = 1, int pageSize = 100)
    {
        var query = _context.Set<ArsHoldDaysMaster>().AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(mj)) query = query.Where(x => x.Mj == mj);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.TotalRows = await _context.Set<ArsHoldDaysMaster>().CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.St = st; ViewBag.Mj = mj;
        ViewBag.Stores = await _context.Set<ArsHoldDaysMaster>().Select(x => x.St).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCats = await _context.Set<ArsHoldDaysMaster>().Select(x => x.Mj).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.Set<ArsHoldDaysMaster>().Select(x => x.St).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.Set<ArsHoldDaysMaster>().Select(x => x.Mj).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.St).ThenBy(x => x.Mj)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ArsHoldDaysMaster());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsHoldDaysMaster model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Set<ArsHoldDaysMaster>().Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.Set<ArsHoldDaysMaster>().FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsHoldDaysMaster model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.Set<ArsHoldDaysMaster>().FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.Set<ArsHoldDaysMaster>().FindAsync(id);
        if (item != null) { _context.Set<ArsHoldDaysMaster>().Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? mj)
    {
        var query = _context.Set<ArsHoldDaysMaster>().AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(mj)) query = query.Where(x => x.Mj == mj);
        var data = await query.OrderBy(x => x.St).ThenBy(x => x.Mj).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST,MJ,HOLD_DAYS");
        foreach (var r in data) sb.AppendLine($"{r.St},{r.Mj},{r.HoldDays}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Hold_Days.csv");
    }
}
