using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsAutoSaleController : Controller
{
    private readonly DataV2DbContext _context;
    public ArsAutoSaleController(DataV2DbContext context) => _context = context;

    public async Task<IActionResult> Index(string? st, string? mj, int page = 1, int pageSize = 100)
    {
        var query = _context.ArsAutoSales.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(mj)) query = query.Where(x => x.Mj == mj);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.TotalRows = await _context.ArsAutoSales.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.St = st; ViewBag.Mj = mj;
        ViewBag.Stores = await _context.ArsAutoSales.Select(x => x.St).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.MajCats = await _context.ArsAutoSales.Select(x => x.Mj).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ArsAutoSales.Select(x => x.St).Distinct().CountAsync();
        ViewBag.TotalCats = await _context.ArsAutoSales.Select(x => x.Mj).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.St).ThenBy(x => x.Mj)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ArsStMjAutoSale());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStMjAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ArsAutoSales.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ArsAutoSales.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsStMjAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ArsAutoSales.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ArsAutoSales.FindAsync(id);
        if (item != null) { _context.ArsAutoSales.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? mj)
    {
        var query = _context.ArsAutoSales.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(mj)) query = query.Where(x => x.Mj == mj);
        var data = await query.OrderBy(x => x.St).ThenBy(x => x.Mj).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST,MJ,CM-REM-DAYS,NM-DAYS,CM-AUTO-SALE-Q,NM-AUTO-SALE-Q,CM_PD_SALE_Q,NM_PD_SALE_Q");
        foreach (var r in data) sb.AppendLine($"{r.St},{r.Mj},{r.CmRemDays},{r.NmDays},{r.CmAutoSaleQ},{r.NmAutoSaleQ},{r.CmPdSaleQ:F4},{r.NmPdSaleQ:F4}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Auto_Sale.csv");
    }
}
