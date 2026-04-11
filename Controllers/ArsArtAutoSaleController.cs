using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsArtAutoSaleController : Controller
{
    private readonly DataV2DbContext _context;
    public ArsArtAutoSaleController(DataV2DbContext context) => _context = context;

    public async Task<IActionResult> Index(string? st, string? genArt, string? clr, int page = 1, int pageSize = 100)
    {
        var query = _context.ArsArtAutoSales.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(genArt)) query = query.Where(x => x.GenArt == genArt);
        if (!string.IsNullOrEmpty(clr)) query = query.Where(x => x.Clr == clr);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.TotalRows = await _context.ArsArtAutoSales.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.St = st; ViewBag.GenArt = genArt; ViewBag.Clr = clr;
        ViewBag.Stores = await _context.ArsArtAutoSales.Select(x => x.St).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.Articles = await _context.ArsArtAutoSales.Select(x => x.GenArt).Distinct().OrderBy(x => x).Take(500).ToListAsync();
        ViewBag.Colors = await _context.ArsArtAutoSales.Select(x => x.Clr).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ArsArtAutoSales.Select(x => x.St).Distinct().CountAsync();
        ViewBag.TotalArticles = await _context.ArsArtAutoSales.Select(x => x.GenArt).Distinct().CountAsync();
        ViewBag.TotalColors = await _context.ArsArtAutoSales.Select(x => x.Clr).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.St).ThenBy(x => x.GenArt).ThenBy(x => x.Clr)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ArsStArtAutoSale());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStArtAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ArsArtAutoSales.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ArsArtAutoSales.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsStArtAutoSale model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ArsArtAutoSales.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ArsArtAutoSales.FindAsync(id);
        if (item != null) { _context.ArsArtAutoSales.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? genArt, string? clr)
    {
        var query = _context.ArsArtAutoSales.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.St == st);
        if (!string.IsNullOrEmpty(genArt)) query = query.Where(x => x.GenArt == genArt);
        if (!string.IsNullOrEmpty(clr)) query = query.Where(x => x.Clr == clr);
        var data = await query.OrderBy(x => x.St).ThenBy(x => x.GenArt).ThenBy(x => x.Clr).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST,GEN-ART,CLR,CM-REM-DAYS,NM-DAYS,CM-AUTO-SALE-Q,NM-AUTO-SALE-Q,CM_PD_SALE_Q,NM_PD_SALE_Q");
        foreach (var r in data) sb.AppendLine($"{r.St},{r.GenArt},{r.Clr},{r.CmRemDays},{r.NmDays},{r.CmAutoSaleQ},{r.NmAutoSaleQ},{r.CmPdSaleQ:F4},{r.NmPdSaleQ:F4}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_Art_Auto_Sale.csv");
    }
}
