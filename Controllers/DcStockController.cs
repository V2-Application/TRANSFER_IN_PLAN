using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DcStockController : Controller
{
    private readonly PlanningDbContext _context;

    public DcStockController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var dcStocks = await _context.DcStocks.OrderBy(d => d.RdcCd).ThenBy(d => d.MajCat).ToListAsync();
        return View(dcStocks);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("RdcCd,Rdc,MajCat,DcStkQ,GrtStkQ,WGrtStkQ,Date")] DcStock dcStock)
    {
        if (ModelState.IsValid)
        {
            _context.Add(dcStock);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(dcStock);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var dcStock = await _context.DcStocks.FindAsync(id);
        if (dcStock == null)
            return NotFound();

        return View(dcStock);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,RdcCd,Rdc,MajCat,DcStkQ,GrtStkQ,WGrtStkQ,Date")] DcStock dcStock)
    {
        if (id != dcStock.Id)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(dcStock);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!DcStockExists(dcStock.Id))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(dcStock);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var dcStock = await _context.DcStocks.FindAsync(id);
        if (dcStock == null)
            return NotFound();

        return View(dcStock);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var dcStock = await _context.DcStocks.FindAsync(id);
        if (dcStock != null)
        {
            _context.DcStocks.Remove(dcStock);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool DcStockExists(int id)
    {
        return _context.DcStocks.Any(e => e.Id == id);
    }
}
