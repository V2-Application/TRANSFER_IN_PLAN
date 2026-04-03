using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreStockController : Controller
{
    private readonly PlanningDbContext _context;

    public StoreStockController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var storeStocks = await _context.StoreStocks.OrderBy(s => s.StCd).ThenBy(s => s.MajCat).ToListAsync();
        return View(storeStocks);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,MajCat,StkQty,Date")] StoreStock storeStock)
    {
        if (ModelState.IsValid)
        {
            _context.Add(storeStock);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(storeStock);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var storeStock = await _context.StoreStocks.FindAsync(id);
        if (storeStock == null)
            return NotFound();

        return View(storeStock);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,StCd,MajCat,StkQty,Date")] StoreStock storeStock)
    {
        if (id != storeStock.Id)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(storeStock);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!StoreStockExists(storeStock.Id))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(storeStock);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var storeStock = await _context.StoreStocks.FindAsync(id);
        if (storeStock == null)
            return NotFound();

        return View(storeStock);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var storeStock = await _context.StoreStocks.FindAsync(id);
        if (storeStock != null)
        {
            _context.StoreStocks.Remove(storeStock);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool StoreStockExists(int id)
    {
        return _context.StoreStocks.Any(e => e.Id == id);
    }
}
