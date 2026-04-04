using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreStockController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<StoreStockController> _logger;

    public StoreStockController(PlanningDbContext context, ILogger<StoreStockController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            var storeStocks = await _context.StoreStocks
                .OrderBy(s => s.StCd)
                .ThenBy(s => s.MajCat)
                .ToListAsync();
            _logger.LogInformation("StoreStock Index loaded {Count} records.", storeStocks.Count);
            return View(storeStocks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading StoreStock Index.");
            ViewBag.ErrorMessage = "An error occurred while loading store stock data.";
            return View(new List<StoreStock>());
        }
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
            try
            {
                _context.Add(storeStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreStock created for Store: {StCd}, Category: {MajCat}.", storeStock.StCd, storeStock.MajCat);
                TempData["SuccessMessage"] = "Store stock record created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating StoreStock for Store: {StCd}.", storeStock.StCd);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(storeStock);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        var storeStock = await _context.StoreStocks.FindAsync(id);
        if (storeStock == null) return NotFound();
        return View(storeStock);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,StCd,MajCat,StkQty,Date")] StoreStock storeStock)
    {
        if (id != storeStock.Id) return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(storeStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreStock updated for Id: {Id}, Store: {StCd}.", storeStock.Id, storeStock.StCd);
                TempData["SuccessMessage"] = "Store stock record updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException ex)
            {
                if (!StoreStockExists(storeStock.Id))
                {
                    _logger.LogWarning("StoreStock Id: {Id} not found during update.", storeStock.Id);
                    return NotFound();
                }
                _logger.LogError(ex, "Concurrency error updating StoreStock Id: {Id}.", storeStock.Id);
                ModelState.AddModelError("", "The record was modified by another user. Please reload and try again.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating StoreStock Id: {Id}.", storeStock.Id);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(storeStock);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        var storeStock = await _context.StoreStocks.FindAsync(id);
        if (storeStock == null) return NotFound();
        return View(storeStock);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            var storeStock = await _context.StoreStocks.FindAsync(id);
            if (storeStock != null)
            {
                _context.StoreStocks.Remove(storeStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreStock deleted Id: {Id}, Store: {StCd}.", id, storeStock.StCd);
                TempData["SuccessMessage"] = "Store stock record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting StoreStock Id: {Id}.", id);
            TempData["ErrorMessage"] = "An error occurred while deleting the record. It may be in use.";
        }
        return RedirectToAction(nameof(Index));
    }

    private bool StoreStockExists(int id)
    {
        return _context.StoreStocks.Any(e => e.Id == id);
    }
}
