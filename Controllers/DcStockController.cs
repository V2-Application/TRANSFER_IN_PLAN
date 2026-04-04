using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DcStockController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<DcStockController> _logger;

    public DcStockController(PlanningDbContext context, ILogger<DcStockController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            var dcStocks = await _context.DcStocks
                .OrderBy(d => d.RdcCd)
                .ThenBy(d => d.MajCat)
                .ToListAsync();
            _logger.LogInformation("DcStock Index loaded {Count} records.", dcStocks.Count);
            return View(dcStocks);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading DcStock Index.");
            ViewBag.ErrorMessage = "An error occurred while loading DC stock data.";
            return View(new List<DcStock>());
        }
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
            try
            {
                _context.Add(dcStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DcStock created for RDC: {RdcCd}, Category: {MajCat}.", dcStock.RdcCd, dcStock.MajCat);
                TempData["SuccessMessage"] = "DC stock record created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating DcStock for RDC: {RdcCd}.", dcStock.RdcCd);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(dcStock);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        var dcStock = await _context.DcStocks.FindAsync(id);
        if (dcStock == null) return NotFound();
        return View(dcStock);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,RdcCd,Rdc,MajCat,DcStkQ,GrtStkQ,WGrtStkQ,Date")] DcStock dcStock)
    {
        if (id != dcStock.Id) return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(dcStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DcStock updated for Id: {Id}, RDC: {RdcCd}.", dcStock.Id, dcStock.RdcCd);
                TempData["SuccessMessage"] = "DC stock record updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException ex)
            {
                if (!DcStockExists(dcStock.Id))
                {
                    _logger.LogWarning("DcStock Id: {Id} not found during update.", dcStock.Id);
                    return NotFound();
                }
                _logger.LogError(ex, "Concurrency error updating DcStock Id: {Id}.", dcStock.Id);
                ModelState.AddModelError("", "The record was modified by another user. Please reload and try again.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating DcStock Id: {Id}.", dcStock.Id);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(dcStock);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        var dcStock = await _context.DcStocks.FindAsync(id);
        if (dcStock == null) return NotFound();
        return View(dcStock);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            var dcStock = await _context.DcStocks.FindAsync(id);
            if (dcStock != null)
            {
                _context.DcStocks.Remove(dcStock);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DcStock deleted Id: {Id}, RDC: {RdcCd}.", id, dcStock.RdcCd);
                TempData["SuccessMessage"] = "DC stock record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting DcStock Id: {Id}.", id);
            TempData["ErrorMessage"] = "An error occurred while deleting the record. It may be in use.";
        }
        return RedirectToAction(nameof(Index));
    }

    private bool DcStockExists(int id)
    {
        return _context.DcStocks.Any(e => e.Id == id);
    }
}
