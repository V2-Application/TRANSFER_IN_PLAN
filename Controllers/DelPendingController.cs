using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DelPendingController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<DelPendingController> _logger;

    public DelPendingController(PlanningDbContext context, ILogger<DelPendingController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            var delPendings = await _context.DelPendings
                .OrderBy(d => d.RdcCd)
                .ToListAsync();
            _logger.LogInformation("DelPending Index loaded {Count} records.", delPendings.Count);
            return View(delPendings);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading DelPending Index.");
            ViewBag.ErrorMessage = "An error occurred while loading delivery pending data.";
            return View(new List<DelPending>());
        }
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("RdcCd,MajCat,DelPendQ,Date")] DelPending delPending)
    {
        if (ModelState.IsValid)
        {
            try
            {
                _context.Add(delPending);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DelPending created for RDC: {RdcCd}, Category: {MajCat}.", delPending.RdcCd, delPending.MajCat);
                TempData["SuccessMessage"] = "Delivery pending record created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating DelPending for RDC: {RdcCd}.", delPending.RdcCd);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(delPending);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        var delPending = await _context.DelPendings.FindAsync(id);
        if (delPending == null) return NotFound();
        return View(delPending);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,RdcCd,MajCat,DelPendQ,Date")] DelPending delPending)
    {
        if (id != delPending.Id) return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(delPending);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DelPending updated for Id: {Id}, RDC: {RdcCd}.", delPending.Id, delPending.RdcCd);
                TempData["SuccessMessage"] = "Delivery pending record updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException ex)
            {
                if (!DelPendingExists(delPending.Id))
                {
                    _logger.LogWarning("DelPending Id: {Id} not found during update.", delPending.Id);
                    return NotFound();
                }
                _logger.LogError(ex, "Concurrency error updating DelPending Id: {Id}.", delPending.Id);
                ModelState.AddModelError("", "The record was modified by another user. Please reload and try again.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating DelPending Id: {Id}.", delPending.Id);
                ModelState.AddModelError("", "An error occurred while saving. Please try again.");
            }
        }
        return View(delPending);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        var delPending = await _context.DelPendings.FindAsync(id);
        if (delPending == null) return NotFound();
        return View(delPending);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            var delPending = await _context.DelPendings.FindAsync(id);
            if (delPending != null)
            {
                _context.DelPendings.Remove(delPending);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DelPending deleted Id: {Id}, RDC: {RdcCd}.", id, delPending.RdcCd);
                TempData["SuccessMessage"] = "Delivery pending record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting DelPending Id: {Id}.", id);
            TempData["ErrorMessage"] = "An error occurred while deleting the record. It may be in use.";
        }
        return RedirectToAction(nameof(Index));
    }

    private bool DelPendingExists(int id)
    {
        return _context.DelPendings.Any(e => e.Id == id);
    }
}
