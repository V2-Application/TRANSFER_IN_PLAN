using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class BinCapacityController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<BinCapacityController> _logger;

    public BinCapacityController(PlanningDbContext context, ILogger<BinCapacityController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            _logger.LogInformation("Loading BinCapacity list.");
            var items = await _context.BinCapacities.OrderBy(b => b.MajCat).ToListAsync();
            return View(items);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading BinCapacity list.");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<BinCapacity>());
        }
    }

    public IActionResult Create() => View();

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("MajCat,BinCapDcTeam,BinCap")] BinCapacity binCapacity)
    {
        if (ModelState.IsValid)
        {
            try
            {
                _context.Add(binCapacity);
                await _context.SaveChangesAsync();
                _logger.LogInformation("BinCapacity created: MajCat={MajCat}", binCapacity.MajCat);
                TempData["SuccessMessage"] = $"Bin Capacity for '{binCapacity.MajCat}' created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating BinCapacity MajCat={MajCat}", binCapacity.MajCat);
                ModelState.AddModelError("", "Error saving record: " + ex.Message);
            }
        }
        return View(binCapacity);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        var item = await _context.BinCapacities.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,MajCat,BinCapDcTeam,BinCap")] BinCapacity binCapacity)
    {
        if (id != binCapacity.Id) return NotFound();
        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(binCapacity);
                await _context.SaveChangesAsync();
                _logger.LogInformation("BinCapacity updated: Id={Id}, MajCat={MajCat}", id, binCapacity.MajCat);
                TempData["SuccessMessage"] = $"Bin Capacity for '{binCapacity.MajCat}' updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException) { if (!BinCapacityExists(binCapacity.Id)) return NotFound(); throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating BinCapacity Id={Id}", id);
                ModelState.AddModelError("", "Error updating record: " + ex.Message);
            }
        }
        return View(binCapacity);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        var item = await _context.BinCapacities.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            var item = await _context.BinCapacities.FindAsync(id);
            if (item != null)
            {
                _context.BinCapacities.Remove(item);
                await _context.SaveChangesAsync();
                _logger.LogInformation("BinCapacity deleted: Id={Id}", id);
                TempData["SuccessMessage"] = "Bin Capacity record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting BinCapacity Id={Id}", id);
            TempData["ErrorMessage"] = "Error deleting record: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private bool BinCapacityExists(int id) => _context.BinCapacities.Any(e => e.Id == id);
}
