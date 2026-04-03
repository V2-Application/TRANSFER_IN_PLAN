using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class BinCapacityController : Controller
{
    private readonly PlanningDbContext _context;

    public BinCapacityController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var binCapacities = await _context.BinCapacities.OrderBy(b => b.MajCat).ToListAsync();
        return View(binCapacities);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("MajCat,BinCapDcTeam,BinCap")] BinCapacity binCapacity)
    {
        if (ModelState.IsValid)
        {
            _context.Add(binCapacity);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(binCapacity);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var binCapacity = await _context.BinCapacities.FindAsync(id);
        if (binCapacity == null)
            return NotFound();

        return View(binCapacity);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,MajCat,BinCapDcTeam,BinCap")] BinCapacity binCapacity)
    {
        if (id != binCapacity.Id)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(binCapacity);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!BinCapacityExists(binCapacity.Id))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(binCapacity);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var binCapacity = await _context.BinCapacities.FindAsync(id);
        if (binCapacity == null)
            return NotFound();

        return View(binCapacity);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var binCapacity = await _context.BinCapacities.FindAsync(id);
        if (binCapacity != null)
        {
            _context.BinCapacities.Remove(binCapacity);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool BinCapacityExists(int id)
    {
        return _context.BinCapacities.Any(e => e.Id == id);
    }
}
