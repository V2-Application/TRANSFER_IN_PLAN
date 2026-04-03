using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DelPendingController : Controller
{
    private readonly PlanningDbContext _context;

    public DelPendingController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var delPendings = await _context.DelPendings.OrderBy(d => d.RdcCd).ToListAsync();
        return View(delPendings);
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
            _context.Add(delPending);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(delPending);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var delPending = await _context.DelPendings.FindAsync(id);
        if (delPending == null)
            return NotFound();

        return View(delPending);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,RdcCd,MajCat,DelPendQ,Date")] DelPending delPending)
    {
        if (id != delPending.Id)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(delPending);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!DelPendingExists(delPending.Id))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(delPending);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var delPending = await _context.DelPendings.FindAsync(id);
        if (delPending == null)
            return NotFound();

        return View(delPending);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var delPending = await _context.DelPendings.FindAsync(id);
        if (delPending != null)
        {
            _context.DelPendings.Remove(delPending);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool DelPendingExists(int id)
    {
        return _context.DelPendings.Any(e => e.Id == id);
    }
}
