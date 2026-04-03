using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class WeekCalendarController : Controller
{
    private readonly PlanningDbContext _context;

    public WeekCalendarController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var weekCalendars = await _context.WeekCalendars.OrderBy(w => w.WeekId).ToListAsync();
        return View(weekCalendars);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("WeekId,WeekSeq,FyWeek,FyYear,CalYear,YearWeek,WkStDt,WkEndDt")] WeekCalendar weekCalendar)
    {
        if (ModelState.IsValid)
        {
            _context.Add(weekCalendar);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(weekCalendar);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var weekCalendar = await _context.WeekCalendars.FindAsync(id);
        if (weekCalendar == null)
            return NotFound();

        return View(weekCalendar);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("WeekId,WeekSeq,FyWeek,FyYear,CalYear,YearWeek,WkStDt,WkEndDt")] WeekCalendar weekCalendar)
    {
        if (id != weekCalendar.WeekId)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(weekCalendar);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!WeekCalendarExists(weekCalendar.WeekId))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(weekCalendar);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var weekCalendar = await _context.WeekCalendars.FindAsync(id);
        if (weekCalendar == null)
            return NotFound();

        return View(weekCalendar);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var weekCalendar = await _context.WeekCalendars.FindAsync(id);
        if (weekCalendar != null)
        {
            _context.WeekCalendars.Remove(weekCalendar);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool WeekCalendarExists(int id)
    {
        return _context.WeekCalendars.Any(e => e.WeekId == id);
    }
}
