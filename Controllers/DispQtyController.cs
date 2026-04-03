using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class DispQtyController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<DispQtyController> _logger;

    public DispQtyController(PlanningDbContext context, ILogger<DispQtyController> logger)
    {
        _context = context; _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try { _logger.LogInformation("Loading DispQty list."); return View(await _context.DispQties.OrderBy(d => d.StCd).ThenBy(d => d.MajCat).ToListAsync()); }
        catch (Exception ex) { _logger.LogError(ex, "Error loading DispQty."); ViewBag.ErrorMessage = ex.Message; return View(new List<DispQty>()); }
    }

    public IActionResult Create() => View();

    [HttpPost][ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] DispQty d)
    {
        if (!ModelState.IsValid) return View(d);
        try { _context.Add(d); await _context.SaveChangesAsync(); _logger.LogInformation("DispQty created: {StCd}/{MajCat}", d.StCd, d.MajCat); TempData["SuccessMessage"] = $"Display Qty for '{d.StCd}/{d.MajCat}' created."; return RedirectToAction(nameof(Index)); }
        catch (Exception ex) { _logger.LogError(ex, "Error creating DispQty"); ModelState.AddModelError("", ex.Message); return View(d); }
    }

    public async Task<IActionResult> Edit(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat)) return NotFound();
        var item = await _context.DispQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost][ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(string stCd, string majCat, [Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] DispQty d)
    {
        if (stCd != d.StCd || majCat != d.MajCat) return NotFound();
        if (!ModelState.IsValid) return View(d);
        try { _context.Update(d); await _context.SaveChangesAsync(); _logger.LogInformation("DispQty updated: {StCd}/{MajCat}", stCd, majCat); TempData["SuccessMessage"] = $"Display Qty '{stCd}/{majCat}' updated."; return RedirectToAction(nameof(Index)); }
        catch (DbUpdateConcurrencyException) { if (!_context.DispQties.Any(e => e.StCd == d.StCd && e.MajCat == d.MajCat)) return NotFound(); throw; }
        catch (Exception ex) { _logger.LogError(ex, "Error updating DispQty"); ModelState.AddModelError("", ex.Message); return View(d); }
    }

    public async Task<IActionResult> Delete(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat)) return NotFound();
        var item = await _context.DispQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(string stCd, string majCat)
    {
        try { var item = await _context.DispQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat); if (item != null) { _context.DispQties.Remove(item); await _context.SaveChangesAsync(); _logger.LogInformation("DispQty deleted: {StCd}/{MajCat}", stCd, majCat); TempData["SuccessMessage"] = "Display Qty record deleted."; } }
        catch (Exception ex) { _logger.LogError(ex, "Error deleting DispQty"); TempData["ErrorMessage"] = ex.Message; }
        return RedirectToAction(nameof(Index));
    }
}
