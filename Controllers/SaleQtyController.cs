using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SaleQtyController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<SaleQtyController> _logger;

    public SaleQtyController(PlanningDbContext context, ILogger<SaleQtyController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<IActionResult> Index()
    {
        try
        {
            _logger.LogInformation("Loading SaleQty list.");
            var items = await _context.SaleQties.OrderBy(s => s.StCd).ThenBy(s => s.MajCat).ToListAsync();
            return View(items);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading SaleQty list.");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<SaleQty>());
        }
    }

    public IActionResult Create() => View();

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] SaleQty saleQty)
    {
        if (ModelState.IsValid)
        {
            try
            {
                _context.Add(saleQty);
                await _context.SaveChangesAsync();
                _logger.LogInformation("SaleQty created: StCd={StCd}, MajCat={MajCat}", saleQty.StCd, saleQty.MajCat);
                TempData["SuccessMessage"] = $"Sale Qty for '{saleQty.StCd} / {saleQty.MajCat}' created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating SaleQty StCd={StCd}", saleQty.StCd);
                ModelState.AddModelError("", "Error saving record: " + ex.Message);
            }
        }
        return View(saleQty);
    }

    public async Task<IActionResult> Edit(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat)) return NotFound();
        var item = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(string stCd, string majCat, [Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] SaleQty saleQty)
    {
        if (stCd != saleQty.StCd || majCat != saleQty.MajCat) return NotFound();
        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(saleQty);
                await _context.SaveChangesAsync();
                _logger.LogInformation("SaleQty updated: StCd={StCd}, MajCat={MajCat}", stCd, majCat);
                TempData["SuccessMessage"] = $"Sale Qty for '{stCd} / {majCat}' updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException) { if (!SaleQtyExists(saleQty.StCd, saleQty.MajCat)) return NotFound(); throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating SaleQty StCd={StCd}", stCd);
                ModelState.AddModelError("", "Error updating record: " + ex.Message);
            }
        }
        return View(saleQty);
    }

    public async Task<IActionResult> Delete(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat)) return NotFound();
        var item = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(string stCd, string majCat)
    {
        try
        {
            var item = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
            if (item != null)
            {
                _context.SaleQties.Remove(item);
                await _context.SaveChangesAsync();
                _logger.LogInformation("SaleQty deleted: StCd={StCd}, MajCat={MajCat}", stCd, majCat);
                TempData["SuccessMessage"] = "Sale Qty record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting SaleQty StCd={StCd}", stCd);
            TempData["ErrorMessage"] = "Error deleting record: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private bool SaleQtyExists(string? stCd, string? majCat) =>
        _context.SaleQties.Any(e => e.StCd == stCd && e.MajCat == majCat);
}
