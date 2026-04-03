using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class SaleQtyController : Controller
{
    private readonly PlanningDbContext _context;

    public SaleQtyController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var saleQties = await _context.SaleQties.OrderBy(s => s.StCd).ThenBy(s => s.MajCat).ToListAsync();
        return View(saleQties);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] SaleQty saleQty)
    {
        if (ModelState.IsValid)
        {
            _context.Add(saleQty);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(saleQty);
    }

    public async Task<IActionResult> Edit(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat))
            return NotFound();

        var saleQty = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
        if (saleQty == null)
            return NotFound();

        return View(saleQty);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(string stCd, string majCat, [Bind("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2")] SaleQty saleQty)
    {
        if (stCd != saleQty.StCd || majCat != saleQty.MajCat)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(saleQty);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!SaleQtyExists(saleQty.StCd, saleQty.MajCat))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(saleQty);
    }

    public async Task<IActionResult> Delete(string stCd, string majCat)
    {
        if (string.IsNullOrEmpty(stCd) || string.IsNullOrEmpty(majCat))
            return NotFound();

        var saleQty = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
        if (saleQty == null)
            return NotFound();

        return View(saleQty);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(string stCd, string majCat)
    {
        var saleQty = await _context.SaleQties.FirstOrDefaultAsync(s => s.StCd == stCd && s.MajCat == majCat);
        if (saleQty != null)
        {
            _context.SaleQties.Remove(saleQty);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool SaleQtyExists(string? stCd, string? majCat)
    {
        return _context.SaleQties.Any(e => e.StCd == stCd && e.MajCat == majCat);
    }
}
