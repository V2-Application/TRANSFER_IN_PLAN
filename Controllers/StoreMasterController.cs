using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreMasterController : Controller
{
    private readonly PlanningDbContext _context;

    public StoreMasterController(PlanningDbContext context)
    {
        _context = context;
    }

    public async Task<IActionResult> Index()
    {
        var storeMasters = await _context.StoreMasters.OrderBy(s => s.StCd).ToListAsync();
        return View(storeMasters);
    }

    public IActionResult Create()
    {
        return View();
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Status,GridStSts,OpDate,Area,State,RefState,SaleGrp,RefStCd,RefStNm,RefGrpNew,RefGrpOld,Date")] StoreMaster storeMaster)
    {
        if (ModelState.IsValid)
        {
            _context.Add(storeMaster);
            await _context.SaveChangesAsync();
            return RedirectToAction(nameof(Index));
        }
        return View(storeMaster);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null)
            return NotFound();

        var storeMaster = await _context.StoreMasters.FindAsync(id);
        if (storeMaster == null)
            return NotFound();

        return View(storeMaster);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Status,GridStSts,OpDate,Area,State,RefState,SaleGrp,RefStCd,RefStNm,RefGrpNew,RefGrpOld,Date")] StoreMaster storeMaster)
    {
        if (id != storeMaster.Id)
            return NotFound();

        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(storeMaster);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!StoreMasterExists(storeMaster.Id))
                    return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }
        return View(storeMaster);
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null)
            return NotFound();

        var storeMaster = await _context.StoreMasters.FindAsync(id);
        if (storeMaster == null)
            return NotFound();

        return View(storeMaster);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var storeMaster = await _context.StoreMasters.FindAsync(id);
        if (storeMaster != null)
        {
            _context.StoreMasters.Remove(storeMaster);
            await _context.SaveChangesAsync();
        }
        return RedirectToAction(nameof(Index));
    }

    private bool StoreMasterExists(int id)
    {
        return _context.StoreMasters.Any(e => e.Id == id);
    }
}
