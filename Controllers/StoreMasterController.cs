using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreMasterController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<StoreMasterController> _logger;

    public StoreMasterController(PlanningDbContext context, ILogger<StoreMasterController> logger)
    {
        _context = context;
        _logger = logger;
    }

    // GET: StoreMaster
    public async Task<IActionResult> Index(string? statusFilter)
    {
        try
        {
            _logger.LogInformation("Loading StoreMaster list. StatusFilter={StatusFilter}", statusFilter);
            var query = _context.StoreMasters.AsQueryable();
            if (!string.IsNullOrEmpty(statusFilter))
                query = query.Where(s => s.Status == statusFilter);
            var storeMasters = await query.OrderBy(s => s.StCd).ToListAsync();
            ViewBag.StatusFilter = statusFilter;
            ViewBag.StatusList = await _context.StoreMasters
                .Where(s => s.Status != null).Select(s => s.Status).Distinct().OrderBy(s => s).ToListAsync();
            return View(storeMasters);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading StoreMaster list.");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<StoreMaster>());
        }
    }

    public IActionResult Create() => View();

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Create([Bind("StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Status,GridStSts,OpDate,Area,State,RefState,SaleGrp,RefStCd,RefStNm,RefGrpNew,RefGrpOld,Date")] StoreMaster storeMaster)
    {
        if (ModelState.IsValid)
        {
            try
            {
                _context.Add(storeMaster);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreMaster created: StCd={StCd}", storeMaster.StCd);
                TempData["SuccessMessage"] = $"Store '{storeMaster.StCd} - {storeMaster.StNm}' created successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating StoreMaster StCd={StCd}", storeMaster.StCd);
                ModelState.AddModelError("", "Error saving record: " + ex.Message);
            }
        }
        return View(storeMaster);
    }

    public async Task<IActionResult> Edit(int? id)
    {
        if (id == null) return NotFound();
        var storeMaster = await _context.StoreMasters.FindAsync(id);
        if (storeMaster == null) return NotFound();
        return View(storeMaster);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, [Bind("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Status,GridStSts,OpDate,Area,State,RefState,SaleGrp,RefStCd,RefStNm,RefGrpNew,RefGrpOld,Date")] StoreMaster storeMaster)
    {
        if (id != storeMaster.Id) return NotFound();
        if (ModelState.IsValid)
        {
            try
            {
                _context.Update(storeMaster);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreMaster updated: StCd={StCd}", storeMaster.StCd);
                TempData["SuccessMessage"] = $"Store '{storeMaster.StCd}' updated successfully.";
                return RedirectToAction(nameof(Index));
            }
            catch (DbUpdateConcurrencyException) { if (!StoreMasterExists(storeMaster.Id)) return NotFound(); throw; }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating StoreMaster StCd={StCd}", storeMaster.StCd);
                ModelState.AddModelError("", "Error updating record: " + ex.Message);
            }
        }
        return View(storeMaster);
    }

    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> ToggleStatus(int id)
    {
        try
        {
            var store = await _context.StoreMasters.FindAsync(id);
            if (store == null) return NotFound();
            store.Status = store.Status == "Active" ? "Inactive" : "Active";
            _context.Update(store);
            await _context.SaveChangesAsync();
            _logger.LogInformation("StoreMaster status toggled: StCd={StCd}, NewStatus={Status}", store.StCd, store.Status);
            TempData["SuccessMessage"] = $"Store '{store.StCd}' status changed to '{store.Status}'.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error toggling StoreMaster status Id={Id}", id);
            TempData["ErrorMessage"] = "Error updating status: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int? id)
    {
        if (id == null) return NotFound();
        var storeMaster = await _context.StoreMasters.FindAsync(id);
        if (storeMaster == null) return NotFound();
        return View(storeMaster);
    }

    [HttpPost, ActionName("Delete")]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            var storeMaster = await _context.StoreMasters.FindAsync(id);
            if (storeMaster != null)
            {
                _context.StoreMasters.Remove(storeMaster);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreMaster deleted: Id={Id}", id);
                TempData["SuccessMessage"] = "Store record deleted successfully.";
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting StoreMaster Id={Id}", id);
            TempData["ErrorMessage"] = "Error deleting record: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private bool StoreMasterExists(int id) => _context.StoreMasters.Any(e => e.Id == id);
}
