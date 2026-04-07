using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class StoreStockController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<StoreStockController> _logger;
        public StoreStockController(PlanningDbContext context, ILogger<StoreStockController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? stCd, string? majCat, int page = 1, int pageSize = 100)
        {
            var query = _context.StoreStocks.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.Categories = await _context.StoreStocks.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.StoreCodes = await _context.StoreStocks.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.StCd = stCd; ViewBag.MajCat = majCat;
            ViewBag.TotalRows = await _context.StoreStocks.CountAsync();
            ViewBag.TotalStores = await _context.StoreStocks.Select(x => x.StCd).Distinct().CountAsync();
            ViewBag.TotalCats = await _context.StoreStocks.Select(x => x.MajCat).Distinct().CountAsync();
            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? stCd, string? majCat)
        {
            var query = _context.StoreStocks.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("StoreStock ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Id,StCd,MajCat,StkQty,Date");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", r.Id, Q(r.StCd), Q(r.MajCat), r.StkQty, r.Date?.ToString("yyyy-MM-dd")));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreStock.csv");
        }

        [HttpGet] public async Task<IActionResult> Create() { await LoadDropdowns(); return View(); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(StoreStock model) { if (!ModelState.IsValid) { await LoadDropdowns(); return View(model); } _context.StoreStocks.Add(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Edit(int id) { var m = await _context.StoreStocks.FindAsync(id); if (m == null) return NotFound(); await LoadDropdowns(); return View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, StoreStock model) { if (id != model.Id) return NotFound(); if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.StoreStocks.AnyAsync(x => x.Id == id)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Delete(int id) { var m = await _context.StoreStocks.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id) { var m = await _context.StoreStocks.FindAsync(id); if (m != null) { _context.StoreStocks.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }
        private async Task LoadDropdowns()
        {
            ViewBag.StoreCodes = await _context.StoreMasters.Select(x => x.StCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.MajCats = await _context.BinCapacities.Select(x => x.MajCat).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        }
        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
