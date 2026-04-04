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
        public async Task<IActionResult> Index(string? stCd, string? majCat)
        {
            var query = _context.StoreStock.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.Categories = await _context.StoreStock.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.StCd = stCd; ViewBag.MajCat = majCat;
            return View(await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync());
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? stCd, string? majCat)
        {
            var query = _context.StoreStock.AsQueryable();
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

        [HttpGet] public IActionResult Create() => View();
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(StoreStock model) { if (!ModelState.IsValid) return View(model); _context.StoreStock.Add(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Edit(int id) { var m = await _context.StoreStock.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, StoreStock model) { if (id != model.Id) return NotFound(); if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.StoreStock.AnyAsync(x => x.Id == id)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Delete(int id) { var m = await _context.StoreStock.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id) { var m = await _context.StoreStock.FindAsync(id); if (m != null) { _context.StoreStock.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }
        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\""") + "\""; }
    }
}
