using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class BinCapacityController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<BinCapacityController> _logger;
        public BinCapacityController(PlanningDbContext context, ILogger<BinCapacityController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? majCat)
        {
            var query = _context.BinCapacities.AsQueryable();
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.Categories = await _context.BinCapacities.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.MajCat = majCat;

            // Analytics
            var all = await _context.BinCapacities.ToListAsync();
            ViewBag.TotalRows = all.Count;
            ViewBag.TotalCategories = all.Select(x => x.MajCat).Distinct().Count();
            ViewBag.AvgBinCap = all.Any() ? all.Average(x => x.BinCap ?? 0) : 0;
            ViewBag.AvgDcTeam = all.Any() ? all.Average(x => x.BinCapDcTeam ?? 0) : 0;
            ViewBag.ChartLabels = all.OrderBy(x => x.MajCat).Select(x => x.MajCat ?? "NA").ToList();
            ViewBag.ChartBinCap = all.OrderBy(x => x.MajCat).Select(x => x.BinCap ?? 0).ToList();
            ViewBag.ChartDcTeam = all.OrderBy(x => x.MajCat).Select(x => x.BinCapDcTeam ?? 0).ToList();

            return View(await query.OrderBy(x => x.MajCat).ToListAsync());
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? majCat)
        {
            var query = _context.BinCapacities.AsQueryable();
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("BinCapacity ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Id,MajCat,BinCapDcTeam,BinCap");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", r.Id, Q(r.MajCat), r.BinCapDcTeam, r.BinCap));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "BinCapacity.csv");
        }

        [HttpGet] public IActionResult Create() => View();
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(BinCapacity model) { if (!ModelState.IsValid) return View(model); _context.BinCapacities.Add(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Edit(int id) { var m = await _context.BinCapacities.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, BinCapacity model) { if (id != model.Id) return NotFound(); if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.BinCapacities.AnyAsync(x => x.Id == id)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Delete(int id) { var m = await _context.BinCapacities.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id) { var m = await _context.BinCapacities.FindAsync(id); if (m != null) { _context.BinCapacities.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }
        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
