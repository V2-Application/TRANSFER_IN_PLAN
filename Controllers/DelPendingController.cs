using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class DelPendingController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<DelPendingController> _logger;
        public DelPendingController(PlanningDbContext context, ILogger<DelPendingController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? rdcCd, string? majCat, int page = 1, int pageSize = 100)
        {
            var query = _context.DelPendings.AsQueryable();
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.RdcCodes = await _context.DelPendings.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Categories = await _context.DelPendings.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.RdcCd = rdcCd; ViewBag.MajCat = majCat;
            ViewBag.TotalRows = await _context.DelPendings.CountAsync();
            ViewBag.TotalRdcs = await _context.DelPendings.Select(x => x.RdcCd).Distinct().CountAsync();
            ViewBag.TotalCats = await _context.DelPendings.Select(x => x.MajCat).Distinct().CountAsync();
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? rdcCd, string? majCat)
        {
            var query = _context.DelPendings.AsQueryable();
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("DelPending ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Id,RdcCd,MajCat,DelPendQ,Date");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", r.Id, Q(r.RdcCd), Q(r.MajCat), r.DelPendQ, r.Date?.ToString("yyyy-MM-dd")));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "DelPending.csv");
        }

        [HttpGet] public async Task<IActionResult> Create() { await LoadDropdowns(); return View(); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(DelPending model) { if (!ModelState.IsValid) { await LoadDropdowns(); return View(model); } _context.DelPendings.Add(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Edit(int id) { var m = await _context.DelPendings.FindAsync(id); if (m == null) return NotFound(); await LoadDropdowns(); return View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, DelPending model) { if (id != model.Id) return NotFound(); if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.DelPendings.AnyAsync(x => x.Id == id)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Delete(int id) { var m = await _context.DelPendings.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id) { var m = await _context.DelPendings.FindAsync(id); if (m != null) { _context.DelPendings.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }
        private async Task LoadDropdowns()
        {
            ViewBag.RdcCodes = await _context.StoreMasters.Select(x => x.RdcCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.MajCats = await _context.BinCapacities.Select(x => x.MajCat).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        }
        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
