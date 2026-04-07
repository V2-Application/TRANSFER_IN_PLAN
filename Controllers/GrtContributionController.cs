using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class GrtContributionController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<GrtContributionController> _logger;
        public GrtContributionController(PlanningDbContext context, ILogger<GrtContributionController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? ssn, int page = 1, int pageSize = 100)
        {
            var query = _context.GrtContributions.AsQueryable();
            if (!string.IsNullOrEmpty(ssn)) query = query.Where(x => x.Ssn == ssn);
            ViewBag.TotalCount = await query.CountAsync();
            ViewBag.Page = page; ViewBag.PageSize = pageSize;
            ViewBag.Ssns = await _context.GrtContributions.Select(x => x.Ssn).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Ssn = ssn;
            // Analytics
            ViewBag.TotalRows = await _context.GrtContributions.CountAsync();
            ViewBag.TotalSsns = await _context.GrtContributions.Select(x => x.Ssn).Distinct().CountAsync();
            var data = await query.OrderBy(x => x.Ssn)
                .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? ssn)
        {
            var query = _context.GrtContributions.AsQueryable();
            if (!string.IsNullOrEmpty(ssn)) query = query.Where(x => x.Ssn == ssn);
            var data = await query.OrderBy(x => x.Ssn).ToListAsync();
            _logger.LogInformation("GrtContribution ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Ssn," + string.Join(",", Enumerable.Range(1, 48).Select(w => $"Wk{w}")));
            foreach (var r in data)
            {
                var v = new decimal[] { r.Wk1,r.Wk2,r.Wk3,r.Wk4,r.Wk5,r.Wk6,r.Wk7,r.Wk8,r.Wk9,r.Wk10,r.Wk11,r.Wk12,r.Wk13,r.Wk14,r.Wk15,r.Wk16,r.Wk17,r.Wk18,r.Wk19,r.Wk20,r.Wk21,r.Wk22,r.Wk23,r.Wk24,r.Wk25,r.Wk26,r.Wk27,r.Wk28,r.Wk29,r.Wk30,r.Wk31,r.Wk32,r.Wk33,r.Wk34,r.Wk35,r.Wk36,r.Wk37,r.Wk38,r.Wk39,r.Wk40,r.Wk41,r.Wk42,r.Wk43,r.Wk44,r.Wk45,r.Wk46,r.Wk47,r.Wk48 };
                sb.AppendLine(Q(r.Ssn) + "," + string.Join(",", v.Select(x => x.ToString())));
            }
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "GrtContribution.csv");
        }

        [HttpGet] public IActionResult Create() => View();
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(GrtContribution model) { if (!ModelState.IsValid) return View(model); _context.GrtContributions.Add(model); await _context.SaveChangesAsync(); _logger.LogInformation("GrtContribution created: Ssn={Ssn}", model.Ssn); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }

        [HttpGet] public async Task<IActionResult> Edit(string ssn) { var m = await _context.GrtContributions.FirstOrDefaultAsync(x => x.Ssn == ssn); if (m == null) return NotFound(); return View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(GrtContribution model) { if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.GrtContributions.AnyAsync(x => x.Ssn == model.Ssn)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }

        [HttpGet] public async Task<IActionResult> Delete(string ssn) { var m = await _context.GrtContributions.FirstOrDefaultAsync(x => x.Ssn == ssn); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(string ssn) { var m = await _context.GrtContributions.FirstOrDefaultAsync(x => x.Ssn == ssn); if (m != null) { _context.GrtContributions.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }

        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
