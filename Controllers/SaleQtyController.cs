using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class SaleQtyController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<SaleQtyController> _logger;
        public SaleQtyController(PlanningDbContext context, ILogger<SaleQtyController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? stCd, string? majCat)
        {
            var query = _context.SaleQties.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.Categories = await _context.SaleQties.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.StCd = stCd; ViewBag.MajCat = majCat;
            return View(await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync());
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? stCd, string? majCat)
        {
            var query = _context.SaleQties.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("SaleQty ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("StCd,MajCat,Wk1,Wk2,Wk3,Wk4,Wk5,Wk6,Wk7,Wk8,Wk9,Wk10,Wk11,Wk12,Wk13,Wk14,Wk15,Wk16,Wk17,Wk18,Wk19,Wk20,Wk21,Wk22,Wk23,Wk24,Wk25,Wk26,Wk27,Wk28,Wk29,Wk30,Wk31,Wk32,Wk33,Wk34,Wk35,Wk36,Wk37,Wk38,Wk39,Wk40,Wk41,Wk42,Wk43,Wk44,Wk45,Wk46,Wk47,Wk48,Col2");
            foreach (var r in data)
            {
                var v = new decimal?[] { r.Wk1,r.Wk2,r.Wk3,r.Wk4,r.Wk5,r.Wk6,r.Wk7,r.Wk8,r.Wk9,r.Wk10,r.Wk11,r.Wk12,r.Wk13,r.Wk14,r.Wk15,r.Wk16,r.Wk17,r.Wk18,r.Wk19,r.Wk20,r.Wk21,r.Wk22,r.Wk23,r.Wk24,r.Wk25,r.Wk26,r.Wk27,r.Wk28,r.Wk29,r.Wk30,r.Wk31,r.Wk32,r.Wk33,r.Wk34,r.Wk35,r.Wk36,r.Wk37,r.Wk38,r.Wk39,r.Wk40,r.Wk41,r.Wk42,r.Wk43,r.Wk44,r.Wk45,r.Wk46,r.Wk47,r.Wk48 };
                sb.AppendLine(Q(r.StCd) + "," + Q(r.MajCat) + "," + string.Join(",", v.Select(x => x?.ToString() ?? "")) + "," + r.Col2);
            }
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "SaleQty.csv");
        }

        [HttpGet] public IActionResult Create() => View();
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(SaleQty model) { if (!ModelState.IsValid) return View(model); _context.SaleQties.Add(model); await _context.SaveChangesAsync(); _logger.LogInformation("SaleQty created: StCd={StCd}", model.StCd); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Edit(string stCd, string majCat) { var m = await _context.SaleQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat); return m == null ? NotFound() : View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(SaleQty model) { if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.SaleQties.AnyAsync(x => x.StCd == model.StCd && x.MajCat == model.MajCat)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }
        [HttpGet] public async Task<IActionResult> Delete(string stCd, string majCat) { var m = await _context.SaleQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(string stCd, string majCat) { var m = await _context.SaleQties.FirstOrDefaultAsync(x => x.StCd == stCd && x.MajCat == majCat); if (m != null) { _context.SaleQties.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }
        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
