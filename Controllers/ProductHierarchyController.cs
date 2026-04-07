using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class ProductHierarchyController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<ProductHierarchyController> _logger;
        public ProductHierarchyController(PlanningDbContext context, ILogger<ProductHierarchyController> logger) { _context = context; _logger = logger; }

        [HttpGet]
        public async Task<IActionResult> Index(string? seg, string? div, string? subDiv, string? majCatNm)
        {
            var query = _context.ProductHierarchies.AsQueryable();
            if (!string.IsNullOrEmpty(seg)) query = query.Where(x => x.Seg == seg);
            if (!string.IsNullOrEmpty(div)) query = query.Where(x => x.Div == div);
            if (!string.IsNullOrEmpty(subDiv)) query = query.Where(x => x.SubDiv == subDiv);
            if (!string.IsNullOrEmpty(majCatNm)) query = query.Where(x => x.MajCatNm == majCatNm);

            ViewBag.Segs = await _context.ProductHierarchies.Select(x => x.Seg).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Divs = await _context.ProductHierarchies.Select(x => x.Div).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.SubDivs = await _context.ProductHierarchies.Select(x => x.SubDiv).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.MajCatNms = await _context.ProductHierarchies.Select(x => x.MajCatNm).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Seg = seg; ViewBag.Div = div; ViewBag.SubDiv = subDiv; ViewBag.MajCatNm = majCatNm;

            // Analytics
            ViewBag.TotalRows = await _context.ProductHierarchies.CountAsync();
            ViewBag.TotalSegs = await _context.ProductHierarchies.Select(x => x.Seg).Distinct().CountAsync();
            ViewBag.TotalDivs = await _context.ProductHierarchies.Select(x => x.Div).Distinct().CountAsync();
            ViewBag.TotalMajCats = await _context.ProductHierarchies.Select(x => x.MajCatNm).Distinct().CountAsync();

            return View(await query.OrderBy(x => x.Seg).ThenBy(x => x.Div).ThenBy(x => x.MajCatNm).ToListAsync());
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? seg, string? div, string? subDiv, string? majCatNm)
        {
            var query = _context.ProductHierarchies.AsQueryable();
            if (!string.IsNullOrEmpty(seg)) query = query.Where(x => x.Seg == seg);
            if (!string.IsNullOrEmpty(div)) query = query.Where(x => x.Div == div);
            if (!string.IsNullOrEmpty(subDiv)) query = query.Where(x => x.SubDiv == subDiv);
            if (!string.IsNullOrEmpty(majCatNm)) query = query.Where(x => x.MajCatNm == majCatNm);
            var data = await query.OrderBy(x => x.Seg).ThenBy(x => x.Div).ThenBy(x => x.MajCatNm).ToListAsync();
            _logger.LogInformation("ProductHierarchy ExportCsv: {Count} rows", data.Count);
            var sb = new StringBuilder();
            sb.AppendLine("Seg,Div,SubDiv,MajCatNm,Ssn");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", Q(r.Seg), Q(r.Div), Q(r.SubDiv), Q(r.MajCatNm), Q(r.Ssn)));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ProductHierarchy.csv");
        }

        [HttpGet] public IActionResult Create() => View();
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(ProductHierarchy model) { if (!ModelState.IsValid) return View(model); _context.ProductHierarchies.Add(model); await _context.SaveChangesAsync(); _logger.LogInformation("ProductHierarchy created: {Seg}/{MajCatNm}", model.Seg, model.MajCatNm); TempData["SuccessMessage"] = "Created."; return RedirectToAction(nameof(Index)); }

        [HttpGet] public async Task<IActionResult> Edit(int id) { var m = await _context.ProductHierarchies.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost][ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, ProductHierarchy model) { if (id != model.Id) return NotFound(); if (!ModelState.IsValid) return View(model); try { _context.Update(model); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Updated."; } catch (DbUpdateConcurrencyException) { if (!await _context.ProductHierarchies.AnyAsync(x => x.Id == id)) return NotFound(); throw; } return RedirectToAction(nameof(Index)); }

        [HttpGet] public async Task<IActionResult> Delete(int id) { var m = await _context.ProductHierarchies.FindAsync(id); return m == null ? NotFound() : View(m); }
        [HttpPost, ActionName("Delete")][ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id) { var m = await _context.ProductHierarchies.FindAsync(id); if (m != null) { _context.ProductHierarchies.Remove(m); await _context.SaveChangesAsync(); TempData["SuccessMessage"] = "Deleted."; } return RedirectToAction(nameof(Index)); }

        private static string Q(string? s) { if (string.IsNullOrEmpty(s)) return ""; return "\"" + s.Replace("\"", "\"\"") + "\""; }
    }
}
