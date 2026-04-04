using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class DcStockController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<DcStockController> _logger;

        public DcStockController(PlanningDbContext context, ILogger<DcStockController> logger)
        {
            _context = context;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Index(string? rdcCd, string? majCat)
        {
            var query = _context.DcStocks.AsQueryable();
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            ViewBag.RdcCodes = await _context.DcStocks.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Categories = await _context.DcStocks.Select(x => x.MajCat).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.RdcCd = rdcCd;
            ViewBag.MajCat = majCat;
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? rdcCd, string? majCat)
        {
            var query = _context.DcStocks.AsQueryable();
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            _logger.LogInformation("DcStock ExportCsv: {Count} rows exported", data.Count);

            var sb = new StringBuilder();
            sb.AppendLine("Id,RdcCd,Rdc,MajCat,DcStkQ,GrtStkQ,WGrtStkQ,Date");
            foreach (var r in data)
            {
                sb.AppendLine(string.Join(",",
                    r.Id, Q(r.RdcCd), Q(r.Rdc), Q(r.MajCat),
                    r.DcStkQ, r.GrtStkQ, r.WGrtStkQ,
                    r.Date?.ToString("yyyy-MM-dd")));
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return File(bytes, "text/csv", "DcStock.csv");
        }

        [HttpGet]
        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(DcStock model)
        {
            if (!ModelState.IsValid) return View(model);
            _context.DcStocks.Add(model);
            await _context.SaveChangesAsync();
            _logger.LogInformation("DcStock created: RdcCd={RdcCd} MajCat={MajCat}", model.RdcCd, model.MajCat);
            TempData["SuccessMessage"] = "DC Stock record created.";
            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public async Task<IActionResult> Edit(int id)
        {
            var model = await _context.DcStocks.FindAsync(id);
            if (model == null) return NotFound();
            return View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, DcStock model)
        {
            if (id != model.Id) return NotFound();
            if (!ModelState.IsValid) return View(model);
            try
            {
                _context.Update(model);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DcStock updated: Id={Id}", id);
                TempData["SuccessMessage"] = "DC Stock record updated.";
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!await _context.DcStocks.AnyAsync(x => x.Id == id)) return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public async Task<IActionResult> Delete(int id)
        {
            var model = await _context.DcStocks.FindAsync(id);
            if (model == null) return NotFound();
            return View(model);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var model = await _context.DcStocks.FindAsync(id);
            if (model != null)
            {
                _context.DcStocks.Remove(model);
                await _context.SaveChangesAsync();
                _logger.LogInformation("DcStock deleted: Id={Id}", id);
                TempData["SuccessMessage"] = "DC Stock record deleted.";
            }
            return RedirectToAction(nameof(Index));
        }

        private static string Q(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        }
    }
}
