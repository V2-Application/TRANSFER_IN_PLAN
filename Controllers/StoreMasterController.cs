using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class StoreMasterController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<StoreMasterController> _logger;

        public StoreMasterController(PlanningDbContext context, ILogger<StoreMasterController> logger)
        {
            _context = context;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> Index(string? stCd, string? rdcCd, string? area, bool? activeOnly)
        {
            var query = _context.StoreMasters.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(area)) query = query.Where(x => x.Area == area);
            if (activeOnly == true) query = query.Where(x => x.Status == "A");
            ViewBag.RdcCodes = await _context.StoreMasters.Select(x => x.RdcCd).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.Areas = await _context.StoreMasters.Select(x => x.Area).Distinct().OrderBy(x => x).ToListAsync();
            ViewBag.StCd = stCd;
            ViewBag.RdcCd = rdcCd;
            ViewBag.Area = area;
            ViewBag.ActiveOnly = activeOnly;
            var data = await query.OrderBy(x => x.StCd).ToListAsync();
            return View(data);
        }

        [HttpGet]
        public async Task<IActionResult> ExportCsv(string? stCd, string? rdcCd, string? area, bool? activeOnly)
        {
            var query = _context.StoreMasters.AsQueryable();
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(area)) query = query.Where(x => x.Area == area);
            if (activeOnly == true) query = query.Where(x => x.Status == "A");
            var data = await query.OrderBy(x => x.StCd).ToListAsync();
            _logger.LogInformation("StoreMaster ExportCsv: {Count} rows exported", data.Count);

            var sb = new StringBuilder();
            sb.AppendLine("Id,StCd,StNm,RdcCd,RdcNm,HubCd,HubNm,Status,GridStSts,OpDate,Area,State,RefState,SaleGrp,RefStCd,RefStNm,RefGrpNew,RefGrpOld,Date");
            foreach (var r in data)
            {
                sb.AppendLine(string.Join(",",
                    r.Id, Q(r.StCd), Q(r.StNm), Q(r.RdcCd), Q(r.RdcNm),
                    Q(r.HubCd), Q(r.HubNm), Q(r.Status), Q(r.GridStSts),
                    r.OpDate?.ToString("yyyy-MM-dd"),
                    Q(r.Area), Q(r.State), Q(r.RefState), Q(r.SaleGrp),
                    Q(r.RefStCd), Q(r.RefStNm), Q(r.RefGrpNew), Q(r.RefGrpOld),
                    r.Date?.ToString("yyyy-MM-dd")));
            }

            var bytes = Encoding.UTF8.GetBytes(sb.ToString());
            return File(bytes, "text/csv", "StoreMaster.csv");
        }

        [HttpGet]
        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(StoreMaster model)
        {
            if (!ModelState.IsValid) return View(model);
            _context.StoreMasters.Add(model);
            await _context.SaveChangesAsync();
            _logger.LogInformation("StoreMaster created: StCd={StCd}", model.StCd);
            TempData["SuccessMessage"] = "Store created successfully.";
            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public async Task<IActionResult> Edit(int id)
        {
            var model = await _context.StoreMasters.FindAsync(id);
            if (model == null) return NotFound();
            return View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, StoreMaster model)
        {
            if (id != model.Id) return NotFound();
            if (!ModelState.IsValid) return View(model);
            try
            {
                _context.Update(model);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreMaster updated: Id={Id} StCd={StCd}", id, model.StCd);
                TempData["SuccessMessage"] = "Store updated successfully.";
            }
            catch (DbUpdateConcurrencyException)
            {
                if (!await _context.StoreMasters.AnyAsync(x => x.Id == id)) return NotFound();
                throw;
            }
            return RedirectToAction(nameof(Index));
        }

        [HttpPost]
        public async Task<IActionResult> ToggleStatus(int id)
        {
            var model = await _context.StoreMasters.FindAsync(id);
            if (model == null) return NotFound();
            model.Status = model.Status == "A" ? "I" : "A";
            await _context.SaveChangesAsync();
            _logger.LogInformation("StoreMaster status toggled: Id={Id} NewStatus={Status}", id, model.Status);
            TempData["SuccessMessage"] = "Store status updated.";
            return RedirectToAction(nameof(Index));
        }

        [HttpGet]
        public async Task<IActionResult> Delete(int id)
        {
            var model = await _context.StoreMasters.FindAsync(id);
            if (model == null) return NotFound();
            return View(model);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var model = await _context.StoreMasters.FindAsync(id);
            if (model != null)
            {
                _context.StoreMasters.Remove(model);
                await _context.SaveChangesAsync();
                _logger.LogInformation("StoreMaster deleted: Id={Id}", id);
                TempData["SuccessMessage"] = "Store deleted.";
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
