using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsStMasterController : Controller
{
    private readonly DataV2DbContext _context;
    public ArsStMasterController(DataV2DbContext context) => _context = context;

    public async Task<IActionResult> Index(string? st, string? taggedRdc, string? hubCd, int page = 1, int pageSize = 100)
    {
        var query = _context.ArsStMasters.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.StCd == st);
        if (!string.IsNullOrEmpty(taggedRdc)) query = query.Where(x => x.TaggedRdc == taggedRdc);
        if (!string.IsNullOrEmpty(hubCd)) query = query.Where(x => x.HubCd == hubCd);

        ViewBag.TotalCount = await query.CountAsync();
        ViewBag.TotalRows = await _context.ArsStMasters.CountAsync();
        ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.St = st; ViewBag.TaggedRdc = taggedRdc; ViewBag.HubCd = hubCd;
        ViewBag.Stores = await _context.ArsStMasters.Select(x => x.StCd).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.Rdcs = await _context.ArsStMasters.Select(x => x.TaggedRdc).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.Hubs = await _context.ArsStMasters.Select(x => x.HubCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync();
        ViewBag.TotalStores = await _context.ArsStMasters.CountAsync();
        ViewBag.TotalRdcs = await _context.ArsStMasters.Select(x => x.TaggedRdc).Where(x => x != null).Distinct().CountAsync();
        ViewBag.TotalHubs = await _context.ArsStMasters.Select(x => x.HubCd).Where(x => x != null).Distinct().CountAsync();

        var data = await query.OrderBy(x => x.StCd)
            .Skip((page - 1) * pageSize).Take(pageSize).ToListAsync();
        return View(data);
    }

    public IActionResult Create() => View(new ArsStMaster());

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(ArsStMaster model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.ArsStMasters.Add(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record added.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Edit(int id)
    {
        var item = await _context.ArsStMasters.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(ArsStMaster model)
    {
        if (!ModelState.IsValid) return View(model);
        _context.Update(model);
        await _context.SaveChangesAsync();
        TempData["SuccessMessage"] = "Record updated.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> Delete(int id)
    {
        var item = await _context.ArsStMasters.FindAsync(id);
        if (item == null) return NotFound();
        return View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        var item = await _context.ArsStMasters.FindAsync(id);
        if (item != null) { _context.ArsStMasters.Remove(item); await _context.SaveChangesAsync(); }
        TempData["SuccessMessage"] = "Record deleted.";
        return RedirectToAction(nameof(Index));
    }

    public async Task<IActionResult> ExportCsv(string? st, string? taggedRdc, string? hubCd)
    {
        var query = _context.ArsStMasters.AsQueryable();
        if (!string.IsNullOrEmpty(st)) query = query.Where(x => x.StCd == st);
        if (!string.IsNullOrEmpty(taggedRdc)) query = query.Where(x => x.TaggedRdc == taggedRdc);
        if (!string.IsNullOrEmpty(hubCd)) query = query.Where(x => x.HubCd == hubCd);
        var data = await query.OrderBy(x => x.StCd).ToListAsync();
        var sb = new StringBuilder();
        sb.AppendLine("ST_CD,ST_NM,HUB_CD,HUB_NM,DIRECT_HUB,TAGGED_RDC,DH24_DC_TO_HUB_INTRA,DH24_HUB_TO_ST_INTRA,DW01_DC_TO_HUB_INTRA,DW01_HUB_TO_ST_INTRA,ST_OP_DT,ST_STAT,SALE_COVER_DAYS,PRD_DAYS,INTRA_DAYS,TTL_ALC_DAYS");
        foreach (var r in data)
            sb.AppendLine($"{r.StCd},{r.StNm},{r.HubCd},{r.HubNm},{r.DirectHub},{r.TaggedRdc},{r.Dh24DcToHubIntra},{r.Dh24HubToStIntra},{r.Dw01DcToHubIntra},{r.Dw01HubToStIntra},{r.StOpDt:yyyy-MM-dd},{r.StStat},{r.SaleCoverDays},{r.PrdDays},{r.IntraDays:F2},{r.TtlAlcDays:F2}");
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "ARS_ST_MASTER.csv");
    }
}
