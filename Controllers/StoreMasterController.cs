using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class StoreMasterController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<StoreMasterController> _logger;

    public StoreMasterController(IConfiguration config, ILogger<StoreMasterController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    private static StoreMaster ReadRow(IDataReader r) => new()
    {
        Id        = SnowflakeCrudHelper.Int(r, 0),
        StCd      = SnowflakeCrudHelper.StrNull(r, 1),
        StNm      = SnowflakeCrudHelper.StrNull(r, 2),
        RdcCd     = SnowflakeCrudHelper.StrNull(r, 3),
        RdcNm     = SnowflakeCrudHelper.StrNull(r, 4),
        HubCd     = SnowflakeCrudHelper.StrNull(r, 5),
        HubNm     = SnowflakeCrudHelper.StrNull(r, 6),
        Status    = SnowflakeCrudHelper.StrNull(r, 7),
        GridStSts = SnowflakeCrudHelper.StrNull(r, 8),
        OpDate    = SnowflakeCrudHelper.DateNull(r, 9),
        Area      = SnowflakeCrudHelper.StrNull(r, 10),
        State     = SnowflakeCrudHelper.StrNull(r, 11),
        RefState  = SnowflakeCrudHelper.StrNull(r, 12),
        SaleGrp   = SnowflakeCrudHelper.StrNull(r, 13),
        RefStCd   = SnowflakeCrudHelper.StrNull(r, 14),
        RefStNm   = SnowflakeCrudHelper.StrNull(r, 15),
        RefGrpNew = SnowflakeCrudHelper.StrNull(r, 16),
        RefGrpOld = SnowflakeCrudHelper.StrNull(r, 17),
        Date      = SnowflakeCrudHelper.DateNull(r, 18)
    };

    private const string TABLE = "MASTER_ST_MASTER";
    private const string COLS = "ID, ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, STATUS, GRID_ST_STS, OP_DATE, AREA, STATE, REF_STATE, SALE_GRP, REF_ST_CD, REF_ST_NM, REF_GRP_NEW, REF_GRP_OLD, DATE";

    [HttpGet]
    public async Task<IActionResult> Index(string? stCd, string? rdcCd, string? area, bool? activeOnly)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            // Build filter
            var conditions = new List<string>();
            var parms = new List<SnowflakeDbParameter>();
            int idx = 0;
            if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
            if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
            if (!string.IsNullOrEmpty(area)) { idx++; conditions.Add("AREA = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), area)); }
            if (activeOnly == true) conditions.Add("STATUS = 'A'");
            string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

            ViewBag.RdcCodes = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "RDC_CD");
            ViewBag.Areas = await SnowflakeCrudHelper.DistinctAsync(conn, TABLE, "AREA");
            ViewBag.StCd = stCd;
            ViewBag.RdcCd = rdcCd;
            ViewBag.Area = area;
            ViewBag.ActiveOnly = activeOnly;

            var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "ST_CD", 1, 100000, ReadRow);

            // Analytics from ALL stores
            var allStores = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, null, null, "ST_CD", 1, 100000, ReadRow);
            ViewBag.TotalStores = allStores.Count;
            ViewBag.NewCount = allStores.Count(x => x.Status?.ToUpper() == "NEW");
            ViewBag.OldCount = allStores.Count(x => x.Status?.ToUpper() == "OLD");
            ViewBag.UpcCount = allStores.Count(x => x.Status?.ToUpper() == "UPC");
            ViewBag.RdcCount = allStores.Select(x => x.RdcCd).Distinct().Count();
            ViewBag.StateCount = allStores.Select(x => x.State).Where(x => !string.IsNullOrEmpty(x)).Distinct().Count();

            // By Status
            ViewBag.StatusLabels = allStores.GroupBy(x => x.Status?.ToUpper() ?? "NA").Select(g => g.Key).OrderBy(x => x).ToList();
            ViewBag.StatusCounts = allStores.GroupBy(x => x.Status?.ToUpper() ?? "NA").OrderBy(g => g.Key).Select(g => g.Count()).ToList();

            // By RDC
            var byRdc = allStores.GroupBy(x => x.RdcCd ?? "NA").OrderBy(g => g.Key).ToList();
            ViewBag.RdcLabels = byRdc.Select(g => g.Key + (g.First().RdcNm != null ? " " + g.First().RdcNm : "")).ToList();
            ViewBag.RdcStoreCounts = byRdc.Select(g => g.Count()).ToList();

            // By State
            var byState = allStores.Where(x => !string.IsNullOrEmpty(x.State)).GroupBy(x => x.State!).OrderByDescending(g => g.Count()).Take(15).ToList();
            ViewBag.StateLabels = byState.Select(g => g.Key).ToList();
            ViewBag.StateStoreCounts = byState.Select(g => g.Count()).ToList();

            return View(data);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error loading StoreMaster");
            ViewBag.ErrorMessage = "Error loading data: " + ex.Message;
            return View(new List<StoreMaster>());
        }
    }

    [HttpGet]
    public async Task<IActionResult> ExportCsv(string? stCd, string? rdcCd, string? area, bool? activeOnly)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

        var conditions = new List<string>();
        var parms = new List<SnowflakeDbParameter>();
        int idx = 0;
        if (!string.IsNullOrEmpty(stCd)) { idx++; conditions.Add("ST_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), stCd)); }
        if (!string.IsNullOrEmpty(rdcCd)) { idx++; conditions.Add("RDC_CD = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), rdcCd)); }
        if (!string.IsNullOrEmpty(area)) { idx++; conditions.Add("AREA = ?"); parms.Add(SnowflakeCrudHelper.Param(idx.ToString(), area)); }
        if (activeOnly == true) conditions.Add("STATUS = 'A'");
        string? where = conditions.Count > 0 ? string.Join(" AND ", conditions) : null;

        var data = await SnowflakeCrudHelper.PagedQueryAsync(conn, TABLE, COLS, where, parms, "ST_CD", 1, 100000, ReadRow);
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

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "StoreMaster.csv");
    }

    [HttpGet]
    public IActionResult Create() => View();

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Create(StoreMaster model)
    {
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.InsertAsync(conn, TABLE,
                new[] { "ST_CD", "ST_NM", "RDC_CD", "RDC_NM", "HUB_CD", "HUB_NM", "STATUS", "GRID_ST_STS", "OP_DATE", "AREA", "STATE", "REF_STATE", "SALE_GRP", "REF_ST_CD", "REF_ST_NM", "REF_GRP_NEW", "REF_GRP_OLD", "DATE" },
                new object?[] { model.StCd, model.StNm, model.RdcCd, model.RdcNm, model.HubCd, model.HubNm, model.Status, model.GridStSts, model.OpDate, model.Area, model.State, model.RefState, model.SaleGrp, model.RefStCd, model.RefStNm, model.RefGrpNew, model.RefGrpOld, model.Date });
            _logger.LogInformation("StoreMaster created: StCd={StCd}", model.StCd);
            TempData["SuccessMessage"] = "Store created successfully.";
            return RedirectToAction(nameof(Index));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error creating StoreMaster");
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
    }

    [HttpGet]
    public async Task<IActionResult> Edit(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ValidateAntiForgeryToken]
    public async Task<IActionResult> Edit(int id, StoreMaster model)
    {
        if (id != model.Id) return NotFound();
        if (!ModelState.IsValid) return View(model);
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.UpdateAsync(conn, TABLE,
                new[] { "ST_CD", "ST_NM", "RDC_CD", "RDC_NM", "HUB_CD", "HUB_NM", "STATUS", "GRID_ST_STS", "OP_DATE", "AREA", "STATE", "REF_STATE", "SALE_GRP", "REF_ST_CD", "REF_ST_NM", "REF_GRP_NEW", "REF_GRP_OLD", "DATE" },
                new object?[] { model.StCd, model.StNm, model.RdcCd, model.RdcNm, model.HubCd, model.HubNm, model.Status, model.GridStSts, model.OpDate, model.Area, model.State, model.RefState, model.SaleGrp, model.RefStCd, model.RefStNm, model.RefGrpNew, model.RefGrpOld, model.Date }, id);
            _logger.LogInformation("StoreMaster updated: Id={Id} StCd={StCd}", id, model.StCd);
            TempData["SuccessMessage"] = "Store updated successfully.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error updating StoreMaster Id={Id}", id);
            ModelState.AddModelError("", "Error: " + ex.Message);
            return View(model);
        }
        return RedirectToAction(nameof(Index));
    }

    [HttpPost]
    public async Task<IActionResult> Details(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View("Edit", item);
    }

    [HttpGet]
    public async Task<IActionResult> Delete(int id)
    {
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var item = await SnowflakeCrudHelper.FindByIdAsync(conn, TABLE, COLS, id, ReadRow);
        return item == null ? NotFound() : View(item);
    }

    [HttpPost, ActionName("Delete"), ValidateAntiForgeryToken]
    public async Task<IActionResult> DeleteConfirmed(int id)
    {
        try
        {
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await SnowflakeCrudHelper.DeleteAsync(conn, TABLE, id);
            _logger.LogInformation("StoreMaster deleted: Id={Id}", id);
            TempData["SuccessMessage"] = "Store deleted.";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error deleting StoreMaster Id={Id}", id);
            TempData["ErrorMessage"] = "Error: " + ex.Message;
        }
        return RedirectToAction(nameof(Index));
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
