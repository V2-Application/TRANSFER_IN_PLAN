using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using System.Drawing;

namespace TRANSFER_IN_PLAN.Controllers;

public class BulkUploadController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<BulkUploadController> _logger;
    private const int BATCH_SIZE = 1000; // rows per SaveChanges call

    public BulkUploadController(PlanningDbContext context, ILogger<BulkUploadController> logger)
    {
        _context = context;
        _logger = logger;
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
    }

    public IActionResult Index() => View();

    // ──────────────────────────────────────────────────────────────
    // SAMPLE DOWNLOAD
    // ──────────────────────────────────────────────────────────────
    public IActionResult DownloadSample(string table)
    {
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
        using var package = new ExcelPackage();
        var ws = package.Workbook.Worksheets.Add("Sample");
        var headers = GetHeaders(table);
        if (headers == null) return BadRequest("Unknown table.");

        for (int i = 0; i < headers.Length; i++)
        {
            ws.Cells[1, i + 1].Value = headers[i];
            ws.Cells[1, i + 1].Style.Font.Bold = true;
            ws.Cells[1, i + 1].Style.Fill.PatternType = ExcelFillStyle.Solid;
            ws.Cells[1, i + 1].Style.Fill.BackgroundColor.SetColor(Color.FromArgb(68, 114, 196));
            ws.Cells[1, i + 1].Style.Font.Color.SetColor(Color.White);
            ws.Cells[1, i + 1].Style.Border.BorderAround(ExcelBorderStyle.Thin);
        }
        var sample = GetSampleRow(table);
        for (int i = 0; i < sample.Length; i++) ws.Cells[2, i + 1].Value = sample[i];
        ws.Cells[ws.Dimension.Address].AutoFitColumns();

        _logger.LogInformation("Sample downloaded for table: {Table}", table);
        return File(package.GetAsByteArray(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            $"Sample_{table}.xlsx");
    }

    // ──────────────────────────────────────────────────────────────
    // UPLOAD  (batch insert — 30 000 rows in seconds)
    // ──────────────────────────────────────────────────────────────
    [HttpPost]
    [ValidateAntiForgeryToken]
    public async Task<IActionResult> Upload(IFormFile file, string table, string mode)
    {
        if (file == null || file.Length == 0)
        { TempData["ErrorMessage"] = "Please select an Excel file to upload."; return RedirectToAction(nameof(Index)); }

        var ext = Path.GetExtension(file.FileName).ToLower();
        if (ext != ".xlsx" && ext != ".xls")
        { TempData["ErrorMessage"] = "Only Excel files (.xlsx / .xls) are supported."; return RedirectToAction(nameof(Index)); }

        try
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);
            using var package = new ExcelPackage(stream);
            var ws = package.Workbook.Worksheets[0];

            if (ws == null || ws.Dimension == null)
            { TempData["ErrorMessage"] = "The uploaded Excel file is empty or invalid."; return RedirectToAction(nameof(Index)); }

            int totalRows = ws.Dimension.Rows;
            int inserted = 0, skipped = 0;

            // ── Replace mode: TRUNCATE (instant, no row-by-row delete) ──
            if (mode == "Replace")
            {
                await TruncateTable(table);
                _logger.LogInformation("BulkUpload Replace: truncated {Table}", table);
            }

            // ── Disable EF change tracking for bulk insert performance ──
            _context.ChangeTracker.AutoDetectChangesEnabled = false;

            int batchCount = 0;

            for (int r = 2; r <= totalRows; r++)
            {
                // Skip fully empty rows
                if (string.IsNullOrWhiteSpace(ws.Cells[r, 1].Value?.ToString())) { skipped++; continue; }

                bool added = BuildAndAdd(table, ws, r);
                if (added) { inserted++; batchCount++; }
                else       { skipped++; continue; }

                // Flush every BATCH_SIZE rows
                if (batchCount >= BATCH_SIZE)
                {
                    await _context.SaveChangesAsync();
                    _context.ChangeTracker.Clear(); // free memory between batches
                    batchCount = 0;
                }
            }

            // Flush remaining
            if (batchCount > 0)
                await _context.SaveChangesAsync();

            _context.ChangeTracker.AutoDetectChangesEnabled = true;

            _logger.LogInformation("BulkUpload {Mode} {Table}: {Inserted} inserted, {Skipped} skipped.", mode, table, inserted, skipped);
            TempData["SuccessMessage"] = $"Upload complete — {inserted:N0} records inserted, {skipped:N0} skipped.";
        }
        catch (Exception ex)
        {
            _context.ChangeTracker.AutoDetectChangesEnabled = true;
            _logger.LogError(ex, "BulkUpload error on table {Table}", table);
            var innerMsg = ex.InnerException?.InnerException?.Message ?? ex.InnerException?.Message ?? ex.Message;
                TempData["ErrorMessage"] = $"Upload failed: {innerMsg}";
        }

        return RedirectToAction(nameof(Index));
    }

    // ──────────────────────────────────────────────────────────────
    // TRUNCATE (Replace mode — far faster than RemoveRange)
    // ──────────────────────────────────────────────────────────────
    private async Task TruncateTable(string table)
    {
        // Use raw SQL TRUNCATE so we don't load 30k rows into EF memory
        string sql = table switch
        {
            "WeekCalendar" => "TRUNCATE TABLE [dbo].[WEEK_CALENDAR]",
            "StoreMaster"  => "DELETE FROM [dbo].[MASTER_ST_MASTER]",  // has identity — DELETE resets auto-inc cleaner
            "BinCapacity"  => "TRUNCATE TABLE [dbo].[MASTER_BIN_CAPACITY]",
            "SaleQty"      => "TRUNCATE TABLE [dbo].[QTY_SALE_QTY]",
            "DispQty"      => "TRUNCATE TABLE [dbo].[QTY_DISP_QTY]",
            "StoreStock"   => "TRUNCATE TABLE [dbo].[QTY_ST_STK_Q]",
            "DcStock"      => "TRUNCATE TABLE [dbo].[QTY_MSA_AND_GRT]",
            "DelPending"   => "TRUNCATE TABLE [dbo].[QTY_DEL_PENDING]",
            _              => throw new ArgumentException($"Unknown table: {table}")
        };
        await _context.Database.ExecuteSqlRawAsync(sql);
    }

    // ──────────────────────────────────────────────────────────────
    // BUILD ENTITY + ADD TO CONTEXT (no SaveChanges here)
    // ──────────────────────────────────────────────────────────────
    private bool BuildAndAdd(string table, ExcelWorksheet ws, int r)
    {
        try
        {
            switch (table)
            {
                case "WeekCalendar":
                    _context.WeekCalendars.Add(new WeekCalendar
                    {
                        WeekId   = Int(ws,r,1) ?? 0, WeekSeq = Int(ws,r,2) ?? 0,
                        FyWeek   = Int(ws,r,3) ?? 0, FyYear  = Int(ws,r,4) ?? 0,
                        CalYear  = Int(ws,r,5) ?? 0, YearWeek = Cell(ws,r,6),
                        WkStDt   = Dt(ws,r,7),       WkEndDt  = Dt(ws,r,8)
                    });
                    return true;

                case "StoreMaster":
                    _context.StoreMasters.Add(new StoreMaster
                    {
                        StCd=Cell(ws,r,1), StNm=Cell(ws,r,2), RdcCd=Cell(ws,r,3), RdcNm=Cell(ws,r,4),
                        HubCd=Cell(ws,r,5), HubNm=Cell(ws,r,6), Status=Cell(ws,r,7)??"Active",
                        GridStSts=Cell(ws,r,8), OpDate=Dt(ws,r,9), Area=Cell(ws,r,10),
                        State=Cell(ws,r,11), RefState=Cell(ws,r,12), SaleGrp=Cell(ws,r,13),
                        RefStCd=Cell(ws,r,14), RefStNm=Cell(ws,r,15), RefGrpNew=Cell(ws,r,16),
                        RefGrpOld=Cell(ws,r,17), Date=Dt(ws,r,18)
                    });
                    return true;

                case "BinCapacity":
                    _context.BinCapacities.Add(new BinCapacity
                    { MajCat=Cell(ws,r,1), BinCapDcTeam=Dec(ws,r,2), BinCap=Dec(ws,r,3) });
                    return true;

                case "SaleQty":
                    _context.SaleQties.Add(new SaleQty
                    {
                        StCd=Cell(ws,r,1), MajCat=Cell(ws,r,2),
                        Wk1=Dec(ws,r,3),Wk2=Dec(ws,r,4),Wk3=Dec(ws,r,5),Wk4=Dec(ws,r,6),
                        Wk5=Dec(ws,r,7),Wk6=Dec(ws,r,8),Wk7=Dec(ws,r,9),Wk8=Dec(ws,r,10),
                        Wk9=Dec(ws,r,11),Wk10=Dec(ws,r,12),Wk11=Dec(ws,r,13),Wk12=Dec(ws,r,14),
                        Wk13=Dec(ws,r,15),Wk14=Dec(ws,r,16),Wk15=Dec(ws,r,17),Wk16=Dec(ws,r,18),
                        Wk17=Dec(ws,r,19),Wk18=Dec(ws,r,20),Wk19=Dec(ws,r,21),Wk20=Dec(ws,r,22),
                        Wk21=Dec(ws,r,23),Wk22=Dec(ws,r,24),Wk23=Dec(ws,r,25),Wk24=Dec(ws,r,26),
                        Wk25=Dec(ws,r,27),Wk26=Dec(ws,r,28),Wk27=Dec(ws,r,29),Wk28=Dec(ws,r,30),
                        Wk29=Dec(ws,r,31),Wk30=Dec(ws,r,32),Wk31=Dec(ws,r,33),Wk32=Dec(ws,r,34),
                        Wk33=Dec(ws,r,35),Wk34=Dec(ws,r,36),Wk35=Dec(ws,r,37),Wk36=Dec(ws,r,38),
                        Wk37=Dec(ws,r,39),Wk38=Dec(ws,r,40),Wk39=Dec(ws,r,41),Wk40=Dec(ws,r,42),
                        Wk41=Dec(ws,r,43),Wk42=Dec(ws,r,44),Wk43=Dec(ws,r,45),Wk44=Dec(ws,r,46),
                        Wk45=Dec(ws,r,47),Wk46=Dec(ws,r,48),Wk47=Dec(ws,r,49),Wk48=Dec(ws,r,50),
                        Col2=Dec(ws,r,51)
                    });
                    return true;

                case "DispQty":
                    _context.DispQties.Add(new DispQty
                    {
                        StCd=Cell(ws,r,1), MajCat=Cell(ws,r,2),
                        Wk1=Dec(ws,r,3),Wk2=Dec(ws,r,4),Wk3=Dec(ws,r,5),Wk4=Dec(ws,r,6),
                        Wk5=Dec(ws,r,7),Wk6=Dec(ws,r,8),Wk7=Dec(ws,r,9),Wk8=Dec(ws,r,10),
                        Wk9=Dec(ws,r,11),Wk10=Dec(ws,r,12),Wk11=Dec(ws,r,13),Wk12=Dec(ws,r,14),
                        Wk13=Dec(ws,r,15),Wk14=Dec(ws,r,16),Wk15=Dec(ws,r,17),Wk16=Dec(ws,r,18),
                        Wk17=Dec(ws,r,19),Wk18=Dec(ws,r,20),Wk19=Dec(ws,r,21),Wk20=Dec(ws,r,22),
                        Wk21=Dec(ws,r,23),Wk22=Dec(ws,r,24),Wk23=Dec(ws,r,25),Wk24=Dec(ws,r,26),
                        Wk25=Dec(ws,r,27),Wk26=Dec(ws,r,28),Wk27=Dec(ws,r,29),Wk28=Dec(ws,r,30),
                        Wk29=Dec(ws,r,31),Wk30=Dec(ws,r,32),Wk31=Dec(ws,r,33),Wk32=Dec(ws,r,34),
                        Wk33=Dec(ws,r,35),Wk34=Dec(ws,r,36),Wk35=Dec(ws,r,37),Wk36=Dec(ws,r,38),
                        Wk37=Dec(ws,r,39),Wk38=Dec(ws,r,40),Wk39=Dec(ws,r,41),Wk40=Dec(ws,r,42),
                        Wk41=Dec(ws,r,43),Wk42=Dec(ws,r,44),Wk43=Dec(ws,r,45),Wk44=Dec(ws,r,46),
                        Wk45=Dec(ws,r,47),Wk46=Dec(ws,r,48),Wk47=Dec(ws,r,49),Wk48=Dec(ws,r,50),
                        Col2=Dec(ws,r,51)
                    });
                    return true;

                case "StoreStock":
                    _context.StoreStocks.Add(new StoreStock
                    { StCd=Cell(ws,r,1), MajCat=Cell(ws,r,2), StkQty=Dec(ws,r,3), Date=Dt(ws,r,4) });
                    return true;

                case "DcStock":
                    _context.DcStocks.Add(new DcStock
                    {
                        RdcCd=Cell(ws,r,1), Rdc=Cell(ws,r,2), MajCat=Cell(ws,r,3),
                        DcStkQ=Dec(ws,r,4), GrtStkQ=Dec(ws,r,5), WGrtStkQ=Dec(ws,r,6), Date=Dt(ws,r,7)
                    });
                    return true;

                case "DelPending":
                    _context.DelPendings.Add(new DelPending
                    { RdcCd=Cell(ws,r,1), MajCat=Cell(ws,r,2), DelPendQ=Dec(ws,r,3), Date=Dt(ws,r,4) });
                    return true;

                default: return false;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Skipped row {Row} in {Table}.", r, table);
            return false;
        }
    }

    // ──────────────────────────────────────────────────────────────
    // CELL READERS
    // ──────────────────────────────────────────────────────────────
    private string? Cell(ExcelWorksheet ws, int r, int c) => ws.Cells[r, c].Value?.ToString()?.Trim();

    private decimal? Dec(ExcelWorksheet ws, int r, int c)
        => decimal.TryParse(Cell(ws, r, c), out var d) ? d : null;

    private int? Int(ExcelWorksheet ws, int r, int c)
        => int.TryParse(Cell(ws, r, c), out var i) ? i : null;

    private DateTime? Dt(ExcelWorksheet ws, int r, int c)
    {
        var v = ws.Cells[r, c].Value;
        if (v == null) return null;
        if (v is DateTime dt) return dt;
        if (v is double d)    return DateTime.FromOADate(d);
        return DateTime.TryParse(v.ToString(), out var p) ? p : null;
    }

    // ──────────────────────────────────────────────────────────────
    // HEADER / SAMPLE METADATA
    // ──────────────────────────────────────────────────────────────
    private string[]? GetHeaders(string table) => table switch
    {
        "WeekCalendar" => new[] { "WeekId","WeekSeq","FyWeek","FyYear","CalYear","YearWeek","WkStDt(yyyy-MM-dd)","WkEndDt(yyyy-MM-dd)" },
        "StoreMaster"  => new[] { "StCd","StNm","RdcCd","RdcNm","HubCd","HubNm","Status","GridStSts","OpDate(yyyy-MM-dd)","Area","State","RefState","SaleGrp","RefStCd","RefStNm","RefGrpNew","RefGrpOld","Date(yyyy-MM-dd)" },
        "BinCapacity"  => new[] { "MajCat","BinCapDcTeam","BinCap" },
        "SaleQty"      => new[] { "StCd","MajCat","WK-1","WK-2","WK-3","WK-4","WK-5","WK-6","WK-7","WK-8","WK-9","WK-10","WK-11","WK-12","WK-13","WK-14","WK-15","WK-16","WK-17","WK-18","WK-19","WK-20","WK-21","WK-22","WK-23","WK-24","WK-25","WK-26","WK-27","WK-28","WK-29","WK-30","WK-31","WK-32","WK-33","WK-34","WK-35","WK-36","WK-37","WK-38","WK-39","WK-40","WK-41","WK-42","WK-43","WK-44","WK-45","WK-46","WK-47","WK-48","Col2" },
        "DispQty"      => new[] { "StCd","MajCat","WK-1","WK-2","WK-3","WK-4","WK-5","WK-6","WK-7","WK-8","WK-9","WK-10","WK-11","WK-12","WK-13","WK-14","WK-15","WK-16","WK-17","WK-18","WK-19","WK-20","WK-21","WK-22","WK-23","WK-24","WK-25","WK-26","WK-27","WK-28","WK-29","WK-30","WK-31","WK-32","WK-33","WK-34","WK-35","WK-36","WK-37","WK-38","WK-39","WK-40","WK-41","WK-42","WK-43","WK-44","WK-45","WK-46","WK-47","WK-48","Col2" },
        "StoreStock"   => new[] { "StCd","MajCat","StkQty","Date(yyyy-MM-dd)" },
        "DcStock"      => new[] { "RdcCd","Rdc","MajCat","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        "DelPending"   => new[] { "RdcCd","MajCat","DelPendQ","Date(yyyy-MM-dd)" },
        _              => null
    };

    private string[] GetSampleRow(string table) => table switch
    {
        "WeekCalendar" => new[] { "1","1","1","2026","2026","2026-W01","2026-04-03","2026-04-09" },
        "StoreMaster"  => new[] { "ST001","Store One","RDC01","RDC Name","HUB01","Hub Name","Active","Open","2026-01-01","North","Maharashtra","MH","SG1","RST001","Ref Store One","GRP-NEW","GRP-OLD","2026-04-01" },
        "BinCapacity"  => new[] { "Electronics","50","45" },
        "SaleQty"      => new[] { "ST001","Electronics","10","12","15","11","13","14","10","9","11","12","13","14","10","11","12","13","14","10","11","12","10","11","12","13","14","10","11","12","13","14","10","11","12","13","14","10","11","12","13","14","10","11","12","13","14","10","11","12","0" },
        "DispQty"      => new[] { "ST001","Electronics","5","6","7","5","6","7","5","4","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","5","6","7","0" },
        "StoreStock"   => new[] { "ST001","Electronics","120","2026-04-01" },
        "DcStock"      => new[] { "RDC01","RDC Name One","Electronics","500","200","300","2026-04-01" },
        "DelPending"   => new[] { "RDC01","Electronics","75","2026-04-01" },
        _              => Array.Empty<string>()
    };
}
