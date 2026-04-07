using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using System.Data;
using System.Diagnostics;
using System.Text;
using SysColor = System.Drawing.Color;
using DocumentFormat.OpenXml;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers;

public class BulkUploadController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<BulkUploadController> _logger;

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
            ws.Cells[1, i + 1].Style.Fill.BackgroundColor.SetColor(SysColor.FromArgb(68, 114, 196));
            ws.Cells[1, i + 1].Style.Font.Color.SetColor(SysColor.White);
        }
        var sample = GetSampleRow(table);
        for (int i = 0; i < sample.Length; i++) ws.Cells[2, i + 1].Value = sample[i];
        ws.Cells[ws.Dimension.Address].AutoFitColumns();

        return File(package.GetAsByteArray(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            $"Sample_{table}.xlsx");
    }

    // ──────────────────────────────────────────────────────────────
    // UPLOAD — SqlBulkCopy (20 lakh rows in seconds)
    // ──────────────────────────────────────────────────────────────
    [HttpPost]
    [ValidateAntiForgeryToken]
    [RequestSizeLimit(500_000_000)] // 500 MB max
    public async Task<IActionResult> Upload(IFormFile file, string table, string mode)
    {
        if (file == null || file.Length == 0)
        { TempData["ErrorMessage"] = "Please select an Excel file."; return RedirectToAction(nameof(Index)); }

        var ext = Path.GetExtension(file.FileName).ToLower();
        if (ext != ".xlsx" && ext != ".xls")
        { TempData["ErrorMessage"] = "Only .xlsx / .xls files supported."; return RedirectToAction(nameof(Index)); }

        var sw = Stopwatch.StartNew();

        try
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);
            using var package = new ExcelPackage(stream);

            if (package.Workbook.Worksheets.Count == 0)
            { TempData["ErrorMessage"] = "Excel file has no worksheets."; return RedirectToAction(nameof(Index)); }

            var sqlTableName = GetSqlTableName(table);
            var sqlColumns = GetSqlColumns(table);
            if (sqlTableName == null || sqlColumns == null)
            { TempData["ErrorMessage"] = $"Unknown table: {table}"; return RedirectToAction(nameof(Index)); }

            // Step 1: Read ALL sheets into DataTable (merges multi-sheet files)
            _logger.LogInformation("BulkUpload: Reading Excel for {Table} ({Sheets} sheets)...", table, package.Workbook.Worksheets.Count);
            DataTable? dt = null;
            foreach (var ws in package.Workbook.Worksheets)
            {
                if (ws?.Dimension == null) continue;
                var sheetDt = ReadExcelToDataTable(ws, sqlColumns, table);
                if (dt == null)
                    dt = sheetDt;
                else
                {
                    foreach (DataRow row in sheetDt.Rows)
                        dt.ImportRow(row);
                }
                _logger.LogInformation("BulkUpload: Sheet '{Sheet}' — {Rows} rows", ws.Name, sheetDt.Rows.Count);
            }
            if (dt == null || dt.Rows.Count == 0)
            { TempData["ErrorMessage"] = "Excel file is empty (no data rows in any sheet)."; return RedirectToAction(nameof(Index)); }
            _logger.LogInformation("BulkUpload: {Rows} total rows from {Sheets} sheets in {Ms}ms", dt.Rows.Count, package.Workbook.Worksheets.Count, sw.ElapsedMilliseconds);

            // Step 2: Truncate if Replace mode
            if (mode == "Replace")
            {
                await _context.Database.ExecuteSqlRawAsync(GetTruncateSql(table));
                _logger.LogInformation("BulkUpload: Truncated {Table}", sqlTableName);
            }

            // Step 3: SqlBulkCopy — the fast path
            var connStr = _context.Database.GetConnectionString()!;
            using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock | SqlBulkCopyOptions.FireTriggers, null);
                bulkCopy.DestinationTableName = sqlTableName;
                bulkCopy.BatchSize = 50_000;
                bulkCopy.BulkCopyTimeout = 600; // 10 min timeout
                bulkCopy.EnableStreaming = true;

                // Map DataTable columns to SQL columns
                for (int i = 0; i < sqlColumns.Length; i++)
                    bulkCopy.ColumnMappings.Add(sqlColumns[i], sqlColumns[i]);

                await bulkCopy.WriteToServerAsync(dt);
            }

            sw.Stop();
            _logger.LogInformation("BulkUpload {Mode} {Table}: {Rows:N0} rows inserted in {Sec:N1}s",
                mode, table, dt.Rows.Count, sw.Elapsed.TotalSeconds);

            TempData["SuccessMessage"] = $"Upload complete — {dt.Rows.Count:N0} rows inserted in {sw.Elapsed.TotalSeconds:N1} seconds.";
        }
        catch (Exception ex)
        {
            sw.Stop();
            _logger.LogError(ex, "BulkUpload error: {Table}", table);
            var msg = ex.InnerException?.Message ?? ex.Message;
            TempData["ErrorMessage"] = $"Upload failed ({sw.Elapsed.TotalSeconds:N1}s): {msg}";
        }

        return RedirectToAction(nameof(Index));
    }

    // ──────────────────────────────────────────────────────────────
    // FAST EXCEL READER — EPPlus bulk range-to-array (single call)
    // ──────────────────────────────────────────────────────────────
    private DataTable ReadExcelToDataTable(ExcelWorksheet ws, string[] sqlColumns, string table)
    {
        var colTypes = GetColumnTypes(table);
        int numCols = sqlColumns.Length;
        int totalRows = ws.Dimension.Rows;
        int totalCols = ws.Dimension.Columns;
        int dataCols = Math.Min(totalCols, numCols);

        // Pre-allocate DataTable with correct capacity
        var dt = new DataTable();
        dt.MinimumCapacity = totalRows;
        for (int i = 0; i < numCols; i++)
            dt.Columns.Add(sqlColumns[i], i < colTypes.Length ? colTypes[i] : typeof(string));

        // KEY OPTIMIZATION: Read entire range into 2D object array in ONE call
        // This is 10-50x faster than cell-by-cell access
        var range = ws.Cells[2, 1, totalRows, dataCols]; // skip header row
        var rawData = range.Value as object[,];

        if (rawData == null)
        {
            // Single row edge case
            var singleRow = dt.NewRow();
            for (int c = 0; c < dataCols; c++)
            {
                var val = ws.Cells[2, c + 1].Value;
                singleRow[c] = ConvertCell(val, c < colTypes.Length ? colTypes[c] : typeof(string));
            }
            dt.Rows.Add(singleRow);
            return dt;
        }

        int rowCount = rawData.GetLength(0);
        int colCount = rawData.GetLength(1);

        // Batch-add rows from the 2D array
        dt.BeginLoadData(); // disable constraints/indexing during load
        for (int r = 0; r < rowCount; r++)
        {
            // Skip empty rows (first column null/empty)
            var firstCell = rawData[r, 0];
            if (firstCell == null || string.IsNullOrWhiteSpace(firstCell.ToString())) continue;

            var row = dt.NewRow();
            for (int c = 0; c < numCols; c++)
            {
                var val = c < colCount ? rawData[r, c] : null;
                row[c] = ConvertCell(val, c < colTypes.Length ? colTypes[c] : typeof(string));
            }
            dt.Rows.Add(row);
        }
        dt.EndLoadData();

        return dt;
    }

    private static object ConvertCell(object? val, Type targetType)
    {
        if (val == null || (val is string s && string.IsNullOrWhiteSpace(s)))
            return targetType == typeof(string) ? (object)"NA"
                 : targetType == typeof(DateTime) ? (object)DBNull.Value
                 : (object)0m;

        if (targetType == typeof(decimal))
            return val is double d ? (decimal)d : decimal.TryParse(val.ToString(), out var dec) ? dec : 0m;

        if (targetType == typeof(int))
            return val is double d2 ? (int)d2 : int.TryParse(val.ToString(), out var iv) ? iv : 0;

        if (targetType == typeof(DateTime))
        {
            if (val is DateTime dtv) return dtv;
            if (val is double dbl) return DateTime.FromOADate(dbl);
            return DateTime.TryParse(val.ToString(), out var p) ? (object)p : DBNull.Value;
        }

        return val.ToString()?.Trim() ?? "NA";
    }

    // ──────────────────────────────────────────────────────────────
    // TABLE METADATA
    // ──────────────────────────────────────────────────────────────
    private string? GetSqlTableName(string table) => table switch
    {
        "WeekCalendar" => "[dbo].[WEEK_CALENDAR]",
        "StoreMaster"  => "[dbo].[MASTER_ST_MASTER]",
        "BinCapacity"  => "[dbo].[MASTER_BIN_CAPACITY]",
        "SaleQty"      => "[dbo].[QTY_SALE_QTY]",
        "DispQty"      => "[dbo].[QTY_DISP_QTY]",
        "StoreStock"   => "[dbo].[QTY_ST_STK_Q]",
        "DcStock"      => "[dbo].[QTY_MSA_AND_GRT]",
        "DelPending"       => "[dbo].[QTY_DEL_PENDING]",
        "GrtContribution"  => "[dbo].[MASTER_GRT_CONTRIBUTION]",
        "ProductHierarchy" => "[dbo].[MASTER_PRODUCT_HIERARCHY]",
        "ContMacroMvgr" => "[dbo].[ST_MAJ_CAT_MACRO_MVGR_PLAN]",
        "ContSz"        => "[dbo].[ST_MAJ_CAT_SZ_PLAN]",
        "ContSeg"       => "[dbo].[ST_MAJ_CAT_SEG_PLAN]",
        "ContVnd"       => "[dbo].[ST_MAJ_CAT_VND_PLAN]",
        "SubStStkMvgr"  => "[dbo].[SUB_ST_STK_MVGR]",
        "SubStStkSz"    => "[dbo].[SUB_ST_STK_SZ]",
        "SubStStkSeg"   => "[dbo].[SUB_ST_STK_SEG]",
        "SubStStkVnd"   => "[dbo].[SUB_ST_STK_VND]",
        "SubDcStkMvgr"  => "[dbo].[SUB_DC_STK_MVGR]",
        "SubDcStkSz"    => "[dbo].[SUB_DC_STK_SZ]",
        "SubDcStkSeg"   => "[dbo].[SUB_DC_STK_SEG]",
        "SubDcStkVnd"   => "[dbo].[SUB_DC_STK_VND]",
        _ => null
    };

    private string GetTruncateSql(string table) => table switch
    {
        "StoreMaster" => "DELETE FROM [dbo].[MASTER_ST_MASTER]",
        "StoreStock"  => "DELETE FROM [dbo].[QTY_ST_STK_Q]",
        "DcStock"     => "DELETE FROM [dbo].[QTY_MSA_AND_GRT]",
        "DelPending"  => "DELETE FROM [dbo].[QTY_DEL_PENDING]",
        "GrtContribution"  => "TRUNCATE TABLE [dbo].[MASTER_GRT_CONTRIBUTION]",
        "ProductHierarchy" => "DELETE FROM [dbo].[MASTER_PRODUCT_HIERARCHY]",
        "ContMacroMvgr" => "DELETE FROM [dbo].[ST_MAJ_CAT_MACRO_MVGR_PLAN]",
        "ContSz"        => "DELETE FROM [dbo].[ST_MAJ_CAT_SZ_PLAN]",
        "ContSeg"       => "DELETE FROM [dbo].[ST_MAJ_CAT_SEG_PLAN]",
        "ContVnd"       => "DELETE FROM [dbo].[ST_MAJ_CAT_VND_PLAN]",
        "SubStStkMvgr"  => "DELETE FROM [dbo].[SUB_ST_STK_MVGR]",
        "SubStStkSz"    => "DELETE FROM [dbo].[SUB_ST_STK_SZ]",
        "SubStStkSeg"   => "DELETE FROM [dbo].[SUB_ST_STK_SEG]",
        "SubStStkVnd"   => "DELETE FROM [dbo].[SUB_ST_STK_VND]",
        "SubDcStkMvgr"  => "DELETE FROM [dbo].[SUB_DC_STK_MVGR]",
        "SubDcStkSz"    => "DELETE FROM [dbo].[SUB_DC_STK_SZ]",
        "SubDcStkSeg"   => "DELETE FROM [dbo].[SUB_DC_STK_SEG]",
        "SubDcStkVnd"   => "DELETE FROM [dbo].[SUB_DC_STK_VND]",
        _ => $"TRUNCATE TABLE {GetSqlTableName(table)}"
    };

    private string[]? GetSqlColumns(string table) => table switch
    {
        "WeekCalendar" => new[] { "WEEK_ID","WEEK_SEQ","FY_WEEK","FY_YEAR","CAL_YEAR","YEAR_WEEK","WK_ST_DT","WK_END_DT" },
        "StoreMaster"  => new[] { "ST CD","ST NM","RDC_CD","RDC_NM","HUB_CD","HUB_NM","STATUS","GRID_ST_STS","OP-DATE","AREA","STATE","REF STATE","SALE GRP","REF_ST CD","REF_ST NM","REF-GRP-NEW","REF-GRP-OLD","Date" },
        "BinCapacity"  => new[] { "MAJ-CAT","BIN CAP DC TEAM","BIN CAP" },
        "SaleQty"      => new string[] { "ST-CD","MAJ-CAT" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).Append("2").ToArray(),
        "DispQty"      => new string[] { "ST-CD","MAJ-CAT" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).Append("2").ToArray(),
        "StoreStock"   => new[] { "ST_CD","MAJ_CAT","STK_QTY","DATE" },
        "DcStock"      => new[] { "RDC_CD","RDC","MAJ-CAT","DC-STK-Q","GRT-STK-Q","W-GRT-STK-Q","DATE" },
        "DelPending"       => new[] { "RDC_CD","MAJ_CAT","DEL_PEND_Q","DATE" },
        "GrtContribution"  => new string[] { "SSN" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).ToArray(),
        "ProductHierarchy" => new[] { "SEG","DIV","SUB_DIV","MAJ_CAT_NM","SSN" },
        "ContMacroMvgr" => new[] { "ST_CD","MAJ_CAT_CD","DISP_MVGR_MATRIX","CONT_PCT" },
        "ContSz"        => new[] { "ST_CD","MAJ_CAT_CD","SZ","CONT_PCT" },
        "ContSeg"       => new[] { "ST_CD","MAJ_CAT_CD","SEG","CONT_PCT" },
        "ContVnd"       => new[] { "ST_CD","MAJ_CAT_CD","M_VND_CD","CONT_PCT" },
        "SubStStkMvgr"  => new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
        "SubStStkSz"    => new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
        "SubStStkSeg"   => new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
        "SubStStkVnd"   => new[] { "ST_CD","MAJ_CAT","SUB_VALUE","STK_QTY","DATE" },
        "SubDcStkMvgr"  => new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
        "SubDcStkSz"    => new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
        "SubDcStkSeg"   => new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
        "SubDcStkVnd"   => new[] { "RDC_CD","MAJ_CAT","SUB_VALUE","DC_STK_Q","GRT_STK_Q","W_GRT_STK_Q","DATE" },
        _ => null
    };

    private Type[] GetColumnTypes(string table) => table switch
    {
        "WeekCalendar" => new[] { typeof(int),typeof(int),typeof(int),typeof(int),typeof(int),typeof(string),typeof(DateTime),typeof(DateTime) },
        "StoreMaster"  => Enumerable.Repeat(typeof(string), 8).Append(typeof(DateTime)).Concat(Enumerable.Repeat(typeof(string), 8)).Append(typeof(DateTime)).ToArray(),
        "BinCapacity"  => new Type[] { typeof(string),typeof(decimal),typeof(decimal) },
        "SaleQty"      => new Type[] { typeof(string),typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 49)).ToArray(),
        "DispQty"      => new Type[] { typeof(string),typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 49)).ToArray(),
        "StoreStock"   => new Type[] { typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "DcStock"      => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
        "DelPending"       => new Type[] { typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "GrtContribution"  => new Type[] { typeof(string) }.Concat(Enumerable.Repeat(typeof(decimal), 48)).ToArray(),
        "ProductHierarchy" => new Type[] { typeof(string),typeof(string),typeof(string),typeof(string),typeof(string) },
        "ContMacroMvgr" => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
        "ContSz"        => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
        "ContSeg"       => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
        "ContVnd"       => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal) },
        "SubStStkMvgr"  => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "SubStStkSz"    => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "SubStStkSeg"   => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "SubStStkVnd"   => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(DateTime) },
        "SubDcStkMvgr"  => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
        "SubDcStkSz"    => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
        "SubDcStkSeg"   => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
        "SubDcStkVnd"   => new Type[] { typeof(string),typeof(string),typeof(string),typeof(decimal),typeof(decimal),typeof(decimal),typeof(DateTime) },
        _ => Array.Empty<Type>()
    };

    // ──────────────────────────────────────────────────────────────
    // DOWNLOAD ACTUAL DATA (CSV)
    // ──────────────────────────────────────────────────────────────
    [HttpGet]
    public async Task<IActionResult> DownloadData(string table)
    {
        var sqlTableName = GetSqlTableName(table);
        var sqlColumns = GetSqlColumns(table);
        var headers = GetHeaders(table);
        if (sqlTableName == null || sqlColumns == null || headers == null)
            return BadRequest("Unknown table.");

        var connStr = _context.Database.GetConnectionString()!;
        var sb = new StringBuilder();

        // Header row (friendly names)
        sb.AppendLine(string.Join(",", headers.Select(h => Q(h))));

        using (var conn = new SqlConnection(connStr))
        {
            await conn.OpenAsync();
            var colList = string.Join(",", sqlColumns.Select(c => $"[{c}]"));
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT {colList} FROM {sqlTableName} WITH (NOLOCK)";
            cmd.CommandTimeout = 300;
            using var reader = await cmd.ExecuteReaderAsync();
            var colTypes = GetColumnTypes(table);
            while (await reader.ReadAsync())
            {
                for (int i = 0; i < sqlColumns.Length; i++)
                {
                    if (i > 0) sb.Append(',');
                    if (reader.IsDBNull(i))
                    {
                        sb.Append(i < colTypes.Length && colTypes[i] == typeof(string) ? "NA" : "0");
                    }
                    else if (i < colTypes.Length && colTypes[i] == typeof(DateTime))
                    {
                        sb.Append(reader.GetDateTime(i).ToString("yyyy-MM-dd"));
                    }
                    else if (i < colTypes.Length && colTypes[i] == typeof(string))
                    {
                        sb.Append(Q(reader.GetValue(i).ToString() ?? "NA"));
                    }
                    else
                    {
                        sb.Append(reader.GetValue(i));
                    }
                }
                sb.AppendLine();
            }
        }

        _logger.LogInformation("DownloadData: {Table} exported as CSV", table);
        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", $"{table}_Data.csv");
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        return s;
    }

    // ──────────────────────────────────────────────────────────────
    // TABLE ROW COUNTS (for UI)
    // ──────────────────────────────────────────────────────────────
    [HttpGet]
    public async Task<IActionResult> TableCounts()
    {
        var connStr = _context.Database.GetConnectionString()!;
        var counts = new Dictionary<string, int>();
        var tableKeys = new[] { "WeekCalendar","StoreMaster","BinCapacity","SaleQty","DispQty","StoreStock","DcStock","DelPending","GrtContribution","ProductHierarchy","ContMacroMvgr","ContSz","ContSeg","ContVnd","SubStStkMvgr","SubStStkSz","SubStStkSeg","SubStStkVnd","SubDcStkMvgr","SubDcStkSz","SubDcStkSeg","SubDcStkVnd" };

        using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        foreach (var key in tableKeys)
        {
            var sqlName = GetSqlTableName(key);
            if (sqlName == null) continue;
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT COUNT(*) FROM {sqlName} WITH (NOLOCK)";
            counts[key] = (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }
        return Json(counts);
    }

    // ──────────────────────────────────────────────────────────────
    // SAMPLE HEADERS / DATA
    // ──────────────────────────────────────────────────────────────
    private string[]? GetHeaders(string table) => table switch
    {
        "WeekCalendar" => new[] { "WeekId","WeekSeq","FyWeek","FyYear","CalYear","YearWeek","WkStDt(yyyy-MM-dd)","WkEndDt(yyyy-MM-dd)" },
        "StoreMaster"  => new[] { "StCd","StNm","RdcCd","RdcNm","HubCd","HubNm","Status","GridStSts","OpDate(yyyy-MM-dd)","Area","State","RefState","SaleGrp","RefStCd","RefStNm","RefGrpNew","RefGrpOld","Date(yyyy-MM-dd)" },
        "BinCapacity"  => new[] { "MajCat","BinCapDcTeam","BinCap" },
        "SaleQty"      => new[] { "StCd","MajCat" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).Append("Col2").ToArray(),
        "DispQty"      => new[] { "StCd","MajCat" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).Append("Col2").ToArray(),
        "StoreStock"   => new[] { "StCd","MajCat","StkQty","Date(yyyy-MM-dd)" },
        "DcStock"      => new[] { "RdcCd","Rdc","MajCat","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        "DelPending"       => new[] { "RdcCd","MajCat","DelPendQ","Date(yyyy-MM-dd)" },
        "GrtContribution"  => new[] { "SSN" }.Concat(Enumerable.Range(1,48).Select(w=>$"WK-{w}")).ToArray(),
        "ProductHierarchy" => new[] { "Seg","Div","SubDiv","MajCatNm","Ssn" },
        "ContMacroMvgr" => new[] { "StCd","MajCatCd","DispMvgrMatrix","ContPct" },
        "ContSz"        => new[] { "StCd","MajCatCd","Sz","ContPct" },
        "ContSeg"       => new[] { "StCd","MajCatCd","Seg","ContPct" },
        "ContVnd"       => new[] { "StCd","MajCatCd","MVndCd","ContPct" },
        "SubStStkMvgr"  => new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
        "SubStStkSz"    => new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
        "SubStStkSeg"   => new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
        "SubStStkVnd"   => new[] { "StCd","MajCat","SubValue","StkQty","Date(yyyy-MM-dd)" },
        "SubDcStkMvgr"  => new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        "SubDcStkSz"    => new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        "SubDcStkSeg"   => new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        "SubDcStkVnd"   => new[] { "RdcCd","MajCat","SubValue","DcStkQ","GrtStkQ","WGrtStkQ","Date(yyyy-MM-dd)" },
        _ => null
    };

    private string[] GetSampleRow(string table) => table switch
    {
        "WeekCalendar" => new[] { "1","1","1","2026","2026","2026-W01","2026-04-03","2026-04-09" },
        "StoreMaster"  => new[] { "ST001","Store One","RDC01","RDC North","HUB01","Hub Delhi","NEW","A","2026-01-01","NORTH","DELHI","DELHI","GRP-N1","ST001","Ref Store","NGRP-1","NGRP-OLD","2026-04-01" },
        "BinCapacity"  => new[] { "APPAREL","150","120" },
        "SaleQty"      => new[] { "ST001","APPAREL" }.Concat(Enumerable.Repeat("100", 48)).Append("0").ToArray(),
        "DispQty"      => new[] { "ST001","APPAREL" }.Concat(Enumerable.Repeat("50", 48)).Append("0").ToArray(),
        "StoreStock"   => new[] { "ST001","APPAREL","120","2026-04-01" },
        "DcStock"      => new[] { "RDC01","RDC North","APPAREL","500","200","300","2026-04-01" },
        "DelPending"       => new[] { "RDC01","APPAREL","75","2026-04-01" },
        "GrtContribution"  => new[] { "A" }.Concat(Enumerable.Repeat("0.35", 48)).ToArray(),
        "ProductHierarchy" => new[] { "APP","MENS","MU","M_PW_SHIRT_FS","A" },
        "ContMacroMvgr" => new[] { "HA10","IB_B_SUIT_FS","WSH","0.01" },
        "ContSz"        => new[] { "HA10","IB_B_SUIT_FS","O-3M","0.16" },
        "ContSeg"       => new[] { "HA10","FW_W_FUR_SLIPPER","GM","0.01" },
        "ContVnd"       => new[] { "HA10","IB_ROMPER_SU","200854","0.50" },
        "SubStStkMvgr"  => new[] { "HA10","IB_B_SUIT_FS","WSH","120","2026-04-01" },
        "SubStStkSz"    => new[] { "HA10","IB_B_SUIT_FS","O-3M","80","2026-04-01" },
        "SubStStkSeg"   => new[] { "HA10","FW_K_FUR_SLIPPER","GM","90","2026-04-01" },
        "SubStStkVnd"   => new[] { "HA10","IB_ROMPER_SU","200854","60","2026-04-01" },
        "SubDcStkMvgr"  => new[] { "DW01","IB_B_SUIT_FS","WSH","500","200","150","2026-04-01" },
        "SubDcStkSz"    => new[] { "DW01","IB_B_SUIT_FS","O-3M","400","180","120","2026-04-01" },
        "SubDcStkSeg"   => new[] { "DW01","FW_K_FUR_SLIPPER","GM","350","150","100","2026-04-01" },
        "SubDcStkVnd"   => new[] { "DW01","IB_ROMPER_SU","200854","300","120","80","2026-04-01" },
        _ => Array.Empty<string>()
    };
}
