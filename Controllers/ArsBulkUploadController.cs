using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using System.Data;
using System.Diagnostics;
using System.Text;
using SysColor = System.Drawing.Color;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsBulkUploadController : Controller
{
    private readonly string _connStr;
    private readonly ILogger<ArsBulkUploadController> _logger;

    public ArsBulkUploadController(IConfiguration config, ILogger<ArsBulkUploadController> logger)
    {
        _connStr = config.GetConnectionString("DataV2Database")!;
        _logger = logger;
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
    }

    // ──────────────────────────────────────────────────────────
    //  INDEX — Allocation Bulk Upload page
    // ──────────────────────────────────────────────────────────
    public async Task<IActionResult> Index()
    {
        var tables = GetAllTables();
        var counts = new Dictionary<string, int>();
        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        foreach (var t in tables)
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(1) FROM {GetSqlTableName(t.Key)} WITH (NOLOCK)";
                counts[t.Key] = (int)(await cmd.ExecuteScalarAsync() ?? 0);
            }
            catch { counts[t.Key] = 0; }
        }
        ViewBag.Tables = tables;
        ViewBag.Counts = counts;
        return View();
    }

    // ──────────────────────────────────────────────────────────
    //  UPLOAD — SqlBulkCopy
    // ──────────────────────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    [RequestSizeLimit(500_000_000)]
    public async Task<IActionResult> Upload(IFormFile file, string table, string mode)
    {
        if (file == null || file.Length == 0)
        { TempData["ErrorMessage"] = "Please select a file."; return RedirectToAction(nameof(Index)); }

        var sqlTableName = GetSqlTableName(table);
        var sqlColumns = GetSqlColumns(table);
        var colTypes = GetColumnTypes(table);
        if (sqlTableName == null || sqlColumns == null)
        { TempData["ErrorMessage"] = "Unknown table."; return RedirectToAction(nameof(Index)); }

        var sw = Stopwatch.StartNew();
        try
        {
            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);
            stream.Position = 0;

            using var package = new ExcelPackage(stream);
            var dt = new DataTable();
            for (int i = 0; i < sqlColumns.Length; i++)
                dt.Columns.Add(sqlColumns[i], colTypes.Length > i ? colTypes[i] : typeof(string));

            foreach (var ws in package.Workbook.Worksheets)
            {
                if (ws.Dimension == null) continue;
                int startRow = 2; // row 1 = headers
                for (int row = startRow; row <= ws.Dimension.End.Row; row++)
                {
                    var dr = dt.NewRow();
                    bool hasData = false;
                    for (int col = 0; col < sqlColumns.Length && col < ws.Dimension.End.Column; col++)
                    {
                        var val = ws.Cells[row, col + 1].Value;
                        if (val != null) hasData = true;
                        try
                        {
                            if (val == null) dr[col] = DBNull.Value;
                            else if (dt.Columns[col].DataType == typeof(decimal)) dr[col] = Convert.ToDecimal(val);
                            else if (dt.Columns[col].DataType == typeof(int)) dr[col] = Convert.ToInt32(val);
                            else if (dt.Columns[col].DataType == typeof(DateTime)) dr[col] = Convert.ToDateTime(val);
                            else dr[col] = val.ToString()?.Trim() ?? "";
                        }
                        catch { dr[col] = DBNull.Value; }
                    }
                    if (hasData) dt.Rows.Add(dr);
                }
            }

            await using var conn = new SqlConnection(_connStr);
            await conn.OpenAsync();

            if (mode == "Replace")
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"TRUNCATE TABLE {sqlTableName}";
                await cmd.ExecuteNonQueryAsync();
            }

            using var bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null);
            bulkCopy.DestinationTableName = sqlTableName;
            bulkCopy.BulkCopyTimeout = 600;
            bulkCopy.BatchSize = 50000;
            for (int i = 0; i < sqlColumns.Length; i++)
                bulkCopy.ColumnMappings.Add(i, sqlColumns[i]);
            await bulkCopy.WriteToServerAsync(dt);

            sw.Stop();
            TempData["SuccessMessage"] = $"Uploaded {dt.Rows.Count:N0} rows to {table} in {sw.ElapsedMilliseconds}ms ({mode} mode).";
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ArsBulkUpload failed for {Table}", table);
            TempData["ErrorMessage"] = $"Upload failed: {ex.Message}";
        }
        return RedirectToAction(nameof(Index));
    }

    // ──────────────────────────────────────────────────────────
    //  DOWNLOAD SAMPLE
    // ──────────────────────────────────────────────────────────
    public IActionResult DownloadSample(string table)
    {
        var headers = GetHeaders(table);
        if (headers == null) return BadRequest("Unknown table.");

        using var package = new ExcelPackage();
        var ws = package.Workbook.Worksheets.Add("Sample");
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

    // ──────────────────────────────────────────────────────────
    //  DOWNLOAD DATA (CSV)
    // ──────────────────────────────────────────────────────────
    public async Task DownloadData(string table)
    {
        var sqlTableName = GetSqlTableName(table);
        var headers = GetHeaders(table);
        if (sqlTableName == null || headers == null) { Response.StatusCode = 400; return; }

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename={table}.csv");

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT * FROM {sqlTableName} WITH (NOLOCK)";
        cmd.CommandTimeout = 300;

        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync(string.Join(",", headers));

        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < rdr.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = rdr.IsDBNull(i) ? "" : rdr.GetValue(i).ToString() ?? "";
                if (val.Contains(',') || val.Contains('"'))
                    sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    // ──────────────────────────────────────────────────────────
    //  TABLE MAPPINGS
    // ──────────────────────────────────────────────────────────
    private static Dictionary<string, string> GetAllTables() => new()
    {
        ["ArsDisplayMaster"] = "Display Master (ST x MJ → DISP_Q)",
        ["ArsAutoSale"] = "MJ Auto Sale (ST x MJ → CM/NM Sale)",
        ["ArsArtAutoSale"] = "Article Auto Sale (ST x ART x CLR → CM/NM Sale)",
        ["ArsHoldDays"] = "Hold Days Master (ST x MJ → HOLD_DAYS)",
        ["ArsStMaster"] = "Store Master (ST_CD → RDC, Hub, Cover Days, Intra)",
    };

    private string? GetSqlTableName(string table) => table switch
    {
        "ArsDisplayMaster" => "[dbo].[ARS_ST_MJ_DISPLAY_MASTER]",
        "ArsAutoSale"      => "[dbo].[ARS_ST_MJ_AUTO_SALE]",
        "ArsArtAutoSale"   => "[dbo].[ARS_ST_ART_AUTO_SALE]",
        "ArsHoldDays"      => "[dbo].[ARS_HOLD_DAYS_MASTER]",
        "ArsStMaster"      => "[dbo].[ARS_ST_MASTER]",
        _ => null
    };

    private string[]? GetSqlColumns(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { "ST", "MJ", "ST_MJ_DISP_Q" },
        "ArsAutoSale"      => new[] { "ST", "MJ", "CM-REM-DAYS", "NM-DAYS", "CM-AUTO-SALE-Q", "NM-AUTO-SALE-Q" },
        "ArsArtAutoSale"   => new[] { "ST", "GEN-ART", "CLR", "CM-REM-DAYS", "NM-DAYS", "CM-AUTO-SALE-Q", "NM-AUTO-SALE-Q" },
        "ArsHoldDays"      => new[] { "ST", "MJ", "HOLD_DAYS" },
        "ArsStMaster"      => new[] { "ST_CD", "ST_NM", "HUB_CD", "HUB_NM", "DIRECT_HUB", "TAGGED_RDC", "DH24_DC_TO_HUB_INTRA", "DH24_HUB_TO_ST_INTRA", "DW01_DC_TO_HUB_INTRA", "DW01_HUB_TO_ST_INTRA", "ST_OP_DT", "ST_STAT", "SALE_COVER_DAYS", "PRD_DAYS" },
        _ => null
    };

    private string[]? GetHeaders(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { "ST", "MJ", "ST_MJ_DISP_Q" },
        "ArsAutoSale"      => new[] { "ST", "MJ", "CM-REM-DAYS", "NM-DAYS", "CM-AUTO-SALE-Q", "NM-AUTO-SALE-Q" },
        "ArsArtAutoSale"   => new[] { "ST", "GEN-ART", "CLR", "CM-REM-DAYS", "NM-DAYS", "CM-AUTO-SALE-Q", "NM-AUTO-SALE-Q" },
        "ArsHoldDays"      => new[] { "ST", "MJ", "HOLD_DAYS" },
        "ArsStMaster"      => new[] { "ST_CD", "ST_NM", "HUB_CD", "HUB_NM", "DIRECT_HUB", "TAGGED_RDC", "DH24_DC_TO_HUB_INTRA", "DH24_HUB_TO_ST_INTRA", "DW01_DC_TO_HUB_INTRA", "DW01_HUB_TO_ST_INTRA", "ST_OP_DT", "ST_STAT", "SALE_COVER_DAYS", "PRD_DAYS" },
        _ => null
    };

    private Type[] GetColumnTypes(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { typeof(string), typeof(string), typeof(decimal) },
        "ArsAutoSale"      => new[] { typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal) },
        "ArsArtAutoSale"   => new[] { typeof(string), typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal) },
        "ArsHoldDays"      => new[] { typeof(string), typeof(string), typeof(decimal) },
        "ArsStMaster"      => new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal), typeof(DateTime), typeof(string), typeof(decimal), typeof(decimal) },
        _ => Array.Empty<Type>()
    };

    private object[] GetSampleRow(string table) => table switch
    {
        "ArsDisplayMaster" => new object[] { "HB05", "M_JEANS", 25.5 },
        "ArsAutoSale"      => new object[] { "HB05", "M_JEANS", 15, 30, 120.5, 95.3 },
        "ArsArtAutoSale"   => new object[] { "HB05", "1130140482", "BLK", 15, 30, 8.5, 6.2 },
        "ArsHoldDays"      => new object[] { "HB05", "M_JEANS", 20 },
        "ArsStMaster"      => new object[] { "HB05", "PTN", "DB03", "PATNA", "DB03", "DH24", 2, 1, 3, 2, "2012-05-01", "OLD", 2, 3 },
        _ => Array.Empty<object>()
    };
}
