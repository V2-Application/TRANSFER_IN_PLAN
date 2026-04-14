using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using Snowflake.Data.Client;
using System.Data;
using System.Diagnostics;
using System.Text;
using SysColor = System.Drawing.Color;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsBulkUploadController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<ArsBulkUploadController> _logger;

    public ArsBulkUploadController(IConfiguration config, ILogger<ArsBulkUploadController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
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
        await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
        await conn.OpenAsync();
        foreach (var t in tables)
        {
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(1) FROM {GetSnowflakeTableName(t.Key)}";
                counts[t.Key] = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
            }
            catch (Exception ex) { _logger.LogWarning("BulkUpload count failed for {Table}: {Err}", t.Key, ex.Message); counts[t.Key] = 0; }
        }
        ViewBag.Tables = tables;
        ViewBag.Counts = counts;
        return View();
    }

    // ──────────────────────────────────────────────────────────
    //  UPLOAD — Batch INSERT to Snowflake
    // ──────────────────────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    [RequestSizeLimit(500_000_000)]
    public async Task<IActionResult> Upload(IFormFile file, string table, string mode)
    {
        if (file == null || file.Length == 0)
        { TempData["ErrorMessage"] = "Please select a file."; return RedirectToAction(nameof(Index)); }

        var sfTableName = GetSnowflakeTableName(table);
        var sfColumns = GetSnowflakeColumns(table);
        var colTypes = GetColumnTypes(table);
        if (sfTableName == null || sfColumns == null)
        { TempData["ErrorMessage"] = "Unknown table."; return RedirectToAction(nameof(Index)); }

        var sw = Stopwatch.StartNew();
        try
        {
            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);
            stream.Position = 0;

            using var package = new ExcelPackage(stream);
            var dt = new DataTable();
            for (int i = 0; i < sfColumns.Length; i++)
                dt.Columns.Add(sfColumns[i], colTypes.Length > i ? colTypes[i] : typeof(string));

            foreach (var ws in package.Workbook.Worksheets)
            {
                if (ws.Dimension == null) continue;
                int startRow = 2; // row 1 = headers
                for (int row = startRow; row <= ws.Dimension.End.Row; row++)
                {
                    var dr = dt.NewRow();
                    bool hasData = false;
                    for (int col = 0; col < sfColumns.Length && col < ws.Dimension.End.Column; col++)
                    {
                        var val = ws.Cells[row, col + 1].Value;
                        if (val != null) hasData = true;
                        try
                        {
                            if (val == null) dr[col] = DBNull.Value;
                            else if (dt.Columns[col].DataType == typeof(decimal)) dr[col] = Convert.ToDecimal(val);
                            else if (dt.Columns[col].DataType == typeof(int)) dr[col] = Convert.ToInt32(val);
                            else if (dt.Columns[col].DataType == typeof(DateTime))
                            {
                                // Handle: DateTime, OLE double (42883), or string "28-05-2017" / "28-04-2026"
                                if (val is DateTime dtv) dr[col] = dtv;
                                else if (val is double dv) dr[col] = DateTime.FromOADate(dv);
                                else
                                {
                                    var s = val.ToString()?.Trim() ?? "";
                                    // Try dd-MM-yyyy first (Indian format), then other formats
                                    var fmts = new[] { "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "d-M-yyyy" };
                                    if (DateTime.TryParseExact(s, fmts, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsed))
                                        dr[col] = parsed;
                                    else if (DateTime.TryParse(s, out var parsed2))
                                        dr[col] = parsed2;
                                    else
                                        dr[col] = DBNull.Value;
                                }
                            }
                            else dr[col] = val.ToString()?.Trim() ?? "";
                        }
                        catch { dr[col] = DBNull.Value; }
                    }
                    if (hasData) dt.Rows.Add(dr);
                }
            }

            // ── Snowflake bulk load: Write CSV temp file → PUT → COPY INTO ──
            var tempCsv = Path.Combine(Path.GetTempPath(), $"ars_upload_{Guid.NewGuid():N}.csv");
            try
            {
                // Write DataTable to CSV
                await using (var csvWriter = new StreamWriter(tempCsv, false, Encoding.UTF8))
                {
                    // Header
                    await csvWriter.WriteLineAsync(string.Join(",", sfColumns));
                    // Data rows
                    foreach (DataRow dr in dt.Rows)
                    {
                        var line = new StringBuilder();
                        for (int ci = 0; ci < sfColumns.Length; ci++)
                        {
                            if (ci > 0) line.Append(',');
                            if (dr[ci] == DBNull.Value || dr[ci] == null)
                                line.Append("");
                            else
                            {
                                var raw = dr[ci];
                                string val;
                                // Force date formatting for DateTime columns
                                if (raw is DateTime dtVal)
                                    val = dtVal.ToString("yyyy-MM-dd");
                                else if (colTypes.Length > ci && colTypes[ci] == typeof(DateTime))
                                {
                                    if (raw is double dbl)
                                        val = DateTime.FromOADate(dbl).ToString("yyyy-MM-dd");
                                    else
                                    {
                                        var s = raw.ToString() ?? "";
                                        var fmts = new[] { "dd-MM-yyyy", "dd/MM/yyyy", "yyyy-MM-dd", "d-M-yyyy" };
                                        if (DateTime.TryParseExact(s, fmts, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var p))
                                            val = p.ToString("yyyy-MM-dd");
                                        else
                                            val = s;
                                    }
                                }
                                else
                                    val = raw.ToString() ?? "";

                                if (val.Contains(',') || val.Contains('"') || val.Contains('\n'))
                                    line.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                                else
                                    line.Append(val);
                            }
                        }
                        await csvWriter.WriteLineAsync(line.ToString());
                    }
                }

                await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
                await conn.OpenAsync();

                if (mode == "Replace")
                {
                    using var delCmd = conn.CreateCommand();
                    delCmd.CommandText = $"DELETE FROM {sfTableName}";
                    delCmd.CommandTimeout = 60;
                    await Task.Run(() => delCmd.ExecuteNonQuery());
                }

                // PUT file to Snowflake internal stage (SYNC only — Snowflake driver limitation)
                var stageName = $"@%{sfTableName}";
                using (var putCmd = conn.CreateCommand())
                {
                    putCmd.CommandText = $"PUT 'file://{tempCsv.Replace("\\", "/")}' {stageName} AUTO_COMPRESS=TRUE OVERWRITE=TRUE";
                    putCmd.CommandTimeout = 300;
                    await Task.Run(() => putCmd.ExecuteNonQuery());
                }

                // COPY INTO table from stage (SYNC)
                int inserted = 0, rowsParsed = 0, rowsLoaded = 0, errorsSeen = 0;
                string firstError = "";
                using (var copyCmd = conn.CreateCommand())
                {
                    var colList = string.Join(",", sfColumns);
                    copyCmd.CommandText = $@"COPY INTO {sfTableName} ({colList})
                        FROM {stageName}
                        FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='""' NULL_IF=('') EMPTY_FIELD_AS_NULL=TRUE)
                        PURGE = TRUE
                        ON_ERROR = 'CONTINUE'";
                    copyCmd.CommandTimeout = 300;
                    using var rdr = await Task.Run(() => copyCmd.ExecuteReader());
                    while (rdr.Read())
                    {
                        for (int i = 0; i < rdr.FieldCount; i++)
                        {
                            var colName = rdr.GetName(i).ToUpperInvariant();
                            if (colName == "ROWS_PARSED" && !rdr.IsDBNull(i))
                                rowsParsed += Convert.ToInt32(rdr.GetValue(i));
                            if (colName == "ROWS_LOADED" && !rdr.IsDBNull(i))
                                rowsLoaded += Convert.ToInt32(rdr.GetValue(i));
                            if (colName == "ERRORS_SEEN" && !rdr.IsDBNull(i))
                                errorsSeen += Convert.ToInt32(rdr.GetValue(i));
                            if (colName == "FIRST_ERROR" && !rdr.IsDBNull(i))
                                firstError = rdr.GetValue(i)?.ToString() ?? "";
                        }
                    }
                    inserted = rowsLoaded;
                }

                sw.Stop();
                if (errorsSeen > 0)
                    TempData["ErrorMessage"] = $"Upload partial: {rowsLoaded:N0} loaded, {errorsSeen} errors of {rowsParsed:N0} parsed in {sw.ElapsedMilliseconds}ms. First error: {firstError}";
                else
                    TempData["SuccessMessage"] = $"Uploaded {rowsLoaded:N0} rows to {table} via Snowflake COPY in {sw.ElapsedMilliseconds}ms ({mode} mode). Parsed: {rowsParsed:N0}";
            }
            finally
            {
                if (System.IO.File.Exists(tempCsv)) System.IO.File.Delete(tempCsv);
            }
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
    //  DOWNLOAD DATA (CSV) — from Snowflake
    // ──────────────────────────────────────────────────────────
    public async Task DownloadData(string table)
    {
        var sfTableName = GetSnowflakeTableName(table);
        var sfColumns = GetSnowflakeColumns(table);
        var headers = GetHeaders(table);
        if (sfTableName == null || headers == null) { Response.StatusCode = 400; return; }

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", $"attachment; filename={table}.csv");

        await using var conn = new SnowflakeDbConnection { ConnectionString = _sfConnStr };
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {string.Join(",", sfColumns!)} FROM {sfTableName} ORDER BY 1";
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
    //  TABLE MAPPINGS (Snowflake)
    // ──────────────────────────────────────────────────────────
    private static Dictionary<string, string> GetAllTables() => new()
    {
        ["ArsDisplayMaster"] = "Display Master (ST x MJ → DISP_Q, ACC_DENSITY)",
        ["ArsAutoSale"] = "MJ Auto Sale (ST x MJ → CM/NM Sale)",
        ["ArsArtAutoSale"] = "Article Auto Sale (ST x ART x CLR x MJ → Sale, ART_TAG)",
        ["ArsArtAging"] = "Article Aging (GEN_ART x CLR → AGING_DAYS)",
        ["ArsHoldDays"] = "Hold Days Master (ST x MJ → HOLD_DAYS)",
        ["ArsStMaster"] = "Store Master (ST_CD → RDC, Hub, Cover Days, Intra)",
    };

    private string? GetSnowflakeTableName(string table) => table switch
    {
        "ArsDisplayMaster" => "ARS_ST_MJ_DISPLAY_MASTER",
        "ArsAutoSale"      => "ARS_ST_MJ_AUTO_SALE",
        "ArsArtAutoSale"   => "ARS_ST_ART_AUTO_SALE",
        "ArsArtAging"      => "ARS_ART_AGING",
        "ArsHoldDays"      => "ARS_HOLD_DAYS_MASTER",
        "ArsStMaster"      => "ARS_ST_MASTER",
        _ => null
    };

    private string[]? GetSnowflakeColumns(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { "ST", "MJ", "ST_MJ_DISP_Q", "ACC_DENSITY" },
        "ArsAutoSale"      => new[] { "ST", "MJ", "CM_REM_DAYS", "NM_DAYS", "CM_AUTO_SALE_Q", "NM_AUTO_SALE_Q" },
        "ArsArtAutoSale"   => new[] { "ST", "GEN_ART", "CLR", "MJ", "CM_REM_DAYS", "NM_DAYS", "CM_AUTO_SALE_Q", "NM_AUTO_SALE_Q", "ART_TAG" },
        "ArsArtAging"      => new[] { "GEN_ART", "CLR", "AGING_DAYS" },
        "ArsHoldDays"      => new[] { "ST", "MJ", "HOLD_DAYS" },
        "ArsStMaster"      => new[] { "ST_CD", "ST_NM", "HUB_CD", "HUB_NM", "DIRECT_HUB", "TAGGED_RDC", "DH24_DC_TO_HUB_INTRA", "DH24_HUB_TO_ST_INTRA", "DW01_DC_TO_HUB_INTRA", "DW01_HUB_TO_ST_INTRA", "ST_OP_DT", "ST_STAT", "SALE_COVER_DAYS", "PRD_DAYS" },
        _ => null
    };

    private string[]? GetHeaders(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { "ST", "MJ", "ST_MJ_DISP_Q", "ACC_DENSITY" },
        "ArsAutoSale"      => new[] { "ST", "MJ", "CM_REM_DAYS", "NM_DAYS", "CM_AUTO_SALE_Q", "NM_AUTO_SALE_Q" },
        "ArsArtAutoSale"   => new[] { "ST", "GEN_ART", "CLR", "MJ", "CM_REM_DAYS", "NM_DAYS", "CM_AUTO_SALE_Q", "NM_AUTO_SALE_Q", "ART_TAG" },
        "ArsArtAging"      => new[] { "GEN_ART", "CLR", "AGING_DAYS" },
        "ArsHoldDays"      => new[] { "ST", "MJ", "HOLD_DAYS" },
        "ArsStMaster"      => new[] { "ST_CD", "ST_NM", "HUB_CD", "HUB_NM", "DIRECT_HUB", "TAGGED_RDC", "DH24_DC_TO_HUB_INTRA", "DH24_HUB_TO_ST_INTRA", "DW01_DC_TO_HUB_INTRA", "DW01_HUB_TO_ST_INTRA", "ST_OP_DT", "ST_STAT", "SALE_COVER_DAYS", "PRD_DAYS" },
        _ => null
    };

    private Type[] GetColumnTypes(string table) => table switch
    {
        "ArsDisplayMaster" => new[] { typeof(string), typeof(string), typeof(decimal), typeof(decimal) },
        "ArsAutoSale"      => new[] { typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal) },
        "ArsArtAutoSale"   => new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal), typeof(string) },
        "ArsArtAging"      => new[] { typeof(string), typeof(string), typeof(int) },
        "ArsHoldDays"      => new[] { typeof(string), typeof(string), typeof(decimal) },
        "ArsStMaster"      => new[] { typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(string), typeof(decimal), typeof(decimal), typeof(decimal), typeof(decimal), typeof(DateTime), typeof(string), typeof(decimal), typeof(decimal) },
        _ => Array.Empty<Type>()
    };

    private object[] GetSampleRow(string table) => table switch
    {
        "ArsDisplayMaster" => new object[] { "HB05", "M_JEANS", 25.5, 12.0 },
        "ArsAutoSale"      => new object[] { "HB05", "M_JEANS", 15, 30, 120.5, 95.3 },
        "ArsArtAutoSale"   => new object[] { "HB05", "1130140482", "BLK", "M_JEANS", 15, 30, 8.5, 6.2, "" },
        "ArsArtAging"      => new object[] { "1130140482", "BLK", 180 },
        "ArsHoldDays"      => new object[] { "HB05", "M_JEANS", 20 },
        "ArsStMaster"      => new object[] { "HB05", "PTN", "DB03", "PATNA", "DB03", "DH24", 2, 1, 3, 2, "2012-05-01", "OLD", 2, 3 },
        _ => Array.Empty<object>()
    };
}
