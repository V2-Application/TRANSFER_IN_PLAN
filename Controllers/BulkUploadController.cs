using Microsoft.AspNetCore.Mvc;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using Snowflake.Data.Client;
using System.Data;
using System.Diagnostics;
using System.Text;
using SysColor = System.Drawing.Color;
using TRANSFER_IN_PLAN.Helpers;

namespace TRANSFER_IN_PLAN.Controllers;

public class BulkUploadController : Controller
{
    private readonly string _sfConnStr;
    private readonly ILogger<BulkUploadController> _logger;

    public BulkUploadController(IConfiguration config, ILogger<BulkUploadController> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
        ExcelPackage.LicenseContext = LicenseContext.NonCommercial;
    }

    // ──────────────────────────────────────────────────────────
    //  INDEX
    // ──────────────────────────────────────────────────────────
    public IActionResult Index() => View();

    // ──────────────────────────────────────────────────────────
    //  DOWNLOAD SAMPLE EXCEL
    // ──────────────────────────────────────────────────────────
    [HttpGet]
    public IActionResult DownloadSample(string table)
    {
        var def = PlanningTableConfig.Get(table);
        if (def == null) return BadRequest("Unknown table.");

        using var package = new ExcelPackage();
        var ws = package.Workbook.Worksheets.Add("Sample");
        for (int i = 0; i < def.Headers.Length; i++)
        {
            ws.Cells[1, i + 1].Value = def.Headers[i];
            ws.Cells[1, i + 1].Style.Font.Bold = true;
            ws.Cells[1, i + 1].Style.Fill.PatternType = ExcelFillStyle.Solid;
            ws.Cells[1, i + 1].Style.Fill.BackgroundColor.SetColor(SysColor.FromArgb(68, 114, 196));
            ws.Cells[1, i + 1].Style.Font.Color.SetColor(SysColor.White);
        }
        for (int i = 0; i < def.SampleRow.Length; i++)
            ws.Cells[2, i + 1].Value = def.SampleRow[i];
        ws.Cells[ws.Dimension.Address].AutoFitColumns();

        return File(package.GetAsByteArray(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            $"Sample_{table}.xlsx");
    }

    // ──────────────────────────────────────────────────────────
    //  UPLOAD — Snowflake PUT/COPY INTO
    // ──────────────────────────────────────────────────────────
    [HttpPost, ValidateAntiForgeryToken]
    [RequestSizeLimit(500_000_000)]
    public async Task<IActionResult> Upload(IFormFile file, string table, string mode)
    {
        if (file == null || file.Length == 0)
        { TempData["ErrorMessage"] = "Please select a file."; return RedirectToAction(nameof(Index)); }

        var def = PlanningTableConfig.Get(table);
        if (def == null)
        { TempData["ErrorMessage"] = "Unknown table."; return RedirectToAction(nameof(Index)); }

        var sw = Stopwatch.StartNew();
        try
        {
            // ── Read Excel into DataTable ──
            using var stream = new MemoryStream();
            await file.CopyToAsync(stream);
            stream.Position = 0;

            using var package = new ExcelPackage(stream);
            var dt = new DataTable();
            for (int i = 0; i < def.Columns.Length; i++)
                dt.Columns.Add(def.Columns[i], i < def.Types.Length ? def.Types[i] : typeof(string));

            foreach (var ws in package.Workbook.Worksheets)
            {
                if (ws.Dimension == null) continue;
                int dataCols = Math.Min(def.Columns.Length, ws.Dimension.End.Column);
                int totalRows = ws.Dimension.End.Row;
                if (totalRows < 2) continue;

                // Bulk read: entire range into 2D array (10-50x faster than cell-by-cell)
                var range = ws.Cells[2, 1, totalRows, dataCols];
                var values = range.Value as object[,];
                if (values == null) continue;

                dt.BeginLoadData();
                for (int row = 0; row <= values.GetUpperBound(0); row++)
                {
                    if (values[row, 0] == null) continue; // skip empty rows
                    var dr = dt.NewRow();
                    for (int col = 0; col < dataCols && col < def.Columns.Length; col++)
                    {
                        var val = values[row, col];
                        try
                        {
                            if (val == null) dr[col] = DBNull.Value;
                            else if (dt.Columns[col].DataType == typeof(decimal)) dr[col] = Convert.ToDecimal(val);
                            else if (dt.Columns[col].DataType == typeof(int)) dr[col] = Convert.ToInt32(val);
                            else if (dt.Columns[col].DataType == typeof(DateTime))
                            {
                                if (val is DateTime dtv) dr[col] = dtv;
                                else if (val is double dv) dr[col] = DateTime.FromOADate(dv);
                                else
                                {
                                    var s = val.ToString()?.Trim() ?? "";
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
                    dt.Rows.Add(dr);
                }
                dt.EndLoadData();
            }

            // ── Write CSV temp file → PUT → COPY INTO Snowflake ──
            var tempCsv = Path.Combine(Path.GetTempPath(), $"pln_upload_{Guid.NewGuid():N}.csv");
            try
            {
                await using (var csvWriter = new StreamWriter(tempCsv, false, Encoding.UTF8))
                {
                    await csvWriter.WriteLineAsync(string.Join(",", def.Columns));
                    foreach (DataRow dr in dt.Rows)
                    {
                        var line = new StringBuilder();
                        for (int ci = 0; ci < def.Columns.Length; ci++)
                        {
                            if (ci > 0) line.Append(',');
                            if (dr[ci] == DBNull.Value || dr[ci] == null)
                                line.Append("");
                            else
                            {
                                var raw = dr[ci];
                                string val;
                                if (raw is DateTime dtVal) val = dtVal.ToString("yyyy-MM-dd");
                                else if (ci < def.Types.Length && def.Types[ci] == typeof(DateTime))
                                {
                                    if (raw is double dbl) val = DateTime.FromOADate(dbl).ToString("yyyy-MM-dd");
                                    else val = raw.ToString() ?? "";
                                }
                                else val = raw.ToString() ?? "";

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
                    delCmd.CommandText = $"DELETE FROM {def.SnowflakeTable}";
                    delCmd.CommandTimeout = 60;
                    await Task.Run(() => delCmd.ExecuteNonQuery());
                }

                // PUT file to table stage
                var stageName = $"@%{def.SnowflakeTable}";
                using (var putCmd = conn.CreateCommand())
                {
                    putCmd.CommandText = $"PUT 'file://{tempCsv.Replace("\\", "/")}' {stageName} AUTO_COMPRESS=TRUE OVERWRITE=TRUE";
                    putCmd.CommandTimeout = 300;
                    await Task.Run(() => putCmd.ExecuteNonQuery());
                }

                // COPY INTO
                int rowsLoaded = 0, errorsSeen = 0;
                string firstError = "";
                using (var copyCmd = conn.CreateCommand())
                {
                    var colList = string.Join(",", def.Columns);
                    copyCmd.CommandText = $@"COPY INTO {def.SnowflakeTable} ({colList})
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
                            if (colName == "ROWS_LOADED" && !rdr.IsDBNull(i))
                                rowsLoaded += Convert.ToInt32(rdr.GetValue(i));
                            if (colName == "ERRORS_SEEN" && !rdr.IsDBNull(i))
                                errorsSeen += Convert.ToInt32(rdr.GetValue(i));
                            if (colName == "FIRST_ERROR" && !rdr.IsDBNull(i) && string.IsNullOrEmpty(firstError))
                                firstError = rdr.GetValue(i)?.ToString() ?? "";
                        }
                    }
                }

                sw.Stop();
                var msg = $"Uploaded {rowsLoaded:N0} rows to {table} in {sw.ElapsedMilliseconds}ms ({mode} mode).";
                if (errorsSeen > 0) msg += $" {errorsSeen} errors. First: {firstError}";
                TempData[errorsSeen > 0 ? "ErrorMessage" : "SuccessMessage"] = msg;
            }
            finally
            {
                try { System.IO.File.Delete(tempCsv); } catch { }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "BulkUpload failed for {Table}", table);
            TempData["ErrorMessage"] = $"Upload failed: {ex.Message}";
        }
        return RedirectToAction(nameof(Index));
    }

    // ──────────────────────────────────────────────────────────
    //  DOWNLOAD DATA (CSV) — from Snowflake
    // ──────────────────────────────────────────────────────────
    [HttpGet]
    public async Task<IActionResult> DownloadData(string table)
    {
        var def = PlanningTableConfig.Get(table);
        if (def == null) return BadRequest("Unknown table.");

        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        var colList = string.Join(",", def.Columns);
        var sb = new StringBuilder();
        sb.AppendLine(string.Join(",", def.Headers.Select(h => Q(h))));

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT {colList} FROM {def.SnowflakeTable} ORDER BY 1";
        cmd.CommandTimeout = 300;
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            for (int i = 0; i < def.Columns.Length; i++)
            {
                if (i > 0) sb.Append(',');
                if (reader.IsDBNull(i))
                {
                    sb.Append(i < def.Types.Length && def.Types[i] == typeof(string) ? "NA" : "0");
                }
                else if (i < def.Types.Length && def.Types[i] == typeof(DateTime))
                {
                    sb.Append(Convert.ToDateTime(reader.GetValue(i)).ToString("yyyy-MM-dd"));
                }
                else
                {
                    sb.Append(Q(reader.GetValue(i).ToString() ?? ""));
                }
            }
            sb.AppendLine();
        }

        return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", $"{table}_Data.csv");
    }

    // ──────────────────────────────────────────────────────────
    //  TABLE COUNTS (JSON for UI auto-refresh)
    // ──────────────────────────────────────────────────────────
    [HttpGet]
    public async Task<IActionResult> TableCounts()
    {
        var counts = new Dictionary<string, int>();
        await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
        foreach (var (key, def) in PlanningTableConfig.Tables)
        {
            try
            {
                counts[key] = await SnowflakeCrudHelper.CountAsync(conn, def.SnowflakeTable);
            }
            catch { counts[key] = 0; }
        }
        return Json(counts);
    }

    private static string Q(string? s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.Contains(',') || s.Contains('"') || s.Contains('\n'))
            return "\"" + s.Replace("\"", "\"\"") + "\"";
        return s;
    }
}
