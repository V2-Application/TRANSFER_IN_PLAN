using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsStockDataController : Controller
{
    private readonly string _connStr;
    public ArsStockDataController(IConfiguration config) => _connStr = config.GetConnectionString("DataV2Database")!;

    public async Task<IActionResult> Index(string? werks, string? matnr, string? lgort, string sortCol = "WERKS", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(werks)) { where.Append(" AND WERKS = @w"); parms.Add(new SqlParameter("@w", werks)); }
        if (!string.IsNullOrEmpty(matnr)) { where.Append(" AND MATNR = @m"); parms.Add(new SqlParameter("@m", matnr)); }
        if (!string.IsNullOrEmpty(lgort)) { where.Append(" AND LGORT = @l"); parms.Add(new SqlParameter("@l", lgort)); }

        // Whitelist sort columns
        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "WERKS", "MATNR", "LGORT", "LABST", "TRAME", "stock_Date" };
        if (!validCols.Contains(sortCol)) sortCol = "WERKS";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();

        // Total count
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1) FROM dbo.ET_STOCK_DATA WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            ViewBag.TotalCount = (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT WERKS), COUNT(DISTINCT MATNR), COUNT(DISTINCT LGORT), ISNULL(SUM(LABST),0), ISNULL(SUM(TRAME),0), MAX(stock_Date) FROM dbo.ET_STOCK_DATA WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalStores = r.GetInt32(0); ViewBag.TotalArticles = r.GetInt32(1);
                ViewBag.TotalLocations = r.GetInt32(2);
                ViewBag.TotalLabst = r.GetDecimal(3); ViewBag.TotalTrame = r.GetDecimal(4);
                ViewBag.StockDate = r.IsDBNull(5) ? null : r.GetDateTime(5).ToString("dd-MMM-yyyy");
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT MATNR, WERKS, LGORT, CHARG, MEINS, LABST, TRAME, LABST_DMBTR, TRAME_DMBTR, V_MENGE, V_DMBTR, stock_Date
                FROM dbo.ET_STOCK_DATA WITH (NOLOCK) {where}
                ORDER BY [{sortCol}] {dir}
                OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++)
                    row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
                rows.Add(row);
            }
        }

        // Dropdowns
        ViewBag.StoreList = await GetDistinctAsync("WERKS"); ViewBag.LgortList = await GetDistinctAsync("LGORT");
        ViewBag.Rows = rows; ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = dir;
        ViewBag.Werks = werks; ViewBag.Matnr = matnr; ViewBag.Lgort = lgort;
        return View();
    }

    public async Task ExportCsv(string? werks, string? matnr, string? lgort)
    {
        var where = new StringBuilder("WHERE 1=1");
        if (!string.IsNullOrEmpty(werks)) where.Append($" AND WERKS = '{werks}'");
        if (!string.IsNullOrEmpty(matnr)) where.Append($" AND MATNR = '{matnr}'");
        if (!string.IsNullOrEmpty(lgort)) where.Append($" AND LGORT = '{lgort}'");

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=ET_STOCK_DATA.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("MATNR,WERKS,LGORT,CHARG,MEINS,LABST,TRAME,LABST_DMBTR,TRAME_DMBTR,V_MENGE,V_DMBTR,stock_Date");

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MATNR,WERKS,LGORT,CHARG,MEINS,LABST,TRAME,LABST_DMBTR,TRAME_DMBTR,V_MENGE,V_DMBTR,stock_Date FROM dbo.ET_STOCK_DATA WITH (NOLOCK) {where} ORDER BY WERKS,MATNR";
        cmd.CommandTimeout = 300;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync())
        {
            var sb = new StringBuilder();
            for (int i = 0; i < r.FieldCount; i++)
            {
                if (i > 0) sb.Append(',');
                var val = r.IsDBNull(i) ? "" : r.GetValue(i).ToString() ?? "";
                if (val.Contains(',') || val.Contains('"')) sb.Append('"').Append(val.Replace("\"", "\"\"")).Append('"');
                else sb.Append(val);
            }
            await writer.WriteLineAsync(sb.ToString());
        }
        await writer.FlushAsync();
    }

    private async Task<List<string>> GetDistinctAsync(string col)
    {
        var list = new List<string>();
        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT DISTINCT [{col}] FROM dbo.ET_STOCK_DATA WITH (NOLOCK) WHERE [{col}] IS NOT NULL ORDER BY 1";
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) list.Add(r.GetString(0));
        return list;
    }

    private static SqlParameter Clone(SqlParameter p) => new(p.ParameterName, p.SqlDbType) { Value = p.Value };
}
