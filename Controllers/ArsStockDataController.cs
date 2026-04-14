using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsStockDataController : Controller
{
    private readonly string _connStr;
    public ArsStockDataController(IConfiguration config) => _connStr = config.GetConnectionString("Snowflake")!;

    public async Task<IActionResult> Index(string? werks, string? matnr, string? lgort, string sortCol = "WERKS", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder("WHERE STOCK_DATE = CURRENT_DATE() - 1");
        var parms = new List<(string Name, string Value)>();
        if (!string.IsNullOrEmpty(werks)) { where.Append(" AND WERKS = :w"); parms.Add((":w", werks)); }
        if (!string.IsNullOrEmpty(matnr)) { where.Append(" AND MATNR = :m"); parms.Add((":m", matnr)); }
        if (!string.IsNullOrEmpty(lgort)) { where.Append(" AND LGORT = :l"); parms.Add((":l", lgort)); }

        // Whitelist sort columns
        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "WERKS", "MATNR", "LGORT", "LABST", "TRAME", "STOCK_DATE" };
        if (!validCols.Contains(sortCol)) sortCol = "WERKS";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        await using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _connStr;
        await conn.OpenAsync();

        // Total count
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1) FROM ET_STOCK_DATA {where}";
            AddParams(cmd, parms);
            var result = await cmd.ExecuteScalarAsync();
            ViewBag.TotalCount = Convert.ToInt32(result ?? 0);
        }

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT WERKS), COUNT(DISTINCT MATNR), COUNT(DISTINCT LGORT), NVL(SUM(LABST),0), NVL(SUM(TRAME),0), MAX(STOCK_DATE) FROM ET_STOCK_DATA {where}";
            AddParams(cmd, parms);
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalStores = Convert.ToInt32(r.GetValue(0));
                ViewBag.TotalArticles = Convert.ToInt32(r.GetValue(1));
                ViewBag.TotalLocations = Convert.ToInt32(r.GetValue(2));
                ViewBag.TotalLabst = Convert.ToDecimal(r.GetValue(3));
                ViewBag.TotalTrame = Convert.ToDecimal(r.GetValue(4));
                ViewBag.StockDate = r.IsDBNull(5) ? null : Convert.ToDateTime(r.GetValue(5)).ToString("dd-MMM-yyyy");
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT MATNR, WERKS, LGORT, CHARG, MEINS, LABST, TRAME, LABST_DMBTR, TRAME_DMBTR, V_MENGE, V_DMBTR, STOCK_DATE
                FROM ET_STOCK_DATA {where}
                ORDER BY {sortCol} {dir}
                LIMIT {pageSize} OFFSET {offset}";
            AddParams(cmd, parms);
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
        ViewBag.StoreList = await GetDistinctAsync("WERKS");
        ViewBag.LgortList = await GetDistinctAsync("LGORT");
        ViewBag.Rows = rows; ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = dir;
        ViewBag.Werks = werks; ViewBag.Matnr = matnr; ViewBag.Lgort = lgort;
        return View();
    }

    public async Task ExportCsv(string? werks, string? matnr, string? lgort)
    {
        var where = new StringBuilder("WHERE STOCK_DATE = CURRENT_DATE() - 1");
        var parms = new List<(string Name, string Value)>();
        if (!string.IsNullOrEmpty(werks)) { where.Append(" AND WERKS = :w"); parms.Add((":w", werks)); }
        if (!string.IsNullOrEmpty(matnr)) { where.Append(" AND MATNR = :m"); parms.Add((":m", matnr)); }
        if (!string.IsNullOrEmpty(lgort)) { where.Append(" AND LGORT = :l"); parms.Add((":l", lgort)); }

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=ET_STOCK_DATA.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("MATNR,WERKS,LGORT,CHARG,MEINS,LABST,TRAME,LABST_DMBTR,TRAME_DMBTR,V_MENGE,V_DMBTR,STOCK_DATE");

        await using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _connStr;
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT MATNR,WERKS,LGORT,CHARG,MEINS,LABST,TRAME,LABST_DMBTR,TRAME_DMBTR,V_MENGE,V_DMBTR,STOCK_DATE FROM ET_STOCK_DATA {where} ORDER BY WERKS,MATNR";
        cmd.CommandTimeout = 300;
        AddParams(cmd, parms);
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
        await using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = _connStr;
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT DISTINCT {col} FROM ET_STOCK_DATA WHERE STOCK_DATE = CURRENT_DATE() - 1 AND {col} IS NOT NULL ORDER BY 1";
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) list.Add(r.GetString(0));
        return list;
    }

    private static void AddParams(IDbCommand cmd, List<(string Name, string Value)> parms)
    {
        foreach (var (name, value) in parms)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            p.Value = value;
            p.DbType = DbType.String;
            cmd.Parameters.Add(p);
        }
    }
}
