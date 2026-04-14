using Microsoft.AspNetCore.Mvc;
using Snowflake.Data.Client;
using System.Data;
using System.Text;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsMsaStockController : Controller
{
    private readonly string _connStr;
    public ArsMsaStockController(IConfiguration config) => _connStr = config.GetConnectionString("Snowflake")!;

    private SnowflakeDbConnection OpenConn()
    {
        var conn = new SnowflakeDbConnection { ConnectionString = _connStr };
        conn.Open();
        return conn;
    }

    public async Task<IActionResult> Index(string? storeCode, string? articleNumber, string? mcCode, string sortCol = "STORE_CODE", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder(" AND MSA_STOCK_DATE = CURRENT_DATE() - 1");
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 0;

        if (!string.IsNullOrEmpty(storeCode))
        {
            where.Append(" AND STORE_CODE = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = storeCode, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(articleNumber))
        {
            where.Append(" AND ARTICLE_NUMBER = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = articleNumber, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(mcCode))
        {
            where.Append(" AND MC_CODE = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = mcCode, DbType = DbType.String });
        }

        string filter = " WHERE " + where.ToString()[5..];

        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "STORE_CODE", "ARTICLE_NUMBER", "QTY", "VAL", "MC_CODE", "MSA_STOCK_DATE" };
        if (!validCols.Contains(sortCol)) sortCol = "STORE_CODE";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        using var conn = OpenConn();

        // Total count
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1) FROM ET_MSA_STOCK{filter}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            ViewBag.TotalCount = Convert.ToInt32(await Task.Run(() => cmd.ExecuteScalar()));
        }

        // KPIs
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT ARTICLE_NUMBER), COUNT(DISTINCT MC_CODE), NVL(SUM(QTY),0), NVL(SUM(VAL),0), MAX(MSA_STOCK_DATE) FROM ET_MSA_STOCK{filter}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            using var r = await Task.Run(() => cmd.ExecuteReader());
            if (r.Read())
            {
                ViewBag.TotalStores = Convert.ToInt32(r.GetValue(0));
                ViewBag.TotalArticles = Convert.ToInt32(r.GetValue(1));
                ViewBag.TotalMcCodes = Convert.ToInt32(r.GetValue(2));
                ViewBag.TotalQty = Convert.ToDecimal(r.GetValue(3));
                ViewBag.TotalVal = Convert.ToDecimal(r.GetValue(4));
                ViewBag.MsaDate = r.IsDBNull(5) ? null : Convert.ToDateTime(r.GetValue(5)).ToString("dd-MMM-yyyy");
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT ARTICLE_NUMBER, STORE_CODE, LOCATION, LGNUM, LGTYP, LGPLA, MEINS, VAL, PPK_QTY, QTY, VEMNG2, MC_CODE, ATTYP, ERDAT, KUNNR, LPTYP, MSA_STOCK_DATE
                FROM ET_MSA_STOCK{filter}
                ORDER BY {sortCol} {dir}
                LIMIT {pageSize} OFFSET {offset}";
            foreach (var p in parms) cmd.Parameters.Add(CloneParam(p));
            using var r = await Task.Run(() => cmd.ExecuteReader());
            while (r.Read())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < r.FieldCount; i++)
                    row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
                rows.Add(row);
            }
        }

        // Dropdowns
        ViewBag.StoreList = await GetDistinctAsync("STORE_CODE");
        ViewBag.McCodeList = await GetDistinctAsync("MC_CODE");
        ViewBag.Rows = rows; ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = dir;
        ViewBag.StoreCode = storeCode; ViewBag.ArticleNumber = articleNumber; ViewBag.McCode = mcCode;
        return View();
    }

    public async Task ExportCsv(string? storeCode, string? articleNumber, string? mcCode)
    {
        var where = new StringBuilder(" AND MSA_STOCK_DATE = CURRENT_DATE() - 1");
        var parms = new List<SnowflakeDbParameter>();
        int pIdx = 0;

        if (!string.IsNullOrEmpty(storeCode))
        {
            where.Append(" AND STORE_CODE = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = storeCode, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(articleNumber))
        {
            where.Append(" AND ARTICLE_NUMBER = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = articleNumber, DbType = DbType.String });
        }
        if (!string.IsNullOrEmpty(mcCode))
        {
            where.Append(" AND MC_CODE = ?");
            parms.Add(new SnowflakeDbParameter { ParameterName = (++pIdx).ToString(), Value = mcCode, DbType = DbType.String });
        }

        string filter = " WHERE " + where.ToString()[5..];

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=ET_MSA_STOCK.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("ARTICLE_NUMBER,STORE_CODE,LOCATION,LGNUM,LGTYP,LGPLA,MEINS,VAL,PPK_QTY,QTY,VEMNG2,MC_CODE,ATTYP,ERDAT,KUNNR,LPTYP,MSA_STOCK_DATE");

        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT ARTICLE_NUMBER,STORE_CODE,LOCATION,LGNUM,LGTYP,LGPLA,MEINS,VAL,PPK_QTY,QTY,VEMNG2,MC_CODE,ATTYP,ERDAT,KUNNR,LPTYP,MSA_STOCK_DATE FROM ET_MSA_STOCK{filter} ORDER BY STORE_CODE,ARTICLE_NUMBER";
        foreach (var p in parms) cmd.Parameters.Add(p);
        using var r = await Task.Run(() => cmd.ExecuteReader());
        while (r.Read())
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
        using var conn = OpenConn();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT DISTINCT {col} FROM ET_MSA_STOCK WHERE MSA_STOCK_DATE = CURRENT_DATE() - 1 AND {col} IS NOT NULL ORDER BY 1";
        using var r = await Task.Run(() => cmd.ExecuteReader());
        while (r.Read()) list.Add(r.GetString(0));
        return list;
    }

    private static SnowflakeDbParameter CloneParam(SnowflakeDbParameter src) =>
        new() { ParameterName = src.ParameterName, Value = src.Value, DbType = src.DbType };
}
