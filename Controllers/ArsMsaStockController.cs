using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using System.Text;

namespace TRANSFER_IN_PLAN.Controllers;

public class ArsMsaStockController : Controller
{
    private readonly string _connStr;
    public ArsMsaStockController(IConfiguration config) => _connStr = config.GetConnectionString("DataV2Database")!;

    public async Task<IActionResult> Index(string? storeCode, string? articleNumber, string? mcCode, string sortCol = "STORE_CODE", string sortDir = "ASC", int page = 1, int pageSize = 100)
    {
        var where = new StringBuilder("WHERE 1=1");
        var parms = new List<SqlParameter>();
        if (!string.IsNullOrEmpty(storeCode)) { where.Append(" AND STORE_CODE = @s"); parms.Add(new SqlParameter("@s", storeCode)); }
        if (!string.IsNullOrEmpty(articleNumber)) { where.Append(" AND Article_Number = @a"); parms.Add(new SqlParameter("@a", articleNumber)); }
        if (!string.IsNullOrEmpty(mcCode)) { where.Append(" AND MC_CODE = @mc"); parms.Add(new SqlParameter("@mc", mcCode)); }

        var validCols = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "STORE_CODE", "Article_Number", "QTY", "VAL", "MC_CODE", "MSA_Stock_Date" };
        if (!validCols.Contains(sortCol)) sortCol = "STORE_CODE";
        var dir = sortDir == "DESC" ? "DESC" : "ASC";

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();

        // Total count
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(1) FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            ViewBag.TotalCount = (int)(await cmd.ExecuteScalarAsync() ?? 0);
        }

        // KPIs
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $"SELECT COUNT(DISTINCT STORE_CODE), COUNT(DISTINCT Article_Number), COUNT(DISTINCT MC_CODE), ISNULL(SUM(QTY),0), ISNULL(SUM(VAL),0), MAX(MSA_Stock_Date) FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK) {where}";
            parms.ForEach(p => cmd.Parameters.Add(Clone(p)));
            await using var r = await cmd.ExecuteReaderAsync();
            if (await r.ReadAsync())
            {
                ViewBag.TotalStores = r.GetInt32(0); ViewBag.TotalArticles = r.GetInt32(1);
                ViewBag.TotalMcCodes = r.GetInt32(2);
                ViewBag.TotalQty = r.GetDecimal(3); ViewBag.TotalVal = r.GetDecimal(4);
                ViewBag.MsaDate = r.IsDBNull(5) ? null : r.GetDateTime(5).ToString("dd-MMM-yyyy");
            }
        }

        // Data
        int offset = (page - 1) * pageSize;
        var rows = new List<Dictionary<string, object?>>();
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = $@"SELECT Article_Number, STORE_CODE, LOCATION, LGNUM, LGTYP, LGPLA, MEINS, VAL, PPK_QTY, QTY, VEMNG2, MC_CODE, ATTYP, ERDAT, KUNNR, LPTYP, MSA_Stock_Date
                FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK) {where}
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
        ViewBag.StoreList = await GetDistinctAsync("STORE_CODE"); ViewBag.McCodeList = await GetDistinctAsync("MC_CODE");
        ViewBag.Rows = rows; ViewBag.Page = page; ViewBag.PageSize = pageSize;
        ViewBag.SortCol = sortCol; ViewBag.SortDir = dir;
        ViewBag.StoreCode = storeCode; ViewBag.ArticleNumber = articleNumber; ViewBag.McCode = mcCode;
        return View();
    }

    public async Task ExportCsv(string? storeCode, string? articleNumber, string? mcCode)
    {
        var where = new StringBuilder("WHERE 1=1");
        if (!string.IsNullOrEmpty(storeCode)) where.Append($" AND STORE_CODE = '{storeCode}'");
        if (!string.IsNullOrEmpty(articleNumber)) where.Append($" AND Article_Number = '{articleNumber}'");
        if (!string.IsNullOrEmpty(mcCode)) where.Append($" AND MC_CODE = '{mcCode}'");

        Response.ContentType = "text/csv";
        Response.Headers.Append("Content-Disposition", "attachment; filename=VIEW_ET_MSA_STOCK.csv");
        await using var writer = new StreamWriter(Response.Body, Encoding.UTF8, leaveOpen: true);
        await writer.WriteLineAsync("Article_Number,STORE_CODE,LOCATION,LGNUM,LGTYP,LGPLA,MEINS,VAL,PPK_QTY,QTY,VEMNG2,MC_CODE,ATTYP,ERDAT,KUNNR,LPTYP,MSA_Stock_Date");

        await using var conn = new SqlConnection(_connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT Article_Number,STORE_CODE,LOCATION,LGNUM,LGTYP,LGPLA,MEINS,VAL,PPK_QTY,QTY,VEMNG2,MC_CODE,ATTYP,ERDAT,KUNNR,LPTYP,MSA_Stock_Date FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK) {where} ORDER BY STORE_CODE,Article_Number";
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
        cmd.CommandText = $"SELECT DISTINCT [{col}] FROM dbo.VIEW_ET_MSA_STOCK WITH (NOLOCK) WHERE [{col}] IS NOT NULL ORDER BY 1";
        cmd.CommandTimeout = 60;
        await using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) list.Add(r.GetString(0));
        return list;
    }

    private static SqlParameter Clone(SqlParameter p) => new(p.ParameterName, p.SqlDbType) { Value = p.Value };
}
