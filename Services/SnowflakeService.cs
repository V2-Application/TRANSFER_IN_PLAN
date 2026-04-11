using System.Data;
using Microsoft.Data.SqlClient;
using Snowflake.Data.Client;

namespace TRANSFER_IN_PLAN.Services;

public class SnowflakeService
{
    private readonly IConfiguration _config;
    private readonly ILogger<SnowflakeService> _logger;

    public SnowflakeService(IConfiguration config, ILogger<SnowflakeService> logger)
    {
        _config = config;
        _logger = logger;
    }

    private string SnowflakeConnStr => _config.GetConnectionString("Snowflake")!;
    private string SqlConnStr => _config.GetConnectionString("PlanningDatabase")!;

    // ───────────────────────────────────────────────────────
    //  1. Fetch actual sales (Store x MajCat x Month)
    // ───────────────────────────────────────────────────────
    public async Task<int> FetchSaleActualsAsync(Action<string>? onStatus = null)
    {
        onStatus?.Invoke("Querying Snowflake for sale actuals...");

        var dt = new DataTable();
        dt.Columns.Add("STORE_CODE", typeof(string));
        dt.Columns.Add("MAJOR_CATEGORY", typeof(string));
        dt.Columns.Add("DIVISION", typeof(string));
        dt.Columns.Add("SUBDIVISION", typeof(string));
        dt.Columns.Add("SEGMENT", typeof(string));
        dt.Columns.Add("SALE_MONTH", typeof(DateTime));
        dt.Columns.Add("SALE_QTY", typeof(decimal));
        dt.Columns.Add("SALE_VAL", typeof(decimal));
        dt.Columns.Add("GM_VAL", typeof(decimal));
        dt.Columns.Add("LYSP_SALE_QTY", typeof(decimal));
        dt.Columns.Add("LYSP_SALE_VAL", typeof(decimal));
        dt.Columns.Add("LYSP_GM_VAL", typeof(decimal));
        dt.Columns.Add("FETCHED_AT", typeof(DateTime));

        const string sql = @"
            SELECT f.STORE_CODE,
                   a.MAJOR_CATEGORY, a.DIVISION, a.SUBDIVISION, a.SEGMENT,
                   DATE_TRUNC('MONTH', f.WEEK_START) AS SALE_MONTH,
                   SUM(f.SALE_QTY)      AS SALE_QTY,
                   SUM(f.SALE_VAL)      AS SALE_VAL,
                   SUM(f.GM_VAL)        AS GM_VAL,
                   SUM(f.LYSP_SALE_QTY) AS LYSP_SALE_QTY,
                   SUM(f.LYSP_SALE_VAL) AS LYSP_SALE_VAL,
                   SUM(f.LYSP_GM_VAL)   AS LYSP_GM_VAL
            FROM FACT_SALE_GENCOLOR f
            JOIN DIM_ARTICLE_GENCOLOR a ON a.GENCOLOR_KEY = f.GENCOLOR_KEY AND a.RN = 1
            GROUP BY f.STORE_CODE, a.MAJOR_CATEGORY, a.DIVISION, a.SUBDIVISION, a.SEGMENT,
                     DATE_TRUNC('MONTH', f.WEEK_START)
            ORDER BY SALE_VAL DESC";

        var now = DateTime.Now;
        await ReadSnowflakeAsync(sql, reader =>
        {
            dt.Rows.Add(
                reader.GetString(0),                                    // STORE_CODE
                reader.GetString(1),                                    // MAJOR_CATEGORY
                reader.IsDBNull(2) ? null : reader.GetString(2),        // DIVISION
                reader.IsDBNull(3) ? null : reader.GetString(3),        // SUBDIVISION
                reader.IsDBNull(4) ? null : reader.GetString(4),        // SEGMENT
                reader.GetDateTime(5),                                  // SALE_MONTH
                ToDecimal(reader, 6),
                ToDecimal(reader, 7),
                ToDecimal(reader, 8),
                ToDecimal(reader, 9),
                ToDecimal(reader, 10),
                ToDecimal(reader, 11),
                now
            );
        });

        onStatus?.Invoke($"Fetched {dt.Rows.Count:N0} sale actual rows. Staging...");
        await TruncateAndBulkInsertAsync("STG_SF_SALE_ACTUAL", dt);
        _logger.LogInformation("SnowflakeService: STG_SF_SALE_ACTUAL loaded {Rows} rows", dt.Rows.Count);
        return dt.Rows.Count;
    }

    // ───────────────────────────────────────────────────────
    //  2. Fetch ML demand forecasts
    // ───────────────────────────────────────────────────────
    public async Task<int> FetchDemandForecastsAsync(Action<string>? onStatus = null)
    {
        onStatus?.Invoke("Querying Snowflake for ML forecasts...");

        var dt = new DataTable();
        dt.Columns.Add("TARGET_MONTH", typeof(DateTime));
        dt.Columns.Add("STORE_CODE", typeof(string));
        dt.Columns.Add("MAJOR_CATEGORY", typeof(string));
        dt.Columns.Add("STORE_NAME", typeof(string));
        dt.Columns.Add("ENSEMBLE_FORECAST", typeof(decimal));
        dt.Columns.Add("STORE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("STORE_FORECAST", typeof(decimal));
        dt.Columns.Add("FORECAST_LOW", typeof(decimal));
        dt.Columns.Add("FORECAST_HIGH", typeof(decimal));
        dt.Columns.Add("BEST_METHOD", typeof(string));
        dt.Columns.Add("WEIGHTED_MAPE", typeof(decimal));
        dt.Columns.Add("DATA_MONTHS_USED", typeof(int));
        dt.Columns.Add("FETCHED_AT", typeof(DateTime));

        const string sql = @"
            SELECT TARGET_MONTH, STORE_CODE, MAJOR_CATEGORY, STORE_NAME,
                   ENSEMBLE_FORECAST_NATIONAL, STORE_CONT_PCT, STORE_FORECAST,
                   FORECAST_LOW, FORECAST_HIGH, BEST_METHOD,
                   WEIGHTED_MAPE, DATA_MONTHS_USED
            FROM FACT_DEMAND_FORECAST
            ORDER BY TARGET_MONTH, MAJOR_CATEGORY, STORE_CODE";

        var now = DateTime.Now;
        await ReadSnowflakeAsync(sql, reader =>
        {
            dt.Rows.Add(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                ToDecimal(reader, 4),
                ToDecimal(reader, 5),
                ToDecimal(reader, 6),
                ToDecimal(reader, 7),
                ToDecimal(reader, 8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                ToDecimal(reader, 10),
                reader.IsDBNull(11) ? 0 : reader.GetInt32(11),
                now
            );
        });

        onStatus?.Invoke($"Fetched {dt.Rows.Count:N0} forecast rows. Staging...");
        await TruncateAndBulkInsertAsync("STG_SF_DEMAND_FORECAST", dt);
        _logger.LogInformation("SnowflakeService: STG_SF_DEMAND_FORECAST loaded {Rows} rows", dt.Rows.Count);
        return dt.Rows.Count;
    }

    // ───────────────────────────────────────────────────────
    //  3. Fetch store contribution percentages
    // ───────────────────────────────────────────────────────
    public async Task<int> FetchContributionPctsAsync(Action<string>? onStatus = null)
    {
        onStatus?.Invoke("Querying Snowflake for contribution %...");

        var dt = new DataTable();
        dt.Columns.Add("STORE_CODE", typeof(string));
        dt.Columns.Add("MAJOR_CATEGORY", typeof(string));
        dt.Columns.Add("DIVISION", typeof(string));
        dt.Columns.Add("SEGMENT", typeof(string));
        dt.Columns.Add("STATE", typeof(string));
        dt.Columns.Add("ZONE", typeof(string));
        dt.Columns.Add("REGION", typeof(string));
        dt.Columns.Add("L7D_SALE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("MTD_SALE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("LM_SALE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("L3M_SALE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("YTD_SALE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("CL_STK_Q", typeof(decimal));
        dt.Columns.Add("CL_STK_V", typeof(decimal));
        dt.Columns.Add("YTD_GM_PCT", typeof(decimal));
        dt.Columns.Add("YTD_SALES_PSF", typeof(decimal));
        dt.Columns.Add("FETCHED_AT", typeof(DateTime));

        const string sql = @"
            SELECT STORE_CODE, MAJOR_CATEGORY, DIVISION, SEGMENT,
                   STATE, ZONE, REGION,
                   L7D_SALE_CONT_PCT, MTD_SALE_CONT_PCT, LM_SALE_CONT_PCT,
                   L3M_SALE_CONT_PCT, YTD_SALE_CONT_PCT,
                   CL_STK_Q, CL_STK_V, YTD_GM_PCT, YTD_SALES_PSF
            FROM FACT_CONT_PCT_STORE
            ORDER BY MAJOR_CATEGORY, STORE_CODE";

        var now = DateTime.Now;
        await ReadSnowflakeAsync(sql, reader =>
        {
            dt.Rows.Add(
                reader.GetString(0), reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                ToDecimal(reader, 7), ToDecimal(reader, 8), ToDecimal(reader, 9),
                ToDecimal(reader, 10), ToDecimal(reader, 11),
                ToDecimal(reader, 12), ToDecimal(reader, 13),
                ToDecimal(reader, 14), ToDecimal(reader, 15),
                now
            );
        });

        onStatus?.Invoke($"Fetched {dt.Rows.Count:N0} contribution rows. Staging...");
        await TruncateAndBulkInsertAsync("STG_SF_CONT_PCT", dt);
        _logger.LogInformation("SnowflakeService: STG_SF_CONT_PCT loaded {Rows} rows", dt.Rows.Count);
        return dt.Rows.Count;
    }

    // ───────────────────────────────────────────────────────
    //  4. Fetch DIM_STORE
    // ───────────────────────────────────────────────────────
    public async Task<int> FetchDimStoreAsync(Action<string>? onStatus = null)
    {
        onStatus?.Invoke("Querying Snowflake for store dimension...");

        var dt = new DataTable();
        dt.Columns.Add("STORE_CODE", typeof(string));
        dt.Columns.Add("STORE_NAME", typeof(string));
        dt.Columns.Add("STATE", typeof(string));
        dt.Columns.Add("STORE_SIZE_SQFT", typeof(decimal));
        dt.Columns.Add("SIZE_CATEGORY", typeof(string));
        dt.Columns.Add("COHORT", typeof(string));
        dt.Columns.Add("OLD_NEW", typeof(string));
        dt.Columns.Add("ZONE", typeof(string));
        dt.Columns.Add("REGION", typeof(string));
        dt.Columns.Add("IS_ACTIVE", typeof(bool));
        dt.Columns.Add("FETCHED_AT", typeof(DateTime));

        const string sql = @"
            SELECT STORE_CODE, STORE_NAME, STATE, STORE_SIZE_SQFT,
                   SIZE_CATEGORY, COHORT, OLD_NEW, ZONE, REGION,
                   CASE WHEN IS_ACTIVE = 1 THEN TRUE ELSE FALSE END AS IS_ACTIVE
            FROM DIM_STORE
            ORDER BY STORE_CODE";

        var now = DateTime.Now;
        await ReadSnowflakeAsync(sql, reader =>
        {
            dt.Rows.Add(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                ToDecimal(reader, 3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                !reader.IsDBNull(9) && reader.GetBoolean(9),
                now
            );
        });

        onStatus?.Invoke($"Fetched {dt.Rows.Count:N0} stores. Staging...");
        await TruncateAndBulkInsertAsync("STG_SF_DIM_STORE", dt);
        _logger.LogInformation("SnowflakeService: STG_SF_DIM_STORE loaded {Rows} rows", dt.Rows.Count);
        return dt.Rows.Count;
    }

    // ───────────────────────────────────────────────────────
    //  5. Fetch article hierarchy (MajCat-level)
    // ───────────────────────────────────────────────────────
    public async Task<int> FetchDimArticleAsync(Action<string>? onStatus = null)
    {
        onStatus?.Invoke("Querying Snowflake for article hierarchy...");

        var dt = new DataTable();
        dt.Columns.Add("MAJOR_CATEGORY", typeof(string));
        dt.Columns.Add("DIVISION", typeof(string));
        dt.Columns.Add("SUBDIVISION", typeof(string));
        dt.Columns.Add("SEGMENT", typeof(string));
        dt.Columns.Add("RNG_SEG", typeof(string));
        dt.Columns.Add("AVG_MRP", typeof(decimal));
        dt.Columns.Add("ARTICLE_COUNT", typeof(int));
        dt.Columns.Add("FETCHED_AT", typeof(DateTime));

        const string sql = @"
            SELECT MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT, RNG_SEG,
                   ROUND(AVG(MRP), 2) AS AVG_MRP,
                   COUNT(DISTINCT GENCOLOR_KEY) AS ARTICLE_COUNT
            FROM DIM_ARTICLE_GENCOLOR
            WHERE RN = 1 AND MAJOR_CATEGORY IS NOT NULL
            GROUP BY MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT, RNG_SEG
            ORDER BY MAJOR_CATEGORY";

        var now = DateTime.Now;
        await ReadSnowflakeAsync(sql, reader =>
        {
            dt.Rows.Add(
                reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                ToDecimal(reader, 5),
                reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)),
                now
            );
        });

        onStatus?.Invoke($"Fetched {dt.Rows.Count:N0} category rows. Staging...");
        await TruncateAndBulkInsertAsync("STG_SF_DIM_ARTICLE", dt);
        _logger.LogInformation("SnowflakeService: STG_SF_DIM_ARTICLE loaded {Rows} rows", dt.Rows.Count);
        return dt.Rows.Count;
    }

    // ───────────────────────────────────────────────────────
    //  Get staging table status
    // ───────────────────────────────────────────────────────
    public async Task<List<Models.StagingStatusItem>> GetStagingStatusAsync()
    {
        var tables = new[]
        {
            ("STG_SF_SALE_ACTUAL",      "Sale Actuals"),
            ("STG_SF_DEMAND_FORECAST",  "ML Forecasts"),
            ("STG_SF_CONT_PCT",         "Contribution %"),
            ("STG_SF_DIM_STORE",        "Store Dimension"),
            ("STG_SF_DIM_ARTICLE",      "Article Hierarchy"),
        };

        var result = new List<Models.StagingStatusItem>();
        await using var conn = new SqlConnection(SqlConnStr);
        await conn.OpenAsync();

        foreach (var (tableName, displayName) in tables)
        {
            var item = new Models.StagingStatusItem { TableName = tableName, DisplayName = displayName };
            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT COUNT(1), MAX(FETCHED_AT) FROM dbo.[{tableName}] WITH (NOLOCK)";
                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    item.RowCount = rdr.GetInt32(0);
                    item.LastFetched = rdr.IsDBNull(1) ? null : rdr.GetDateTime(1);
                }
            }
            catch { /* table may not exist yet */ }
            result.Add(item);
        }
        return result;
    }

    // ───────────────────────────────────────────────────────
    //  Helpers
    // ───────────────────────────────────────────────────────
    private async Task ReadSnowflakeAsync(string sql, Action<IDataReader> rowHandler)
    {
        await using var conn = new SnowflakeDbConnection();
        conn.ConnectionString = SnowflakeConnStr;
        await conn.OpenAsync();
        _logger.LogInformation("Snowflake connected");

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 300; // 5 min

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            rowHandler(reader);
    }

    private async Task TruncateAndBulkInsertAsync(string tableName, DataTable dt)
    {
        await using var conn = new SqlConnection(SqlConnStr);
        await conn.OpenAsync();

        await using (var truncCmd = conn.CreateCommand())
        {
            truncCmd.CommandText = $"TRUNCATE TABLE dbo.[{tableName}]";
            await truncCmd.ExecuteNonQueryAsync();
        }

        using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null);
        bulk.DestinationTableName = $"dbo.[{tableName}]";
        bulk.BatchSize = 50_000;
        bulk.BulkCopyTimeout = 600;
        // Map columns by name (skip ID identity column)
        foreach (DataColumn col in dt.Columns)
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        await bulk.WriteToServerAsync(dt);
    }

    private static decimal ToDecimal(IDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal)) return 0m;
        var val = reader.GetValue(ordinal);
        return val switch
        {
            decimal d => d,
            double dbl => (decimal)dbl,
            float f => (decimal)f,
            long l => l,
            int i => i,
            _ => decimal.TryParse(val?.ToString(), out var p) ? p : 0m
        };
    }
}
