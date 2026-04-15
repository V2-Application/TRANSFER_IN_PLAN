using Snowflake.Data.Client;
using TRANSFER_IN_PLAN.Helpers;

namespace TRANSFER_IN_PLAN.Services;

public class PlanService
{
    private readonly string _sfConnStr;
    private readonly ILogger<PlanService> _logger;

    public PlanService(IConfiguration config, ILogger<PlanService> logger)
    {
        _sfConnStr = config.GetConnectionString("Snowflake")!;
        _logger = logger;
    }

    public async Task<(int RowsInserted, DateTime ExecutionTime)> ExecutePlanGeneration(
        int startWeekId, int endWeekId, string? storeCode = null, string? majCat = null,
        decimal coverDaysCm1 = 14, decimal coverDaysCm2 = 0)
    {
        try
        {
            var startTime = DateTime.UtcNow;
            var sc = string.IsNullOrEmpty(storeCode) ? "NULL" : $"'{storeCode}'";
            var mc = string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'";

            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CALL SF_SP_GENERATE_TRF_IN_PLAN({startWeekId}, {endWeekId}, {sc}, {mc}, {coverDaysCm1}, {coverDaysCm2})";
            cmd.CommandTimeout = 3600;
            await cmd.ExecuteNonQueryAsync();

            // Get row count
            var rowsInserted = await SnowflakeCrudHelper.CountAsync(conn, "TRF_IN_PLAN",
                $"WEEK_ID BETWEEN {startWeekId} AND {endWeekId}" +
                (string.IsNullOrEmpty(storeCode) ? "" : $" AND ST_CD = '{storeCode}'") +
                (string.IsNullOrEmpty(majCat) ? "" : $" AND MAJ_CAT = '{majCat}'"));

            _logger.LogInformation("SF_SP_GENERATE_TRF_IN_PLAN executed. Rows: {Rows}", rowsInserted);
            return (rowsInserted, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing SF_SP_GENERATE_TRF_IN_PLAN");
            throw;
        }
    }

    public async Task<(int RowsInserted, DateTime ExecutionTime)> ExecutePurchasePlanAsync(
        int startWeekId, int endWeekId, string? rdcCode = null, string? majCat = null)
    {
        try
        {
            var rdc = string.IsNullOrEmpty(rdcCode) ? "NULL" : $"'{rdcCode}'";
            var mc = string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'";

            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CALL SF_SP_GENERATE_PURCHASE_PLAN({startWeekId}, {endWeekId}, {rdc}, {mc})";
            cmd.CommandTimeout = 3600;
            await cmd.ExecuteNonQueryAsync();

            var rowsInserted = await SnowflakeCrudHelper.CountAsync(conn, "PURCHASE_PLAN");
            _logger.LogInformation("SF_SP_GENERATE_PURCHASE_PLAN executed. Rows: {Rows}", rowsInserted);
            return (rowsInserted, DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing SF_SP_GENERATE_PURCHASE_PLAN");
            throw;
        }
    }
}
