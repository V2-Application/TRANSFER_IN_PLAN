using Snowflake.Data.Client;
using System.Text.Json;

namespace TRANSFER_IN_PLAN.Services;

/// <summary>
/// Singleton service that manages background execution of SF_SP_RUN_ALL_PLANS on Snowflake.
/// Tracks job state so the UI can poll for progress.
/// </summary>
public class PlanJobService
{
    private readonly IConfiguration _config;
    private readonly ILogger<PlanJobService> _logger;
    private readonly object _lock = new();

    // Job state
    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public int TrfRows { get; private set; }
    public int PpRows { get; private set; }
    public string? ErrorMessage { get; private set; }

    public PlanJobService(IConfiguration config, ILogger<PlanJobService> logger)
    {
        _config = config;
        _logger = logger;
    }

    public bool TryStartFullRun(int startWeekId, int endWeekId)
    {
        lock (_lock)
        {
            if (IsRunning) return false;
            IsRunning = true;
            Status = "Starting...";
            Phase = "Initializing";
            StartedAt = DateTime.Now;
            CompletedAt = null;
            TrfRows = 0;
            PpRows = 0;
            ErrorMessage = null;
        }

        Task.Run(() => RunFullPlanAsync(startWeekId, endWeekId));
        return true;
    }

    private async Task RunFullPlanAsync(int startWeekId, int endWeekId)
    {
        var sfConnStr = _config.GetConnectionString("Snowflake")!;

        try
        {
            _logger.LogInformation("PlanJob: Starting full run WeekID {Start}-{End} on Snowflake", startWeekId, endWeekId);

            await using var conn = new SnowflakeDbConnection { ConnectionString = sfConnStr };
            await conn.OpenAsync();

            // Step 1: Truncate
            lock (_lock) { Phase = "Cleaning"; Status = "Truncating old data..."; }
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM TRF_IN_PLAN";
                cmd.CommandTimeout = 120;
                await cmd.ExecuteNonQueryAsync();
            }
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM PURCHASE_PLAN";
                cmd.CommandTimeout = 120;
                await cmd.ExecuteNonQueryAsync();
            }
            _logger.LogInformation("PlanJob: Tables truncated");

            // Step 2: Execute SF_SP_RUN_ALL_PLANS
            lock (_lock) { Phase = "Running"; Status = "Executing SP_RUN_ALL_PLANS on Snowflake..."; }
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"CALL SF_SP_RUN_ALL_PLANS({startWeekId}, {endWeekId}, NULL, 14, 0)";
                cmd.CommandTimeout = 7200; // 2 hours max
                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    try
                    {
                        var json = rdr.GetString(0);
                        var result = JsonDocument.Parse(json).RootElement;
                        TrfRows = GetInt(result, "trf_rows");
                        PpRows = GetInt(result, "pp_rows");
                    }
                    catch { /* fallback to count queries below */ }
                }
            }

            // Step 3: Get final counts (in case SP didn't return them)
            if (TrfRows == 0)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM TRF_IN_PLAN";
                TrfRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
            }
            if (PpRows == 0)
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT COUNT(*) FROM PURCHASE_PLAN";
                PpRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
            }

            lock (_lock)
            {
                IsRunning = false;
                Phase = "Done";
                CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalMinutes:N1} min — {TrfRows:N0} TRF + {PpRows:N0} PP rows";
            }
            _logger.LogInformation("PlanJob: Completed. TRF={Trf:N0}, PP={Pp:N0}", TrfRows, PpRows);
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                IsRunning = false;
                Phase = "Error";
                CompletedAt = DateTime.Now;
                ErrorMessage = ex.InnerException?.Message ?? ex.Message;
                Status = "Failed: " + ErrorMessage;
            }
            _logger.LogError(ex, "PlanJob: Failed");
        }
    }

    private static int GetInt(JsonElement el, string prop)
    {
        if (el.TryGetProperty(prop, out var val))
        {
            if (val.ValueKind == JsonValueKind.Number) return val.GetInt32();
            if (val.ValueKind == JsonValueKind.String && int.TryParse(val.GetString(), out var i)) return i;
        }
        return 0;
    }

    public object GetStatus() => new
    {
        isRunning = IsRunning,
        status = Status,
        phase = Phase,
        startedAt = StartedAt?.ToString("HH:mm:ss"),
        completedAt = CompletedAt?.ToString("HH:mm:ss"),
        elapsedSeconds = IsRunning && StartedAt.HasValue
            ? (int)(DateTime.Now - StartedAt.Value).TotalSeconds : 0,
        trfRows = TrfRows,
        ppRows = PpRows,
        error = ErrorMessage
    };
}
