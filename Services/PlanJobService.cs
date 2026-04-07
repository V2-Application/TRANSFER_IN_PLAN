using Microsoft.Data.SqlClient;

namespace TRANSFER_IN_PLAN.Services;

/// <summary>
/// Singleton service that manages background execution of SP_RUN_ALL_PLANS.
/// Tracks job state so the UI can poll for progress.
/// </summary>
public class PlanJobService
{
    private readonly IServiceScopeFactory _scopeFactory;
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

    public PlanJobService(IServiceScopeFactory scopeFactory, ILogger<PlanJobService> logger)
    {
        _scopeFactory = scopeFactory;
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

        // Fire and forget — runs in background
        _ = Task.Run(() => RunFullPlanAsync(startWeekId, endWeekId));
        return true;
    }

    private async Task RunFullPlanAsync(int startWeekId, int endWeekId)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var connStr = config.GetConnectionString("PlanningDatabase")!;

            _logger.LogInformation("PlanJob: Starting full run WeekID {Start}-{End}", startWeekId, endWeekId);

            // Step 1: Truncate
            Phase = "Cleaning";
            Status = "Truncating old data...";
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "TRUNCATE TABLE dbo.TRF_IN_PLAN; TRUNCATE TABLE dbo.PURCHASE_PLAN;";
                cmd.CommandTimeout = 60;
                await cmd.ExecuteNonQueryAsync();
            }
            _logger.LogInformation("PlanJob: Tables truncated");

            // Step 2: Execute SP_RUN_ALL_PLANS
            Phase = "Running";
            Status = "Executing SP_RUN_ALL_PLANS...";
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "EXEC dbo.SP_RUN_ALL_PLANS @StartWeekID = @s, @EndWeekID = @e;";
                cmd.Parameters.AddWithValue("@s", startWeekId);
                cmd.Parameters.AddWithValue("@e", endWeekId);
                cmd.CommandTimeout = 7200; // 2 hours max
                await cmd.ExecuteNonQueryAsync();
            }

            // Step 3: Get final counts
            Phase = "Done";
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = @"SELECT COUNT(*) FROM dbo.TRF_IN_PLAN WITH (NOLOCK);
                                    SELECT COUNT(*) FROM dbo.PURCHASE_PLAN WITH (NOLOCK);";
                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync()) TrfRows = reader.GetInt32(0);
                await reader.NextResultAsync();
                if (await reader.ReadAsync()) PpRows = reader.GetInt32(0);
            }

            lock (_lock)
            {
                IsRunning = false;
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
