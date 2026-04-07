using Microsoft.Data.SqlClient;

namespace TRANSFER_IN_PLAN.Services;

/// <summary>
/// Singleton service that manages background execution of sub-level plan generation.
/// Runs SP_GENERATE_SUB_LEVEL_TRF + SP_GENERATE_SUB_LEVEL_PP per level.
/// </summary>
public class SubLevelJobService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<SubLevelJobService> _logger;
    private readonly object _lock = new();

    // Job state
    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public string CurrentLevel { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public int LevelsCompleted { get; private set; }
    public int TotalLevels { get; private set; }
    public int TotalTrfRows { get; private set; }
    public int TotalPpRows { get; private set; }
    public string? ErrorMessage { get; private set; }

    public SubLevelJobService(IServiceScopeFactory scopeFactory, ILogger<SubLevelJobService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public bool TryStartFullRun(string[] levels, int startWeekId, int endWeekId, string? storeCode = null, string? majCat = null)
    {
        lock (_lock)
        {
            if (IsRunning) return false;
            IsRunning = true;
            Status = "Starting...";
            Phase = "Initializing";
            CurrentLevel = "";
            StartedAt = DateTime.Now;
            CompletedAt = null;
            LevelsCompleted = 0;
            TotalLevels = levels.Length;
            TotalTrfRows = 0;
            TotalPpRows = 0;
            ErrorMessage = null;
        }

        _ = Task.Run(() => RunSubLevelAsync(levels, startWeekId, endWeekId, storeCode, majCat));
        return true;
    }

    private async Task RunSubLevelAsync(string[] levels, int startWeekId, int endWeekId, string? storeCode, string? majCat)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var connStr = config.GetConnectionString("PlanningDatabase")!;

            _logger.LogInformation("SubLevelJob: Starting levels [{Levels}] weeks {Start}-{End}", string.Join(",", levels), startWeekId, endWeekId);

            for (int idx = 0; idx < levels.Length; idx++)
            {
                var levelKey = levels[idx].ToUpper();
                CurrentLevel = levelKey;

                // ── TRF Phase ──
                Phase = "TRF";
                Status = $"Running TRF for {levelKey} ({idx + 1}/{levels.Length})...";
                _logger.LogInformation("SubLevelJob: TRF [{Level}]", levelKey);

                await using (var conn = new SqlConnection(connStr))
                {
                    await conn.OpenAsync();
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = "EXEC dbo.SP_GENERATE_SUB_LEVEL_TRF @Level=@lv, @StartWeekID=@s, @EndWeekID=@e, @StoreCode=@sc, @MajCat=@mc";
                    cmd.Parameters.AddWithValue("@lv", levelKey);
                    cmd.Parameters.AddWithValue("@s", startWeekId);
                    cmd.Parameters.AddWithValue("@e", endWeekId);
                    cmd.Parameters.AddWithValue("@sc", string.IsNullOrEmpty(storeCode) ? DBNull.Value : storeCode);
                    cmd.Parameters.AddWithValue("@mc", string.IsNullOrEmpty(majCat) ? DBNull.Value : majCat);
                    cmd.CommandTimeout = 3600; // 1 hour
                    await cmd.ExecuteNonQueryAsync();
                }

                // ── PP Phase ──
                Phase = "PP";
                Status = $"Running PP for {levelKey} ({idx + 1}/{levels.Length})...";
                _logger.LogInformation("SubLevelJob: PP [{Level}]", levelKey);

                await using (var conn = new SqlConnection(connStr))
                {
                    await conn.OpenAsync();
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = "EXEC dbo.SP_GENERATE_SUB_LEVEL_PP @Level=@lv, @StartWeekID=@s, @EndWeekID=@e";
                    cmd.Parameters.AddWithValue("@lv", levelKey);
                    cmd.Parameters.AddWithValue("@s", startWeekId);
                    cmd.Parameters.AddWithValue("@e", endWeekId);
                    cmd.CommandTimeout = 3600;
                    await cmd.ExecuteNonQueryAsync();
                }

                lock (_lock) { LevelsCompleted = idx + 1; }
                _logger.LogInformation("SubLevelJob: [{Level}] done ({Done}/{Total})", levelKey, idx + 1, levels.Length);
            }

            // ── Final counts ──
            Phase = "Counting";
            Status = "Getting final row counts...";
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = @"SELECT COUNT(*) FROM dbo.SUB_LEVEL_TRF_PLAN WITH (NOLOCK);
                                    SELECT COUNT(*) FROM dbo.SUB_LEVEL_PP_PLAN WITH (NOLOCK);";
                cmd.CommandTimeout = 60;
                await using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync()) TotalTrfRows = reader.GetInt32(0);
                await reader.NextResultAsync();
                if (await reader.ReadAsync()) TotalPpRows = reader.GetInt32(0);
            }

            lock (_lock)
            {
                IsRunning = false;
                Phase = "Done";
                CurrentLevel = "";
                CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalMinutes:N1} min — {TotalTrfRows:N0} TRF + {TotalPpRows:N0} PP rows ({levels.Length} levels)";
            }
            _logger.LogInformation("SubLevelJob: Completed. TRF={Trf:N0}, PP={Pp:N0}", TotalTrfRows, TotalPpRows);
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                IsRunning = false;
                Phase = "Error";
                CompletedAt = DateTime.Now;
                ErrorMessage = ex.InnerException?.Message ?? ex.Message;
                Status = $"Failed at {CurrentLevel}: {ErrorMessage}";
            }
            _logger.LogError(ex, "SubLevelJob: Failed at level {Level}", CurrentLevel);
        }
    }

    public object GetStatus() => new
    {
        isRunning = IsRunning,
        status = Status,
        phase = Phase,
        currentLevel = CurrentLevel,
        startedAt = StartedAt?.ToString("HH:mm:ss"),
        completedAt = CompletedAt?.ToString("HH:mm:ss"),
        elapsedSeconds = IsRunning && StartedAt.HasValue
            ? (int)(DateTime.Now - StartedAt.Value).TotalSeconds : 0,
        levelsCompleted = LevelsCompleted,
        totalLevels = TotalLevels,
        trfRows = TotalTrfRows,
        ppRows = TotalPpRows,
        error = ErrorMessage
    };
}
