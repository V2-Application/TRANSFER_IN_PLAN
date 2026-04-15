using Snowflake.Data.Client;

namespace TRANSFER_IN_PLAN.Services;

/// <summary>
/// Singleton service that manages background execution of sub-level plan generation on Snowflake.
/// Runs SF_SP_GENERATE_SUB_LEVEL_TRF + SF_SP_GENERATE_SUB_LEVEL_PP per level.
/// </summary>
public class SubLevelJobService
{
    private readonly IConfiguration _config;
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

    public SubLevelJobService(IConfiguration config, ILogger<SubLevelJobService> logger)
    {
        _config = config;
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

        Task.Run(() => RunSubLevelAsync(levels, startWeekId, endWeekId, storeCode, majCat));
        return true;
    }

    private async Task RunSubLevelAsync(string[] levels, int startWeekId, int endWeekId, string? storeCode, string? majCat)
    {
        var sfConnStr = _config.GetConnectionString("Snowflake")!;

        try
        {
            _logger.LogInformation("SubLevelJob: Starting levels [{Levels}] weeks {Start}-{End} on Snowflake", string.Join(",", levels), startWeekId, endWeekId);

            await using var conn = new SnowflakeDbConnection { ConnectionString = sfConnStr };
            await conn.OpenAsync();

            var scParam = string.IsNullOrEmpty(storeCode) ? "NULL" : $"'{storeCode}'";
            var mcParam = string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'";

            for (int idx = 0; idx < levels.Length; idx++)
            {
                var levelKey = levels[idx].ToUpper();
                lock (_lock) { CurrentLevel = levelKey; }

                // ── TRF Phase ──
                lock (_lock) { Phase = "TRF"; Status = $"Running TRF for {levelKey} ({idx + 1}/{levels.Length})..."; }
                _logger.LogInformation("SubLevelJob: TRF [{Level}]", levelKey);

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"CALL SF_SP_GENERATE_SUB_LEVEL_TRF('{levelKey}', {startWeekId}, {endWeekId}, {scParam}, {mcParam})";
                    cmd.CommandTimeout = 3600;
                    await cmd.ExecuteNonQueryAsync();
                }

                // ── PP Phase ──
                lock (_lock) { Phase = "PP"; Status = $"Running PP for {levelKey} ({idx + 1}/{levels.Length})..."; }
                _logger.LogInformation("SubLevelJob: PP [{Level}]", levelKey);

                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"CALL SF_SP_GENERATE_SUB_LEVEL_PP('{levelKey}', {startWeekId}, {endWeekId}, NULL, {mcParam})";
                    cmd.CommandTimeout = 3600;
                    await cmd.ExecuteNonQueryAsync();
                }

                lock (_lock) { LevelsCompleted = idx + 1; }
                _logger.LogInformation("SubLevelJob: [{Level}] done ({Done}/{Total})", levelKey, idx + 1, levels.Length);
            }

            // ── Final counts ──
            lock (_lock) { Phase = "Counting"; Status = "Getting final row counts..."; }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM SUB_LEVEL_TRF_PLAN";
                TotalTrfRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
            }
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(*) FROM SUB_LEVEL_PP_PLAN";
                TotalPpRows = Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
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
