using Snowflake.Data.Client;
using System.Text.Json;

namespace TRANSFER_IN_PLAN.Services;

public class ArsAllocationJobService
{
    private readonly IConfiguration _config;
    private readonly ILogger<ArsAllocationJobService> _logger;
    private readonly object _lock = new();

    // Status properties
    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public string? ErrorMessage { get; private set; }
    public string? RunId { get; private set; }

    // Stats
    public int PreparedRows { get; private set; }
    public int AllocatedCount { get; private set; }
    public int HeldCount { get; private set; }
    public int OutputRows { get; private set; }
    public int StoresProcessed { get; private set; }
    public int LCount { get; private set; }
    public int MixCount { get; private set; }
    public int OldCount { get; private set; }
    public int NewLCount { get; private set; }

    public ArsAllocationJobService(IConfiguration config, ILogger<ArsAllocationJobService> logger)
    {
        _config = config;
        _logger = logger;
    }

    public bool TryStartRun()
    {
        lock (_lock)
        {
            if (IsRunning) return false;
            IsRunning = true;
            Phase = "Initializing";
            Status = "Starting allocation run...";
            StartedAt = DateTime.Now;
            CompletedAt = null;
            ErrorMessage = null;
            PreparedRows = 0; AllocatedCount = 0; HeldCount = 0; OutputRows = 0;
            StoresProcessed = 0; LCount = 0; MixCount = 0; OldCount = 0; NewLCount = 0;
            RunId = $"ARS-{DateTime.Now:yyyyMMdd-HHmmss}";
        }

        Task.Run(() => RunAllocationAsync());
        return true;
    }

    private async Task RunAllocationAsync()
    {
        var sfConnStr = _config.GetConnectionString("Snowflake")!;

        try
        {
            _logger.LogInformation("ARS Allocation: Starting run {RunId} on Snowflake", RunId);

            await using var conn = new SnowflakeDbConnection { ConnectionString = sfConnStr };
            await conn.OpenAsync();

            // ── Phase 1: Create Run Log in Snowflake ──
            lock (_lock) { Phase = "Logging"; Status = "Creating run log..."; }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO ARS_RUN_LOG (RUN_ID, STATUS, STARTED_DT) VALUES (:1, 'RUNNING', CURRENT_TIMESTAMP())";
                var p = cmd.CreateParameter();
                p.ParameterName = "1";
                p.Value = RunId;
                cmd.Parameters.Add(p);
                await cmd.ExecuteNonQueryAsync();
            }

            // ── Phase 2: Call Snowflake SP (PREPARE + ALLOCATE + FINALIZE) ──
            lock (_lock) { Phase = "Running"; Status = "Running allocation SP on Snowflake (prepare → allocate → finalize)..."; }
            _logger.LogInformation("ARS: Calling SP_ARS_ALLOCATION_RUN on Snowflake");

            await using (var cmd = conn.CreateCommand())
            {
                // Pass RunId inline — it's internally generated (ARS-yyyyMMdd-HHmmss), safe from injection
                cmd.CommandText = $"CALL SP_ARS_ALLOCATION_RUN('{RunId}')";
                cmd.CommandTimeout = 600; // 10 min

                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    // SP returns a VARIANT (JSON object)
                    var resultJson = rdr.GetString(0);
                    var result = JsonDocument.Parse(resultJson).RootElement;

                    PreparedRows    = GetInt(result, "prepared_rows");
                    StoresProcessed = GetInt(result, "stores");
                    AllocatedCount  = GetInt(result, "allocated_count");
                    HeldCount       = GetInt(result, "held_count");
                    OutputRows      = GetInt(result, "output_rows");
                    LCount          = GetInt(result, "l_count");
                    MixCount        = GetInt(result, "mix_count");
                    OldCount        = GetInt(result, "old_count");
                    NewLCount       = GetInt(result, "new_l_count");
                }
            }

            // ── Done ──
            lock (_lock)
            {
                IsRunning = false;
                Phase = "Done";
                CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalMinutes:N1} min — {OutputRows:N0} rows, {AllocatedCount:N0} allocated, {HeldCount:N0} held, {NewLCount:N0} NEW-L (Run: {RunId})";
            }
            _logger.LogInformation("ARS: Completed. {Output} rows in {Elapsed}", OutputRows, (CompletedAt!.Value - StartedAt!.Value));
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
            _logger.LogError(ex, "ARS Allocation: Failed");

            // Update run log with error
            try
            {
                await using var conn2 = new SnowflakeDbConnection { ConnectionString = sfConnStr };
                await conn2.OpenAsync();
                await using var cmd = conn2.CreateCommand();
                cmd.CommandText = "UPDATE ARS_RUN_LOG SET STATUS='FAILED', ERROR_MSG=:1, COMPLETED_DT=CURRENT_TIMESTAMP() WHERE RUN_ID=:2";
                var p1 = cmd.CreateParameter(); p1.ParameterName = "1"; p1.Value = ErrorMessage ?? "Unknown error"; cmd.Parameters.Add(p1);
                var p2 = cmd.CreateParameter(); p2.ParameterName = "2"; p2.Value = RunId; cmd.Parameters.Add(p2);
                await cmd.ExecuteNonQueryAsync();
            }
            catch { /* ignore logging errors */ }
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
        runId = RunId,
        startedAt = StartedAt?.ToString("HH:mm:ss"),
        completedAt = CompletedAt?.ToString("HH:mm:ss"),
        elapsedSeconds = IsRunning && StartedAt.HasValue ? (int)(DateTime.Now - StartedAt.Value).TotalSeconds : 0,
        preparedRows = PreparedRows,
        storesProcessed = StoresProcessed,
        allocatedCount = AllocatedCount,
        heldCount = HeldCount,
        outputRows = OutputRows,
        lCount = LCount,
        mixCount = MixCount,
        oldCount = OldCount,
        newLCount = NewLCount,
        error = ErrorMessage
    };
}
