using Microsoft.Data.SqlClient;

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
        var connStr = _config.GetConnectionString("DataV2Database")!;

        try
        {
            _logger.LogInformation("ARS Allocation: Starting run {RunId}", RunId);

            // Use a single connection so temp tables persist across SPs
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            // ── Phase 1: Create Run Log ──
            lock (_lock) { Phase = "Logging"; Status = "Creating run log..."; }
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO dbo.ARS_RUN_LOG (RUN_ID, STATUS) VALUES (@r, 'RUNNING')";
                cmd.Parameters.AddWithValue("@r", RunId);
                await cmd.ExecuteNonQueryAsync();
            }

            // ── Phase 2: Prepare Data ──
            lock (_lock) { Phase = "Preparing"; Status = "Preparing data (stock pivot, MBQ, classification)..."; }
            _logger.LogInformation("ARS: Phase 2 — Prepare Data");

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "EXEC dbo.SP_ARS_PREPARE_DATA @RunId = @r";
                cmd.Parameters.AddWithValue("@r", RunId);
                cmd.CommandTimeout = 300;

                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    PreparedRows = rdr.GetInt32(0);
                    StoresProcessed = rdr.GetInt32(1);
                    LCount = rdr.GetInt32(3);
                    MixCount = rdr.GetInt32(4);
                    OldCount = rdr.GetInt32(5);
                }
            }

            lock (_lock) { Status = $"Prepared {PreparedRows:N0} rows ({StoresProcessed} stores, L:{LCount} MIX:{MixCount} OLD:{OldCount})"; }
            _logger.LogInformation("ARS: Prepared {Rows} rows, {Stores} stores", PreparedRows, StoresProcessed);

            // ── Phase 3: Allocate ──
            lock (_lock) { Phase = "Allocating"; Status = "Running allocation algorithm (iterative per store)..."; }
            _logger.LogInformation("ARS: Phase 3 — Allocate");

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "EXEC dbo.SP_ARS_ALLOCATE @RunId = @r";
                cmd.Parameters.AddWithValue("@r", RunId);
                cmd.CommandTimeout = 600; // 10 min for 600 stores

                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    AllocatedCount = rdr.GetInt32(1);
                    HeldCount = rdr.GetInt32(2);
                    var totalAlcQty = rdr.GetDecimal(3);
                    var totalHoldQty = rdr.GetDecimal(4);
                    lock (_lock) { Status = $"Allocated: {AllocatedCount:N0} articles, Held: {HeldCount:N0} (Qty: {totalAlcQty:N0} alc + {totalHoldQty:N0} hold)"; }
                }
            }

            _logger.LogInformation("ARS: Allocated {Alc}, Held {Held}", AllocatedCount, HeldCount);

            // ── Phase 4: Finalize ──
            lock (_lock) { Phase = "Finalizing"; Status = "Finalizing — NEW-L tagging, writing output..."; }
            _logger.LogInformation("ARS: Phase 4 — Finalize");

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "EXEC dbo.SP_ARS_FINALIZE @RunId = @r";
                cmd.Parameters.AddWithValue("@r", RunId);
                cmd.CommandTimeout = 300;

                await using var rdr = await cmd.ExecuteReaderAsync();
                if (await rdr.ReadAsync())
                {
                    OutputRows = rdr.GetInt32(0);
                    NewLCount = rdr.GetInt32(3);
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
            _logger.LogInformation("ARS: Completed. {Output} rows, {Elapsed}", OutputRows, (CompletedAt!.Value - StartedAt!.Value));
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
                await using var conn2 = new SqlConnection(connStr);
                await conn2.OpenAsync();
                await using var cmd = conn2.CreateCommand();
                cmd.CommandText = "UPDATE dbo.ARS_RUN_LOG SET STATUS='FAILED', ERROR_MSG=@e, COMPLETED_DT=GETDATE() WHERE RUN_ID=@r";
                cmd.Parameters.AddWithValue("@r", RunId);
                cmd.Parameters.AddWithValue("@e", ErrorMessage ?? "Unknown error");
                await cmd.ExecuteNonQueryAsync();
            }
            catch { /* ignore logging errors */ }
        }
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
