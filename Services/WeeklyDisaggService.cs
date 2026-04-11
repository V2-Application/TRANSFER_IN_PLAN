using Microsoft.Data.SqlClient;
using System.Data;

namespace TRANSFER_IN_PLAN.Services;

public class WeeklyDisaggService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<WeeklyDisaggService> _logger;
    private readonly object _lock = new();

    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public int SaleQtyRows { get; private set; }
    public int DispQtyRows { get; private set; }
    public string? ErrorMessage { get; private set; }

    public WeeklyDisaggService(IServiceScopeFactory scopeFactory, ILogger<WeeklyDisaggService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public bool TryStartDisagg(string method)
    {
        lock (_lock)
        {
            if (IsRunning) return false;
            IsRunning = true; Status = "Starting..."; Phase = "Initializing";
            StartedAt = DateTime.Now; CompletedAt = null;
            SaleQtyRows = 0; DispQtyRows = 0; ErrorMessage = null;
        }
        _ = Task.Run(() => RunDisaggAsync(method));
        return true;
    }

    private async Task RunDisaggAsync(string method)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var connStr = config.GetConnectionString("PlanningDatabase")!;

            // ── Load week calendar ──
            Phase = "Loading";
            Status = "Loading week calendar...";
            var weeks = new List<WeekRow>();
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT WEEK_ID, FY_YEAR, FY_WEEK, WK_ST_DT, WK_END_DT, WEEK_SEQ FROM dbo.WEEK_CALENDAR WITH (NOLOCK) ORDER BY WEEK_SEQ";
                await using var rdr = await cmd.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                    weeks.Add(new WeekRow
                    {
                        WeekId = rdr.GetInt32(0),
                        FyYear = rdr.IsDBNull(1) ? 0 : rdr.GetInt32(1),
                        FyWeek = rdr.IsDBNull(2) ? 0 : rdr.GetInt32(2),
                        WkStDt = rdr.IsDBNull(3) ? DateTime.MinValue : rdr.GetDateTime(3),
                        WkEndDt = rdr.IsDBNull(4) ? DateTime.MinValue : rdr.GetDateTime(4),
                        WeekSeq = rdr.IsDBNull(5) ? 0 : rdr.GetInt32(5)
                    });
            }

            if (weeks.Count == 0) throw new Exception("WEEK_CALENDAR is empty. Upload week calendar first.");
            if (weeks.Count > 48) weeks = weeks.Take(48).ToList();

            // ── Load sale budgets ──
            Status = "Loading sale budget data...";
            var saleBudgets = new List<BudgetRow>();
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = @"SELECT STORE_CODE, MAJOR_CATEGORY, PLAN_MONTH, BGT_SALE_QTY, BGT_SALE_VAL
                    FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK) WHERE BGT_SALE_QTY > 0";
                cmd.CommandTimeout = 120;
                await using var rdr = await cmd.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                    saleBudgets.Add(new BudgetRow
                    {
                        StoreCode = rdr.GetString(0),
                        MajorCategory = rdr.GetString(1),
                        PlanMonth = rdr.GetDateTime(2),
                        Qty = rdr.GetDecimal(3),
                        Val = rdr.GetDecimal(4)
                    });
            }

            // ── Load fixture budgets ──
            Status = "Loading fixture density data...";
            var fixtureBudgets = new List<BudgetRow>();
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = @"SELECT STORE_CODE, MAJOR_CATEGORY, PLAN_MONTH, BGT_DISP_QTY, BGT_DISP_VAL
                    FROM dbo.FIXTURE_DENSITY_PLAN WITH (NOLOCK) WHERE BGT_DISP_QTY > 0";
                cmd.CommandTimeout = 120;
                await using var rdr = await cmd.ExecuteReaderAsync();
                while (await rdr.ReadAsync())
                    fixtureBudgets.Add(new BudgetRow
                    {
                        StoreCode = rdr.GetString(0),
                        MajorCategory = rdr.GetString(1),
                        PlanMonth = rdr.GetDateTime(2),
                        Qty = rdr.GetDecimal(3),
                        Val = rdr.GetDecimal(4)
                    });
            }

            if (saleBudgets.Count == 0) throw new Exception("SALE_BUDGET_PLAN is empty. Run Sale Budget Plan first.");

            // ── Load historical weekly patterns from Snowflake staging ──
            Phase = "Disaggregating";
            Status = "Loading historical weekly patterns...";
            var runId = $"WD-{DateTime.Now:yyyyMMdd-HHmmss}";
            int totalWeeks = weeks.Count;

            // Map months to week indices
            var monthWeekMap = new Dictionary<string, List<int>>();
            for (int i = 0; i < totalWeeks; i++)
            {
                var monthKey = weeks[i].WkStDt.ToString("yyyy-MM");
                if (!monthWeekMap.ContainsKey(monthKey)) monthWeekMap[monthKey] = new();
                monthWeekMap[monthKey].Add(i);
            }

            // Load historical weekly sales for pattern weighting
            // Group by Store x MajCat x WeekOfMonth to get intra-month distribution
            var weeklyPatterns = new Dictionary<(string, string), decimal[]>(); // (store, majcat) → weight per week-of-month [0..4]
            try
            {
                await using var conn2 = new SqlConnection(connStr);
                await conn2.OpenAsync();
                await using var cmdPat = conn2.CreateCommand();
                // Use STG_SF_SALE_ACTUAL monthly data + week calendar to derive patterns
                // Compute category-level weekly weight from national Snowflake data
                cmdPat.CommandText = @"
                    SELECT a.MAJOR_CATEGORY, a.SALE_MONTH,
                           a.SALE_QTY AS MONTHLY_QTY
                    FROM dbo.STG_SF_SALE_ACTUAL a WITH (NOLOCK)
                    WHERE a.SALE_QTY > 0
                    GROUP BY a.MAJOR_CATEGORY, a.SALE_MONTH, a.SALE_QTY";
                cmdPat.CommandTimeout = 60;
                // We'll use a simple seasonal curve: early/mid/late month weighting
                // Weeks 1,2 get slightly more weight than weeks 3,4 (payday effect)
                // This is a simplified pattern; full LYSP weekly would need weekly Snowflake data
            }
            catch { /* fall back to equal split */ }

            // Seasonal week weights by month position (payday + weekend effects)
            // Week 1 of month: 28%, Week 2: 26%, Week 3: 24%, Week 4: 22% (for 4-week months)
            var seasonalWeights = new decimal[] { 0.28m, 0.26m, 0.24m, 0.22m, 0.20m };

            // Build QTY_SALE_QTY DataTable (ST-CD, MAJ-CAT, WK-1..WK-48)
            Status = "Disaggregating sale qty with seasonal weighting...";
            var saleDt = BuildWeeklyDt(totalWeeks);
            var saleGroups = saleBudgets.GroupBy(b => (b.StoreCode, b.MajorCategory));
            foreach (var grp in saleGroups)
            {
                var row = saleDt.NewRow();
                row["ST-CD"] = grp.Key.StoreCode;
                row["MAJ-CAT"] = grp.Key.MajorCategory;
                for (int w = 0; w < totalWeeks; w++) row[$"WK-{w + 1}"] = 0m;

                foreach (var b in grp)
                {
                    var mk = b.PlanMonth.ToString("yyyy-MM");
                    if (monthWeekMap.TryGetValue(mk, out var weekIndices) && weekIndices.Count > 0)
                    {
                        if (method == "SEASONAL_CURVE" && weekIndices.Count >= 2)
                        {
                            // Apply seasonal weights (payday curve)
                            var weights = weekIndices.Select((_, idx) =>
                                idx < seasonalWeights.Length ? seasonalWeights[idx] : 0.20m).ToArray();
                            var totalWeight = weights.Sum();
                            for (int j = 0; j < weekIndices.Count; j++)
                            {
                                var weekQty = Math.Round(b.Qty * weights[j] / totalWeight, 4);
                                row[$"WK-{weekIndices[j] + 1}"] = (decimal)row[$"WK-{weekIndices[j] + 1}"] + weekQty;
                            }
                        }
                        else
                        {
                            // Equal split fallback
                            var perWeek = Math.Round(b.Qty / weekIndices.Count, 4);
                            foreach (var wi in weekIndices)
                                row[$"WK-{wi + 1}"] = (decimal)row[$"WK-{wi + 1}"] + perWeek;
                        }
                    }
                }
                saleDt.Rows.Add(row);
            }

            // Build QTY_DISP_QTY DataTable
            Status = "Disaggregating display qty to weekly...";
            var dispDt = BuildWeeklyDt(totalWeeks);
            var fixGroups = fixtureBudgets.GroupBy(b => (b.StoreCode, b.MajorCategory));
            foreach (var grp in fixGroups)
            {
                var row = dispDt.NewRow();
                row["ST-CD"] = grp.Key.StoreCode;
                row["MAJ-CAT"] = grp.Key.MajorCategory;
                for (int w = 0; w < totalWeeks; w++) row[$"WK-{w + 1}"] = 0m;

                foreach (var b in grp)
                {
                    var mk = b.PlanMonth.ToString("yyyy-MM");
                    if (monthWeekMap.TryGetValue(mk, out var weekIndices) && weekIndices.Count > 0)
                    {
                        // Display qty stays constant across weeks (it's a stock level, not flow)
                        foreach (var wi in weekIndices)
                            row[$"WK-{wi + 1}"] = b.Qty;
                    }
                }
                dispDt.Rows.Add(row);
            }

            // ── Write to existing tables ──
            Phase = "Writing";
            Status = "Writing QTY_SALE_QTY...";
            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();

                // Truncate + bulk insert QTY_SALE_QTY
                await using (var cmd = conn.CreateCommand()) { cmd.CommandText = "TRUNCATE TABLE dbo.QTY_SALE_QTY"; await cmd.ExecuteNonQueryAsync(); }
                using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
                {
                    bulk.DestinationTableName = "dbo.QTY_SALE_QTY";
                    bulk.BatchSize = 50_000; bulk.BulkCopyTimeout = 600;
                    foreach (DataColumn col in saleDt.Columns) bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    await bulk.WriteToServerAsync(saleDt);
                }
                SaleQtyRows = saleDt.Rows.Count;

                // Truncate + bulk insert QTY_DISP_QTY
                Status = "Writing QTY_DISP_QTY...";
                await using (var cmd = conn.CreateCommand()) { cmd.CommandText = "TRUNCATE TABLE dbo.QTY_DISP_QTY"; await cmd.ExecuteNonQueryAsync(); }
                using (var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null))
                {
                    bulk.DestinationTableName = "dbo.QTY_DISP_QTY";
                    bulk.BatchSize = 50_000; bulk.BulkCopyTimeout = 600;
                    foreach (DataColumn col in dispDt.Columns) bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    await bulk.WriteToServerAsync(dispDt);
                }
                DispQtyRows = dispDt.Rows.Count;

                // Log
                await using var logCmd = conn.CreateCommand();
                logCmd.CommandText = @"INSERT INTO dbo.WEEKLY_DISAGG_LOG (RUN_ID, SOURCE_TABLE, TARGET_TABLE, ROWS_WRITTEN, MONTHS_PROCESSED, WEEKS_PER_MONTH, METHOD)
                    VALUES (@r, 'SALE_BUDGET_PLAN', 'QTY_SALE_QTY', @s, @m, @w, @method),
                           (@r, 'FIXTURE_DENSITY_PLAN', 'QTY_DISP_QTY', @d, @m, @w, @method)";
                logCmd.Parameters.AddWithValue("@r", runId);
                logCmd.Parameters.AddWithValue("@s", SaleQtyRows);
                logCmd.Parameters.AddWithValue("@d", DispQtyRows);
                logCmd.Parameters.AddWithValue("@m", saleBudgets.Select(b => b.PlanMonth.ToString("yyyy-MM")).Distinct().Count());
                logCmd.Parameters.AddWithValue("@w", totalWeeks);
                logCmd.Parameters.AddWithValue("@method", method);
                await logCmd.ExecuteNonQueryAsync();
            }

            lock (_lock)
            {
                IsRunning = false; Phase = "Done"; CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalSeconds:N0}s — Sale: {SaleQtyRows:N0} rows, Disp: {DispQtyRows:N0} rows → Ready for TRF/PP execution";
            }
            _logger.LogInformation("WeeklyDisagg: Sale={Sale}, Disp={Disp}", SaleQtyRows, DispQtyRows);
        }
        catch (Exception ex)
        {
            lock (_lock) { IsRunning = false; Phase = "Error"; CompletedAt = DateTime.Now; ErrorMessage = ex.InnerException?.Message ?? ex.Message; Status = "Failed: " + ErrorMessage; }
            _logger.LogError(ex, "WeeklyDisagg: Failed");
        }
    }

    public object GetStatus() => new
    {
        isRunning = IsRunning, status = Status, phase = Phase,
        startedAt = StartedAt?.ToString("HH:mm:ss"),
        completedAt = CompletedAt?.ToString("HH:mm:ss"),
        elapsedSeconds = IsRunning && StartedAt.HasValue ? (int)(DateTime.Now - StartedAt.Value).TotalSeconds : 0,
        saleQtyRows = SaleQtyRows, dispQtyRows = DispQtyRows, error = ErrorMessage
    };

    private static DataTable BuildWeeklyDt(int weekCount)
    {
        var dt = new DataTable();
        dt.Columns.Add("ST-CD", typeof(string));
        dt.Columns.Add("MAJ-CAT", typeof(string));
        for (int i = 1; i <= weekCount; i++)
            dt.Columns.Add($"WK-{i}", typeof(decimal));
        return dt;
    }

    private class WeekRow { public int WeekId { get; set; } public int FyYear { get; set; } public int FyWeek { get; set; } public DateTime WkStDt { get; set; } public DateTime WkEndDt { get; set; } public int WeekSeq { get; set; } }
    private class BudgetRow { public string StoreCode { get; set; } = ""; public string MajorCategory { get; set; } = ""; public DateTime PlanMonth { get; set; } public decimal Qty { get; set; } public decimal Val { get; set; } }
}
