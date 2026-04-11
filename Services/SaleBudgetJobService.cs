using Microsoft.Data.SqlClient;
using System.Data;

namespace TRANSFER_IN_PLAN.Services;

public class SaleBudgetJobService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<SaleBudgetJobService> _logger;
    private readonly object _lock = new();

    // Job state
    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public int OutputRows { get; private set; }
    public string? ErrorMessage { get; private set; }

    // Fetch counts
    public int SaleActualRows { get; private set; }
    public int ForecastRows { get; private set; }
    public int ContPctRows { get; private set; }

    public SaleBudgetJobService(IServiceScopeFactory scopeFactory, ILogger<SaleBudgetJobService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public bool TryStartBudgetRun(List<string> targetMonths, string algoMethod, decimal? growthOverride)
    {
        lock (_lock)
        {
            if (IsRunning) return false;
            IsRunning = true;
            Status = "Starting...";
            Phase = "Initializing";
            StartedAt = DateTime.Now;
            CompletedAt = null;
            OutputRows = 0;
            SaleActualRows = 0;
            ForecastRows = 0;
            ContPctRows = 0;
            ErrorMessage = null;
        }

        _ = Task.Run(() => RunBudgetAsync(targetMonths, algoMethod, growthOverride));
        return true;
    }

    private async Task RunBudgetAsync(List<string> targetMonths, string algoMethod, decimal? growthOverride)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var sfService = scope.ServiceProvider.GetRequiredService<SnowflakeService>();
            var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var connStr = config.GetConnectionString("PlanningDatabase")!;

            // ── Phase 1: Fetch Snowflake data ──
            Phase = "Fetching";
            Status = "Fetching sale actuals from Snowflake...";
            SaleActualRows = await sfService.FetchSaleActualsAsync(s => { lock (_lock) Status = s; });

            Status = "Fetching ML forecasts from Snowflake...";
            ForecastRows = await sfService.FetchDemandForecastsAsync(s => { lock (_lock) Status = s; });

            Status = "Fetching contribution percentages...";
            ContPctRows = await sfService.FetchContributionPctsAsync(s => { lock (_lock) Status = s; });

            Status = "Fetching store dimension...";
            await sfService.FetchDimStoreAsync(s => { lock (_lock) Status = s; });

            Status = "Fetching article hierarchy...";
            await sfService.FetchDimArticleAsync(s => { lock (_lock) Status = s; });

            // ── Phase 2: Load config ──
            Phase = "Calculating";
            Status = "Loading algorithm config...";
            var cfg = await LoadConfigAsync(connStr);
            var defaultGrowth = growthOverride ?? GetCfg(cfg, "DEFAULT_GROWTH_RATE", 0.10m);
            var fillLowThreshold = GetCfg(cfg, "FILL_RATE_LOW_THRESHOLD", 0.70m);
            var fillHighThreshold = GetCfg(cfg, "FILL_RATE_HIGH_THRESHOLD", 1.30m);
            var fillAdjFactor = GetCfg(cfg, "FILL_RATE_ADJ_FACTOR", 0.50m);
            var mlHybridWeight = GetCfg(cfg, "ML_HYBRID_WEIGHT", 0.60m);
            var minBgtQty = GetCfg(cfg, "MIN_BGT_QTY_PER_STORE", 5m);
            var maxGrowthCap = GetCfg(cfg, "MAX_BGT_GROWTH_CAP", 3.00m);
            var reconcileEnabled = GetCfg(cfg, "RECONCILIATION_ENABLED", 1m) == 1m;
            var reconcileTolerance = GetCfg(cfg, "RECONCILIATION_TOLERANCE", 0.05m);

            // ── Phase 3: Load staging data into memory ──
            Status = "Loading staging data...";
            var stores = await LoadTableAsync<StoreRow>(connStr, @"
                SELECT STORE_CODE, STORE_NAME, STATE, STORE_SIZE_SQFT,
                       SIZE_CATEGORY, COHORT, OLD_NEW, ZONE, REGION
                FROM dbo.STG_SF_DIM_STORE WITH (NOLOCK) WHERE IS_ACTIVE = 1");

            var articles = await LoadTableAsync<ArticleRow>(connStr, @"
                SELECT MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT, RNG_SEG, AVG_MRP
                FROM dbo.STG_SF_DIM_ARTICLE WITH (NOLOCK)");

            var actuals = await LoadTableAsync<ActualRow>(connStr, @"
                SELECT STORE_CODE, MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT,
                       SALE_MONTH, SALE_QTY, SALE_VAL, GM_VAL,
                       LYSP_SALE_QTY, LYSP_SALE_VAL, LYSP_GM_VAL
                FROM dbo.STG_SF_SALE_ACTUAL WITH (NOLOCK)");

            var forecasts = await LoadTableAsync<ForecastRow>(connStr, @"
                SELECT TARGET_MONTH, STORE_CODE, MAJOR_CATEGORY, STORE_NAME,
                       STORE_FORECAST, FORECAST_LOW, FORECAST_HIGH,
                       BEST_METHOD, WEIGHTED_MAPE, STORE_CONT_PCT, ENSEMBLE_FORECAST
                FROM dbo.STG_SF_DEMAND_FORECAST WITH (NOLOCK)");

            var contPcts = await LoadTableAsync<ContPctRow>(connStr, @"
                SELECT STORE_CODE, MAJOR_CATEGORY, L3M_SALE_CONT_PCT, YTD_SALE_CONT_PCT
                FROM dbo.STG_SF_CONT_PCT WITH (NOLOCK)");

            // Load festival calendar
            Status = "Loading festival calendar...";
            var festivals = await LoadTableAsync<FestivalRow>(connStr, @"
                SELECT STATE, MONTH_NUM, SUM(IMPACT_PCT) AS IMPACT_PCT
                FROM dbo.FESTIVAL_CALENDAR WITH (NOLOCK)
                GROUP BY STATE, MONTH_NUM");
            var festivalLookup = festivals
                .GroupBy(f => (f.State?.ToUpper(), f.MonthNum))
                .ToDictionary(g => g.Key, g => g.Sum(x => x.ImpactPct));

            // Build lookups
            var actualLookup = actuals.ToLookup(a => (a.StoreCode, a.MajorCategory, a.SaleMonth.ToString("yyyy-MM")));
            var forecastLookup = forecasts.ToLookup(f => (f.StoreCode, f.MajorCategory, f.TargetMonth.ToString("yyyy-MM")));
            var contPctLookup = contPcts
                .GroupBy(c => (c.StoreCode, c.MajorCategory))
                .ToDictionary(g => g.Key, g => g.First());
            var articleLookup = articles
                .GroupBy(a => a.MajorCategory)
                .ToDictionary(g => g.Key, g => g.First());

            // National-level growth by category
            var catGrowth = actuals
                .GroupBy(a => a.MajorCategory)
                .ToDictionary(
                    g => g.Key,
                    g =>
                    {
                        var totalSale = g.Sum(x => x.SaleVal);
                        var totalLysp = g.Sum(x => x.LyspSaleVal);
                        return totalLysp > 0 ? (totalSale - totalLysp) / totalLysp : defaultGrowth;
                    });

            // Store-level growth
            var storeGrowth = actuals
                .GroupBy(a => a.StoreCode)
                .ToDictionary(
                    g => g.Key,
                    g =>
                    {
                        var totalSale = g.Sum(x => x.SaleVal);
                        var totalLysp = g.Sum(x => x.LyspSaleVal);
                        return totalLysp > 0 ? (totalSale - totalLysp) / totalLysp : defaultGrowth;
                    });

            // National forecast totals (for bottom-up reconciliation)
            var nationalForecastByMajCat = forecasts
                .GroupBy(f => (f.MajorCategory, f.TargetMonth.ToString("yyyy-MM")))
                .ToDictionary(g => g.Key, g => g.Sum(x => x.StoreForecast));

            // ── Phase 4: Calculate budgets ──
            Status = "Calculating budgets...";
            var runId = $"SBP-{DateTime.Now:yyyyMMdd-HHmmss}";
            var outputDt = BuildOutputDataTable();
            int processed = 0;

            foreach (var monthStr in targetMonths)
            {
                var planMonth = DateTime.Parse(monthStr + "-01");
                var monthKey = planMonth.ToString("yyyy-MM");

                foreach (var store in stores)
                {
                    foreach (var article in articles)
                    {
                        // Get LYSP
                        var actualKey = (store.StoreCode, article.MajorCategory, monthKey);
                        var actRow = actualLookup[actualKey].FirstOrDefault();
                        var lyspQty = actRow?.LyspSaleQty ?? 0m;
                        var lyspVal = actRow?.LyspSaleVal ?? 0m;
                        var lyspGm = actRow?.LyspGmVal ?? 0m;

                        // Growth rates (capped -50% to +200% to avoid absurd values)
                        var grStCat = defaultGrowth;
                        if (lyspVal > 100m && actRow != null && actRow.SaleVal > 0)
                        {
                            var rawGrowth = (actRow.SaleVal - lyspVal) / lyspVal;
                            grStCat = Math.Clamp(rawGrowth, -0.50m, 2.00m);
                        }
                        var grCat = catGrowth.GetValueOrDefault(article.MajorCategory, defaultGrowth);
                        var grStore = storeGrowth.GetValueOrDefault(store.StoreCode, defaultGrowth);
                        var grCombined = lyspVal > 100m ? grStCat : grCat;

                        // Fill rate adjustment
                        var fillAdj = 1.0m;
                        if (lyspVal > 0 && actRow != null && actRow.SaleVal > 0)
                        {
                            var expectedSale = lyspVal * (1m + grCombined);
                            var fillRate = expectedSale > 0 ? actRow.SaleVal / expectedSale : 1m;
                            if (fillRate < fillLowThreshold)
                                fillAdj = 1m + (fillLowThreshold / fillRate - 1m) * fillAdjFactor;
                            else if (fillRate > fillHighThreshold)
                                fillAdj = 1m - (1m - fillHighThreshold / fillRate) * fillAdjFactor;
                        }

                        // Festival adjustment (state x month)
                        var festKey = (store.State?.ToUpper(), planMonth.Month);
                        var festivalAdj = festivalLookup.GetValueOrDefault(festKey, 0m);

                        // LYSP-based budget (with festival)
                        var bgtQtyLysp = lyspQty > 0
                            ? Math.Max(0, lyspQty * (1m + grCombined) * fillAdj * (1m + festivalAdj))
                            : 0m;

                        // Apply min/max cap
                        if (bgtQtyLysp > 0)
                        {
                            bgtQtyLysp = Math.Max(bgtQtyLysp, minBgtQty);
                            if (lyspQty > 0)
                                bgtQtyLysp = Math.Min(bgtQtyLysp, lyspQty * maxGrowthCap);
                        }

                        // ML forecast
                        var fcKey = (store.StoreCode, article.MajorCategory, monthKey);
                        var fcRow = forecastLookup[fcKey].FirstOrDefault();
                        var mlQty = fcRow?.StoreForecast ?? 0m;
                        var mlMape = fcRow?.WeightedMape;
                        var mlBest = fcRow?.BestMethod;
                        var mlLow = fcRow?.ForecastLow ?? 0m;
                        var mlHigh = fcRow?.ForecastHigh ?? 0m;

                        // Final BGT based on algorithm
                        decimal bgtQty;
                        string method;
                        if (algoMethod == "ML_FORECAST" && mlQty > 0)
                        {
                            bgtQty = Math.Max(0, mlQty);
                            method = "ML_FORECAST";
                        }
                        else if (algoMethod == "HYBRID" && mlQty > 0 && bgtQtyLysp > 0)
                        {
                            bgtQty = Math.Max(0, mlHybridWeight * mlQty + (1m - mlHybridWeight) * bgtQtyLysp);
                            method = "HYBRID";
                        }
                        else if (algoMethod == "HYBRID" && mlQty > 0)
                        {
                            bgtQty = Math.Max(0, mlQty);
                            method = "ML_FORECAST";
                        }
                        else if (bgtQtyLysp > 0)
                        {
                            bgtQty = bgtQtyLysp;
                            method = "LYSP_GROWTH";
                        }
                        else
                        {
                            // No LYSP, no ML — use contribution % from national forecast
                            var contKey = (store.StoreCode, article.MajorCategory);
                            var contPct = contPctLookup.GetValueOrDefault(contKey)?.YtdSaleContPct ?? 0m;
                            var nationalFc = fcRow?.EnsembleForecast ?? 0m;
                            bgtQty = Math.Max(0, nationalFc * contPct / 100m);
                            method = "CONT_PCT";
                        }

                        if (bgtQty <= 0) continue; // skip zero-budget combos

                        // Value calculation
                        var asp = lyspQty > 0 ? lyspVal / lyspQty : article.AvgMrp;
                        if (asp <= 0) asp = article.AvgMrp;
                        var bgtVal = Math.Round(bgtQty * asp, 2);
                        var gmPct = lyspVal > 0 ? lyspGm / lyspVal : 0.30m;
                        var bgtGm = Math.Round(bgtVal * gmPct, 2);
                        var contRow = contPctLookup.GetValueOrDefault((store.StoreCode, article.MajorCategory));

                        outputDt.Rows.Add(
                            runId, store.StoreCode, store.StoreName, store.State,
                            store.Zone, store.Region, store.SizeCategory, store.OldNew,
                            article.MajorCategory, article.Division, article.Subdivision, article.Segment,
                            planMonth,
                            Math.Round(lyspQty, 4), Math.Round(lyspVal, 4), Math.Round(lyspGm, 4),
                            Math.Round(grStCat, 6), Math.Round(grCat, 6), Math.Round(grStore, 6), Math.Round(grCombined, 6),
                            Math.Round(fillAdj, 6), Math.Round(festivalAdj, 6),
                            Math.Round(mlQty, 4), mlLow, mlHigh, mlMape, mlBest,
                            Math.Round(bgtQty, 4), Math.Round(bgtVal, 4), Math.Round(bgtGm, 4), Math.Round(asp, 4),
                            method, contRow?.YtdSaleContPct ?? 0m,
                            DateTime.Now
                        );

                        processed++;
                        if (processed % 10000 == 0)
                            lock (_lock) Status = $"Calculated {processed:N0} budget rows...";
                    }
                }
            }

            // ── Phase 4b: Bottom-up reconciliation ──
            if (reconcileEnabled && nationalForecastByMajCat.Count > 0)
            {
                Status = "Running bottom-up reconciliation...";
                _logger.LogInformation("SaleBudgetJob: Reconciling {Cats} categories", nationalForecastByMajCat.Count);

                // Group output rows by MajCat + Month, compare sum to national forecast
                var rowsByKey = new Dictionary<(string, string), List<int>>(); // (majcat, month) → row indices
                for (int i = 0; i < outputDt.Rows.Count; i++)
                {
                    var mc = outputDt.Rows[i]["MAJOR_CATEGORY"]?.ToString() ?? "";
                    var pm = ((DateTime)outputDt.Rows[i]["PLAN_MONTH"]).ToString("yyyy-MM");
                    var key = (mc, pm);
                    if (!rowsByKey.ContainsKey(key)) rowsByKey[key] = new();
                    rowsByKey[key].Add(i);
                }

                foreach (var kvp in rowsByKey)
                {
                    var (majCat, monthKey) = kvp.Key;
                    var indices = kvp.Value;
                    if (!nationalForecastByMajCat.TryGetValue((majCat, monthKey), out var nationalTarget) || nationalTarget <= 0)
                        continue;

                    var storeSum = indices.Sum(i => (decimal)outputDt.Rows[i]["BGT_SALE_QTY"]);
                    if (storeSum <= 0) continue;

                    var ratio = nationalTarget / storeSum;
                    // Only reconcile if variance exceeds tolerance
                    if (Math.Abs(ratio - 1m) <= reconcileTolerance) continue;

                    // Proportionally scale all stores for this majcat+month
                    foreach (var i in indices)
                    {
                        var oldQty = (decimal)outputDt.Rows[i]["BGT_SALE_QTY"];
                        var newQty = Math.Round(oldQty * ratio, 4);
                        outputDt.Rows[i]["BGT_SALE_QTY"] = newQty;

                        var asp = (decimal)outputDt.Rows[i]["AVG_SELLING_PRICE"];
                        if (asp > 0)
                        {
                            outputDt.Rows[i]["BGT_SALE_VAL"] = Math.Round(newQty * asp, 4);
                            var gmPct = (decimal)outputDt.Rows[i]["LYSP_SALE_VAL"] > 0
                                ? (decimal)outputDt.Rows[i]["LYSP_GM_VAL"] / (decimal)outputDt.Rows[i]["LYSP_SALE_VAL"]
                                : 0.30m;
                            outputDt.Rows[i]["BGT_GM_VAL"] = Math.Round(newQty * asp * gmPct, 4);
                        }
                    }
                }
            }

            // ── Phase 5: Store results ──
            Phase = "Storing";
            Status = $"Storing {outputDt.Rows.Count:N0} budget rows...";
            _logger.LogInformation("SaleBudgetJob: Storing {Rows} rows, RunID={Run}", outputDt.Rows.Count, runId);

            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();

                // Delete old data for these months
                await using var delCmd = conn.CreateCommand();
                var monthList = string.Join(",", targetMonths.Select(m => $"'{m}-01'"));
                delCmd.CommandText = $"DELETE FROM dbo.SALE_BUDGET_PLAN WHERE PLAN_MONTH IN ({monthList})";
                delCmd.CommandTimeout = 120;
                await delCmd.ExecuteNonQueryAsync();

                // Bulk insert
                using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null);
                bulk.DestinationTableName = "dbo.SALE_BUDGET_PLAN";
                bulk.BatchSize = 50_000;
                bulk.BulkCopyTimeout = 600;
                foreach (DataColumn col in outputDt.Columns)
                    bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                await bulk.WriteToServerAsync(outputDt);
            }

            OutputRows = outputDt.Rows.Count;

            lock (_lock)
            {
                IsRunning = false;
                Phase = "Done";
                CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalMinutes:N1} min — {OutputRows:N0} budget rows generated (Run: {runId})";
            }
            _logger.LogInformation("SaleBudgetJob: Completed. {Rows} rows in {Elapsed}", OutputRows, (CompletedAt!.Value - StartedAt!.Value));
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
            _logger.LogError(ex, "SaleBudgetJob: Failed");
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
        outputRows = OutputRows,
        saleActualRows = SaleActualRows,
        forecastRows = ForecastRows,
        contPctRows = ContPctRows,
        error = ErrorMessage
    };

    // ── Helpers ──

    private static DataTable BuildOutputDataTable()
    {
        var dt = new DataTable();
        dt.Columns.Add("RUN_ID", typeof(string));
        dt.Columns.Add("STORE_CODE", typeof(string));
        dt.Columns.Add("STORE_NAME", typeof(string));
        dt.Columns.Add("STATE", typeof(string));
        dt.Columns.Add("ZONE", typeof(string));
        dt.Columns.Add("REGION", typeof(string));
        dt.Columns.Add("SIZE_CATEGORY", typeof(string));
        dt.Columns.Add("OLD_NEW", typeof(string));
        dt.Columns.Add("MAJOR_CATEGORY", typeof(string));
        dt.Columns.Add("DIVISION", typeof(string));
        dt.Columns.Add("SUBDIVISION", typeof(string));
        dt.Columns.Add("SEGMENT", typeof(string));
        dt.Columns.Add("PLAN_MONTH", typeof(DateTime));
        dt.Columns.Add("LYSP_SALE_QTY", typeof(decimal));
        dt.Columns.Add("LYSP_SALE_VAL", typeof(decimal));
        dt.Columns.Add("LYSP_GM_VAL", typeof(decimal));
        dt.Columns.Add("GROWTH_RATE_ST_CAT", typeof(decimal));
        dt.Columns.Add("GROWTH_RATE_CATEGORY", typeof(decimal));
        dt.Columns.Add("GROWTH_RATE_STORE", typeof(decimal));
        dt.Columns.Add("GROWTH_RATE_COMBINED", typeof(decimal));
        dt.Columns.Add("FILL_RATE_ADJ", typeof(decimal));
        dt.Columns.Add("FESTIVAL_ADJ", typeof(decimal));
        dt.Columns.Add("ML_FORECAST_QTY", typeof(decimal));
        dt.Columns.Add("ML_FORECAST_LOW", typeof(decimal));
        dt.Columns.Add("ML_FORECAST_HIGH", typeof(decimal));
        dt.Columns.Add("ML_FORECAST_MAPE", typeof(decimal));
        dt.Columns.Add("ML_BEST_METHOD", typeof(string));
        dt.Columns.Add("BGT_SALE_QTY", typeof(decimal));
        dt.Columns.Add("BGT_SALE_VAL", typeof(decimal));
        dt.Columns.Add("BGT_GM_VAL", typeof(decimal));
        dt.Columns.Add("AVG_SELLING_PRICE", typeof(decimal));
        dt.Columns.Add("ALGO_METHOD", typeof(string));
        dt.Columns.Add("STORE_CONT_PCT", typeof(decimal));
        dt.Columns.Add("CREATED_DT", typeof(DateTime));
        return dt;
    }

    private static async Task<Dictionary<string, string>> LoadConfigAsync(string connStr)
    {
        var cfg = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT CONFIG_KEY, CONFIG_VALUE FROM dbo.SALE_BUDGET_CONFIG WITH (NOLOCK)";
        await using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync())
            cfg[rdr.GetString(0)] = rdr.GetString(1);
        return cfg;
    }

    private static decimal GetCfg(Dictionary<string, string> cfg, string key, decimal fallback)
        => cfg.TryGetValue(key, out var v) && decimal.TryParse(v, out var d) ? d : fallback;

    private static async Task<List<T>> LoadTableAsync<T>(string connStr, string sql) where T : new()
    {
        var list = new List<T>();
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.CommandTimeout = 120;
        await using var rdr = await cmd.ExecuteReaderAsync();

        var props = typeof(T).GetProperties();
        while (await rdr.ReadAsync())
        {
            var item = new T();
            for (int i = 0; i < rdr.FieldCount && i < props.Length; i++)
            {
                if (!rdr.IsDBNull(i))
                {
                    var val = rdr.GetValue(i);
                    var prop = props[i];
                    if (prop.PropertyType == typeof(decimal) || prop.PropertyType == typeof(decimal?))
                        prop.SetValue(item, Convert.ToDecimal(val));
                    else if (prop.PropertyType == typeof(DateTime) || prop.PropertyType == typeof(DateTime?))
                        prop.SetValue(item, Convert.ToDateTime(val));
                    else if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(int?))
                        prop.SetValue(item, Convert.ToInt32(val));
                    else
                        prop.SetValue(item, val?.ToString());
                }
            }
            list.Add(item);
        }
        return list;
    }

    // ── Internal row types (property order must match SQL column order) ──
    private class StoreRow
    {
        public string StoreCode { get; set; } = "";
        public string? StoreName { get; set; }
        public string? State { get; set; }
        public decimal StoreSizeSqft { get; set; }
        public string? SizeCategory { get; set; }
        public string? Cohort { get; set; }
        public string? OldNew { get; set; }
        public string? Zone { get; set; }
        public string? Region { get; set; }
    }

    private class ArticleRow
    {
        public string MajorCategory { get; set; } = "";
        public string? Division { get; set; }
        public string? Subdivision { get; set; }
        public string? Segment { get; set; }
        public string? RngSeg { get; set; }
        public decimal AvgMrp { get; set; }
    }

    private class ActualRow
    {
        public string StoreCode { get; set; } = "";
        public string MajorCategory { get; set; } = "";
        public string? Division { get; set; }
        public string? Subdivision { get; set; }
        public string? Segment { get; set; }
        public DateTime SaleMonth { get; set; }
        public decimal SaleQty { get; set; }
        public decimal SaleVal { get; set; }
        public decimal GmVal { get; set; }
        public decimal LyspSaleQty { get; set; }
        public decimal LyspSaleVal { get; set; }
        public decimal LyspGmVal { get; set; }
    }

    private class ForecastRow
    {
        public DateTime TargetMonth { get; set; }
        public string StoreCode { get; set; } = "";
        public string MajorCategory { get; set; } = "";
        public string? StoreName { get; set; }
        public decimal StoreForecast { get; set; }
        public decimal ForecastLow { get; set; }
        public decimal ForecastHigh { get; set; }
        public string? BestMethod { get; set; }
        public decimal? WeightedMape { get; set; }
        public decimal StoreContPct { get; set; }
        public decimal EnsembleForecast { get; set; }
    }

    private class ContPctRow
    {
        public string StoreCode { get; set; } = "";
        public string MajorCategory { get; set; } = "";
        public decimal L3mSaleContPct { get; set; }
        public decimal YtdSaleContPct { get; set; }
    }

    private class FestivalRow
    {
        public string? State { get; set; }
        public int MonthNum { get; set; }
        public decimal ImpactPct { get; set; }
    }
}
