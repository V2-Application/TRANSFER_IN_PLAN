using Microsoft.Data.SqlClient;
using System.Data;

namespace TRANSFER_IN_PLAN.Services;

public class FixtureDensityJobService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<FixtureDensityJobService> _logger;
    private readonly object _lock = new();

    public bool IsRunning { get; private set; }
    public string Status { get; private set; } = "Idle";
    public string Phase { get; private set; } = "";
    public DateTime? StartedAt { get; private set; }
    public DateTime? CompletedAt { get; private set; }
    public int OutputRows { get; private set; }
    public string? ErrorMessage { get; private set; }

    public FixtureDensityJobService(IServiceScopeFactory scopeFactory, ILogger<FixtureDensityJobService> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public bool TryStartRun(List<string> targetMonths, string algoMethod)
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
            ErrorMessage = null;
        }
        _ = Task.Run(() => RunFixtureAsync(targetMonths, algoMethod));
        return true;
    }

    private async Task RunFixtureAsync(List<string> targetMonths, string algoMethod)
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
            var connStr = config.GetConnectionString("PlanningDatabase")!;

            // ── Phase 1: Load data from staging + budget ──
            Phase = "Loading";
            Status = "Loading store and article data from staging...";

            var stores = await LoadAsync<StoreRow>(connStr, @"
                SELECT STORE_CODE, STORE_NAME, STATE, ZONE, REGION, STORE_SIZE_SQFT, SIZE_CATEGORY
                FROM dbo.STG_SF_DIM_STORE WITH (NOLOCK) WHERE IS_ACTIVE = 1");

            var articles = await LoadAsync<ArticleRow>(connStr, @"
                SELECT MAJOR_CATEGORY, DIVISION, SUBDIVISION, SEGMENT, AVG_MRP, ARTICLE_COUNT
                FROM dbo.STG_SF_DIM_ARTICLE WITH (NOLOCK)");

            var contPcts = await LoadAsync<ContRow>(connStr, @"
                SELECT STORE_CODE, MAJOR_CATEGORY, STATE, SEGMENT, CL_STK_Q, CL_STK_V, YTD_SALES_PSF, YTD_GM_PCT
                FROM dbo.STG_SF_CONT_PCT WITH (NOLOCK)");

            var budgets = await LoadAsync<BudgetRow>(connStr, @"
                SELECT STORE_CODE, MAJOR_CATEGORY, PLAN_MONTH, BGT_SALE_QTY, BGT_SALE_VAL, BGT_GM_VAL
                FROM dbo.SALE_BUDGET_PLAN WITH (NOLOCK)");

            var contLookup = contPcts
                .GroupBy(c => (c.StoreCode, c.MajorCategory))
                .ToDictionary(g => g.Key, g => g.First());

            var budgetLookup = budgets
                .ToLookup(b => (b.StoreCode, b.MajorCategory, b.PlanMonth.ToString("yyyy-MM")));

            var articleLookup = articles
                .GroupBy(a => a.MajorCategory)
                .ToDictionary(g => g.Key, g => g.First());

            // ── Phase 2: Calculate fixture & density (V-0081 algorithm) ──
            Phase = "Calculating";
            var runId = $"FDP-{DateTime.Now:yyyyMMdd-HHmmss}";
            var dt = BuildOutputDt();
            int count = 0;
            const decimal AREA_PER_FIX = 4.0m; // sqft per fixture

            // ── Step 1: State Model — base fixtures per State x Segment ──
            Status = "Step 1/4: Building state model...";
            // Compute average fixtures per state from historical contribution data
            var stateModel = new Dictionary<(string, string), decimal>(); // (state, segment) → avg fix per store
            var stateGroups = contPcts
                .GroupBy(c => (c.State?.ToUpper() ?? "UNKNOWN", c.Segment ?? "OTHER"));
            foreach (var sg in stateGroups)
            {
                var avgSaleVal = sg.Average(x => x.ClStkV > 0 ? x.ClStkV : 0m);
                // Base fixtures = estimated display value / avg MRP / density
                var baseFix = avgSaleVal > 0 ? Math.Max(1, Math.Round(avgSaleVal / 500m / 10m, 0)) : 2m;
                stateModel[sg.Key] = baseFix;
            }

            // ── Step 2: National Model — min/max caps per segment ──
            var segMinFix = new Dictionary<string, decimal> { ["APP"] = 1, ["GM"] = 1, ["FAB"] = 1, ["ACC"] = 1 };
            var segMaxFix = new Dictionary<string, decimal> { ["APP"] = 50, ["GM"] = 30, ["FAB"] = 20, ["ACC"] = 15 };

            foreach (var monthStr in targetMonths)
            {
                var planMonth = DateTime.Parse(monthStr + "-01");
                var monthKey = planMonth.ToString("yyyy-MM");

                // ── Step 3: ROI Scoring — rank categories by GP/PSF within each store ──
                Status = "Step 2/4: ROI scoring & fixture allocation...";
                var storeAllocations = new Dictionary<string, List<FixCalcRow>>();

                foreach (var store in stores)
                {
                    var storeRows = new List<FixCalcRow>();
                    var totalFloorArea = store.StoreSizeSqft > 0 ? store.StoreSizeSqft : 5000m;
                    decimal usedArea = 0;

                    foreach (var art in articles)
                    {
                        var bgtKey = (store.StoreCode, art.MajorCategory, monthKey);
                        var bgtRow = budgetLookup[bgtKey].FirstOrDefault();
                        if (bgtRow == null || bgtRow.BgtSaleVal <= 0) continue;

                        var contKey = (store.StoreCode, art.MajorCategory);
                        var cont = contLookup.GetValueOrDefault(contKey);

                        // Base display % by segment
                        var dispPct = art.Segment == "APP" ? 0.28m : art.Segment == "GM" ? 0.45m : art.Segment == "FAB" ? 0.30m : 0.35m;
                        var baseDispQty = Math.Round(bgtRow.BgtSaleQty * dispPct, 0);

                        var asp = bgtRow.BgtSaleQty > 0 ? bgtRow.BgtSaleVal / bgtRow.BgtSaleQty : (art.AvgMrp > 0 ? art.AvgMrp : 500m);
                        var density = art.ArticleCount > 0 ? Math.Max(1, Math.Min(art.ArticleCount, baseDispQty)) : Math.Max(1, baseDispQty);

                        // State model base
                        var stKey = (store.State?.ToUpper() ?? "UNKNOWN", art.Segment ?? "OTHER");
                        var stateBaseFix = stateModel.GetValueOrDefault(stKey, 2m);

                        // Initial fixture from state model
                        var initFix = Math.Max(1, Math.Min(stateBaseFix, baseDispQty / Math.Max(1, density)));

                        // National cap
                        var minFix = segMinFix.GetValueOrDefault(art.Segment ?? "OTHER", 1m);
                        var maxFix = segMaxFix.GetValueOrDefault(art.Segment ?? "OTHER", 50m);
                        initFix = Math.Clamp(initFix, minFix, maxFix);

                        // ROI metrics
                        var salesPsf = cont?.YtdSalesPsf ?? 0m;
                        var gmPct = cont?.YtdGmPct ?? 30m;
                        var gpPsf = salesPsf * gmPct / 100m;
                        var strPct = cont != null && cont.ClStkQ > 0
                            ? bgtRow.BgtSaleQty / (bgtRow.BgtSaleQty + cont.ClStkQ) * 100m : 50m;

                        storeRows.Add(new FixCalcRow
                        {
                            MajorCategory = art.MajorCategory, Division = art.Division,
                            Subdivision = art.Subdivision, Segment = art.Segment,
                            DispQty = baseDispQty, Asp = asp, Density = density,
                            FixCount = initFix, GpPsf = gpPsf, SalesPsf = salesPsf, StrPct = strPct,
                            BgtSaleVal = bgtRow.BgtSaleVal, ClStkQ = cont?.ClStkQ ?? 0m,
                            ClStkV = cont?.ClStkV ?? 0m, AvgMrp = art.AvgMrp
                        });
                    }

                    // ── Step 4: 4 Rounds of incremental fixture allocation ──
                    // Round 1-2: APP (high GP PSF first get more fixtures)
                    // Round 3: GM additions
                    // Round 4: Final balance
                    var appRows = storeRows.Where(r => r.Segment == "APP").OrderByDescending(r => r.GpPsf).ToList();
                    var gmRows = storeRows.Where(r => r.Segment != "APP").OrderByDescending(r => r.GpPsf).ToList();

                    usedArea = storeRows.Sum(r => r.FixCount * AREA_PER_FIX);
                    var availableArea = totalFloorArea * 0.70m; // 70% of floor for fixtures (rest is walkway)

                    // Round 1: Boost top APP categories (top 30% by GP PSF)
                    var boostCount = Math.Max(1, appRows.Count / 3);
                    for (int i = 0; i < boostCount && usedArea < availableArea; i++)
                    {
                        var maxAdd = segMaxFix.GetValueOrDefault("APP", 50m);
                        var add = Math.Min(2, maxAdd - appRows[i].FixCount);
                        if (add > 0 && usedArea + add * AREA_PER_FIX <= availableArea)
                        { appRows[i].FixCount += add; usedArea += add * AREA_PER_FIX; }
                    }

                    // Round 2: Boost remaining APP
                    for (int i = boostCount; i < appRows.Count && usedArea < availableArea; i++)
                    {
                        if (appRows[i].FixCount < 3 && usedArea + AREA_PER_FIX <= availableArea)
                        { appRows[i].FixCount += 1; usedArea += AREA_PER_FIX; }
                    }

                    // Round 3: GM additions (high GP PSF)
                    for (int i = 0; i < gmRows.Count && usedArea < availableArea; i++)
                    {
                        var maxAdd = segMaxFix.GetValueOrDefault(gmRows[i].Segment ?? "OTHER", 30m);
                        if (gmRows[i].FixCount < maxAdd && usedArea + AREA_PER_FIX <= availableArea)
                        { gmRows[i].FixCount += 1; usedArea += AREA_PER_FIX; }
                    }

                    // Round 4: Floor balance — if over-allocated, trim lowest GP PSF
                    if (usedArea > availableArea)
                    {
                        var allSorted = storeRows.OrderBy(r => r.GpPsf).ToList();
                        foreach (var r in allSorted)
                        {
                            if (usedArea <= availableArea) break;
                            if (r.FixCount > 1)
                            { r.FixCount -= 1; usedArea -= AREA_PER_FIX; }
                        }
                    }

                    // ── Write output rows ──
                    foreach (var r in storeRows)
                    {
                        var finalDispQty = Math.Round(r.FixCount * r.Density, 0);
                        var dispVal = Math.Round(finalDispQty * r.Asp, 2);
                        var areaSqft = r.FixCount * AREA_PER_FIX;

                        dt.Rows.Add(
                            runId, store.StoreCode, store.StoreName, store.State,
                            store.Zone, store.Region, store.StoreSizeSqft, store.SizeCategory,
                            r.MajorCategory, r.Division, r.Subdivision, r.Segment,
                            planMonth,
                            finalDispQty, dispVal, r.Density, r.FixCount, areaSqft,
                            r.BgtSaleVal, r.ClStkQ, r.ClStkV, r.Asp,
                            r.GpPsf, r.SalesPsf, r.StrPct,
                            algoMethod, DateTime.Now
                        );
                        count++;
                    }

                    if (count % 5000 == 0)
                        lock (_lock) Status = $"Processed {count:N0} fixture rows ({store.StoreCode})...";
                }
            }

            // ── Phase 3: Store ──
            Phase = "Storing";
            Status = $"Storing {dt.Rows.Count:N0} fixture rows...";

            await using (var conn = new SqlConnection(connStr))
            {
                await conn.OpenAsync();
                var monthList = string.Join(",", targetMonths.Select(m => $"'{m}-01'"));
                await using var delCmd = conn.CreateCommand();
                delCmd.CommandText = $"DELETE FROM dbo.FIXTURE_DENSITY_PLAN WHERE PLAN_MONTH IN ({monthList})";
                delCmd.CommandTimeout = 120;
                await delCmd.ExecuteNonQueryAsync();

                using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, null);
                bulk.DestinationTableName = "dbo.FIXTURE_DENSITY_PLAN";
                bulk.BatchSize = 50_000;
                bulk.BulkCopyTimeout = 600;
                foreach (DataColumn col in dt.Columns)
                    bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                await bulk.WriteToServerAsync(dt);
            }

            OutputRows = dt.Rows.Count;
            lock (_lock)
            {
                IsRunning = false;
                Phase = "Done";
                CompletedAt = DateTime.Now;
                var elapsed = CompletedAt.Value - StartedAt!.Value;
                Status = $"Completed in {elapsed.TotalMinutes:N1} min — {OutputRows:N0} rows (Run: {runId})";
            }
            _logger.LogInformation("FixtureDensityJob: {Rows} rows generated", OutputRows);
        }
        catch (Exception ex)
        {
            lock (_lock) { IsRunning = false; Phase = "Error"; CompletedAt = DateTime.Now; ErrorMessage = ex.InnerException?.Message ?? ex.Message; Status = "Failed: " + ErrorMessage; }
            _logger.LogError(ex, "FixtureDensityJob: Failed");
        }
    }

    public object GetStatus() => new
    {
        isRunning = IsRunning, status = Status, phase = Phase,
        startedAt = StartedAt?.ToString("HH:mm:ss"),
        completedAt = CompletedAt?.ToString("HH:mm:ss"),
        elapsedSeconds = IsRunning && StartedAt.HasValue ? (int)(DateTime.Now - StartedAt.Value).TotalSeconds : 0,
        outputRows = OutputRows, error = ErrorMessage
    };

    private static DataTable BuildOutputDt()
    {
        var dt = new DataTable();
        foreach (var c in new[] { "RUN_ID","STORE_CODE","STORE_NAME","STATE","ZONE","REGION" }) dt.Columns.Add(c, typeof(string));
        dt.Columns.Add("STORE_SIZE_SQFT", typeof(decimal));
        dt.Columns.Add("SIZE_CATEGORY", typeof(string));
        foreach (var c in new[] { "MAJOR_CATEGORY","DIVISION","SUBDIVISION","SEGMENT" }) dt.Columns.Add(c, typeof(string));
        dt.Columns.Add("PLAN_MONTH", typeof(DateTime));
        foreach (var c in new[] { "BGT_DISP_QTY","BGT_DISP_VAL","ACC_DENSITY","FIX_COUNT","AREA_SQFT","SALE_BGT_VAL","CL_STK_QTY","CL_STK_VAL","AVG_MRP","GP_PSF","SALES_PSF","STR_PCT" })
            dt.Columns.Add(c, typeof(decimal));
        dt.Columns.Add("ALGO_METHOD", typeof(string));
        dt.Columns.Add("CREATED_DT", typeof(DateTime));
        return dt;
    }

    private static async Task<List<T>> LoadAsync<T>(string connStr, string sql) where T : new()
    {
        var list = new List<T>();
        await using var conn = new SqlConnection(connStr);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql; cmd.CommandTimeout = 120;
        await using var rdr = await cmd.ExecuteReaderAsync();
        var props = typeof(T).GetProperties();
        while (await rdr.ReadAsync())
        {
            var item = new T();
            for (int i = 0; i < rdr.FieldCount && i < props.Length; i++)
                if (!rdr.IsDBNull(i))
                {
                    var val = rdr.GetValue(i); var prop = props[i];
                    if (prop.PropertyType == typeof(decimal) || prop.PropertyType == typeof(decimal?)) prop.SetValue(item, Convert.ToDecimal(val));
                    else if (prop.PropertyType == typeof(DateTime) || prop.PropertyType == typeof(DateTime?)) prop.SetValue(item, Convert.ToDateTime(val));
                    else if (prop.PropertyType == typeof(int) || prop.PropertyType == typeof(int?)) prop.SetValue(item, Convert.ToInt32(val));
                    else prop.SetValue(item, val?.ToString());
                }
            list.Add(item);
        }
        return list;
    }

    private class StoreRow { public string StoreCode { get; set; } = ""; public string? StoreName { get; set; } public string? State { get; set; } public string? Zone { get; set; } public string? Region { get; set; } public decimal StoreSizeSqft { get; set; } public string? SizeCategory { get; set; } }
    private class ArticleRow { public string MajorCategory { get; set; } = ""; public string? Division { get; set; } public string? Subdivision { get; set; } public string? Segment { get; set; } public decimal AvgMrp { get; set; } public int ArticleCount { get; set; } }
    private class ContRow { public string StoreCode { get; set; } = ""; public string MajorCategory { get; set; } = ""; public string? State { get; set; } public string? Segment { get; set; } public decimal ClStkQ { get; set; } public decimal ClStkV { get; set; } public decimal YtdSalesPsf { get; set; } public decimal YtdGmPct { get; set; } }
    private class BudgetRow { public string StoreCode { get; set; } = ""; public string MajorCategory { get; set; } = ""; public DateTime PlanMonth { get; set; } public decimal BgtSaleQty { get; set; } public decimal BgtSaleVal { get; set; } public decimal BgtGmVal { get; set; } }
    private class FixCalcRow
    {
        public string MajorCategory { get; set; } = "";
        public string? Division { get; set; }
        public string? Subdivision { get; set; }
        public string? Segment { get; set; }
        public decimal DispQty { get; set; }
        public decimal Asp { get; set; }
        public decimal Density { get; set; }
        public decimal FixCount { get; set; }
        public decimal GpPsf { get; set; }
        public decimal SalesPsf { get; set; }
        public decimal StrPct { get; set; }
        public decimal BgtSaleVal { get; set; }
        public decimal ClStkQ { get; set; }
        public decimal ClStkV { get; set; }
        public decimal AvgMrp { get; set; }
    }
}
