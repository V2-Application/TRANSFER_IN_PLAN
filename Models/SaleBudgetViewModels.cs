namespace TRANSFER_IN_PLAN.Models;

public class SaleBudgetExecuteParams
{
    public List<string> TargetMonths { get; set; } = new();
    public string AlgoMethod { get; set; } = "HYBRID";   // LYSP_GROWTH, ML_FORECAST, HYBRID
    public decimal? GrowthRateOverride { get; set; }
}

public class SaleBudgetOutputViewModel
{
    // KPIs
    public decimal TotalBgtSaleVal { get; set; }
    public decimal TotalBgtSaleQty { get; set; }
    public decimal TotalLyspSaleVal { get; set; }
    public decimal YoyGrowthPct { get; set; }
    public int StoreCount { get; set; }
    public int CategoryCount { get; set; }
    public int MonthCount { get; set; }
    public string? LatestRunId { get; set; }

    // Data rows (paginated)
    public List<SaleBudgetPlan> Rows { get; set; } = new();
    public int TotalRows { get; set; }
    public int CurrentPage { get; set; }
    public int PageSize { get; set; }
    public int TotalPages => (TotalRows + PageSize - 1) / PageSize;

    // Chart data
    public List<DivisionSummary> DivisionChart { get; set; } = new();
    public List<MonthlySummary> MonthlyChart { get; set; } = new();
}

public class DivisionSummary
{
    public string Division { get; set; } = "";
    public decimal BgtSaleVal { get; set; }
    public decimal LyspSaleVal { get; set; }
    public decimal GrowthPct { get; set; }
    public int CategoryCount { get; set; }
}

public class MonthlySummary
{
    public string Month { get; set; } = "";
    public decimal BgtSaleVal { get; set; }
    public decimal BgtSaleQty { get; set; }
    public decimal LyspSaleVal { get; set; }
}

public class StagingStatusItem
{
    public string TableName { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public int RowCount { get; set; }
    public DateTime? LastFetched { get; set; }
    public bool IsReady => RowCount > 0;
}
