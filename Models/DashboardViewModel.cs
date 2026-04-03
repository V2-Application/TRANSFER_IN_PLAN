namespace TRANSFER_IN_PLAN.Models;

public class DashboardViewModel
{
    public int TotalStores { get; set; }
    public int TotalCategories { get; set; }
    public int TotalPlanRows { get; set; }
    public DateTime? LastExecutionDate { get; set; }
    public List<WeeklySummary> WeeklySummary { get; set; } = new();
    public List<CategorySummary> CategorySummary { get; set; } = new();
    public List<StoreMetric> TopShortStores { get; set; } = new();
    public List<StoreMetric> TopExcessStores { get; set; } = new();
}

public class WeeklySummary
{
    public int FyWeek { get; set; }
    public int FyYear { get; set; }
    public decimal TotalTrfInQty { get; set; }
    public int RowCount { get; set; }
}

public class CategorySummary
{
    public string? MajCat { get; set; }
    public decimal TotalTrfInQty { get; set; }
    public int RowCount { get; set; }
}

public class StoreMetric
{
    public string? StCd { get; set; }
    public string? StNm { get; set; }
    public string? MajCat { get; set; }
    public decimal Quantity { get; set; }
}
