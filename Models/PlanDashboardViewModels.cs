namespace TRANSFER_IN_PLAN.Models;

public class PipelineDashboard
{
    public PlanStatus Plan1 { get; set; } = new() { Name = "Sale Budget Plan", Icon = "bi-coin", Color = "#8B5CF6", Url = "/SaleBudget/Execute" };
    public PlanStatus Plan2 { get; set; } = new() { Name = "Fixture & Density", Icon = "bi-grid-3x3-gap", Color = "#E67E22", Url = "/FixtureDensity/Execute" };
    public PlanStatus Plan3 { get; set; } = new() { Name = "Weekly Disaggregation", Icon = "bi-calendar3", Color = "#0284C7", Url = "/WeeklyDisagg/Execute" };
    public PlanStatus Plan4 { get; set; } = new() { Name = "Transfer In Plan", Icon = "bi-arrow-left-right", Color = "#059669", Url = "/Plan/Execute" };
    public PlanStatus Plan5 { get; set; } = new() { Name = "Purchase Plan", Icon = "bi-bag-check", Color = "#8E44AD", Url = "/PurchasePlan/Execute" };
    public PlanStatus Plan6 { get; set; } = new() { Name = "Sub-Level TRF", Icon = "bi-layers", Color = "#D97706", Url = "/SubLevel/Execute" };
    public PlanStatus Plan7 { get; set; } = new() { Name = "Sub-Level PP", Icon = "bi-layers-half", Color = "#DC2626", Url = "/SubLevel/Execute" };
    public PlanStatus Plan8 { get; set; } = new() { Name = "ARS Allocation", Icon = "bi-robot", Color = "#1E293B", Url = "/ArsStatus" };

    public int StagingSaleActual { get; set; }
    public int StagingForecasts { get; set; }
    public int StagingContPct { get; set; }
    public int StagingDimStore { get; set; }
    public int StagingDimArticle { get; set; }

    public List<DivRow> DivisionData { get; set; } = new();
    public PlanStatus[] AllPlans => new[] { Plan1, Plan2, Plan3, Plan4, Plan5, Plan6, Plan7, Plan8 };
    public int CompletedPlans => AllPlans.Count(p => p.Rows > 0);
}

public class PlanStatus
{
    public string Name { get; set; } = "";
    public string Icon { get; set; } = "";
    public string Color { get; set; } = "";
    public string Url { get; set; } = "";
    public int Rows { get; set; }
    public decimal Val1 { get; set; }
    public decimal Qty1 { get; set; }
    public decimal LyspVal { get; set; }
    public decimal GmVal { get; set; }
    public int Stores { get; set; }
    public int Categories { get; set; }
    public int Months { get; set; }
    public string? RunId { get; set; }
    public DateTime? LastRun { get; set; }
    public bool IsReady => Rows > 0;
}

public class DivRow
{
    public string Name { get; set; } = "";
    public decimal BgtVal { get; set; }
    public decimal LyspVal { get; set; }
}
