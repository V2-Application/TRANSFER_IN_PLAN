namespace TRANSFER_IN_PLAN.Models;

public class SpExecutionParams
{
    public int StartWeekId { get; set; }
    public int EndWeekId { get; set; }
    public string? StoreCode { get; set; }
    public string? MajCat { get; set; }
    public decimal CoverDaysCm1 { get; set; } = 14;
    public decimal CoverDaysCm2 { get; set; } = 0;
}
