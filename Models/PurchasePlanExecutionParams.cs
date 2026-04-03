namespace TRANSFER_IN_PLAN.Models;

public class PurchasePlanExecutionParams
{
    public int StartWeekId { get; set; }
    public int EndWeekId { get; set; }
    public string? RdcCode { get; set; }
    public string? MajCat { get; set; }
}
