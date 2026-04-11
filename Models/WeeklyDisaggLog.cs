using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("WEEKLY_DISAGG_LOG")]
public class WeeklyDisaggLog
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RUN_ID")] [StringLength(50)] public string? RunId { get; set; }
    [Column("SOURCE_TABLE")] [StringLength(50)] public string? SourceTable { get; set; }
    [Column("TARGET_TABLE")] [StringLength(50)] public string? TargetTable { get; set; }
    [Column("ROWS_WRITTEN")] public int RowsWritten { get; set; }
    [Column("MONTHS_PROCESSED")] public int MonthsProcessed { get; set; }
    [Column("WEEKS_PER_MONTH")] public int WeeksPerMonth { get; set; }
    [Column("METHOD")] [StringLength(30)] public string? Method { get; set; }
    [Column("CREATED_DT")] public DateTime? CreatedDt { get; set; }
}

public class WeeklyDisaggViewModel
{
    // Current state
    public int SaleBudgetRows { get; set; }
    public int FixturePlanRows { get; set; }
    public int SaleQtyRows { get; set; }
    public int DispQtyRows { get; set; }
    public int WeekCalendarRows { get; set; }
    public string? LatestRunId { get; set; }
    public List<WeeklyDisaggLog> RecentRuns { get; set; } = new();

    // Available months from budget
    public List<string> AvailableMonths { get; set; } = new();
}
