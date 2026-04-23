using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("STORE_CALENDAR")]
public class StoreCalendar
{
    [Key] [Column("ID")] public int Id { get; set; }

    [Column("ST_CD")] [StringLength(50)]
    public string? StCd { get; set; }

    [Column("BGT_MNTH_DATE")]
    public DateTime? BgtMnthDate { get; set; }

    [Column("YEAR")] [StringLength(10)]
    public string? Year { get; set; }

    [Column("FY")] [StringLength(20)]
    public string? Fy { get; set; }

    [Column("MONTH")] [StringLength(20)]
    public string? Month { get; set; }

    [Column("YEAR_WEEK")] [StringLength(20)]
    public string? YearWeek { get; set; }

    [Column("FY_WEEK")] [StringLength(20)]
    public string? FyWeek { get; set; }

    [Column("FY_WEEK_ST_DT")]
    public DateTime? FyWeekStDt { get; set; }

    [Column("FY_WEEK_END_DT")]
    public DateTime? FyWeekEndDt { get; set; }

    [Column("DAY")] [StringLength(10)]
    public string? Day { get; set; }

    [Column("LY_SAME_DATE")]
    public DateTime? LySameDate { get; set; }
}
