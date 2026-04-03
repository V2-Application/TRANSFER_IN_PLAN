using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("WEEK_CALENDAR")]
public class WeekCalendar
{
    [Key]
    [Column("WEEK_ID")]
    public int WeekId { get; set; }

    [Column("WEEK_SEQ")]
    public int WeekSeq { get; set; }

    [Column("FY_WEEK")]
    public int FyWeek { get; set; }

    [Column("FY_YEAR")]
    public int FyYear { get; set; }

    [Column("CAL_YEAR")]
    public int CalYear { get; set; }

    [Column("YEAR_WEEK")]
    [StringLength(50)]
    public string? YearWeek { get; set; }

    [Column("WK_ST_DT")]
    public DateTime? WkStDt { get; set; }

    [Column("WK_END_DT")]
    public DateTime? WkEndDt { get; set; }
}
