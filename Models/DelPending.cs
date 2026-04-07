using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("QTY_DEL_PENDING")]
public class DelPending
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("RDC_CD")]
    [StringLength(50)]
    public string? RdcCd { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("DEL_PEND_Q")]
    [Precision(18, 4)]
    public decimal? DelPendQ { get; set; }

    [Column("DATE")]
    public DateTime? Date { get; set; }
}
