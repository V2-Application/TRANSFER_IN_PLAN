using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("QTY_ST_STK_Q")]
public class StoreStock
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST_CD")]
    [StringLength(50)]
    public string? StCd { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("STK_QTY")]
    [Precision(18, 4)]
    public decimal? StkQty { get; set; }

    [Column("DATE")]
    public DateTime? Date { get; set; }
}
