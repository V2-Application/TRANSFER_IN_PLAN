using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("QTY_MSA_AND_GRT")]
public class DcStock
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("ID")]
    public int Id { get; set; }

    [Column("RDC_CD")]
    [StringLength(50)]
    public string? RdcCd { get; set; }

    [Column("RDC")]
    [StringLength(255)]
    public string? Rdc { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("DC_STK_Q")]
    public decimal? DcStkQ { get; set; }

    [Column("GRT_STK_Q")]
    public decimal? GrtStkQ { get; set; }

    [Column("W_GRT_STK_Q")]
    public decimal? WGrtStkQ { get; set; }

    [Column("DATE")]
    public DateTime? Date { get; set; }
}
