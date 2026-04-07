using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("ST_MAJ_CAT_VND_PLAN")]
public class ContVnd
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST_CD")]
    [StringLength(50)]
    public string StCd { get; set; } = "NA";

    [Column("MAJ_CAT_CD")]
    [StringLength(100)]
    public string MajCatCd { get; set; } = "NA";

    [Column("M_VND_CD")]
    [StringLength(200)]
    public string MVndCd { get; set; } = "NA";

    [Column("CONT_PCT")]
    [Precision(18, 4)]
    public decimal ContPct { get; set; } = 0;
}
