using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("MASTER_PRODUCT_HIERARCHY")]
public class ProductHierarchy
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("ID")]
    public int Id { get; set; }

    [Column("SEG")]
    [StringLength(100)]
    public string Seg { get; set; } = "NA";

    [Column("DIV")]
    [StringLength(100)]
    public string Div { get; set; } = "NA";

    [Column("SUB_DIV")]
    [StringLength(100)]
    public string SubDiv { get; set; } = "NA";

    [Column("MAJ_CAT_NM")]
    [StringLength(200)]
    public string MajCatNm { get; set; } = "NA";

    [Column("SSN")]
    [StringLength(100)]
    public string Ssn { get; set; } = "NA";
}
