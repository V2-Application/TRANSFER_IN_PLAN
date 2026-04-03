using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("MASTER_BIN_CAPACITY")]
public class BinCapacity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("ID")]
    public int Id { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("BIN_CAP_DC_TEAM")]
    public decimal? BinCapDcTeam { get; set; }

    [Column("BIN_CAP")]
    public decimal? BinCap { get; set; }
}
