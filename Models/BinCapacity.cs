using Microsoft.EntityFrameworkCore;
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

    [Column("MAJ-CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("BIN CAP DC TEAM")]
    [Precision(18, 4)]
    public decimal? BinCapDcTeam { get; set; }

    [Column("BIN CAP")]
    [Precision(18, 4)]
    public decimal? BinCap { get; set; }
}
