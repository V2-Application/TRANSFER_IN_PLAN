using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("SALE_BUDGET_CONFIG")]
public class SaleBudgetConfig
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("CONFIG_KEY")]
    [StringLength(100)]
    public string ConfigKey { get; set; } = "";

    [Column("CONFIG_VALUE")]
    [StringLength(500)]
    public string ConfigValue { get; set; } = "";

    [Column("DESCRIPTION")]
    [StringLength(500)]
    public string? Description { get; set; }

    [Column("UPDATED_DT")]
    public DateTime? UpdatedDt { get; set; }
}
