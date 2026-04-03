using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("MASTER_ST_MASTER")]
public class StoreMaster
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST_CD")]
    [StringLength(50)]
    public string? StCd { get; set; }

    [Column("ST_NM")]
    [StringLength(255)]
    public string? StNm { get; set; }

    [Column("RDC_CD")]
    [StringLength(50)]
    public string? RdcCd { get; set; }

    [Column("RDC_NM")]
    [StringLength(255)]
    public string? RdcNm { get; set; }

    [Column("HUB_CD")]
    [StringLength(50)]
    public string? HubCd { get; set; }

    [Column("HUB_NM")]
    [StringLength(255)]
    public string? HubNm { get; set; }

    [Column("STATUS")]
    [StringLength(50)]
    public string? Status { get; set; }

    [Column("GRID_ST_STS")]
    [StringLength(50)]
    public string? GridStSts { get; set; }

    [Column("OP_DATE")]
    public DateTime? OpDate { get; set; }

    [Column("AREA")]
    [StringLength(100)]
    public string? Area { get; set; }

    [Column("STATE")]
    [StringLength(100)]
    public string? State { get; set; }

    [Column("REF_STATE")]
    [StringLength(100)]
    public string? RefState { get; set; }

    [Column("SALE_GRP")]
    [StringLength(100)]
    public string? SaleGrp { get; set; }

    [Column("REF_ST_CD")]
    [StringLength(50)]
    public string? RefStCd { get; set; }

    [Column("REF_ST_NM")]
    [StringLength(255)]
    public string? RefStNm { get; set; }

    [Column("REF_GRP_NEW")]
    [StringLength(100)]
    public string? RefGrpNew { get; set; }

    [Column("REF_GRP_OLD")]
    [StringLength(100)]
    public string? RefGrpOld { get; set; }

    [Column("Date")]
    public DateTime? Date { get; set; }
}
