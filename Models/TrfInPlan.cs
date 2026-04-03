using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("TRF_IN_PLAN")]
public class TrfInPlan
{
    [Key]
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

    [Column("AREA")]
    [StringLength(100)]
    public string? Area { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("SSN")]
    public int? Ssn { get; set; }

    [Column("WEEK_ID")]
    public int? WeekId { get; set; }

    [Column("WK_ST_DT")]
    public DateTime? WkStDt { get; set; }

    [Column("WK_END_DT")]
    public DateTime? WkEndDt { get; set; }

    [Column("FY_YEAR")]
    public int? FyYear { get; set; }

    [Column("FY_WEEK")]
    public int? FyWeek { get; set; }

    [Column("S_GRT_STK_Q")]
    public decimal? SGrtStkQ { get; set; }

    [Column("W_GRT_STK_Q")]
    public decimal? WGrtStkQ { get; set; }

    [Column("BGT_DISP_CL_Q")]
    public decimal? BgtDispClQ { get; set; }

    [Column("BGT_DISP_CL_OPT")]
    public decimal? BgtDispClOpt { get; set; }

    [Column("CM1_SALE_COVER_DAY")]
    public decimal? Cm1SaleCoverDay { get; set; }

    [Column("CM2_SALE_COVER_DAY")]
    public decimal? Cm2SaleCoverDay { get; set; }

    [Column("COVER_SALE_QTY")]
    public decimal? CoverSaleQty { get; set; }

    [Column("BGT_ST_CL_MBQ")]
    public decimal? BgtStClMbq { get; set; }

    [Column("BGT_DISP_CL_OPT_MBQ")]
    public decimal? BgtDispClOptMbq { get; set; }

    [Column("BGT_TTL_CF_OP_STK_Q")]
    public decimal? BgtTtlCfOpStkQ { get; set; }

    [Column("NT_ACT_Q")]
    public decimal? NtActQ { get; set; }

    [Column("NET_BGT_CF_STK_Q")]
    public decimal? NetBgtCfStkQ { get; set; }

    [Column("CM_BGT_SALE_Q")]
    public decimal? CmBgtSaleQ { get; set; }

    [Column("CM1_BGT_SALE_Q")]
    public decimal? Cm1BgtSaleQ { get; set; }

    [Column("CM2_BGT_SALE_Q")]
    public decimal? Cm2BgtSaleQ { get; set; }

    [Column("TRF_IN_STK_Q")]
    public decimal? TrfInStkQ { get; set; }

    [Column("TRF_IN_OPT_CNT")]
    public decimal? TrfInOptCnt { get; set; }

    [Column("TRF_IN_OPT_MBQ")]
    public decimal? TrfInOptMbq { get; set; }

    [Column("DC_MBQ")]
    public decimal? DcMbq { get; set; }

    [Column("BGT_TTL_CF_CL_STK_Q")]
    public decimal? BgtTtlCfClStkQ { get; set; }

    [Column("BGT_NT_ACT_Q")]
    public decimal? BgtNtActQ { get; set; }

    [Column("NET_ST_CL_STK_Q")]
    public decimal? NetStClStkQ { get; set; }

    [Column("ST_CL_EXCESS_Q")]
    public decimal? StClExcessQ { get; set; }

    [Column("ST_CL_SHORT_Q")]
    public decimal? StClShortQ { get; set; }

    [Column("CREATED_DT")]
    public DateTime? CreatedDt { get; set; }

    [Column("CREATED_BY")]
    [StringLength(100)]
    public string? CreatedBy { get; set; }
}
