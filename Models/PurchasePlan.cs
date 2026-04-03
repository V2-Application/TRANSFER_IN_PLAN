using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("PURCHASE_PLAN")]
public class PurchasePlan
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("RDC_CD")]
    [StringLength(50)]
    public string? RdcCd { get; set; }

    [Column("RDC_NM")]
    [StringLength(255)]
    public string? RdcNm { get; set; }

    [Column("MAJ_CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("SSN")]
    public int? Ssn { get; set; }

    [Column("WEEK_ID")]
    public int? WeekId { get; set; }

    [Column("FY_WEEK")]
    public int? FyWeek { get; set; }

    [Column("FY_YEAR")]
    public int? FyYear { get; set; }

    [Column("WK_ST_DT")]
    public DateTime? WkStDt { get; set; }

    [Column("WK_END_DT")]
    public DateTime? WkEndDt { get; set; }

    [Column("DC_STK_Q")]
    public decimal? DcStkQ { get; set; }

    [Column("GRT_STK_Q")]
    public decimal? GrtStkQ { get; set; }

    [Column("S_GRT_STK_Q")]
    public decimal? SGrtStkQ { get; set; }

    [Column("W_GRT_STK_Q")]
    public decimal? WGrtStkQ { get; set; }

    [Column("BIN_CAP_DC_TEAM")]
    public decimal? BinCapDcTeam { get; set; }

    [Column("BIN_CAP")]
    public decimal? BinCap { get; set; }

    [Column("BGT_DISP_CL_Q")]
    public decimal? BgtDispClQ { get; set; }

    [Column("CW_BGT_SALE_Q")]
    public decimal? CwBgtSaleQ { get; set; }

    [Column("CW1_BGT_SALE_Q")]
    public decimal? Cw1BgtSaleQ { get; set; }

    [Column("CW2_BGT_SALE_Q")]
    public decimal? Cw2BgtSaleQ { get; set; }

    [Column("CW3_BGT_SALE_Q")]
    public decimal? Cw3BgtSaleQ { get; set; }

    [Column("CW4_BGT_SALE_Q")]
    public decimal? Cw4BgtSaleQ { get; set; }

    [Column("CW5_BGT_SALE_Q")]
    public decimal? Cw5BgtSaleQ { get; set; }

    [Column("BGT_ST_OP_MBQ")]
    public decimal? BgtStOpMbq { get; set; }

    [Column("NET_ST_OP_STK_Q")]
    public decimal? NetStOpStkQ { get; set; }

    [Column("BGT_DC_OP_STK_Q")]
    public decimal? BgtDcOpStkQ { get; set; }

    [Column("PP_NT_ACT_Q")]
    public decimal? PpNtActQ { get; set; }

    [Column("BGT_CF_STK_Q")]
    public decimal? BgtCfStkQ { get; set; }

    [Column("TTL_STK")]
    public decimal? TtlStk { get; set; }

    [Column("OP_STK")]
    public decimal? OpStk { get; set; }

    [Column("NT_ACT_STK")]
    public decimal? NtActStk { get; set; }

    [Column("GRT_CONS_PCT")]
    public decimal? GrtConsPct { get; set; }

    [Column("GRT_CONS_Q")]
    public decimal? GrtConsQ { get; set; }

    [Column("DEL_PEND_Q")]
    public decimal? DelPendQ { get; set; }

    [Column("PP_NET_BGT_CF_STK_Q")]
    public decimal? PpNetBgtCfStkQ { get; set; }

    [Column("CW_TRF_OUT_Q")]
    public decimal? CwTrfOutQ { get; set; }

    [Column("CW1_TRF_OUT_Q")]
    public decimal? Cw1TrfOutQ { get; set; }

    [Column("TTL_TRF_OUT_Q")]
    public decimal? TtlTrfOutQ { get; set; }

    [Column("BGT_ST_CL_MBQ")]
    public decimal? BgtStClMbq { get; set; }

    [Column("NET_BGT_ST_CL_STK_Q")]
    public decimal? NetBgtStClStkQ { get; set; }

    [Column("NET_SSNL_CL_STK_Q")]
    public decimal? NetSsnlClStkQ { get; set; }

    [Column("BGT_DC_MBQ_SALE")]
    public decimal? BgtDcMbqSale { get; set; }

    [Column("BGT_DC_CL_MBQ")]
    public decimal? BgtDcClMbq { get; set; }

    [Column("BGT_DC_CL_STK_Q")]
    public decimal? BgtDcClStkQ { get; set; }

    [Column("BGT_PUR_Q_INIT")]
    public decimal? BgtPurQInit { get; set; }

    [Column("POS_PO_RAISED")]
    public decimal? PosPORaised { get; set; }

    [Column("NEG_PO_RAISED")]
    public decimal? NegPORaised { get; set; }

    [Column("BGT_CO_CL_STK_Q")]
    public decimal? BgtCoClStkQ { get; set; }

    [Column("DC_STK_EXCESS_Q")]
    public decimal? DcStkExcessQ { get; set; }

    [Column("DC_STK_SHORT_Q")]
    public decimal? DcStkShortQ { get; set; }

    [Column("ST_STK_EXCESS_Q")]
    public decimal? StStkExcessQ { get; set; }

    [Column("ST_STK_SHORT_Q")]
    public decimal? StStkShortQ { get; set; }

    [Column("CO_STK_EXCESS_Q")]
    public decimal? CoStkExcessQ { get; set; }

    [Column("CO_STK_SHORT_Q")]
    public decimal? CoStkShortQ { get; set; }

    [Column("FRESH_BIN_REQ")]
    public decimal? FreshBinReq { get; set; }

    [Column("GRT_BIN_REQ")]
    public decimal? GrtBinReq { get; set; }

    [Column("CREATED_DT")]
    public DateTime? CreatedDt { get; set; }

    [Column("CREATED_BY")]
    [StringLength(100)]
    public string? CreatedBy { get; set; }
}
