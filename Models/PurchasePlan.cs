using Microsoft.EntityFrameworkCore;
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

    [Column("SEG")]
    [StringLength(100)]
    public string? Seg { get; set; }

    [Column("DIV")]
    [StringLength(100)]
    public string? Div { get; set; }

    [Column("SUB_DIV")]
    [StringLength(100)]
    public string? SubDiv { get; set; }

    [Column("MAJ_CAT_NM")]
    [StringLength(100)]
    public string? MajCatNm { get; set; }

    [Column("SSN")]
    [StringLength(10)]
    public string? Ssn { get; set; }

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
    [Precision(18, 4)]
    public decimal? DcStkQ { get; set; }

    [Column("GRT_STK_Q")]
    [Precision(18, 4)]
    public decimal? GrtStkQ { get; set; }

    [Column("S_GRT_STK_Q")]
    [Precision(18, 4)]
    public decimal? SGrtStkQ { get; set; }

    [Column("W_GRT_STK_Q")]
    [Precision(18, 4)]
    public decimal? WGrtStkQ { get; set; }

    [Column("BIN_CAP_DC_TEAM")]
    [Precision(18, 4)]
    public decimal? BinCapDcTeam { get; set; }

    [Column("BIN_CAP")]
    [Precision(18, 4)]
    public decimal? BinCap { get; set; }

    [Column("BGT_DISP_CL_Q")]
    [Precision(18, 4)]
    public decimal? BgtDispClQ { get; set; }

    [Column("CW_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? CwBgtSaleQ { get; set; }

    [Column("CW1_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? Cw1BgtSaleQ { get; set; }

    [Column("CW2_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? Cw2BgtSaleQ { get; set; }

    [Column("CW3_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? Cw3BgtSaleQ { get; set; }

    [Column("CW4_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? Cw4BgtSaleQ { get; set; }

    [Column("CW5_BGT_SALE_Q")]
    [Precision(18, 4)]
    public decimal? Cw5BgtSaleQ { get; set; }

    [Column("BGT_ST_OP_MBQ")]
    [Precision(18, 4)]
    public decimal? BgtStOpMbq { get; set; }

    [Column("NET_ST_OP_STK_Q")]
    [Precision(18, 4)]
    public decimal? NetStOpStkQ { get; set; }

    [Column("BGT_DC_OP_STK_Q")]
    [Precision(18, 4)]
    public decimal? BgtDcOpStkQ { get; set; }

    [Column("PP_NT_ACT_Q")]
    [Precision(18, 4)]
    public decimal? PpNtActQ { get; set; }

    [Column("BGT_CF_STK_Q")]
    [Precision(18, 4)]
    public decimal? BgtCfStkQ { get; set; }

    [Column("TTL_STK")]
    [Precision(18, 4)]
    public decimal? TtlStk { get; set; }

    [Column("OP_STK")]
    [Precision(18, 4)]
    public decimal? OpStk { get; set; }

    [Column("NT_ACT_STK")]
    [Precision(18, 4)]
    public decimal? NtActStk { get; set; }

    [Column("GRT_CONS_PCT")]
    [Precision(18, 4)]
    public decimal? GrtConsPct { get; set; }

    [Column("GRT_CONS_Q")]
    [Precision(18, 4)]
    public decimal? GrtConsQ { get; set; }

    [Column("DEL_PEND_Q")]
    [Precision(18, 4)]
    public decimal? DelPendQ { get; set; }

    [Column("PP_NET_BGT_CF_STK_Q")]
    [Precision(18, 4)]
    public decimal? PpNetBgtCfStkQ { get; set; }

    [Column("CW_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? CwTrfOutQ { get; set; }

    [Column("CW1_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? Cw1TrfOutQ { get; set; }

    [Column("CW2_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? Cw2TrfOutQ { get; set; }

    [Column("CW3_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? Cw3TrfOutQ { get; set; }

    [Column("CW4_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? Cw4TrfOutQ { get; set; }

    [Column("TTL_TRF_OUT_Q")]
    [Precision(18, 4)]
    public decimal? TtlTrfOutQ { get; set; }

    [Column("BGT_ST_CL_MBQ")]
    [Precision(18, 4)]
    public decimal? BgtStClMbq { get; set; }

    [Column("NET_BGT_ST_CL_STK_Q")]
    [Precision(18, 4)]
    public decimal? NetBgtStClStkQ { get; set; }

    [Column("NET_SSNL_CL_STK_Q")]
    [Precision(18, 4)]
    public decimal? NetSsnlClStkQ { get; set; }

    [Column("BGT_DC_MBQ_SALE")]
    [Precision(18, 4)]
    public decimal? BgtDcMbqSale { get; set; }

    [Column("BGT_DC_CL_MBQ")]
    [Precision(18, 4)]
    public decimal? BgtDcClMbq { get; set; }

    [Column("BGT_DC_CL_STK_Q")]
    [Precision(18, 4)]
    public decimal? BgtDcClStkQ { get; set; }

    [Column("BGT_PUR_Q_INIT")]
    [Precision(18, 4)]
    public decimal? BgtPurQInit { get; set; }

    [Column("POS_PO_RAISED")]
    [Precision(18, 4)]
    public decimal? PosPORaised { get; set; }

    [Column("NEG_PO_RAISED")]
    [Precision(18, 4)]
    public decimal? NegPORaised { get; set; }

    [Column("BGT_CO_CL_STK_Q")]
    [Precision(18, 4)]
    public decimal? BgtCoClStkQ { get; set; }

    [Column("DC_STK_EXCESS_Q")]
    [Precision(18, 4)]
    public decimal? DcStkExcessQ { get; set; }

    [Column("DC_STK_SHORT_Q")]
    [Precision(18, 4)]
    public decimal? DcStkShortQ { get; set; }

    [Column("ST_STK_EXCESS_Q")]
    [Precision(18, 4)]
    public decimal? StStkExcessQ { get; set; }

    [Column("ST_STK_SHORT_Q")]
    [Precision(18, 4)]
    public decimal? StStkShortQ { get; set; }

    [Column("CO_STK_EXCESS_Q")]
    [Precision(18, 4)]
    public decimal? CoStkExcessQ { get; set; }

    [Column("CO_STK_SHORT_Q")]
    [Precision(18, 4)]
    public decimal? CoStkShortQ { get; set; }

    [Column("FRESH_BIN_REQ")]
    [Precision(18, 4)]
    public decimal? FreshBinReq { get; set; }

    [Column("GRT_BIN_REQ")]
    [Precision(18, 4)]
    public decimal? GrtBinReq { get; set; }

    [Column("CREATED_DT")]
    public DateTime? CreatedDt { get; set; }

    [Column("CREATED_BY")]
    [StringLength(100)]
    public string? CreatedBy { get; set; }
}
