using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace TRANSFER_IN_PLAN.Models;

[Table("ARS_ST_MJ_DISPLAY_MASTER")]
public class ArsStMjDisplayMaster
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST")]
    [StringLength(20)]
    public string St { get; set; } = "";

    [Column("MJ")]
    [StringLength(100)]
    public string Mj { get; set; } = "";

    [Column("ST_MJ_DISP_Q")]
    [Precision(18, 4)]
    public decimal? StMjDispQ { get; set; }

    [Column("ACC_DENSITY")]
    [Precision(18, 4)]
    public decimal? AccDensity { get; set; }
}

[Table("ARS_ST_MJ_AUTO_SALE")]
public class ArsStMjAutoSale
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST")]
    [StringLength(20)]
    public string St { get; set; } = "";

    [Column("MJ")]
    [StringLength(100)]
    public string Mj { get; set; } = "";

    [Column("CM_REM_DAYS")]
    [Precision(18, 4)]
    public decimal? CmRemDays { get; set; }

    [Column("NM_DAYS")]
    [Precision(18, 4)]
    public decimal? NmDays { get; set; }

    [Column("CM_AUTO_SALE_Q")]
    [Precision(18, 4)]
    public decimal? CmAutoSaleQ { get; set; }

    [Column("NM_AUTO_SALE_Q")]
    [Precision(18, 4)]
    public decimal? NmAutoSaleQ { get; set; }

    [NotMapped]
    public decimal CmPdSaleQ => CmRemDays > 0 ? (CmAutoSaleQ ?? 0) / (CmRemDays ?? 1) : 0;
    [NotMapped]
    public decimal NmPdSaleQ => NmDays > 0 ? (NmAutoSaleQ ?? 0) / (NmDays ?? 1) : 0;
}

[Table("ARS_ST_ART_AUTO_SALE")]
public class ArsStArtAutoSale
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST")]
    [StringLength(20)]
    public string St { get; set; } = "";

    [Column("GEN_ART")]
    [StringLength(50)]
    public string GenArt { get; set; } = "";

    [Column("CLR")]
    [StringLength(50)]
    public string Clr { get; set; } = "";

    [Column("MJ")]
    [StringLength(100)]
    public string? Mj { get; set; }

    [Column("CM_REM_DAYS")]
    [Precision(18, 4)]
    public decimal? CmRemDays { get; set; }

    [Column("NM_DAYS")]
    [Precision(18, 4)]
    public decimal? NmDays { get; set; }

    [Column("CM_AUTO_SALE_Q")]
    [Precision(18, 4)]
    public decimal? CmAutoSaleQ { get; set; }

    [Column("NM_AUTO_SALE_Q")]
    [Precision(18, 4)]
    public decimal? NmAutoSaleQ { get; set; }

    [Column("ART_TAG")]
    [StringLength(20)]
    public string? ArtTag { get; set; }

    [NotMapped]
    public decimal CmPdSaleQ => CmRemDays > 0 ? (CmAutoSaleQ ?? 0) / (CmRemDays ?? 1) : 0;
    [NotMapped]
    public decimal NmPdSaleQ => NmDays > 0 ? (NmAutoSaleQ ?? 0) / (NmDays ?? 1) : 0;
}

[Table("ARS_ART_AGING")]
public class ArsArtAging
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("GEN_ART")]
    [StringLength(50)]
    public string GenArt { get; set; } = "";

    [Column("CLR")]
    [StringLength(50)]
    public string Clr { get; set; } = "";

    [Column("AGING_DAYS")]
    public int? AgingDays { get; set; }
}

[Table("ARS_HOLD_DAYS_MASTER")]
public class ArsHoldDaysMaster
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST")]
    [StringLength(20)]
    public string St { get; set; } = "";

    [Column("MJ")]
    [StringLength(100)]
    public string Mj { get; set; } = "";

    [Column("HOLD_DAYS")]
    [Precision(18, 4)]
    public decimal? HoldDays { get; set; }
}

[Table("ET_STOCK_DATA")]
public class EtStockData
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("MATNR")]
    [StringLength(50)]
    public string? Matnr { get; set; }

    [Column("WERKS")]
    [StringLength(20)]
    public string? Werks { get; set; }

    [Column("LGORT")]
    [StringLength(20)]
    public string? Lgort { get; set; }

    [Column("CHARG")]
    [StringLength(50)]
    public string? Charg { get; set; }

    [Column("MEINS")]
    [StringLength(10)]
    public string? Meins { get; set; }

    [Column("WAERS")]
    [StringLength(10)]
    public string? Waers { get; set; }

    [Column("LABST")]
    [Precision(18, 4)]
    public decimal? Labst { get; set; }

    [Column("TRAME")]
    [Precision(18, 4)]
    public decimal? Trame { get; set; }

    [Column("LABST_DMBTR")]
    [Precision(18, 4)]
    public decimal? LabstDmbtr { get; set; }

    [Column("TRAME_DMBTR")]
    [Precision(18, 4)]
    public decimal? TrameDmbtr { get; set; }

    [Column("V_MENGE")]
    [Precision(18, 4)]
    public decimal? VMenge { get; set; }

    [Column("V_DMBTR")]
    [Precision(18, 4)]
    public decimal? VDmbtr { get; set; }

    [Column("STOCK_DATE")]
    public DateTime? StockDate { get; set; }
}

[Table("ET_MSA_STOCK")]
public class ViewEtMsaStock
{
    [Key]
    [Column("ARTICLE_NUMBER")]
    [StringLength(50)]
    public string ArticleNumber { get; set; } = "";

    [Column("STORE_CODE")]
    [StringLength(20)]
    public string? StoreCode { get; set; }

    [Column("LOCATION")]
    [StringLength(20)]
    public string? Location { get; set; }

    [Column("LGNUM")]
    [StringLength(20)]
    public string? Lgnum { get; set; }

    [Column("LGTYP")]
    [StringLength(20)]
    public string? Lgtyp { get; set; }

    [Column("LGPLA")]
    [StringLength(50)]
    public string? Lgpla { get; set; }

    [Column("MEINS")]
    [StringLength(10)]
    public string? Meins { get; set; }

    [Column("VAL")]
    [Precision(18, 4)]
    public decimal? Val { get; set; }

    [Column("PPK_QTY")]
    [Precision(18, 4)]
    public decimal? PpkQty { get; set; }

    [Column("QTY")]
    [Precision(18, 4)]
    public decimal? Qty { get; set; }

    [Column("VEMNG2")]
    [Precision(18, 4)]
    public decimal? Vemng2 { get; set; }

    [Column("MC_CODE")]
    [StringLength(50)]
    public string? McCode { get; set; }

    [Column("ATTYP")]
    [StringLength(10)]
    public string? Attyp { get; set; }

    [Column("ERDAT")]
    [StringLength(20)]
    public string? Erdat { get; set; }

    [Column("KUNNR")]
    [StringLength(20)]
    public string? Kunnr { get; set; }

    [Column("LPTYP")]
    [StringLength(20)]
    public string? Lptyp { get; set; }

    [Column("MSA_STOCK_DATE")]
    public DateTime? MsaStockDate { get; set; }
}

[Table("ARS_ST_MASTER")]
public class ArsStMaster
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("ST_CD")]
    [StringLength(20)]
    public string StCd { get; set; } = "";

    [Column("ST_NM")]
    [StringLength(200)]
    public string? StNm { get; set; }

    [Column("HUB_CD")]
    [StringLength(20)]
    public string? HubCd { get; set; }

    [Column("HUB_NM")]
    [StringLength(200)]
    public string? HubNm { get; set; }

    [Column("DIRECT_HUB")]
    [StringLength(20)]
    public string? DirectHub { get; set; }

    [Column("TAGGED_RDC")]
    [StringLength(20)]
    public string? TaggedRdc { get; set; }

    [Column("DH24_DC_TO_HUB_INTRA")]
    [Precision(18, 4)]
    public decimal? Dh24DcToHubIntra { get; set; }

    [Column("DH24_HUB_TO_ST_INTRA")]
    [Precision(18, 4)]
    public decimal? Dh24HubToStIntra { get; set; }

    [Column("DW01_DC_TO_HUB_INTRA")]
    [Precision(18, 4)]
    public decimal? Dw01DcToHubIntra { get; set; }

    [Column("DW01_HUB_TO_ST_INTRA")]
    [Precision(18, 4)]
    public decimal? Dw01HubToStIntra { get; set; }

    [Column("ST_OP_DT")]
    public DateTime? StOpDt { get; set; }

    [Column("ST_STAT")]
    [StringLength(20)]
    public string? StStat { get; set; }

    [Column("SALE_COVER_DAYS")]
    [Precision(18, 4)]
    public decimal? SaleCoverDays { get; set; }

    [Column("PRD_DAYS")]
    [Precision(18, 4)]
    public decimal? PrdDays { get; set; }

    // Computed (not mapped) — same as VW_ARS_ST_MASTER
    [NotMapped]
    public decimal IntraDays => TaggedRdc == "DW01"
        ? (Dw01DcToHubIntra ?? 0) + (Dw01HubToStIntra ?? 0)
        : (Dh24DcToHubIntra ?? 0) + (Dh24HubToStIntra ?? 0);

    [NotMapped]
    public decimal TtlAlcDays => (SaleCoverDays ?? 0) + (PrdDays ?? 0) + IntraDays;
}
