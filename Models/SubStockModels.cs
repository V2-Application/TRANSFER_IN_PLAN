using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

// ── Store Stock per sub-level ────────────────────────────────

[Table("SUB_ST_STK_MVGR")]
public class SubStStkMvgr
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("ST_CD")] [StringLength(50)] public string StCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("STK_QTY")] [Precision(18, 4)] public decimal StkQty { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_ST_STK_SZ")]
public class SubStStkSz
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("ST_CD")] [StringLength(50)] public string StCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("STK_QTY")] [Precision(18, 4)] public decimal StkQty { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_ST_STK_SEG")]
public class SubStStkSeg
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("ST_CD")] [StringLength(50)] public string StCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("STK_QTY")] [Precision(18, 4)] public decimal StkQty { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_ST_STK_VND")]
public class SubStStkVnd
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("ST_CD")] [StringLength(50)] public string StCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("STK_QTY")] [Precision(18, 4)] public decimal StkQty { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

// ── DC Stock per sub-level ───────────────────────────────────

[Table("SUB_DC_STK_MVGR")]
public class SubDcStkMvgr
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RDC_CD")] [StringLength(50)] public string RdcCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("DC_STK_Q")] [Precision(18, 4)] public decimal DcStkQ { get; set; } = 0;
    [Column("GRT_STK_Q")] [Precision(18, 4)] public decimal GrtStkQ { get; set; } = 0;
    [Column("W_GRT_STK_Q")] [Precision(18, 4)] public decimal WGrtStkQ { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_DC_STK_SZ")]
public class SubDcStkSz
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RDC_CD")] [StringLength(50)] public string RdcCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("DC_STK_Q")] [Precision(18, 4)] public decimal DcStkQ { get; set; } = 0;
    [Column("GRT_STK_Q")] [Precision(18, 4)] public decimal GrtStkQ { get; set; } = 0;
    [Column("W_GRT_STK_Q")] [Precision(18, 4)] public decimal WGrtStkQ { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_DC_STK_SEG")]
public class SubDcStkSeg
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RDC_CD")] [StringLength(50)] public string RdcCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("DC_STK_Q")] [Precision(18, 4)] public decimal DcStkQ { get; set; } = 0;
    [Column("GRT_STK_Q")] [Precision(18, 4)] public decimal GrtStkQ { get; set; } = 0;
    [Column("W_GRT_STK_Q")] [Precision(18, 4)] public decimal WGrtStkQ { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}

[Table("SUB_DC_STK_VND")]
public class SubDcStkVnd
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RDC_CD")] [StringLength(50)] public string RdcCd { get; set; } = "NA";
    [Column("MAJ_CAT")] [StringLength(100)] public string MajCat { get; set; } = "NA";
    [Column("SUB_VALUE")] [StringLength(200)] public string SubValue { get; set; } = "NA";
    [Column("DC_STK_Q")] [Precision(18, 4)] public decimal DcStkQ { get; set; } = 0;
    [Column("GRT_STK_Q")] [Precision(18, 4)] public decimal GrtStkQ { get; set; } = 0;
    [Column("W_GRT_STK_Q")] [Precision(18, 4)] public decimal WGrtStkQ { get; set; } = 0;
    [Column("DATE")] public DateTime? Date { get; set; }
}
