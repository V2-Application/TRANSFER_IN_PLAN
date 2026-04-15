using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("QTY_DISP_QTY")]
public class DispQty
{
    [Key] [Column("ID")] public int Id { get; set; }

    [Column("ST-CD")]
    [StringLength(50)]
    public string? StCd { get; set; }

    [Column("MAJ-CAT")]
    [StringLength(100)]
    public string? MajCat { get; set; }

    [Column("WK-1")]
    [Precision(18, 4)]
    public decimal? Wk1 { get; set; }

    [Column("WK-2")]
    [Precision(18, 4)]
    public decimal? Wk2 { get; set; }

    [Column("WK-3")]
    [Precision(18, 4)]
    public decimal? Wk3 { get; set; }

    [Column("WK-4")]
    [Precision(18, 4)]
    public decimal? Wk4 { get; set; }

    [Column("WK-5")]
    [Precision(18, 4)]
    public decimal? Wk5 { get; set; }

    [Column("WK-6")]
    [Precision(18, 4)]
    public decimal? Wk6 { get; set; }

    [Column("WK-7")]
    [Precision(18, 4)]
    public decimal? Wk7 { get; set; }

    [Column("WK-8")]
    [Precision(18, 4)]
    public decimal? Wk8 { get; set; }

    [Column("WK-9")]
    [Precision(18, 4)]
    public decimal? Wk9 { get; set; }

    [Column("WK-10")]
    [Precision(18, 4)]
    public decimal? Wk10 { get; set; }

    [Column("WK-11")]
    [Precision(18, 4)]
    public decimal? Wk11 { get; set; }

    [Column("WK-12")]
    [Precision(18, 4)]
    public decimal? Wk12 { get; set; }

    [Column("WK-13")]
    [Precision(18, 4)]
    public decimal? Wk13 { get; set; }

    [Column("WK-14")]
    [Precision(18, 4)]
    public decimal? Wk14 { get; set; }

    [Column("WK-15")]
    [Precision(18, 4)]
    public decimal? Wk15 { get; set; }

    [Column("WK-16")]
    [Precision(18, 4)]
    public decimal? Wk16 { get; set; }

    [Column("WK-17")]
    [Precision(18, 4)]
    public decimal? Wk17 { get; set; }

    [Column("WK-18")]
    [Precision(18, 4)]
    public decimal? Wk18 { get; set; }

    [Column("WK-19")]
    [Precision(18, 4)]
    public decimal? Wk19 { get; set; }

    [Column("WK-20")]
    [Precision(18, 4)]
    public decimal? Wk20 { get; set; }

    [Column("WK-21")]
    [Precision(18, 4)]
    public decimal? Wk21 { get; set; }

    [Column("WK-22")]
    [Precision(18, 4)]
    public decimal? Wk22 { get; set; }

    [Column("WK-23")]
    [Precision(18, 4)]
    public decimal? Wk23 { get; set; }

    [Column("WK-24")]
    [Precision(18, 4)]
    public decimal? Wk24 { get; set; }

    [Column("WK-25")]
    [Precision(18, 4)]
    public decimal? Wk25 { get; set; }

    [Column("WK-26")]
    [Precision(18, 4)]
    public decimal? Wk26 { get; set; }

    [Column("WK-27")]
    [Precision(18, 4)]
    public decimal? Wk27 { get; set; }

    [Column("WK-28")]
    [Precision(18, 4)]
    public decimal? Wk28 { get; set; }

    [Column("WK-29")]
    [Precision(18, 4)]
    public decimal? Wk29 { get; set; }

    [Column("WK-30")]
    [Precision(18, 4)]
    public decimal? Wk30 { get; set; }

    [Column("WK-31")]
    [Precision(18, 4)]
    public decimal? Wk31 { get; set; }

    [Column("WK-32")]
    [Precision(18, 4)]
    public decimal? Wk32 { get; set; }

    [Column("WK-33")]
    [Precision(18, 4)]
    public decimal? Wk33 { get; set; }

    [Column("WK-34")]
    [Precision(18, 4)]
    public decimal? Wk34 { get; set; }

    [Column("WK-35")]
    [Precision(18, 4)]
    public decimal? Wk35 { get; set; }

    [Column("WK-36")]
    [Precision(18, 4)]
    public decimal? Wk36 { get; set; }

    [Column("WK-37")]
    [Precision(18, 4)]
    public decimal? Wk37 { get; set; }

    [Column("WK-38")]
    [Precision(18, 4)]
    public decimal? Wk38 { get; set; }

    [Column("WK-39")]
    [Precision(18, 4)]
    public decimal? Wk39 { get; set; }

    [Column("WK-40")]
    [Precision(18, 4)]
    public decimal? Wk40 { get; set; }

    [Column("WK-41")]
    [Precision(18, 4)]
    public decimal? Wk41 { get; set; }

    [Column("WK-42")]
    [Precision(18, 4)]
    public decimal? Wk42 { get; set; }

    [Column("WK-43")]
    [Precision(18, 4)]
    public decimal? Wk43 { get; set; }

    [Column("WK-44")]
    [Precision(18, 4)]
    public decimal? Wk44 { get; set; }

    [Column("WK-45")]
    [Precision(18, 4)]
    public decimal? Wk45 { get; set; }

    [Column("WK-46")]
    [Precision(18, 4)]
    public decimal? Wk46 { get; set; }

    [Column("WK-47")]
    [Precision(18, 4)]
    public decimal? Wk47 { get; set; }

    [Column("WK-48")]
    [Precision(18, 4)]
    public decimal? Wk48 { get; set; }

    [Column("2")]
    [Precision(18, 4)]
    public decimal? Col2 { get; set; }
}
