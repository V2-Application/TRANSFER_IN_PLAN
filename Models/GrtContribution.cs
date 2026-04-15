using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("MASTER_GRT_CONTRIBUTION")]
public class GrtContribution
{
    [Key] [Column("ID")] public int Id { get; set; }

    [Column("SSN")]
    [StringLength(100)]
    public string Ssn { get; set; } = "NA";

    [Column("WK-1")]
    [Precision(18, 4)]
    public decimal Wk1 { get; set; } = 0;

    [Column("WK-2")]
    [Precision(18, 4)]
    public decimal Wk2 { get; set; } = 0;

    [Column("WK-3")]
    [Precision(18, 4)]
    public decimal Wk3 { get; set; } = 0;

    [Column("WK-4")]
    [Precision(18, 4)]
    public decimal Wk4 { get; set; } = 0;

    [Column("WK-5")]
    [Precision(18, 4)]
    public decimal Wk5 { get; set; } = 0;

    [Column("WK-6")]
    [Precision(18, 4)]
    public decimal Wk6 { get; set; } = 0;

    [Column("WK-7")]
    [Precision(18, 4)]
    public decimal Wk7 { get; set; } = 0;

    [Column("WK-8")]
    [Precision(18, 4)]
    public decimal Wk8 { get; set; } = 0;

    [Column("WK-9")]
    [Precision(18, 4)]
    public decimal Wk9 { get; set; } = 0;

    [Column("WK-10")]
    [Precision(18, 4)]
    public decimal Wk10 { get; set; } = 0;

    [Column("WK-11")]
    [Precision(18, 4)]
    public decimal Wk11 { get; set; } = 0;

    [Column("WK-12")]
    [Precision(18, 4)]
    public decimal Wk12 { get; set; } = 0;

    [Column("WK-13")]
    [Precision(18, 4)]
    public decimal Wk13 { get; set; } = 0;

    [Column("WK-14")]
    [Precision(18, 4)]
    public decimal Wk14 { get; set; } = 0;

    [Column("WK-15")]
    [Precision(18, 4)]
    public decimal Wk15 { get; set; } = 0;

    [Column("WK-16")]
    [Precision(18, 4)]
    public decimal Wk16 { get; set; } = 0;

    [Column("WK-17")]
    [Precision(18, 4)]
    public decimal Wk17 { get; set; } = 0;

    [Column("WK-18")]
    [Precision(18, 4)]
    public decimal Wk18 { get; set; } = 0;

    [Column("WK-19")]
    [Precision(18, 4)]
    public decimal Wk19 { get; set; } = 0;

    [Column("WK-20")]
    [Precision(18, 4)]
    public decimal Wk20 { get; set; } = 0;

    [Column("WK-21")]
    [Precision(18, 4)]
    public decimal Wk21 { get; set; } = 0;

    [Column("WK-22")]
    [Precision(18, 4)]
    public decimal Wk22 { get; set; } = 0;

    [Column("WK-23")]
    [Precision(18, 4)]
    public decimal Wk23 { get; set; } = 0;

    [Column("WK-24")]
    [Precision(18, 4)]
    public decimal Wk24 { get; set; } = 0;

    [Column("WK-25")]
    [Precision(18, 4)]
    public decimal Wk25 { get; set; } = 0;

    [Column("WK-26")]
    [Precision(18, 4)]
    public decimal Wk26 { get; set; } = 0;

    [Column("WK-27")]
    [Precision(18, 4)]
    public decimal Wk27 { get; set; } = 0;

    [Column("WK-28")]
    [Precision(18, 4)]
    public decimal Wk28 { get; set; } = 0;

    [Column("WK-29")]
    [Precision(18, 4)]
    public decimal Wk29 { get; set; } = 0;

    [Column("WK-30")]
    [Precision(18, 4)]
    public decimal Wk30 { get; set; } = 0;

    [Column("WK-31")]
    [Precision(18, 4)]
    public decimal Wk31 { get; set; } = 0;

    [Column("WK-32")]
    [Precision(18, 4)]
    public decimal Wk32 { get; set; } = 0;

    [Column("WK-33")]
    [Precision(18, 4)]
    public decimal Wk33 { get; set; } = 0;

    [Column("WK-34")]
    [Precision(18, 4)]
    public decimal Wk34 { get; set; } = 0;

    [Column("WK-35")]
    [Precision(18, 4)]
    public decimal Wk35 { get; set; } = 0;

    [Column("WK-36")]
    [Precision(18, 4)]
    public decimal Wk36 { get; set; } = 0;

    [Column("WK-37")]
    [Precision(18, 4)]
    public decimal Wk37 { get; set; } = 0;

    [Column("WK-38")]
    [Precision(18, 4)]
    public decimal Wk38 { get; set; } = 0;

    [Column("WK-39")]
    [Precision(18, 4)]
    public decimal Wk39 { get; set; } = 0;

    [Column("WK-40")]
    [Precision(18, 4)]
    public decimal Wk40 { get; set; } = 0;

    [Column("WK-41")]
    [Precision(18, 4)]
    public decimal Wk41 { get; set; } = 0;

    [Column("WK-42")]
    [Precision(18, 4)]
    public decimal Wk42 { get; set; } = 0;

    [Column("WK-43")]
    [Precision(18, 4)]
    public decimal Wk43 { get; set; } = 0;

    [Column("WK-44")]
    [Precision(18, 4)]
    public decimal Wk44 { get; set; } = 0;

    [Column("WK-45")]
    [Precision(18, 4)]
    public decimal Wk45 { get; set; } = 0;

    [Column("WK-46")]
    [Precision(18, 4)]
    public decimal Wk46 { get; set; } = 0;

    [Column("WK-47")]
    [Precision(18, 4)]
    public decimal Wk47 { get; set; } = 0;

    [Column("WK-48")]
    [Precision(18, 4)]
    public decimal Wk48 { get; set; } = 0;
}
