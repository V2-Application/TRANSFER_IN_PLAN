using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("FIXTURE_DENSITY_PLAN")]
public class FixtureDensityPlan
{
    [Key] [Column("ID")] public int Id { get; set; }
    [Column("RUN_ID")] [StringLength(50)] public string? RunId { get; set; }

    [Column("STORE_CODE")] [StringLength(20)] public string? StoreCode { get; set; }
    [Column("STORE_NAME")] [StringLength(200)] public string? StoreName { get; set; }
    [Column("STATE")] [StringLength(50)] public string? State { get; set; }
    [Column("ZONE")] [StringLength(50)] public string? Zone { get; set; }
    [Column("REGION")] [StringLength(50)] public string? Region { get; set; }
    [Column("STORE_SIZE_SQFT")] [Precision(12, 2)] public decimal? StoreSizeSqft { get; set; }
    [Column("SIZE_CATEGORY")] [StringLength(20)] public string? SizeCategory { get; set; }

    [Column("MAJOR_CATEGORY")] [StringLength(100)] public string? MajorCategory { get; set; }
    [Column("DIVISION")] [StringLength(100)] public string? Division { get; set; }
    [Column("SUBDIVISION")] [StringLength(100)] public string? Subdivision { get; set; }
    [Column("SEGMENT")] [StringLength(100)] public string? Segment { get; set; }

    [Column("PLAN_MONTH")] public DateTime? PlanMonth { get; set; }

    [Column("BGT_DISP_QTY")] [Precision(18, 4)] public decimal? BgtDispQty { get; set; }
    [Column("BGT_DISP_VAL")] [Precision(18, 4)] public decimal? BgtDispVal { get; set; }
    [Column("ACC_DENSITY")] [Precision(18, 4)] public decimal? AccDensity { get; set; }
    [Column("FIX_COUNT")] [Precision(18, 4)] public decimal? FixCount { get; set; }
    [Column("AREA_SQFT")] [Precision(12, 2)] public decimal? AreaSqft { get; set; }

    [Column("SALE_BGT_VAL")] [Precision(18, 4)] public decimal? SaleBgtVal { get; set; }
    [Column("CL_STK_QTY")] [Precision(18, 4)] public decimal? ClStkQty { get; set; }
    [Column("CL_STK_VAL")] [Precision(18, 4)] public decimal? ClStkVal { get; set; }
    [Column("AVG_MRP")] [Precision(18, 4)] public decimal? AvgMrp { get; set; }

    [Column("GP_PSF")] [Precision(18, 4)] public decimal? GpPsf { get; set; }
    [Column("SALES_PSF")] [Precision(18, 4)] public decimal? SalesPsf { get; set; }
    [Column("STR_PCT")] [Precision(18, 6)] public decimal? StrPct { get; set; }

    [Column("ALGO_METHOD")] [StringLength(30)] public string? AlgoMethod { get; set; }
    [Column("CREATED_DT")] public DateTime? CreatedDt { get; set; }
}

public class FixtureDensityOutputViewModel
{
    public decimal TotalDispQty { get; set; }
    public decimal TotalDispVal { get; set; }
    public decimal AvgDensity { get; set; }
    public int StoreCount { get; set; }
    public int CategoryCount { get; set; }
    public string? LatestRunId { get; set; }

    public List<FixtureDensityPlan> Rows { get; set; } = new();
    public int TotalRows { get; set; }
    public int CurrentPage { get; set; }
    public int PageSize { get; set; }
    public int TotalPages => (TotalRows + PageSize - 1) / PageSize;

    public List<DivisionSummary> DivisionChart { get; set; } = new();
}
