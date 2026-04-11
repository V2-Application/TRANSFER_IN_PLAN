using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace TRANSFER_IN_PLAN.Models;

[Table("SALE_BUDGET_PLAN")]
public class SaleBudgetPlan
{
    [Key]
    [Column("ID")]
    public int Id { get; set; }

    [Column("RUN_ID")]
    [StringLength(50)]
    public string? RunId { get; set; }

    // Store dimensions
    [Column("STORE_CODE")]
    [StringLength(20)]
    public string? StoreCode { get; set; }

    [Column("STORE_NAME")]
    [StringLength(200)]
    public string? StoreName { get; set; }

    [Column("STATE")]
    [StringLength(50)]
    public string? State { get; set; }

    [Column("ZONE")]
    [StringLength(50)]
    public string? Zone { get; set; }

    [Column("REGION")]
    [StringLength(50)]
    public string? Region { get; set; }

    [Column("SIZE_CATEGORY")]
    [StringLength(20)]
    public string? SizeCategory { get; set; }

    [Column("OLD_NEW")]
    [StringLength(10)]
    public string? OldNew { get; set; }

    // Category dimensions
    [Column("MAJOR_CATEGORY")]
    [StringLength(100)]
    public string? MajorCategory { get; set; }

    [Column("DIVISION")]
    [StringLength(100)]
    public string? Division { get; set; }

    [Column("SUBDIVISION")]
    [StringLength(100)]
    public string? Subdivision { get; set; }

    [Column("SEGMENT")]
    [StringLength(100)]
    public string? Segment { get; set; }

    // Time
    [Column("PLAN_MONTH")]
    public DateTime? PlanMonth { get; set; }

    // LYSP
    [Column("LYSP_SALE_QTY")]
    [Precision(18, 4)]
    public decimal? LyspSaleQty { get; set; }

    [Column("LYSP_SALE_VAL")]
    [Precision(18, 4)]
    public decimal? LyspSaleVal { get; set; }

    [Column("LYSP_GM_VAL")]
    [Precision(18, 4)]
    public decimal? LyspGmVal { get; set; }

    // Growth rates
    [Column("GROWTH_RATE_ST_CAT")]
    [Precision(18, 6)]
    public decimal? GrowthRateStCat { get; set; }

    [Column("GROWTH_RATE_CATEGORY")]
    [Precision(18, 6)]
    public decimal? GrowthRateCategory { get; set; }

    [Column("GROWTH_RATE_STORE")]
    [Precision(18, 6)]
    public decimal? GrowthRateStore { get; set; }

    [Column("GROWTH_RATE_COMBINED")]
    [Precision(18, 6)]
    public decimal? GrowthRateCombined { get; set; }

    // Adjustments
    [Column("FILL_RATE_ADJ")]
    [Precision(18, 6)]
    public decimal? FillRateAdj { get; set; }

    [Column("FESTIVAL_ADJ")]
    [Precision(18, 6)]
    public decimal? FestivalAdj { get; set; }

    // ML forecast
    [Column("ML_FORECAST_QTY")]
    [Precision(18, 4)]
    public decimal? MlForecastQty { get; set; }

    [Column("ML_FORECAST_LOW")]
    [Precision(18, 4)]
    public decimal? MlForecastLow { get; set; }

    [Column("ML_FORECAST_HIGH")]
    [Precision(18, 4)]
    public decimal? MlForecastHigh { get; set; }

    [Column("ML_FORECAST_MAPE")]
    [Precision(18, 4)]
    public decimal? MlForecastMape { get; set; }

    [Column("ML_BEST_METHOD")]
    [StringLength(50)]
    public string? MlBestMethod { get; set; }

    // Final budget
    [Column("BGT_SALE_QTY")]
    [Precision(18, 4)]
    public decimal? BgtSaleQty { get; set; }

    [Column("BGT_SALE_VAL")]
    [Precision(18, 4)]
    public decimal? BgtSaleVal { get; set; }

    [Column("BGT_GM_VAL")]
    [Precision(18, 4)]
    public decimal? BgtGmVal { get; set; }

    [Column("AVG_SELLING_PRICE")]
    [Precision(18, 4)]
    public decimal? AvgSellingPrice { get; set; }

    // Tracking
    [Column("ALGO_METHOD")]
    [StringLength(20)]
    public string? AlgoMethod { get; set; }

    [Column("STORE_CONT_PCT")]
    [Precision(18, 8)]
    public decimal? StoreContPct { get; set; }

    [Column("CREATED_DT")]
    public DateTime? CreatedDt { get; set; }

    // Actual sales (populated via LEFT JOIN with STG_SF_SALE_ACTUAL, not mapped to SALE_BUDGET_PLAN)
    [NotMapped]
    public decimal? ActualSaleQty { get; set; }
    [NotMapped]
    public decimal? ActualSaleVal { get; set; }
}
