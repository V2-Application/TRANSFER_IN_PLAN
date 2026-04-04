using System;
using System.Collections.Generic;

namespace TRANSFER_IN_PLAN.Models
{
    public class DashboardViewModel
    {
        // KPI Counters
        public int TotalStores { get; set; }
        public int TotalCategories { get; set; }
        public int TotalPlanRows { get; set; }
        public int TotalPurchasePlanRows { get; set; }
        public DateTime? LastExecutionDate { get; set; }

        // Aggregate Totals
        public decimal TotalTrfInQty { get; set; }
        public decimal TotalPurchaseQty { get; set; }
        public decimal TotalDcStkShortQ { get; set; }
        public decimal TotalDcStkExcessQ { get; set; }
        public decimal TotalStStkShortQ { get; set; }
        public decimal TotalStStkExcessQ { get; set; }

        // Transfer In Plan Charts / Tables
        public List<WeeklySummary> WeeklySummary { get; set; } = new();
        public List<CategorySummary> CategorySummary { get; set; } = new();
        public List<StoreMetric> TopShortStores { get; set; } = new();
        public List<StoreMetric> TopExcessStores { get; set; } = new();
        public List<RdcSummary> RdcSummary { get; set; } = new();

        // Purchase Plan Charts / Tables
        public List<PpCategorySummary> PpCategorySummary { get; set; } = new();
        public List<PpWeeklySummary> PpWeeklySummary { get; set; } = new();
        public List<DcStockMetric> TopDcShortCategories { get; set; } = new();
        public List<DcStockMetric> TopDcExcessCategories { get; set; } = new();
    }

    // Transfer In Plan summary classes
    public class WeeklySummary
    {
        public int FyWeek { get; set; }
        public int FyYear { get; set; }
        public decimal TotalTrfInQty { get; set; }
        public int RowCount { get; set; }
    }

    public class CategorySummary
    {
        public string? MajCat { get; set; }
        public decimal TotalTrfInQty { get; set; }
        public decimal TotalShortQ { get; set; }
        public decimal TotalExcessQ { get; set; }
        public int RowCount { get; set; }
    }

    public class StoreMetric
    {
        public string? StCd { get; set; }
        public string? StNm { get; set; }
        public string? MajCat { get; set; }
        public decimal Quantity { get; set; }
    }

    public class RdcSummary
    {
        public string? RdcCd { get; set; }
        public string? RdcNm { get; set; }
        public decimal TotalTrfInQty { get; set; }
        public int StoreCount { get; set; }
    }

    // Purchase Plan summary classes
    public class PpCategorySummary
    {
        public string? MajCat { get; set; }
        public decimal BgtPurQ { get; set; }
        public decimal DcStkShortQ { get; set; }
        public decimal DcStkExcessQ { get; set; }
        public decimal StStkShortQ { get; set; }
        public decimal StStkExcessQ { get; set; }
        public int RowCount { get; set; }
    }

    public class PpWeeklySummary
    {
        public int FyYear { get; set; }
        public int FyWeek { get; set; }
        public decimal BgtPurQ { get; set; }
        public decimal DcStkShortQ { get; set; }
        public decimal DcStkExcessQ { get; set; }
    }

    public class DcStockMetric
    {
        public string? RdcCd { get; set; }
        public string? RdcNm { get; set; }
        public string? MajCat { get; set; }
        public decimal Quantity { get; set; }
    }
}
