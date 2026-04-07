namespace TRANSFER_IN_PLAN.Models;

public class DashboardViewModel
{
    // ===== EXECUTIVE KPIs =====
    public int TotalStores { get; set; }
    public int TotalCategories { get; set; }
    public int TotalRdcs { get; set; }
    public int TotalPlanRows { get; set; }
    public int TotalPurchasePlanRows { get; set; }
    public DateTime? LastExecutionDate { get; set; }

    // ===== TRANSFER IN TOTALS =====
    public decimal TotalTrfInQty { get; set; }
    public decimal TotalOpStkQty { get; set; }
    public decimal TotalClStkQty { get; set; }
    public decimal TotalSaleQty { get; set; }
    public decimal TotalStShortQ { get; set; }
    public decimal TotalStExcessQ { get; set; }

    // ===== PURCHASE PLAN TOTALS =====
    public decimal TotalPurchaseQty { get; set; }
    public decimal TotalDcStkShortQ { get; set; }
    public decimal TotalDcStkExcessQ { get; set; }
    public decimal TotalStStkShortQ { get; set; }
    public decimal TotalStStkExcessQ { get; set; }
    public decimal TotalCoShortQ { get; set; }
    public decimal TotalCoExcessQ { get; set; }
    public decimal TotalPosPO { get; set; }
    public decimal TotalNegPO { get; set; }
    public decimal TotalDelPendQ { get; set; }
    public decimal TotalTrfOutQ { get; set; }

    // ===== ALERT COUNTS =====
    public int CriticalShortStores { get; set; }
    public int ExcessStores { get; set; }
    public int DcShortCategories { get; set; }
    public int DcExcessCategories { get; set; }
    public int ZeroStockStoreCategories { get; set; }

    // ===== TRANSFER IN CHARTS/TABLES =====
    public List<WeeklySummary> WeeklySummary { get; set; } = new();
    public List<CategorySummary> CategorySummary { get; set; } = new();
    public List<StoreMetric> TopShortStores { get; set; } = new();
    public List<StoreMetric> TopExcessStores { get; set; } = new();
    public List<RdcSummary> RdcSummary { get; set; } = new();

    // ===== PURCHASE PLAN CHARTS/TABLES =====
    public List<PpCategorySummary> PpCategorySummary { get; set; } = new();
    public List<PpWeeklySummary> PpWeeklySummary { get; set; } = new();
    public List<DcStockMetric> TopDcShortCategories { get; set; } = new();
    public List<DcStockMetric> TopDcExcessCategories { get; set; } = new();

    // ===== INVENTORY HEALTH =====
    public List<RdcInventoryHealth> RdcInventoryHealth { get; set; } = new();
    public List<CategoryRisk> TopRiskCategories { get; set; } = new();

    // ===== SUB-LEVEL PLANS =====
    public List<SubLevelStatus> SubLevelStatuses { get; set; } = new();

    // ===== DATA HEALTH =====
    public List<DataHealthRow> DataHealth { get; set; } = new();
}

// ===== SUPPORTING CLASSES =====

public class WeeklySummary
{
    public int FyWeek { get; set; }
    public int FyYear { get; set; }
    public decimal TotalTrfInQty { get; set; }
    public decimal TotalSaleQty { get; set; }
    public decimal TotalOpStk { get; set; }
    public decimal TotalClStk { get; set; }
    public int RowCount { get; set; }
}

public class CategorySummary
{
    public string? MajCat { get; set; }
    public decimal TotalTrfInQty { get; set; }
    public decimal TotalShortQ { get; set; }
    public decimal TotalExcessQ { get; set; }
    public decimal TotalSaleQ { get; set; }
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
    public decimal TotalSaleQty { get; set; }
    public int StoreCount { get; set; }
}

public class PpCategorySummary
{
    public string? MajCat { get; set; }
    public decimal BgtPurQ { get; set; }
    public decimal DcStkShortQ { get; set; }
    public decimal DcStkExcessQ { get; set; }
    public decimal StStkShortQ { get; set; }
    public decimal StStkExcessQ { get; set; }
    public decimal TrfOutQ { get; set; }
    public int RowCount { get; set; }
}

public class PpWeeklySummary
{
    public int FyYear { get; set; }
    public int FyWeek { get; set; }
    public decimal BgtPurQ { get; set; }
    public decimal TrfOutQ { get; set; }
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

public class RdcInventoryHealth
{
    public string? RdcCd { get; set; }
    public string? RdcNm { get; set; }
    public decimal DcStock { get; set; }
    public decimal TrfOut { get; set; }
    public decimal PurchaseQty { get; set; }
    public decimal DcShort { get; set; }
    public decimal DcExcess { get; set; }
    public int CategoryCount { get; set; }
}

public class CategoryRisk
{
    public string? MajCat { get; set; }
    public decimal CoShort { get; set; }
    public decimal CoExcess { get; set; }
    public decimal DcShort { get; set; }
    public decimal StShort { get; set; }
    public string Risk { get; set; } = "OK";
}

public class SubLevelStatus
{
    public string Level { get; set; } = "";
    public string Label { get; set; } = "";
    public int TrfRows { get; set; }
    public int PpRows { get; set; }
    public string? LastRun { get; set; }
}

public class DataHealthRow
{
    public string Category { get; set; } = "";
    public string Table { get; set; } = "";
    public int Rows { get; set; }
    public string Icon { get; set; } = "bi-table";
}
