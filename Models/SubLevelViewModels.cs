namespace TRANSFER_IN_PLAN.Models;

public class SubLevelTrfRow
{
    public string StCd { get; set; } = "";
    public string MajCat { get; set; } = "";
    public string SubLevel { get; set; } = "";
    public decimal ContPct { get; set; }
    public int FyYear { get; set; }
    public int FyWeek { get; set; }
    // Sale Qty split by contribution
    public decimal CmBgtSaleQ { get; set; }
    public decimal Cm1BgtSaleQ { get; set; }
    public decimal Cm2BgtSaleQ { get; set; }
    public decimal CoverSaleQty { get; set; }
    // Display Qty split by contribution
    public decimal BgtDispClQ { get; set; }
    // Main-level reference columns (not split)
    public decimal TrfInStkQ { get; set; }
    public decimal DcMbq { get; set; }
    public decimal BgtTtlCfOpStkQ { get; set; }
    public decimal BgtTtlCfClStkQ { get; set; }
    public decimal BgtStClMbq { get; set; }
    public decimal StClExcessQ { get; set; }
    public decimal StClShortQ { get; set; }
}

public class SubLevelPpRow
{
    public string RdcCd { get; set; } = "";
    public string MajCat { get; set; } = "";
    public string SubLevel { get; set; } = "";
    public decimal ContPct { get; set; }
    public int FyYear { get; set; }
    public int FyWeek { get; set; }
    // Sale Qty split by contribution
    public decimal CwBgtSaleQ { get; set; }
    public decimal Cw1BgtSaleQ { get; set; }
    public decimal Cw2BgtSaleQ { get; set; }
    public decimal Cw3BgtSaleQ { get; set; }
    public decimal Cw4BgtSaleQ { get; set; }
    // Display Qty split by contribution
    public decimal BgtDispClQ { get; set; }
    // Main-level reference columns (not split)
    public decimal BgtPurQInit { get; set; }
    public decimal BgtDcClStkQ { get; set; }
    public decimal BgtDcClMbq { get; set; }
    public decimal BgtDcMbqSale { get; set; }
    public decimal DcStkExcessQ { get; set; }
    public decimal DcStkShortQ { get; set; }
}
