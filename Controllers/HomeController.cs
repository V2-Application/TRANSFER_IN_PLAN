using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Memory;
using System.Text;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class HomeController : Controller
    {
        private readonly PlanningDbContext _context;
        private readonly ILogger<HomeController> _logger;
        private readonly IMemoryCache _cache;
        private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(60);

        public HomeController(PlanningDbContext context, ILogger<HomeController> logger, IMemoryCache cache) { _context = context; _logger = logger; _cache = cache; }

        public async Task<IActionResult> Index(string? rdcCd, string? majCat, string? fyWeek)
        {
            // Drill-down filter dropdowns (cached separately — shared across all users)
            ViewBag.RdcCodes = await _cache.GetOrCreateAsync("dd_rdc", async e => { e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5); return await _context.TrfInPlans.Select(x => x.RdcCd).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync(); });
            ViewBag.MajCats = await _cache.GetOrCreateAsync("dd_cat", async e => { e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5); return await _context.TrfInPlans.Select(x => x.MajCat).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync(); });
            ViewBag.FyWeeks = await _cache.GetOrCreateAsync("dd_wk", async e => { e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5); return await _context.TrfInPlans.Select(x => x.FyWeek).Where(x => x != null).Distinct().OrderBy(x => x).ToListAsync(); });
            ViewBag.SelRdc = rdcCd; ViewBag.SelMajCat = majCat; ViewBag.SelFyWeek = fyWeek;

            // Cache key per filter combination
            var cacheKey = $"dash_{rdcCd ?? "ALL"}_{majCat ?? "ALL"}_{fyWeek ?? "ALL"}";

            var vm = await _cache.GetOrCreateAsync(cacheKey, async entry =>
            {
                entry.AbsoluteExpirationRelativeToNow = CacheDuration;
                _logger.LogInformation("Dashboard cache MISS: {Key} - querying DB", cacheKey);
                return await LoadDashboardData(rdcCd, majCat, fyWeek);
            }) ?? new DashboardViewModel();

            return View(vm);
        }

        private async Task<DashboardViewModel> LoadDashboardData(string? rdcCd, string? majCat, string? fyWeek)
        {
            // Build WHERE clauses for raw SQL (parameterized IN lists for multi-select)
            var trfWhere = "WHERE 1=1";
            var ppWhere = "WHERE 1=1";
            if (!string.IsNullOrEmpty(rdcCd))
            {
                var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => $"'{v.Replace("'","''").Trim()}'");
                var inList = string.Join(",", rdcs);
                trfWhere += $" AND [RDC_CD] IN ({inList})";
                ppWhere += $" AND [RDC_CD] IN ({inList})";
            }
            if (!string.IsNullOrEmpty(majCat))
            {
                var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => $"'{v.Replace("'","''").Trim()}'");
                var inList = string.Join(",", cats);
                trfWhere += $" AND [MAJ_CAT] IN ({inList})";
                ppWhere += $" AND [MAJ_CAT] IN ({inList})";
            }
            if (!string.IsNullOrEmpty(fyWeek))
            {
                var weeks = fyWeek.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => v.Trim()).Where(v => int.TryParse(v, out _));
                var inList = string.Join(",", weeks);
                if (!string.IsNullOrEmpty(inList)) { trfWhere += $" AND [FY_WEEK] IN ({inList})"; ppWhere += $" AND [FY_WEEK] IN ({inList})"; }
            }

            var vm = new DashboardViewModel();
            try
            {
                // ===== ALL TRF_IN_PLAN KPIs in ONE query =====
                var connStr = _context.Database.GetConnectionString()!;
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr))
                {
                    await conn.OpenAsync();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT
                            COUNT(DISTINCT [ST_CD]) AS TotalStores,
                            COUNT(DISTINCT [MAJ_CAT]) AS TotalCategories,
                            COUNT(DISTINCT [RDC_CD]) AS TotalRdcs,
                            COUNT(*) AS TotalRows,
                            MAX([CREATED_DT]) AS LastRun,
                            ISNULL(SUM([TRF_IN_STK_Q]),0) AS TrfInQty,
                            ISNULL(SUM([BGT_TTL_CF_OP_STK_Q]),0) AS OpStkQty,
                            ISNULL(SUM([BGT_TTL_CF_CL_STK_Q]),0) AS ClStkQty,
                            ISNULL(SUM([CM_BGT_SALE_Q]),0) AS SaleQty,
                            ISNULL(SUM([ST_CL_SHORT_Q]),0) AS StShortQ,
                            ISNULL(SUM([ST_CL_EXCESS_Q]),0) AS StExcessQ,
                            COUNT(DISTINCT CASE WHEN [ST_CL_SHORT_Q] > [BGT_ST_CL_MBQ] * 0.3 THEN [ST_CD] END) AS CritShort,
                            COUNT(DISTINCT CASE WHEN [ST_CL_EXCESS_Q] > 0 THEN [ST_CD] END) AS ExcessSt,
                            SUM(CASE WHEN [BGT_TTL_CF_CL_STK_Q] <= 0 AND [CM_BGT_SALE_Q] > 0 THEN 1 ELSE 0 END) AS ZeroStk
                        FROM [dbo].[TRF_IN_PLAN] WITH (NOLOCK) " + trfWhere;
                    cmd.CommandTimeout = 120;
                    using var r1 = await cmd.ExecuteReaderAsync();
                    if (await r1.ReadAsync())
                    {
                        vm.TotalStores = r1.GetInt32(0);
                        vm.TotalCategories = r1.GetInt32(1);
                        vm.TotalRdcs = r1.GetInt32(2);
                        vm.TotalPlanRows = r1.GetInt32(3);
                        vm.LastExecutionDate = r1.IsDBNull(4) ? null : r1.GetDateTime(4);
                        vm.TotalTrfInQty = r1.GetDecimal(5);
                        vm.TotalOpStkQty = r1.GetDecimal(6);
                        vm.TotalClStkQty = r1.GetDecimal(7);
                        vm.TotalSaleQty = r1.GetDecimal(8);
                        vm.TotalStShortQ = r1.GetDecimal(9);
                        vm.TotalStExcessQ = r1.GetDecimal(10);
                        vm.CriticalShortStores = r1.GetInt32(11);
                        vm.ExcessStores = r1.GetInt32(12);
                        vm.ZeroStockStoreCategories = r1.GetInt32(13);
                    }
                }

                // ===== ALL PURCHASE_PLAN KPIs in ONE query =====
                using (var conn = new Microsoft.Data.SqlClient.SqlConnection(connStr))
                {
                    await conn.OpenAsync();
                    using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT
                            COUNT(*) AS TotalRows,
                            ISNULL(SUM([BGT_PUR_Q_INIT]),0) AS PurQty,
                            ISNULL(SUM([DC_STK_SHORT_Q]),0) AS DcShort,
                            ISNULL(SUM([DC_STK_EXCESS_Q]),0) AS DcExcess,
                            ISNULL(SUM([ST_STK_SHORT_Q]),0) AS StShort,
                            ISNULL(SUM([ST_STK_EXCESS_Q]),0) AS StExcess,
                            ISNULL(SUM([CO_STK_SHORT_Q]),0) AS CoShort,
                            ISNULL(SUM([CO_STK_EXCESS_Q]),0) AS CoExcess,
                            ISNULL(SUM([POS_PO_RAISED]),0) AS PosPO,
                            ISNULL(SUM([NEG_PO_RAISED]),0) AS NegPO,
                            ISNULL(SUM([DEL_PEND_Q]),0) AS DelPend,
                            ISNULL(SUM([TTL_TRF_OUT_Q]),0) AS TrfOut,
                            (SELECT COUNT(DISTINCT CONCAT([RDC_CD],'|',[MAJ_CAT])) FROM [dbo].[PURCHASE_PLAN] WITH (NOLOCK) " + ppWhere + @" AND [DC_STK_SHORT_Q] > 0) AS DcShortCat,
                            (SELECT COUNT(DISTINCT CONCAT([RDC_CD],'|',[MAJ_CAT])) FROM [dbo].[PURCHASE_PLAN] WITH (NOLOCK) " + ppWhere + @" AND [DC_STK_EXCESS_Q] > 0) AS DcExcessCat
                        FROM [dbo].[PURCHASE_PLAN] WITH (NOLOCK) " + ppWhere;
                    cmd.CommandTimeout = 120;
                    using var r2 = await cmd.ExecuteReaderAsync();
                    if (await r2.ReadAsync())
                    {
                        vm.TotalPurchasePlanRows = r2.GetInt32(0);
                        vm.TotalPurchaseQty = r2.GetDecimal(1);
                        vm.TotalDcStkShortQ = r2.GetDecimal(2);
                        vm.TotalDcStkExcessQ = r2.GetDecimal(3);
                        vm.TotalStStkShortQ = r2.GetDecimal(4);
                        vm.TotalStStkExcessQ = r2.GetDecimal(5);
                        vm.TotalCoShortQ = r2.GetDecimal(6);
                        vm.TotalCoExcessQ = r2.GetDecimal(7);
                        vm.TotalPosPO = r2.GetDecimal(8);
                        vm.TotalNegPO = r2.GetDecimal(9);
                        vm.TotalDelPendQ = r2.GetDecimal(10);
                        vm.TotalTrfOutQ = r2.GetDecimal(11);
                        vm.DcShortCategories = r2.GetInt32(12);
                        vm.DcExcessCategories = r2.GetInt32(13);
                    }
                }

                // ===== APPLY FILTERS =====
                var trfQ = _context.TrfInPlans.AsQueryable();
                var ppQ = _context.PurchasePlans.AsQueryable();
                if (!string.IsNullOrEmpty(rdcCd)) { var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries); trfQ = trfQ.Where(x => rdcs.Contains(x.RdcCd)); ppQ = ppQ.Where(x => rdcs.Contains(x.RdcCd)); }
                if (!string.IsNullOrEmpty(majCat)) { var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries); trfQ = trfQ.Where(x => cats.Contains(x.MajCat)); ppQ = ppQ.Where(x => cats.Contains(x.MajCat)); }
                if (!string.IsNullOrEmpty(fyWeek)) { var weeks = fyWeek.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(w => int.TryParse(w, out var v) ? v : (int?)null).Where(w => w.HasValue).Select(w => w!.Value).ToList(); trfQ = trfQ.Where(x => weeks.Contains(x.FyWeek ?? 0)); ppQ = ppQ.Where(x => weeks.Contains(x.FyWeek ?? 0)); }

                // ===== TRANSFER IN: CATEGORY SUMMARY (Top 15) =====
                vm.CategorySummary = await trfQ
                    .GroupBy(x => x.MajCat)
                    .Select(g => new CategorySummary
                    {
                        MajCat = g.Key,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        TotalShortQ = g.Sum(x => x.StClShortQ ?? 0),
                        TotalExcessQ = g.Sum(x => x.StClExcessQ ?? 0),
                        TotalSaleQ = g.Sum(x => x.CmBgtSaleQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderByDescending(x => x.TotalTrfInQty).Take(15).ToListAsync();

                // ===== TRANSFER IN: WEEKLY TREND =====
                vm.WeeklySummary = await trfQ
                    .GroupBy(x => new { x.FyYear, x.FyWeek })
                    .Select(g => new WeeklySummary
                    {
                        FyYear = g.Key.FyYear ?? 0,
                        FyWeek = g.Key.FyWeek ?? 0,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        TotalSaleQty = g.Sum(x => x.CmBgtSaleQ ?? 0),
                        TotalOpStk = g.Sum(x => x.BgtTtlCfOpStkQ ?? 0),
                        TotalClStk = g.Sum(x => x.BgtTtlCfClStkQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek).ToListAsync();

                // ===== TRANSFER IN: TOP SHORT/EXCESS STORES =====
                vm.TopShortStores = await trfQ
                    .Where(x => x.StClShortQ > 0)
                    .GroupBy(x => new { x.StCd, x.StNm, x.MajCat })
                    .Select(g => new StoreMetric
                    {
                        StCd = g.Key.StCd, StNm = g.Key.StNm, MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.StClShortQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                vm.TopExcessStores = await trfQ
                    .Where(x => x.StClExcessQ > 0)
                    .GroupBy(x => new { x.StCd, x.StNm, x.MajCat })
                    .Select(g => new StoreMetric
                    {
                        StCd = g.Key.StCd, StNm = g.Key.StNm, MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.StClExcessQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                // ===== TRANSFER IN: RDC SUMMARY =====
                vm.RdcSummary = await trfQ
                    .GroupBy(x => new { x.RdcCd, x.RdcNm })
                    .Select(g => new RdcSummary
                    {
                        RdcCd = g.Key.RdcCd, RdcNm = g.Key.RdcNm,
                        TotalTrfInQty = g.Sum(x => x.TrfInStkQ ?? 0),
                        TotalSaleQty = g.Sum(x => x.CmBgtSaleQ ?? 0),
                        StoreCount = g.Select(x => x.StCd).Distinct().Count()
                    })
                    .OrderByDescending(x => x.TotalTrfInQty).Take(10).ToListAsync();

                // ===== PURCHASE PLAN: CATEGORY SUMMARY (Top 15) =====
                vm.PpCategorySummary = await ppQ
                    .GroupBy(x => x.MajCat)
                    .Select(g => new PpCategorySummary
                    {
                        MajCat = g.Key,
                        BgtPurQ = g.Sum(x => x.BgtPurQInit ?? 0),
                        DcStkShortQ = g.Sum(x => x.DcStkShortQ ?? 0),
                        DcStkExcessQ = g.Sum(x => x.DcStkExcessQ ?? 0),
                        StStkShortQ = g.Sum(x => x.StStkShortQ ?? 0),
                        StStkExcessQ = g.Sum(x => x.StStkExcessQ ?? 0),
                        TrfOutQ = g.Sum(x => x.TtlTrfOutQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderByDescending(x => x.BgtPurQ).Take(15).ToListAsync();

                // ===== PURCHASE PLAN: WEEKLY TREND =====
                vm.PpWeeklySummary = await ppQ
                    .GroupBy(x => new { x.FyYear, x.FyWeek })
                    .Select(g => new PpWeeklySummary
                    {
                        FyYear = g.Key.FyYear ?? 0,
                        FyWeek = g.Key.FyWeek ?? 0,
                        BgtPurQ = g.Sum(x => x.BgtPurQInit ?? 0),
                        TrfOutQ = g.Sum(x => x.TtlTrfOutQ ?? 0),
                        DcStkShortQ = g.Sum(x => x.DcStkShortQ ?? 0),
                        DcStkExcessQ = g.Sum(x => x.DcStkExcessQ ?? 0)
                    })
                    .OrderBy(x => x.FyYear).ThenBy(x => x.FyWeek).ToListAsync();

                // ===== PURCHASE PLAN: TOP DC SHORT/EXCESS =====
                vm.TopDcShortCategories = await ppQ
                    .Where(x => x.DcStkShortQ > 0)
                    .GroupBy(x => new { x.RdcCd, x.RdcNm, x.MajCat })
                    .Select(g => new DcStockMetric
                    {
                        RdcCd = g.Key.RdcCd, RdcNm = g.Key.RdcNm, MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.DcStkShortQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                vm.TopDcExcessCategories = await ppQ
                    .Where(x => x.DcStkExcessQ > 0)
                    .GroupBy(x => new { x.RdcCd, x.RdcNm, x.MajCat })
                    .Select(g => new DcStockMetric
                    {
                        RdcCd = g.Key.RdcCd, RdcNm = g.Key.RdcNm, MajCat = g.Key.MajCat,
                        Quantity = g.Sum(x => x.DcStkExcessQ ?? 0)
                    })
                    .OrderByDescending(x => x.Quantity).Take(10).ToListAsync();

                // ===== RDC INVENTORY HEALTH =====
                vm.RdcInventoryHealth = await ppQ
                    .GroupBy(x => new { x.RdcCd, x.RdcNm })
                    .Select(g => new RdcInventoryHealth
                    {
                        RdcCd = g.Key.RdcCd, RdcNm = g.Key.RdcNm,
                        DcStock = g.Sum(x => x.DcStkQ ?? 0),
                        TrfOut = g.Sum(x => x.TtlTrfOutQ ?? 0),
                        PurchaseQty = g.Sum(x => x.BgtPurQInit ?? 0),
                        DcShort = g.Sum(x => x.DcStkShortQ ?? 0),
                        DcExcess = g.Sum(x => x.DcStkExcessQ ?? 0),
                        CategoryCount = g.Select(x => x.MajCat).Distinct().Count()
                    })
                    .OrderBy(x => x.RdcCd).ToListAsync();

                // ===== TOP RISK CATEGORIES (by company short) =====
                vm.TopRiskCategories = await ppQ
                    .GroupBy(x => x.MajCat)
                    .Select(g => new CategoryRisk
                    {
                        MajCat = g.Key,
                        CoShort = g.Sum(x => x.CoStkShortQ ?? 0),
                        CoExcess = g.Sum(x => x.CoStkExcessQ ?? 0),
                        DcShort = g.Sum(x => x.DcStkShortQ ?? 0),
                        StShort = g.Sum(x => x.StStkShortQ ?? 0)
                    })
                    .OrderByDescending(x => x.CoShort).Take(10).ToListAsync();

                foreach (var c in vm.TopRiskCategories)
                    c.Risk = c.CoShort > 1000 ? "CRITICAL" : c.CoShort > 0 ? "AT RISK" : c.CoExcess > 1000 ? "EXCESS" : "OK";

                // ===== SUB-LEVEL PLAN STATUS =====
                try
                {
                    using var connSub = new Microsoft.Data.SqlClient.SqlConnection(connStr);
                    await connSub.OpenAsync();
                    foreach (var (lv, lbl) in new[] { ("MVGR","Macro MVGR"), ("SZ","Size"), ("SEG","Segment"), ("VND","Vendor") })
                    {
                        int trfR = 0, ppR = 0; string? lr = null;
                        try
                        {
                            using (var c = connSub.CreateCommand()) { c.CommandText = $"SELECT COUNT(*), MAX([CREATED_DT]) FROM [dbo].[SUB_LEVEL_TRF_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{lv}'"; c.CommandTimeout = 15; using var rd = await c.ExecuteReaderAsync(); if (await rd.ReadAsync()) { trfR = rd.GetInt32(0); if (!rd.IsDBNull(1)) lr = rd.GetDateTime(1).ToString("dd-MMM HH:mm"); } }
                            using (var c = connSub.CreateCommand()) { c.CommandText = $"SELECT COUNT(*) FROM [dbo].[SUB_LEVEL_PP_PLAN] WITH (NOLOCK) WHERE [LEVEL]='{lv}'"; c.CommandTimeout = 15; ppR = (int)(await c.ExecuteScalarAsync() ?? 0); }
                        }
                        catch { }
                        vm.SubLevelStatuses.Add(new SubLevelStatus { Level = lv, Label = lbl, TrfRows = trfR, PpRows = ppR, LastRun = lr });
                    }
                }
                catch { }

                // ===== DATA HEALTH (reference table row counts) =====
                try
                {
                    using var connH = new Microsoft.Data.SqlClient.SqlConnection(connStr);
                    await connH.OpenAsync();
                    var tables = new[] {
                        ("Plan Output", "TRF_IN_PLAN", "bi-arrow-left-right"),
                        ("Plan Output", "PURCHASE_PLAN", "bi-bag-check"),
                        ("Reference", "WEEK_CALENDAR", "bi-calendar-week"),
                        ("Reference", "MASTER_ST_MASTER", "bi-shop"),
                        ("Reference", "MASTER_BIN_CAPACITY", "bi-box"),
                        ("Reference", "QTY_SALE_QTY", "bi-graph-up"),
                        ("Reference", "QTY_DISP_QTY", "bi-display"),
                        ("Reference", "STORE_STOCK", "bi-boxes"),
                        ("Reference", "DC_STOCK", "bi-building"),
                        ("Reference", "DEL_PENDING", "bi-truck"),
                        ("Contribution", "ST_MAJ_CAT_MACRO_MVGR_PLAN", "bi-pie-chart"),
                        ("Contribution", "ST_MAJ_CAT_SZ_PLAN", "bi-pie-chart"),
                        ("Contribution", "ST_MAJ_CAT_SEG_PLAN", "bi-pie-chart"),
                        ("Contribution", "ST_MAJ_CAT_VND_PLAN", "bi-pie-chart"),
                    };
                    foreach (var (cat, tbl, icon) in tables)
                    {
                        try
                        {
                            using var c = connH.CreateCommand();
                            c.CommandText = $"SELECT COUNT(*) FROM [dbo].[{tbl}] WITH (NOLOCK)";
                            c.CommandTimeout = 10;
                            var cnt = (int)(await c.ExecuteScalarAsync() ?? 0);
                            vm.DataHealth.Add(new DataHealthRow { Category = cat, Table = tbl, Rows = cnt, Icon = icon });
                        }
                        catch { vm.DataHealth.Add(new DataHealthRow { Category = cat, Table = tbl, Rows = -1, Icon = icon }); }
                    }
                }
                catch { }

                _logger.LogInformation("Dashboard loaded: Stores={S} Cat={C} TrfIn={T} PP={P}",
                    vm.TotalStores, vm.TotalCategories, vm.TotalPlanRows, vm.TotalPurchasePlanRows);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading dashboard");
            }
            return vm;
        }

        [HttpGet]
        public async Task<IActionResult> ExportTrfInCsv(int? fyYear, int? fyWeek, string? majCat, string? stCd)
        {
            var query = _context.TrfInPlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            if (!string.IsNullOrEmpty(stCd)) query = query.Where(x => x.StCd == stCd);
            var data = await query.OrderBy(x => x.StCd).ThenBy(x => x.MajCat).ToListAsync();
            var sb = new StringBuilder();
            sb.AppendLine("StCd,StNm,RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,SGrtStkQ,WGrtStkQ,BgtDispClQ,BgtDispClOpt,Cm1SaleCoverDay,Cm2SaleCoverDay,CoverSaleQty,BgtStClMbq,BgtDispClOptMbq,BgtTtlCfOpStkQ,NtActQ,NetBgtCfStkQ,CmBgtSaleQ,Cm1BgtSaleQ,Cm2BgtSaleQ,TrfInStkQ,TrfInOptCnt,TrfInOptMbq,DcMbq,BgtTtlCfClStkQ,BgtNtActQ,NetStClStkQ,StClExcessQ,StClShortQ");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", Q(r.StCd), Q(r.StNm ?? "NA"), Q(r.RdcCd ?? "NA"), Q(r.RdcNm ?? "NA"), Q(r.MajCat), Q(r.Seg ?? "NA"), Q(r.Div ?? "NA"), Q(r.SubDiv ?? "NA"), Q(r.MajCatNm ?? "NA"), Q(r.Ssn ?? "NA"), r.FyYear, r.FyWeek, r.SGrtStkQ ?? 0, r.WGrtStkQ ?? 0, r.BgtDispClQ ?? 0, r.BgtDispClOpt ?? 0, r.Cm1SaleCoverDay ?? 0, r.Cm2SaleCoverDay ?? 0, r.CoverSaleQty ?? 0, r.BgtStClMbq ?? 0, r.BgtDispClOptMbq ?? 0, r.BgtTtlCfOpStkQ ?? 0, r.NtActQ ?? 0, r.NetBgtCfStkQ ?? 0, r.CmBgtSaleQ ?? 0, r.Cm1BgtSaleQ ?? 0, r.Cm2BgtSaleQ ?? 0, r.TrfInStkQ ?? 0, r.TrfInOptCnt ?? 0, r.TrfInOptMbq ?? 0, r.DcMbq ?? 0, r.BgtTtlCfClStkQ ?? 0, r.BgtNtActQ ?? 0, r.NetStClStkQ ?? 0, r.StClExcessQ ?? 0, r.StClShortQ ?? 0));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "TrfInPlan_Export.csv");
        }

        [HttpGet]
        public async Task<IActionResult> ExportPpCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var query = _context.PurchasePlans.AsQueryable();
            if (fyYear.HasValue) query = query.Where(x => x.FyYear == fyYear);
            if (fyWeek.HasValue) query = query.Where(x => x.FyWeek == fyWeek);
            if (!string.IsNullOrEmpty(rdcCd)) query = query.Where(x => x.RdcCd == rdcCd);
            if (!string.IsNullOrEmpty(majCat)) query = query.Where(x => x.MajCat == majCat);
            var data = await query.OrderBy(x => x.RdcCd).ThenBy(x => x.MajCat).ToListAsync();
            var sb = new StringBuilder();
            sb.AppendLine("RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,DcStkQ,GrtStkQ,BgtPurQInit,PosPORaised,NegPORaised,TtlTrfOutQ,DcStkExcessQ,DcStkShortQ,StStkExcessQ,StStkShortQ,CoStkExcessQ,CoStkShortQ,FreshBinReq,GrtBinReq");
            foreach (var r in data)
                sb.AppendLine(string.Join(",", Q(r.RdcCd), Q(r.RdcNm ?? "NA"), Q(r.MajCat), Q(r.Seg ?? "NA"), Q(r.Div ?? "NA"), Q(r.SubDiv ?? "NA"), Q(r.MajCatNm ?? "NA"), Q(r.Ssn ?? "NA"), r.FyYear, r.FyWeek, r.DcStkQ ?? 0, r.GrtStkQ ?? 0, r.BgtPurQInit ?? 0, r.PosPORaised ?? 0, r.NegPORaised ?? 0, r.TtlTrfOutQ ?? 0, r.DcStkExcessQ ?? 0, r.DcStkShortQ ?? 0, r.StStkExcessQ ?? 0, r.StStkShortQ ?? 0, r.CoStkExcessQ ?? 0, r.CoStkShortQ ?? 0, r.FreshBinReq ?? 0, r.GrtBinReq ?? 0));
            return File(Encoding.UTF8.GetBytes(sb.ToString()), "text/csv", "PurchasePlan_Export.csv");
        }

        public IActionResult Privacy() => View();
        private static string Q(string? s) => string.IsNullOrEmpty(s) ? "" : "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
