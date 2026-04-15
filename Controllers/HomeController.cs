using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Data;
using System.Text;
using Snowflake.Data.Client;
using TRANSFER_IN_PLAN.Helpers;
using TRANSFER_IN_PLAN.Models;

namespace TRANSFER_IN_PLAN.Controllers
{
    public class HomeController : Controller
    {
        private readonly string _sfConnStr;
        private readonly ILogger<HomeController> _logger;
        private readonly IMemoryCache _cache;
        private static readonly TimeSpan CacheDuration = TimeSpan.FromSeconds(60);

        public HomeController(IConfiguration config, ILogger<HomeController> logger, IMemoryCache cache)
        {
            _sfConnStr = config.GetConnectionString("Snowflake")!;
            _logger = logger;
            _cache = cache;
        }

        public async Task<IActionResult> Index(string? rdcCd, string? majCat, string? fyWeek)
        {
            // Drill-down filter dropdowns (cached separately — shared across all users)
            ViewBag.RdcCodes = await _cache.GetOrCreateAsync("dd_rdc", async e =>
            {
                e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5);
                await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
                return await SnowflakeCrudHelper.DistinctAsync(conn, "TRF_IN_PLAN", "RDC_CD");
            });
            ViewBag.MajCats = await _cache.GetOrCreateAsync("dd_cat", async e =>
            {
                e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5);
                await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
                return await SnowflakeCrudHelper.DistinctAsync(conn, "TRF_IN_PLAN", "MAJ_CAT");
            });
            ViewBag.FyWeeks = await _cache.GetOrCreateAsync("dd_wk", async e =>
            {
                e.AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(5);
                await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);
                var list = new List<string>();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT DISTINCT FY_WEEK FROM TRF_IN_PLAN WHERE FY_WEEK IS NOT NULL ORDER BY FY_WEEK";
                await using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) list.Add(Convert.ToInt32(r.GetValue(0)).ToString());
                return list;
            });
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
                var rdcs = rdcCd.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => $"'{v.Replace("'", "''").Trim()}'");
                var inList = string.Join(",", rdcs);
                trfWhere += $" AND RDC_CD IN ({inList})";
                ppWhere += $" AND RDC_CD IN ({inList})";
            }
            if (!string.IsNullOrEmpty(majCat))
            {
                var cats = majCat.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => $"'{v.Replace("'", "''").Trim()}'");
                var inList = string.Join(",", cats);
                trfWhere += $" AND MAJ_CAT IN ({inList})";
                ppWhere += $" AND MAJ_CAT IN ({inList})";
            }
            if (!string.IsNullOrEmpty(fyWeek))
            {
                var weeks = fyWeek.Split(',', StringSplitOptions.RemoveEmptyEntries).Select(v => v.Trim()).Where(v => int.TryParse(v, out _));
                var inList = string.Join(",", weeks);
                if (!string.IsNullOrEmpty(inList)) { trfWhere += $" AND FY_WEEK IN ({inList})"; ppWhere += $" AND FY_WEEK IN ({inList})"; }
            }

            var vm = new DashboardViewModel();
            try
            {
                await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

                // ===== ALL TRF_IN_PLAN KPIs in ONE query =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT
                            COUNT(DISTINCT ST_CD) AS TotalStores,
                            COUNT(DISTINCT MAJ_CAT) AS TotalCategories,
                            COUNT(DISTINCT RDC_CD) AS TotalRdcs,
                            COUNT(*) AS TotalRows,
                            MAX(CREATED_DT) AS LastRun,
                            NVL(SUM(TRF_IN_STK_Q),0) AS TrfInQty,
                            NVL(SUM(BGT_TTL_CF_OP_STK_Q),0) AS OpStkQty,
                            NVL(SUM(BGT_TTL_CF_CL_STK_Q),0) AS ClStkQty,
                            NVL(SUM(CM_BGT_SALE_Q),0) AS SaleQty,
                            NVL(SUM(ST_CL_SHORT_Q),0) AS StShortQ,
                            NVL(SUM(ST_CL_EXCESS_Q),0) AS StExcessQ,
                            COUNT(DISTINCT CASE WHEN ST_CL_SHORT_Q > BGT_ST_CL_MBQ * 0.3 THEN ST_CD END) AS CritShort,
                            COUNT(DISTINCT CASE WHEN ST_CL_EXCESS_Q > 0 THEN ST_CD END) AS ExcessSt,
                            SUM(CASE WHEN BGT_TTL_CF_CL_STK_Q <= 0 AND CM_BGT_SALE_Q > 0 THEN 1 ELSE 0 END) AS ZeroStk
                        FROM TRF_IN_PLAN " + trfWhere;
                    cmd.CommandTimeout = 120;
                    await using var r1 = await cmd.ExecuteReaderAsync();
                    if (await r1.ReadAsync())
                    {
                        vm.TotalStores = SnowflakeCrudHelper.Int(r1, 0);
                        vm.TotalCategories = SnowflakeCrudHelper.Int(r1, 1);
                        vm.TotalRdcs = SnowflakeCrudHelper.Int(r1, 2);
                        vm.TotalPlanRows = SnowflakeCrudHelper.Int(r1, 3);
                        vm.LastExecutionDate = SnowflakeCrudHelper.DateNull(r1, 4);
                        vm.TotalTrfInQty = SnowflakeCrudHelper.Dec(r1, 5);
                        vm.TotalOpStkQty = SnowflakeCrudHelper.Dec(r1, 6);
                        vm.TotalClStkQty = SnowflakeCrudHelper.Dec(r1, 7);
                        vm.TotalSaleQty = SnowflakeCrudHelper.Dec(r1, 8);
                        vm.TotalStShortQ = SnowflakeCrudHelper.Dec(r1, 9);
                        vm.TotalStExcessQ = SnowflakeCrudHelper.Dec(r1, 10);
                        vm.CriticalShortStores = SnowflakeCrudHelper.Int(r1, 11);
                        vm.ExcessStores = SnowflakeCrudHelper.Int(r1, 12);
                        vm.ZeroStockStoreCategories = SnowflakeCrudHelper.Int(r1, 13);
                    }
                }

                // ===== ALL PURCHASE_PLAN KPIs in ONE query =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT
                            COUNT(*) AS TotalRows,
                            NVL(SUM(BGT_PUR_Q_INIT),0) AS PurQty,
                            NVL(SUM(DC_STK_SHORT_Q),0) AS DcShort,
                            NVL(SUM(DC_STK_EXCESS_Q),0) AS DcExcess,
                            NVL(SUM(ST_STK_SHORT_Q),0) AS StShort,
                            NVL(SUM(ST_STK_EXCESS_Q),0) AS StExcess,
                            NVL(SUM(CO_STK_SHORT_Q),0) AS CoShort,
                            NVL(SUM(CO_STK_EXCESS_Q),0) AS CoExcess,
                            NVL(SUM(POS_PO_RAISED),0) AS PosPO,
                            NVL(SUM(NEG_PO_RAISED),0) AS NegPO,
                            NVL(SUM(DEL_PEND_Q),0) AS DelPend,
                            NVL(SUM(TTL_TRF_OUT_Q),0) AS TrfOut,
                            (SELECT COUNT(DISTINCT CONCAT(RDC_CD,'|',MAJ_CAT)) FROM PURCHASE_PLAN " + ppWhere + @" AND DC_STK_SHORT_Q > 0) AS DcShortCat,
                            (SELECT COUNT(DISTINCT CONCAT(RDC_CD,'|',MAJ_CAT)) FROM PURCHASE_PLAN " + ppWhere + @" AND DC_STK_EXCESS_Q > 0) AS DcExcessCat
                        FROM PURCHASE_PLAN " + ppWhere;
                    cmd.CommandTimeout = 120;
                    await using var r2 = await cmd.ExecuteReaderAsync();
                    if (await r2.ReadAsync())
                    {
                        vm.TotalPurchasePlanRows = SnowflakeCrudHelper.Int(r2, 0);
                        vm.TotalPurchaseQty = SnowflakeCrudHelper.Dec(r2, 1);
                        vm.TotalDcStkShortQ = SnowflakeCrudHelper.Dec(r2, 2);
                        vm.TotalDcStkExcessQ = SnowflakeCrudHelper.Dec(r2, 3);
                        vm.TotalStStkShortQ = SnowflakeCrudHelper.Dec(r2, 4);
                        vm.TotalStStkExcessQ = SnowflakeCrudHelper.Dec(r2, 5);
                        vm.TotalCoShortQ = SnowflakeCrudHelper.Dec(r2, 6);
                        vm.TotalCoExcessQ = SnowflakeCrudHelper.Dec(r2, 7);
                        vm.TotalPosPO = SnowflakeCrudHelper.Dec(r2, 8);
                        vm.TotalNegPO = SnowflakeCrudHelper.Dec(r2, 9);
                        vm.TotalDelPendQ = SnowflakeCrudHelper.Dec(r2, 10);
                        vm.TotalTrfOutQ = SnowflakeCrudHelper.Dec(r2, 11);
                        vm.DcShortCategories = SnowflakeCrudHelper.Int(r2, 12);
                        vm.DcExcessCategories = SnowflakeCrudHelper.Int(r2, 13);
                    }
                }

                // ===== TRANSFER IN: CATEGORY SUMMARY (Top 15) =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT MAJ_CAT,
                               NVL(SUM(TRF_IN_STK_Q),0),
                               NVL(SUM(ST_CL_SHORT_Q),0),
                               NVL(SUM(ST_CL_EXCESS_Q),0),
                               NVL(SUM(CM_BGT_SALE_Q),0),
                               COUNT(*)
                        FROM TRF_IN_PLAN " + trfWhere + @"
                        GROUP BY MAJ_CAT
                        ORDER BY NVL(SUM(TRF_IN_STK_Q),0) DESC
                        LIMIT 15";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.CategorySummary.Add(new CategorySummary
                        {
                            MajCat = SnowflakeCrudHelper.StrNull(r, 0),
                            TotalTrfInQty = SnowflakeCrudHelper.Dec(r, 1),
                            TotalShortQ = SnowflakeCrudHelper.Dec(r, 2),
                            TotalExcessQ = SnowflakeCrudHelper.Dec(r, 3),
                            TotalSaleQ = SnowflakeCrudHelper.Dec(r, 4),
                            RowCount = SnowflakeCrudHelper.Int(r, 5)
                        });
                    }
                }

                // ===== TRANSFER IN: WEEKLY TREND =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT NVL(FY_YEAR,0), NVL(FY_WEEK,0),
                               NVL(SUM(TRF_IN_STK_Q),0),
                               NVL(SUM(CM_BGT_SALE_Q),0),
                               NVL(SUM(BGT_TTL_CF_OP_STK_Q),0),
                               NVL(SUM(BGT_TTL_CF_CL_STK_Q),0),
                               COUNT(*)
                        FROM TRF_IN_PLAN " + trfWhere + @"
                        GROUP BY FY_YEAR, FY_WEEK
                        ORDER BY NVL(FY_YEAR,0), NVL(FY_WEEK,0)";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.WeeklySummary.Add(new WeeklySummary
                        {
                            FyYear = SnowflakeCrudHelper.Int(r, 0),
                            FyWeek = SnowflakeCrudHelper.Int(r, 1),
                            TotalTrfInQty = SnowflakeCrudHelper.Dec(r, 2),
                            TotalSaleQty = SnowflakeCrudHelper.Dec(r, 3),
                            TotalOpStk = SnowflakeCrudHelper.Dec(r, 4),
                            TotalClStk = SnowflakeCrudHelper.Dec(r, 5),
                            RowCount = SnowflakeCrudHelper.Int(r, 6)
                        });
                    }
                }

                // ===== TRANSFER IN: TOP SHORT STORES =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT ST_CD, ST_NM, MAJ_CAT, NVL(SUM(ST_CL_SHORT_Q),0) AS QTY
                        FROM TRF_IN_PLAN " + trfWhere + @" AND ST_CL_SHORT_Q > 0
                        GROUP BY ST_CD, ST_NM, MAJ_CAT
                        ORDER BY QTY DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.TopShortStores.Add(new StoreMetric
                        {
                            StCd = SnowflakeCrudHelper.StrNull(r, 0),
                            StNm = SnowflakeCrudHelper.StrNull(r, 1),
                            MajCat = SnowflakeCrudHelper.StrNull(r, 2),
                            Quantity = SnowflakeCrudHelper.Dec(r, 3)
                        });
                    }
                }

                // ===== TRANSFER IN: TOP EXCESS STORES =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT ST_CD, ST_NM, MAJ_CAT, NVL(SUM(ST_CL_EXCESS_Q),0) AS QTY
                        FROM TRF_IN_PLAN " + trfWhere + @" AND ST_CL_EXCESS_Q > 0
                        GROUP BY ST_CD, ST_NM, MAJ_CAT
                        ORDER BY QTY DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.TopExcessStores.Add(new StoreMetric
                        {
                            StCd = SnowflakeCrudHelper.StrNull(r, 0),
                            StNm = SnowflakeCrudHelper.StrNull(r, 1),
                            MajCat = SnowflakeCrudHelper.StrNull(r, 2),
                            Quantity = SnowflakeCrudHelper.Dec(r, 3)
                        });
                    }
                }

                // ===== TRANSFER IN: RDC SUMMARY =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT RDC_CD, RDC_NM,
                               NVL(SUM(TRF_IN_STK_Q),0),
                               NVL(SUM(CM_BGT_SALE_Q),0),
                               COUNT(DISTINCT ST_CD)
                        FROM TRF_IN_PLAN " + trfWhere + @"
                        GROUP BY RDC_CD, RDC_NM
                        ORDER BY NVL(SUM(TRF_IN_STK_Q),0) DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.RdcSummary.Add(new RdcSummary
                        {
                            RdcCd = SnowflakeCrudHelper.StrNull(r, 0),
                            RdcNm = SnowflakeCrudHelper.StrNull(r, 1),
                            TotalTrfInQty = SnowflakeCrudHelper.Dec(r, 2),
                            TotalSaleQty = SnowflakeCrudHelper.Dec(r, 3),
                            StoreCount = SnowflakeCrudHelper.Int(r, 4)
                        });
                    }
                }

                // ===== PURCHASE PLAN: CATEGORY SUMMARY (Top 15) =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT MAJ_CAT,
                               NVL(SUM(BGT_PUR_Q_INIT),0),
                               NVL(SUM(DC_STK_SHORT_Q),0),
                               NVL(SUM(DC_STK_EXCESS_Q),0),
                               NVL(SUM(ST_STK_SHORT_Q),0),
                               NVL(SUM(ST_STK_EXCESS_Q),0),
                               NVL(SUM(TTL_TRF_OUT_Q),0),
                               COUNT(*)
                        FROM PURCHASE_PLAN " + ppWhere + @"
                        GROUP BY MAJ_CAT
                        ORDER BY NVL(SUM(BGT_PUR_Q_INIT),0) DESC
                        LIMIT 15";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.PpCategorySummary.Add(new PpCategorySummary
                        {
                            MajCat = SnowflakeCrudHelper.StrNull(r, 0),
                            BgtPurQ = SnowflakeCrudHelper.Dec(r, 1),
                            DcStkShortQ = SnowflakeCrudHelper.Dec(r, 2),
                            DcStkExcessQ = SnowflakeCrudHelper.Dec(r, 3),
                            StStkShortQ = SnowflakeCrudHelper.Dec(r, 4),
                            StStkExcessQ = SnowflakeCrudHelper.Dec(r, 5),
                            TrfOutQ = SnowflakeCrudHelper.Dec(r, 6),
                            RowCount = SnowflakeCrudHelper.Int(r, 7)
                        });
                    }
                }

                // ===== PURCHASE PLAN: WEEKLY TREND =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT NVL(FY_YEAR,0), NVL(FY_WEEK,0),
                               NVL(SUM(BGT_PUR_Q_INIT),0),
                               NVL(SUM(TTL_TRF_OUT_Q),0),
                               NVL(SUM(DC_STK_SHORT_Q),0),
                               NVL(SUM(DC_STK_EXCESS_Q),0)
                        FROM PURCHASE_PLAN " + ppWhere + @"
                        GROUP BY FY_YEAR, FY_WEEK
                        ORDER BY NVL(FY_YEAR,0), NVL(FY_WEEK,0)";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.PpWeeklySummary.Add(new PpWeeklySummary
                        {
                            FyYear = SnowflakeCrudHelper.Int(r, 0),
                            FyWeek = SnowflakeCrudHelper.Int(r, 1),
                            BgtPurQ = SnowflakeCrudHelper.Dec(r, 2),
                            TrfOutQ = SnowflakeCrudHelper.Dec(r, 3),
                            DcStkShortQ = SnowflakeCrudHelper.Dec(r, 4),
                            DcStkExcessQ = SnowflakeCrudHelper.Dec(r, 5)
                        });
                    }
                }

                // ===== PURCHASE PLAN: TOP DC SHORT =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT RDC_CD, RDC_NM, MAJ_CAT, NVL(SUM(DC_STK_SHORT_Q),0) AS QTY
                        FROM PURCHASE_PLAN " + ppWhere + @" AND DC_STK_SHORT_Q > 0
                        GROUP BY RDC_CD, RDC_NM, MAJ_CAT
                        ORDER BY QTY DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.TopDcShortCategories.Add(new DcStockMetric
                        {
                            RdcCd = SnowflakeCrudHelper.StrNull(r, 0),
                            RdcNm = SnowflakeCrudHelper.StrNull(r, 1),
                            MajCat = SnowflakeCrudHelper.StrNull(r, 2),
                            Quantity = SnowflakeCrudHelper.Dec(r, 3)
                        });
                    }
                }

                // ===== PURCHASE PLAN: TOP DC EXCESS =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT RDC_CD, RDC_NM, MAJ_CAT, NVL(SUM(DC_STK_EXCESS_Q),0) AS QTY
                        FROM PURCHASE_PLAN " + ppWhere + @" AND DC_STK_EXCESS_Q > 0
                        GROUP BY RDC_CD, RDC_NM, MAJ_CAT
                        ORDER BY QTY DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.TopDcExcessCategories.Add(new DcStockMetric
                        {
                            RdcCd = SnowflakeCrudHelper.StrNull(r, 0),
                            RdcNm = SnowflakeCrudHelper.StrNull(r, 1),
                            MajCat = SnowflakeCrudHelper.StrNull(r, 2),
                            Quantity = SnowflakeCrudHelper.Dec(r, 3)
                        });
                    }
                }

                // ===== RDC INVENTORY HEALTH =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT RDC_CD, RDC_NM,
                               NVL(SUM(DC_STK_Q),0),
                               NVL(SUM(TTL_TRF_OUT_Q),0),
                               NVL(SUM(BGT_PUR_Q_INIT),0),
                               NVL(SUM(DC_STK_SHORT_Q),0),
                               NVL(SUM(DC_STK_EXCESS_Q),0),
                               COUNT(DISTINCT MAJ_CAT)
                        FROM PURCHASE_PLAN " + ppWhere + @"
                        GROUP BY RDC_CD, RDC_NM
                        ORDER BY RDC_CD";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.RdcInventoryHealth.Add(new RdcInventoryHealth
                        {
                            RdcCd = SnowflakeCrudHelper.StrNull(r, 0),
                            RdcNm = SnowflakeCrudHelper.StrNull(r, 1),
                            DcStock = SnowflakeCrudHelper.Dec(r, 2),
                            TrfOut = SnowflakeCrudHelper.Dec(r, 3),
                            PurchaseQty = SnowflakeCrudHelper.Dec(r, 4),
                            DcShort = SnowflakeCrudHelper.Dec(r, 5),
                            DcExcess = SnowflakeCrudHelper.Dec(r, 6),
                            CategoryCount = SnowflakeCrudHelper.Int(r, 7)
                        });
                    }
                }

                // ===== TOP RISK CATEGORIES (by company short) =====
                {
                    await using var cmd = conn.CreateCommand();
                    cmd.CommandText = @"
                        SELECT MAJ_CAT,
                               NVL(SUM(CO_STK_SHORT_Q),0),
                               NVL(SUM(CO_STK_EXCESS_Q),0),
                               NVL(SUM(DC_STK_SHORT_Q),0),
                               NVL(SUM(ST_STK_SHORT_Q),0)
                        FROM PURCHASE_PLAN " + ppWhere + @"
                        GROUP BY MAJ_CAT
                        ORDER BY NVL(SUM(CO_STK_SHORT_Q),0) DESC
                        LIMIT 10";
                    cmd.CommandTimeout = 120;
                    await using var r = await cmd.ExecuteReaderAsync();
                    while (await r.ReadAsync())
                    {
                        vm.TopRiskCategories.Add(new CategoryRisk
                        {
                            MajCat = SnowflakeCrudHelper.StrNull(r, 0),
                            CoShort = SnowflakeCrudHelper.Dec(r, 1),
                            CoExcess = SnowflakeCrudHelper.Dec(r, 2),
                            DcShort = SnowflakeCrudHelper.Dec(r, 3),
                            StShort = SnowflakeCrudHelper.Dec(r, 4)
                        });
                    }
                }

                foreach (var c in vm.TopRiskCategories)
                    c.Risk = c.CoShort > 1000 ? "CRITICAL" : c.CoShort > 0 ? "AT RISK" : c.CoExcess > 1000 ? "EXCESS" : "OK";

                // ===== SUB-LEVEL PLAN STATUS =====
                try
                {
                    foreach (var (lv, lbl) in new[] { ("MVGR", "Macro MVGR"), ("SZ", "Size"), ("SEG", "Segment"), ("VND", "Vendor") })
                    {
                        int trfR = 0, ppR = 0; string? lr = null;
                        try
                        {
                            await using (var c = conn.CreateCommand())
                            {
                                c.CommandText = $"SELECT COUNT(*), MAX(CREATED_DT) FROM SUB_LEVEL_TRF_PLAN WHERE LEVEL='{lv}'";
                                c.CommandTimeout = 15;
                                await using var rd = await c.ExecuteReaderAsync();
                                if (await rd.ReadAsync())
                                {
                                    trfR = SnowflakeCrudHelper.Int(rd, 0);
                                    var dt = SnowflakeCrudHelper.DateNull(rd, 1);
                                    if (dt.HasValue) lr = dt.Value.ToString("dd-MMM HH:mm");
                                }
                            }
                            await using (var c = conn.CreateCommand())
                            {
                                c.CommandText = $"SELECT COUNT(*) FROM SUB_LEVEL_PP_PLAN WHERE LEVEL='{lv}'";
                                c.CommandTimeout = 15;
                                ppR = Convert.ToInt32(await c.ExecuteScalarAsync() ?? 0);
                            }
                        }
                        catch { }
                        vm.SubLevelStatuses.Add(new SubLevelStatus { Level = lv, Label = lbl, TrfRows = trfR, PpRows = ppR, LastRun = lr });
                    }
                }
                catch { }

                // ===== DATA HEALTH (reference table row counts) =====
                try
                {
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
                            var cnt = await SnowflakeCrudHelper.CountAsync(conn, tbl);
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
            var where = "WHERE 1=1";
            if (fyYear.HasValue) where += $" AND FY_YEAR = {fyYear.Value}";
            if (fyWeek.HasValue) where += $" AND FY_WEEK = {fyWeek.Value}";
            if (!string.IsNullOrEmpty(majCat)) where += $" AND MAJ_CAT = '{majCat.Replace("'", "''")}'";
            if (!string.IsNullOrEmpty(stCd)) where += $" AND ST_CD = '{stCd.Replace("'", "''")}'";

            var sql = $@"SELECT ST_CD, ST_NM, RDC_CD, RDC_NM, MAJ_CAT, SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN,
                                FY_YEAR, FY_WEEK,
                                S_GRT_STK_Q, W_GRT_STK_Q, BGT_DISP_CL_Q, BGT_DISP_CL_OPT,
                                CM1_SALE_COVER_DAY, CM2_SALE_COVER_DAY, COVER_SALE_QTY,
                                BGT_ST_CL_MBQ, BGT_DISP_CL_OPT_MBQ, BGT_TTL_CF_OP_STK_Q,
                                NT_ACT_Q, NET_BGT_CF_STK_Q, CM_BGT_SALE_Q, CM1_BGT_SALE_Q, CM2_BGT_SALE_Q,
                                TRF_IN_STK_Q, TRF_IN_OPT_CNT, TRF_IN_OPT_MBQ, DC_MBQ,
                                BGT_TTL_CF_CL_STK_Q, BGT_NT_ACT_Q, NET_ST_CL_STK_Q,
                                ST_CL_EXCESS_Q, ST_CL_SHORT_Q
                         FROM TRF_IN_PLAN {where}
                         ORDER BY ST_CD, MAJ_CAT";

            Response.ContentType = "text/csv";
            Response.Headers.Append("Content-Disposition", "attachment; filename=TrfInPlan_Export.csv");
            await using var writer = new StreamWriter(Response.Body, Encoding.UTF8);
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            await writer.WriteLineAsync("StCd,StNm,RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,SGrtStkQ,WGrtStkQ,BgtDispClQ,BgtDispClOpt,Cm1SaleCoverDay,Cm2SaleCoverDay,CoverSaleQty,BgtStClMbq,BgtDispClOptMbq,BgtTtlCfOpStkQ,NtActQ,NetBgtCfStkQ,CmBgtSaleQ,Cm1BgtSaleQ,Cm2BgtSaleQ,TrfInStkQ,TrfInOptCnt,TrfInOptMbq,DcMbq,BgtTtlCfClStkQ,BgtNtActQ,NetStClStkQ,StClExcessQ,StClShortQ");

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 300;
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var sb = new StringBuilder();
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 0))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 1))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 2))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 3))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 4))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 5))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 6))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 7))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 8))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 9))); sb.Append(',');
                sb.Append(SnowflakeCrudHelper.Int(r, 10)); sb.Append(',');
                sb.Append(SnowflakeCrudHelper.Int(r, 11)); sb.Append(',');
                for (int i = 12; i <= 35; i++)
                {
                    sb.Append(SnowflakeCrudHelper.Dec(r, i));
                    if (i < 35) sb.Append(',');
                }
                await writer.WriteLineAsync(sb.ToString());
            }
            await writer.FlushAsync();
            return new EmptyResult();
        }

        [HttpGet]
        public async Task<IActionResult> ExportPpCsv(int? fyYear, int? fyWeek, string? rdcCd, string? majCat)
        {
            var where = "WHERE 1=1";
            if (fyYear.HasValue) where += $" AND FY_YEAR = {fyYear.Value}";
            if (fyWeek.HasValue) where += $" AND FY_WEEK = {fyWeek.Value}";
            if (!string.IsNullOrEmpty(rdcCd)) where += $" AND RDC_CD = '{rdcCd.Replace("'", "''")}'";
            if (!string.IsNullOrEmpty(majCat)) where += $" AND MAJ_CAT = '{majCat.Replace("'", "''")}'";

            var sql = $@"SELECT RDC_CD, RDC_NM, MAJ_CAT, SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN,
                                FY_YEAR, FY_WEEK,
                                DC_STK_Q, GRT_STK_Q, BGT_PUR_Q_INIT,
                                POS_PO_RAISED, NEG_PO_RAISED, TTL_TRF_OUT_Q,
                                DC_STK_EXCESS_Q, DC_STK_SHORT_Q,
                                ST_STK_EXCESS_Q, ST_STK_SHORT_Q,
                                CO_STK_EXCESS_Q, CO_STK_SHORT_Q,
                                FRESH_BIN_REQ, GRT_BIN_REQ
                         FROM PURCHASE_PLAN {where}
                         ORDER BY RDC_CD, MAJ_CAT";

            Response.ContentType = "text/csv";
            Response.Headers.Append("Content-Disposition", "attachment; filename=PurchasePlan_Export.csv");
            await using var writer = new StreamWriter(Response.Body, Encoding.UTF8);
            await using var conn = await SnowflakeCrudHelper.OpenAsync(_sfConnStr);

            await writer.WriteLineAsync("RdcCd,RdcNm,MajCat,Seg,Div,SubDiv,MajCatNm,Ssn,FyYear,FyWeek,DcStkQ,GrtStkQ,BgtPurQInit,PosPORaised,NegPORaised,TtlTrfOutQ,DcStkExcessQ,DcStkShortQ,StStkExcessQ,StStkShortQ,CoStkExcessQ,CoStkShortQ,FreshBinReq,GrtBinReq");

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.CommandTimeout = 300;
            await using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                var sb = new StringBuilder();
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 0))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 1))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 2))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 3))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 4))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 5))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 6))); sb.Append(',');
                sb.Append(Q(SnowflakeCrudHelper.Str(r, 7))); sb.Append(',');
                sb.Append(SnowflakeCrudHelper.Int(r, 8)); sb.Append(',');
                sb.Append(SnowflakeCrudHelper.Int(r, 9)); sb.Append(',');
                for (int i = 10; i <= 23; i++)
                {
                    sb.Append(SnowflakeCrudHelper.Dec(r, i));
                    if (i < 23) sb.Append(',');
                }
                await writer.WriteLineAsync(sb.ToString());
            }
            await writer.FlushAsync();
            return new EmptyResult();
        }

        public IActionResult Privacy() => View();
        private static string Q(string? s) => string.IsNullOrEmpty(s) ? "" : "\"" + s.Replace("\"", "\"\"") + "\"";
    }
}
