using Microsoft.EntityFrameworkCore;
using TRANSFER_IN_PLAN.Data;
using TRANSFER_IN_PLAN.Models;
using OfficeOpenXml;

namespace TRANSFER_IN_PLAN.Services;

public class PlanService
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<PlanService> _logger;

    public PlanService(PlanningDbContext context, ILogger<PlanService> logger)
    {
        _context = context;
        _logger = logger;
    }

    public async Task<(int RowsInserted, DateTime ExecutionTime)> ExecutePlanGeneration(
        int startWeekId, int endWeekId, string? storeCode = null, string? majCat = null,
        decimal coverDaysCm1 = 14, decimal coverDaysCm2 = 0)
    {
        try
        {
            var startTime = DateTime.UtcNow;

            var sql = "EXEC SP_GENERATE_TRF_IN_PLAN " +
                $"@StartWeekID={startWeekId}, " +
                $"@EndWeekID={endWeekId}, " +
                $"@StoreCode={(string.IsNullOrEmpty(storeCode) ? "NULL" : $"'{storeCode}'")}, " +
                $"@MajCat={(string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'")}, " +
                $"@CoverDaysCM1={coverDaysCm1}, " +
                $"@CoverDaysCM2={coverDaysCm2}";

            var rowsInserted = await _context.Database.ExecuteSqlRawAsync(sql);

            var executionTime = DateTime.UtcNow;

            _logger.LogInformation($"SP_GENERATE_TRF_IN_PLAN executed successfully. Rows inserted: {rowsInserted}");

            return (rowsInserted, executionTime);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error executing SP_GENERATE_TRF_IN_PLAN: {ex.Message}");
            throw;
        }
    }

    public async Task<DashboardViewModel> GetDashboardData()
    {
        try
        {
            var plans = await _context.TrfInPlans.ToListAsync();

            var model = new DashboardViewModel
            {
                TotalStores = plans.Select(p => p.StCd).Distinct().Count(),
                TotalCategories = plans.Select(p => p.MajCat).Distinct().Count(),
                TotalPlanRows = plans.Count,
                LastExecutionDate = plans.Max(p => p.CreatedDt),
                WeeklySummary = plans
                    .GroupBy(p => new { p.FyWeek, p.FyYear })
                    .Select(g => new WeeklySummary
                    {
                        FyWeek = g.Key.FyWeek ?? 0,
                        FyYear = g.Key.FyYear ?? 0,
                        TotalTrfInQty = g.Sum(p => p.TrfInStkQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderBy(w => w.FyYear)
                    .ThenBy(w => w.FyWeek)
                    .ToList(),
                CategorySummary = plans
                    .GroupBy(p => p.MajCat)
                    .Select(g => new CategorySummary
                    {
                        MajCat = g.Key,
                        TotalTrfInQty = g.Sum(p => p.TrfInStkQ ?? 0),
                        RowCount = g.Count()
                    })
                    .OrderBy(c => c.MajCat)
                    .ToList(),
                TopShortStores = plans
                    .OrderByDescending(p => p.StClShortQ ?? 0)
                    .Take(10)
                    .Select(p => new StoreMetric
                    {
                        StCd = p.StCd,
                        StNm = p.StNm,
                        MajCat = p.MajCat,
                        Quantity = p.StClShortQ ?? 0
                    })
                    .ToList(),
                TopExcessStores = plans
                    .OrderByDescending(p => p.StClExcessQ ?? 0)
                    .Take(10)
                    .Select(p => new StoreMetric
                    {
                        StCd = p.StCd,
                        StNm = p.StNm,
                        MajCat = p.MajCat,
                        Quantity = p.StClExcessQ ?? 0
                    })
                    .ToList()
            };

            return model;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error getting dashboard data: {ex.Message}");
            throw;
        }
    }

    public async Task<byte[]> ExportToExcel(int? startWeekId = null, int? endWeekId = null,
        string? storeCode = null, string? majCat = null)
    {
        try
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            var query = _context.TrfInPlans.AsQueryable();

            if (startWeekId.HasValue)
                query = query.Where(p => p.WeekId >= startWeekId);

            if (endWeekId.HasValue)
                query = query.Where(p => p.WeekId <= endWeekId);

            if (!string.IsNullOrEmpty(storeCode))
                query = query.Where(p => p.StCd == storeCode);

            if (!string.IsNullOrEmpty(majCat))
                query = query.Where(p => p.MajCat == majCat);

            var plans = await query.OrderBy(p => p.StCd).ThenBy(p => p.MajCat).ToListAsync();

            using (var package = new ExcelPackage())
            {
                var worksheet = package.Workbook.Worksheets.Add("Transfer In Plan");

                // Headers
                worksheet.Cells[1, 1].Value = "Store Code";
                worksheet.Cells[1, 2].Value = "Store Name";
                worksheet.Cells[1, 3].Value = "RDC Code";
                worksheet.Cells[1, 4].Value = "RDC Name";
                worksheet.Cells[1, 5].Value = "Hub Code";
                worksheet.Cells[1, 6].Value = "Hub Name";
                worksheet.Cells[1, 7].Value = "Area";
                worksheet.Cells[1, 8].Value = "Major Category";
                worksheet.Cells[1, 9].Value = "Week Start";
                worksheet.Cells[1, 10].Value = "Week End";
                worksheet.Cells[1, 11].Value = "FY Year";
                worksheet.Cells[1, 12].Value = "FY Week";
                worksheet.Cells[1, 13].Value = "Transfer In Stock Qty";
                worksheet.Cells[1, 14].Value = "Transfer In Opt Count";
                worksheet.Cells[1, 15].Value = "Transfer In Opt MBQ";
                worksheet.Cells[1, 16].Value = "DC MBQ";
                worksheet.Cells[1, 17].Value = "Store Close Excess Qty";
                worksheet.Cells[1, 18].Value = "Store Close Short Qty";
                worksheet.Cells[1, 19].Value = "Created Date";
                worksheet.Cells[1, 20].Value = "Created By";

                // Format header
                var headerRange = worksheet.Cells[1, 1, 1, 20];
                headerRange.Style.Font.Bold = true;
                headerRange.Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                headerRange.Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightBlue);

                // Data rows
                int row = 2;
                foreach (var plan in plans)
                {
                    worksheet.Cells[row, 1].Value = plan.StCd;
                    worksheet.Cells[row, 2].Value = plan.StNm;
                    worksheet.Cells[row, 3].Value = plan.RdcCd;
                    worksheet.Cells[row, 4].Value = plan.RdcNm;
                    worksheet.Cells[row, 5].Value = plan.HubCd;
                    worksheet.Cells[row, 6].Value = plan.HubNm;
                    worksheet.Cells[row, 7].Value = plan.Area;
                    worksheet.Cells[row, 8].Value = plan.MajCat;
                    worksheet.Cells[row, 9].Value = plan.WkStDt?.ToString("yyyy-MM-dd");
                    worksheet.Cells[row, 10].Value = plan.WkEndDt?.ToString("yyyy-MM-dd");
                    worksheet.Cells[row, 11].Value = plan.FyYear;
                    worksheet.Cells[row, 12].Value = plan.FyWeek;
                    worksheet.Cells[row, 13].Value = plan.TrfInStkQ;
                    worksheet.Cells[row, 14].Value = plan.TrfInOptCnt;
                    worksheet.Cells[row, 15].Value = plan.TrfInOptMbq;
                    worksheet.Cells[row, 16].Value = plan.DcMbq;
                    worksheet.Cells[row, 17].Value = plan.StClExcessQ;
                    worksheet.Cells[row, 18].Value = plan.StClShortQ;
                    worksheet.Cells[row, 19].Value = plan.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss");
                    worksheet.Cells[row, 20].Value = plan.CreatedBy;

                    row++;
                }

                // Auto-fit columns
                for (int col = 1; col <= 20; col++)
                {
                    worksheet.Column(col).AutoFit();
                }

                return package.GetAsByteArray();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error exporting to Excel: {ex.Message}");
            throw;
        }
    }

    public async Task<(int RowsInserted, DateTime ExecutionTime)> ExecutePurchasePlanAsync(
        int startWeekId, int endWeekId, string? rdcCode = null, string? majCat = null)
    {
        try
        {
            var startTime = DateTime.UtcNow;

            var sql = "EXEC SP_GENERATE_PURCHASE_PLAN " +
                $"@StartWeekID={startWeekId}, " +
                $"@EndWeekID={endWeekId}, " +
                $"@RdcCode={(string.IsNullOrEmpty(rdcCode) ? "NULL" : $"'{rdcCode}'")}, " +
                $"@MajCat={(string.IsNullOrEmpty(majCat) ? "NULL" : $"'{majCat}'")}";

            var rowsInserted = await _context.Database.ExecuteSqlRawAsync(sql);

            var executionTime = DateTime.UtcNow;

            _logger.LogInformation($"SP_GENERATE_PURCHASE_PLAN executed successfully. Rows inserted: {rowsInserted}");

            return (rowsInserted, executionTime);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error executing SP_GENERATE_PURCHASE_PLAN: {ex.Message}");
            throw;
        }
    }

    public async Task<byte[]> ExportPurchasePlanToExcel(int? startWeekId = null, int? endWeekId = null,
        string? rdcCode = null, string? majCat = null)
    {
        try
        {
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            var query = _context.PurchasePlans.AsQueryable();

            if (startWeekId.HasValue)
                query = query.Where(p => p.WeekId >= startWeekId);

            if (endWeekId.HasValue)
                query = query.Where(p => p.WeekId <= endWeekId);

            if (!string.IsNullOrEmpty(rdcCode))
                query = query.Where(p => p.RdcCd == rdcCode);

            if (!string.IsNullOrEmpty(majCat))
                query = query.Where(p => p.MajCat == majCat);

            var plans = await query.OrderBy(p => p.RdcCd).ThenBy(p => p.MajCat).ToListAsync();

            using (var package = new ExcelPackage())
            {
                var worksheet = package.Workbook.Worksheets.Add("Purchase Plan");

                // Headers
                worksheet.Cells[1, 1].Value = "RDC Code";
                worksheet.Cells[1, 2].Value = "RDC Name";
                worksheet.Cells[1, 3].Value = "Major Category";
                worksheet.Cells[1, 4].Value = "Week";
                worksheet.Cells[1, 5].Value = "Week Start";
                worksheet.Cells[1, 6].Value = "Week End";
                worksheet.Cells[1, 7].Value = "DC Stock Qty";
                worksheet.Cells[1, 8].Value = "GRT Stock Qty";
                worksheet.Cells[1, 9].Value = "BGT Purchase Q";
                worksheet.Cells[1, 10].Value = "POS PO Raised";
                worksheet.Cells[1, 11].Value = "NEG PO Raised";
                worksheet.Cells[1, 12].Value = "DC Stock Excess";
                worksheet.Cells[1, 13].Value = "DC Stock Short";
                worksheet.Cells[1, 14].Value = "Store Stock Excess";
                worksheet.Cells[1, 15].Value = "Store Stock Short";
                worksheet.Cells[1, 16].Value = "CO Stock Excess";
                worksheet.Cells[1, 17].Value = "CO Stock Short";
                worksheet.Cells[1, 18].Value = "Created Date";
                worksheet.Cells[1, 19].Value = "Created By";

                // Format header
                var headerRange = worksheet.Cells[1, 1, 1, 19];
                headerRange.Style.Font.Bold = true;
                headerRange.Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                headerRange.Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGreen);

                // Data rows
                int row = 2;
                foreach (var plan in plans)
                {
                    worksheet.Cells[row, 1].Value = plan.RdcCd;
                    worksheet.Cells[row, 2].Value = plan.RdcNm;
                    worksheet.Cells[row, 3].Value = plan.MajCat;
                    worksheet.Cells[row, 4].Value = $"FY{plan.FyYear} W{plan.FyWeek}";
                    worksheet.Cells[row, 5].Value = plan.WkStDt?.ToString("yyyy-MM-dd");
                    worksheet.Cells[row, 6].Value = plan.WkEndDt?.ToString("yyyy-MM-dd");
                    worksheet.Cells[row, 7].Value = plan.DcStkQ;
                    worksheet.Cells[row, 8].Value = plan.GrtStkQ;
                    worksheet.Cells[row, 9].Value = plan.BgtPurQInit;
                    worksheet.Cells[row, 10].Value = plan.PosPORaised;
                    worksheet.Cells[row, 11].Value = plan.NegPORaised;
                    worksheet.Cells[row, 12].Value = plan.DcStkExcessQ;
                    worksheet.Cells[row, 13].Value = plan.DcStkShortQ;
                    worksheet.Cells[row, 14].Value = plan.StStkExcessQ;
                    worksheet.Cells[row, 15].Value = plan.StStkShortQ;
                    worksheet.Cells[row, 16].Value = plan.CoStkExcessQ;
                    worksheet.Cells[row, 17].Value = plan.CoStkShortQ;
                    worksheet.Cells[row, 18].Value = plan.CreatedDt?.ToString("yyyy-MM-dd HH:mm:ss");
                    worksheet.Cells[row, 19].Value = plan.CreatedBy;

                    row++;
                }

                // Auto-fit columns
                for (int col = 1; col <= 19; col++)
                {
                    worksheet.Column(col).AutoFit();
                }

                return package.GetAsByteArray();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error exporting to Excel: {ex.Message}");
            throw;
        }
    }
}
