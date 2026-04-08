using Microsoft.AspNetCore.Mvc;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;

namespace TRANSFER_IN_PLAN.Controllers;

public class InfoController : Controller
{
    public IActionResult Index() => View();

    [HttpGet]
    public IActionResult DownloadWord()
    {
        var ms = new MemoryStream();
        using (var doc = WordprocessingDocument.Create(ms, WordprocessingDocumentType.Document, true))
        {
            var mainPart = doc.AddMainDocumentPart();
            mainPart.Document = new Document();
            var body = mainPart.Document.AppendChild(new Body());

            // ── Page settings ──
            body.AppendChild(new SectionProperties(
                new PageSize { Width = 12240, Height = 15840 },
                new PageMargin { Top = 720, Right = 720, Bottom = 720, Left = 720 }
            ));

            // ══════════════════════════════════════════════
            // TITLE PAGE
            // ══════════════════════════════════════════════
            AddHeading(body, "Transfer In Plan Management System", 28, true, "2F5496");
            AddHeading(body, "Application Knowledge Base — Complete Technical Reference", 14, false, "808080");
            AddParagraph(body, $"Generated on: {DateTime.Now:dd-MMM-yyyy HH:mm}", 10, true, "808080");
            AddParagraph(body, "");
            AddHorizontalLine(body);
            AddParagraph(body, "");

            // ══════════════════════════════════════════════
            // TABLE OF CONTENTS
            // ══════════════════════════════════════════════
            AddHeading(body, "TABLE OF CONTENTS", 16, true, "2F5496");
            AddParagraph(body, "1.  Application Workflow");
            AddParagraph(body, "2.  Architecture (Backend, Frontend, Database)");
            AddParagraph(body, "3.  Transfer In Plan — Complete Algorithm & Formulas");
            AddParagraph(body, "4.  Purchase Plan — Complete Algorithm & Formulas");
            AddParagraph(body, "5.  Sub-Level Plans (TRF + PP per Level)");
            AddParagraph(body, "6.  Database Schema & Tables");
            AddParagraph(body, "7.  System Features");
            AddParagraph(body, "8.  Change Log");
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 1. APPLICATION WORKFLOW
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "1. APPLICATION WORKFLOW");

            AddHeading(body, "What This Application Does", 13, true, "1a3a5c");
            AddParagraph(body, "This is a retail supply chain planning system that answers two fundamental questions every week for every store and every product category:");
            AddParagraph(body, "");
            AddParagraph(body, "Transfer In Plan (TRF): \"How much stock should each store receive from the DC this week?\"", 10, true);
            AddParagraph(body, "  - Granularity: Store + Category + Week");
            AddParagraph(body, "  - Key Output: TRF_IN_STK_Q (Transfer In Quantity)");
            AddParagraph(body, "  - Considers: Current stock, shrinkage, weekly sale forecast, display needs, MBQ");
            AddParagraph(body, "");
            AddParagraph(body, "Purchase Plan (PP): \"How much stock should each DC purchase from vendors this week?\"", 10, true);
            AddParagraph(body, "  - Granularity: RDC + Category + Week");
            AddParagraph(body, "  - Key Output: BGT_PUR_Q_INIT (Purchase Quantity)");
            AddParagraph(body, "  - Considers: Aggregated store transfers, DC stock, GRT consumption, delivery pending");
            AddParagraph(body, "");
            AddParagraph(body, "Sub-Level Plans further break these down by 4 dimensions: Macro MVGR, Size, Segment, Vendor — using the same algorithms with contribution percentages and level-specific stock.");

            AddHeading(body, "End-to-End Workflow", 13, true, "1a3a5c");
            AddParagraph(body, "Step 1: Upload Reference Data — Via Bulk Upload (Excel/CSV) or CRUD pages.");
            AddParagraph(body, "  Tables: WEEK_CALENDAR, MASTER_ST_MASTER, MASTER_BIN_CAPACITY, QTY_SALE_QTY, QTY_DISP_QTY, STORE_STOCK, DC_STOCK, DEL_PENDING, MASTER_GRT_CONTRIBUTION, MASTER_PRODUCT_HIERARCHY");
            AddParagraph(body, "");
            AddParagraph(body, "Step 2: Execute Transfer In Plan — SP_GENERATE_TRF_IN_PLAN for selected store/week range. Or Full Run (background) for all stores.");
            AddParagraph(body, "  Output: TRF_IN_PLAN table");
            AddParagraph(body, "");
            AddParagraph(body, "Step 3: Execute Purchase Plan — SP_GENERATE_PURCHASE_PLAN. READS TRF_IN_PLAN output — aggregates store-level TRF_IN by RDC.");
            AddParagraph(body, "  Output: PURCHASE_PLAN table");
            AddParagraph(body, "");
            AddParagraph(body, "Step 4: Review Output & Export — Paginated output, filters, CSV or Pivot CSV export.");
            AddParagraph(body, "");
            AddParagraph(body, "Step 5: Upload Sub-Level Data — Contribution tables + sub-level stock tables (4 store + 4 DC).");
            AddParagraph(body, "");
            AddParagraph(body, "Step 6: Execute Sub-Level Plans — Per-level or Full Run (background, all 4 levels). TRF runs first per level, then PP reads TRF output.");
            AddParagraph(body, "  Output: SUB_LEVEL_TRF_PLAN + SUB_LEVEL_PP_PLAN");
            AddParagraph(body, "");
            AddParagraph(body, "Step 7: Review Sub-Level Output — Per-level output with filters, pagination, CSV + Pivot CSV.");
            AddParagraph(body, "");
            AddParagraph(body, "Step 8: Dashboard Monitoring — KPIs, charts, alerts, inventory health, short/excess analysis.");

            AddHeading(body, "Critical Dependency", 13, true, "c0392b");
            AddParagraph(body, "Purchase Plan READS from TRF_IN_PLAN output. You must execute Transfer In Plan BEFORE Purchase Plan. Same for sub-level: Sub TRF must run before Sub PP per level.");

            AddHeading(body, "Execution Modes", 13, true, "1a3a5c");
            var execModes = new[]
            {
                new[] { "Mode", "Page", "Stored Procedure", "Type", "Description" },
                new[] { "Single Store TRF", "Execute Plan", "SP_GENERATE_TRF_IN_PLAN", "Sync", "TRF for 1 store + week range. ~10s per store." },
                new[] { "Full Run (All Stores)", "Execute Plan", "SP_RUN_ALL_PLANS", "Background", "Truncates TRF+PP, bulk runs all ~657 stores. ~1-2 min." },
                new[] { "Purchase Plan", "Purchase Plan", "SP_GENERATE_PURCHASE_PLAN", "Sync", "PP for week range + optional RDC/MajCat filter." },
                new[] { "Sub-Level (Per Level)", "Sub-Level Plans", "SP_GENERATE_SUB_LEVEL_TRF + PP", "Sync", "TRF+PP for selected level(s)." },
                new[] { "Sub-Level Full Run", "Sub-Level Plans", "Same SPs x 4 levels", "Background", "All 4 levels sequentially. Live progress tracking." },
            };
            AddTable(body, execModes);
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 2. ARCHITECTURE
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "2. ARCHITECTURE");

            AddHeading(body, "Project Structure", 13, true, "1a3a5c");
            AddCodeBlock(body, @"TRANSFER_IN_PLAN/ (ASP.NET Core 8.0 MVC)
├── Controllers/       30 controllers
├── Models/            21 model files (EF entities, ViewModels)
├── Views/             31 folders, ~100 Razor .cshtml views
│   ├── Shared/        _Layout.cshtml (main layout + sidebar)
│   ├── Home/          Dashboard (KPIs, 6 charts, filters)
│   ├── Plan/          Execute.cshtml + Output.cshtml
│   ├── PurchasePlan/  Execute.cshtml + Output.cshtml
│   ├── SubLevel/      Execute + TrfOutput + PpOutput
│   ├── BulkUpload/    Index.cshtml (22-table upload)
│   ├── Info/          Index.cshtml (knowledge base)
│   └── 22 CRUD folders (Index, Create, Edit, Delete)
├── Services/          PlanService, PlanJobService (singleton), SubLevelJobService (singleton)
├── Data/              PlanningDbContext.cs (EF Core DbContext)
├── sql/               25+ SQL scripts (tables, SPs, views, patches)
├── wwwroot/css/       site.css (Design System v2)
├── wwwroot/js/        site.js, searchable-dropdown.js
└── Program.cs         Service registration, middleware");

            AddHeading(body, "Backend — Controllers (30)", 13, true, "1a3a5c");
            var controllers = new[]
            {
                new[] { "Controller", "Purpose" },
                new[] { "HomeController", "Dashboard — KPIs, charts, filters, CSV export" },
                new[] { "PlanController", "TRF execution — single store, full run, output, pivot CSV, reset" },
                new[] { "PurchasePlanController", "PP execution — filtered run, output, pivot CSV, reset" },
                new[] { "SubLevelController", "Sub-level — per-level/full run, TRF/PP output, reset, status" },
                new[] { "BulkUploadController", "22-table upload — Excel/CSV, replace/append, sample download" },
                new[] { "InfoController", "Knowledge base page" },
                new[] { "22 CRUD Controllers", "One per reference/contribution/stock table" },
            };
            AddTable(body, controllers);

            AddHeading(body, "Backend — Services (3)", 13, true, "1a3a5c");
            var services = new[]
            {
                new[] { "Service", "Lifetime", "Purpose" },
                new[] { "PlanService", "Scoped", "SP execution helper — builds SqlParameters, calls ExecuteSqlRawAsync" },
                new[] { "PlanJobService", "Singleton", "Background job — runs SP_RUN_ALL_PLANS via Task.Run. Polled every 5s." },
                new[] { "SubLevelJobService", "Singleton", "Background job — TRF+PP per level sequentially. Tracks CurrentLevel, LevelsCompleted." },
            };
            AddTable(body, services);

            AddHeading(body, "Backend — Key Packages", 13, true, "1a3a5c");
            AddParagraph(body, "  - EF Core 8 (SqlServer provider) — ORM with 300s command timeout");
            AddParagraph(body, "  - EPPlus 7 — Excel read/write (.xlsx)");
            AddParagraph(body, "  - DocumentFormat.OpenXml — Word document generation");
            AddParagraph(body, "  - Newtonsoft.Json — JSON serialization (date format: yyyy-MM-dd)");
            AddParagraph(body, "  - IMemoryCache — Dashboard caching (60s per filter, 5min dropdown lists)");

            AddHeading(body, "Backend — Program.cs Configuration", 13, true, "1a3a5c");
            AddParagraph(body, "  - Kestrel: 500 MB max request body, 10-min keep-alive timeout");
            AddParagraph(body, "  - Forms: 500 MB upload limit");
            AddParagraph(body, "  - Session: 30-min idle timeout");
            AddParagraph(body, "  - Default Route: {controller=Home}/{action=Index}/{id?}");
            AddParagraph(body, "  - URL: localhost:5005");
            AddParagraph(body, "  - Database: SQL Server — database name 'planning'");

            AddHeading(body, "Frontend Architecture", 13, true, "1a3a5c");
            AddParagraph(body, "UI Framework & CDN Libraries:", 10, true);
            AddParagraph(body, "  - Bootstrap 5.3 — Layout, components, responsive grid");
            AddParagraph(body, "  - Bootstrap Icons — Icon library (CDN)");
            AddParagraph(body, "  - Chart.js 4.4 — Dashboard charts (6 charts: bar, line, horizontal bar, stacked)");
            AddParagraph(body, "  - DataTables 1.13 — Searchable/sortable tables on all CRUD + output pages");
            AddParagraph(body, "  - Inter Font — Google Fonts");
            AddParagraph(body, "");
            AddParagraph(body, "Design System v2 (site.css):", 10, true);
            AddParagraph(body, "  - Primary: #4F46E5 (Indigo)");
            AddParagraph(body, "  - Sidebar: #0F172A → #1E293B (Dark Slate gradient)");
            AddParagraph(body, "  - Background: #F8FAFC  |  Text: #0F172A");
            AddParagraph(body, "  - CSS Variables in :root  |  Cards: 12px radius  |  Buttons: 8px radius");
            AddParagraph(body, "");
            AddParagraph(body, "Custom JavaScript:", 10, true);
            AddParagraph(body, "  - searchable-dropdown.js — Multi-select dropdowns with checkbox, search, Select All");
            AddParagraph(body, "  - site.js — Global utilities, sidebar toggle");
            AddParagraph(body, "  - Responsive: <768px mobile topbar + offcanvas; >=768px sticky desktop sidebar");

            AddHeading(body, "Stored Procedures (6 Total)", 13, true, "1a3a5c");
            var sps = new[]
            {
                new[] { "SP Name", "Purpose", "Reads From", "Writes To" },
                new[] { "SP_GENERATE_TRF_IN_PLAN", "Store-level transfer in plan", "STORE_STOCK, SALE_QTY, DISP_QTY, BIN_CAPACITY, ST_MASTER", "TRF_IN_PLAN" },
                new[] { "SP_GENERATE_PURCHASE_PLAN", "DC-level purchase plan", "TRF_IN_PLAN, DC_STOCK, GRT_CONTRIBUTION, DEL_PENDING", "PURCHASE_PLAN" },
                new[] { "SP_RUN_ALL_PLANS", "Full run: truncate + TRF + PP", "All reference tables", "TRF_IN_PLAN + PURCHASE_PLAN" },
                new[] { "SP_GENERATE_SUB_LEVEL_TRF", "Sub-level TRF per dimension", "SUB_ST_STK_{level}, Contribution, SALE, DISP", "SUB_LEVEL_TRF_PLAN" },
                new[] { "SP_GENERATE_SUB_LEVEL_PP", "Sub-level PP per dimension", "SUB_LEVEL_TRF_PLAN, SUB_DC_STK_{level}", "SUB_LEVEL_PP_PLAN" },
            };
            AddTable(body, sps);
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 3. TRANSFER IN PLAN ALGORITHM
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "3. TRANSFER IN PLAN — COMPLETE ALGORITHM & FORMULAS");

            AddParagraph(body, "SP: SP_GENERATE_TRF_IN_PLAN  |  Granularity: ST_CD + MAJ_CAT + WEEK  |  Output: TRF_IN_PLAN", 10, true);
            AddParagraph(body, "The algorithm runs a week-chaining loop where each week's closing stock becomes the next week's opening stock.");

            AddHeading(body, "SP Execution Steps", 13, true, "1a3a5c");
            AddParagraph(body, "Step 1: Build #Weeks from WEEK_CALENDAR + week offset map (#WkMap) for +1..+4 lookups.");
            AddParagraph(body, "Step 2: Build Store x Category combinations (ST_MASTER CROSS JOIN BIN_CAPACITY + PRODUCT_HIERARCHY).");
            AddParagraph(body, "Step 3: UNPIVOT QTY_SALE_QTY (48 WK columns → rows) into #SC. Same for QTY_DISP_QTY into #LS.");
            AddParagraph(body, "Step 4: Build lean chain table per ST_CD + MAJ_CAT + WEEK (MBQ, SALE, OP_STK, NET_CF, TRF_IN, CL_STK).");
            AddParagraph(body, "Step 5: Week 1 calculations (set-based UPDATE for all store+category combos at once).");
            AddParagraph(body, "Step 6: WHILE loop Week 2..N: OP_STK = prev CL_STK, recalculate NET_CF, TRF_IN, CL_STK.");
            AddParagraph(body, "Step 7: Join chain with all reference data, calculate derived columns, INSERT into TRF_IN_PLAN.");

            AddHeading(body, "Core Chain Algorithm (The Heart of TRF)", 13, true, "27ae60");
            AddCodeBlock(body, @"FOR EACH ST_CD + MAJ_CAT + WEEK:

WEEK 1:
  OP_STK = STORE_STOCK.STK_QTY (latest by DATE for this store+category)
  MBQ    = DISP_QTY(current week) + SALE_QTY(next week)
  SALE   = SALE_QTY(current week)

WEEK 2+:
  OP_STK = Previous Week CL_STK (chained)
  MBQ    = DISP_QTY(current week) + SALE_QTY(next week)
  SALE   = SALE_QTY(current week)

ALL WEEKS — Shrinkage:
  Step 1: base_shrinkage = ROUND(OP_STK * 0.08, 0)        [8% of opening, rounded]
  Step 2: SSN_FACTOR = IF SSN IN ('S','PS') THEN 1.0 ELSE 0.5
  Step 3: SHRINKAGE = ROUND(base_shrinkage * SSN_FACTOR, 0)

ALL WEEKS — Net Carry-Forward:
  NET_CF = MAX(OP_STK - SHRINKAGE, 0)

ALL WEEKS — Transfer In Quantity:
  IF MBQ = 0 AND SALE = 0 THEN TRF_IN = 0
  ELSE IF MBQ + SALE - NET_CF > 0 THEN TRF_IN = MBQ + SALE - NET_CF
  ELSE TRF_IN = 0

ALL WEEKS — Closing Stock:
  IF MBQ = 0 AND SALE = 0 THEN CL_STK = NET_CF
  ELSE IF MBQ + SALE > NET_CF THEN CL_STK = MBQ
  ELSE CL_STK = MAX(NET_CF - SALE, 0)

WEEK CHAIN: CL_STK of Week N → OP_STK of Week N+1");

            AddHeading(body, "All TRF_IN_PLAN Output Columns", 13, true, "27ae60");
            var trfCols = new[]
            {
                new[] { "Column", "Formula / Source", "Type" },
                new[] { "ST_CD, ST_NM, RDC_CD, RDC_NM, HUB_CD, HUB_NM, AREA", "From MASTER_ST_MASTER", "Input" },
                new[] { "MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM", "From BIN_CAPACITY + PRODUCT_HIERARCHY", "Input" },
                new[] { "WEEK_ID, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT", "From WEEK_CALENDAR", "Input" },
                new[] { "S_GRT_STK_Q", "Always 0", "Static" },
                new[] { "W_GRT_STK_Q", "IF SSN IN ('W','PW') THEN prev_CL_STK ELSE 0", "Derived" },
                new[] { "BGT_DISP_CL_Q", "DISP_QTY from QTY_DISP_QTY (unpivoted)", "Input" },
                new[] { "BGT_DISP_CL_OPT", "ROUND(DISP_QTY * 1000.0 / BIN_CAP, 0)", "Derived" },
                new[] { "COVER_SALE_QTY", "Next week sale (SQ1.SALE_QTY)", "Input" },
                new[] { "BGT_ST_CL_MBQ", "MBQ = DISP_QTY(current) + SALE_QTY(next week)", "Derived" },
                new[] { "BGT_DISP_CL_OPT_MBQ", "ROUND(MBQ * 1000.0 / NULLIF(BGT_DISP_CL_OPT, 0), 0)", "Derived" },
                new[] { "BGT_TTL_CF_OP_STK_Q", "WK1: STORE_STOCK.STK_QTY | WK2+: prev_CL_STK", "Chain" },
                new[] { "NT_ACT_Q (Shrinkage)", "ROUND(ROUND(OP*0.08,0) * SSN_FACTOR, 0)  S,PS=1.0; others=0.5", "Derived" },
                new[] { "NET_BGT_CF_STK_Q", "MAX(OP_STK - SHRINKAGE, 0)", "Derived" },
                new[] { "CM_BGT_SALE_Q", "Current week sale (unpivoted)", "Input" },
                new[] { "CM1_BGT_SALE_Q / CM2_BGT_SALE_Q", "Next week / Week+2 sale", "Input" },
                new[] { "TRF_IN_STK_Q", "IF(MBQ=0 AND SALE=0) 0 ELSE MAX(MBQ+SALE-NET_CF, 0)", "Chain" },
                new[] { "TRF_IN_OPT_CNT", "ROUND(TRF_IN * 1000.0 / NULLIF(OPT_MBQ, 0), 0)", "Derived" },
                new[] { "TRF_IN_OPT_MBQ", "TRF_IN * 1000.0 / NULLIF(TRF_IN_OPT_CNT, 0)", "Derived" },
                new[] { "DC_MBQ", "SQ1 + SQ2 + SQ3 + SQ4 (sum of next 4 weeks sale)", "Derived" },
                new[] { "BGT_TTL_CF_CL_STK_Q", "Closing Stock (chains to next week)", "Chain" },
                new[] { "NET_ST_CL_STK_Q", "= CL_STK", "Derived" },
                new[] { "ST_CL_EXCESS_Q", "MAX(CL_STK - MBQ, 0)", "Derived" },
                new[] { "ST_CL_SHORT_Q", "MAX(MBQ - CL_STK, 0)", "Derived" },
            };
            AddTable(body, trfCols);

            AddHeading(body, "Data Sources for TRF", 13, true, "27ae60");
            var trfSources = new[]
            {
                new[] { "Source Table", "Data Provided", "How Used" },
                new[] { "QTY_SALE_QTY", "48 weekly sale columns per ST_CD + MAJ_CAT", "UNPIVOT → CM_BGT_SALE_Q, CM1, CM2, SQ1-SQ4 for DC_MBQ" },
                new[] { "QTY_DISP_QTY", "48 weekly display columns", "UNPIVOT → BGT_DISP_CL_Q, used in MBQ" },
                new[] { "STORE_STOCK", "STK_QTY per ST_CD + MAJ_CAT (with DATE)", "Week 1 OP_STK (latest date)" },
                new[] { "MASTER_BIN_CAPACITY", "MBQ, BIN_CAP, BIN_CAP_DC_TEAM", "MBQ baseline, bin rounding" },
                new[] { "MASTER_ST_MASTER", "ST_CD, RDC_CD, etc.", "Store metadata, RDC mapping" },
                new[] { "MASTER_PRODUCT_HIERARCHY", "SSN per category", "Shrinkage factor (S,PS=100%; others=50%)" },
                new[] { "WEEK_CALENDAR", "WEEK_ID, FY_WEEK, WEEK_SEQ", "Week ordering, sequence" },
            };
            AddTable(body, trfSources);
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 4. PURCHASE PLAN ALGORITHM
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "4. PURCHASE PLAN — COMPLETE ALGORITHM & FORMULAS");

            AddParagraph(body, "SP: SP_GENERATE_PURCHASE_PLAN  |  Granularity: RDC_CD + MAJ_CAT + WEEK  |  Output: PURCHASE_PLAN", 10, true);
            AddParagraph(body, "The Purchase Plan READS from TRF_IN_PLAN output — aggregates store-level TRF_IN_STK_Q by RDC to get DC transfer-out quantities. Then runs a DC-level week-chaining loop.");

            AddHeading(body, "SP Execution Steps", 13, true, "1a3a5c");
            AddParagraph(body, "Step 1: Build #Weeks + offset map (same as TRF).");
            AddParagraph(body, "Step 2: Build RDC x MAJ_CAT combinations (cross join ST_MASTER x BIN_CAPACITY).");
            AddParagraph(body, "Step 3: Aggregate TRF_IN_PLAN by RDC — SUM(TRF_IN_STK_Q) = CW_TRF_OUT_Q. Build offset tables for CW1-CW4 TRF_OUT and CW2-CW5 SALE.");
            AddParagraph(body, "Step 4: UNPIVOT MASTER_GRT_CONTRIBUTION (48 WK cols → SSN+WkNum+GrtPct). Join DC_STOCK, BIN_CAPACITY, DEL_PENDING.");
            AddParagraph(body, "Step 5: Build #PP calculation table (~50 columns). Set TTL_TRF_OUT = CW+CW1+CW2+CW3+CW4.");
            AddParagraph(body, "Step 6: Week 1 calculations + WHILE loop Week 2..N chaining.");
            AddParagraph(body, "Step 7: DELETE old + INSERT into PURCHASE_PLAN.");

            AddHeading(body, "Core DC Chain Algorithm", 13, true, "8e44ad");
            AddCodeBlock(body, @"FOR EACH RDC_CD + MAJ_CAT + WEEK:

WEEK 1 — Opening Values:
  DC_STK_Q       = DC_STOCK table
  GRT_STK_Q      = DC_STOCK.GRT_STK_Q
  TTL_STK        = GRT_STK_Q
  OP_STK         = GRT_STK_Q
  BGT_DC_OP_STK_Q = DC_STK_Q
  BGT_CF_STK_Q   = DC_STK_Q
  NT_ACT_STK     = IF SSN IN ('S','OC','A') THEN GRT_STK_Q * 0.10 ELSE 0

WEEK 2+ — Chained Values:
  OP_STK         = prev_week.NET_SSNL_CL_STK_Q
  DC_STK_Q       = prev_week.BGT_DC_CL_STK_Q
  BGT_DC_OP_STK_Q = prev_week.BGT_DC_CL_STK_Q
  BGT_CF_STK_Q   = MAX(prev_week.BGT_DC_CL_STK_Q, 0)

ALL WEEKS — Transfer Out (from TRF_IN_PLAN):
  CW_TRF_OUT_Q   = SUM(TRF_IN_STK_Q) by RDC for current week
  CW1..CW4       = same at week offsets +1..+4
  TTL_TRF_OUT_Q  = CW + CW1 + CW2 + CW3 + CW4

ALL WEEKS — Sale & MBQ:
  BGT_DC_MBQ_SALE = CW1_SALE + CW2_SALE + CW3_SALE + CW4_SALE (next 4 weeks)
  BGT_DC_CL_MBQ  = MIN(CW1+CW2+CW3+CW4 TRF_OUT, BGT_DC_MBQ_SALE)

ALL WEEKS — GRT Consumption:
  IF TTL_TRF_OUT = 0 THEN GRT_CONS_Q = 0
  ELSE GRT_CONS_Q = MIN of:
    - TTL_TRF_OUT * 0.30
    - MAX(OP_STK - NT_ACT_STK, 0)
    - MAX(TTL_TRF - MAX(BGT_CF_STK - BGT_DC_CL_MBQ, 0), 0)
    - MAX(TTL_STK - NT_ACT_STK, 0) * GRT_CONS_PCT

ALL WEEKS — Net & Seasonal:
  PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q
  NET_SSNL_CL_STK_Q   = MAX(OP_STK - GRT_CONS_Q, 0)

ALL WEEKS — Purchase Quantity:
  POS_PO_RAISED  = MAX(BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q, 0)
  BGT_PUR_Q_INIT = POS_PO_RAISED

ALL WEEKS — NEG PO (Carry-Forward):
  WK-1: NEG_PO_RAISED = MIN(0, BGT_PUR_Q_INIT - DEL_PEND_Q)
  WK-2+: NEG_PO_RAISED = MIN(0, BGT_PUR_Q_INIT - DEL_PEND_Q + prev_NEG_PO)

ALL WEEKS — DC Closing Stock:
  BGT_DC_CL_STK_Q = MAX(BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q, 0)

ALL WEEKS — Excess / Short:
  DC_STK_EXCESS_Q = MAX(BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ, 0)
  DC_STK_SHORT_Q  = MAX(BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q, 0)
  CO_STK_EXCESS   = ST_STK_EXCESS + DC_STK_EXCESS
  CO_STK_SHORT    = ST_STK_SHORT + DC_STK_SHORT
  BGT_CO_CL_STK   = NET_BGT_ST_CL + NET_SSNL_CL + BGT_DC_CL
  FRESH_BIN_REQ   = BGT_DC_CL_STK_Q / BIN_CAP
  GRT_BIN_REQ     = OP_STK / BIN_CAP

WEEK CHAIN: BGT_DC_CL_STK_Q → next DC_STK_Q, BGT_DC_OP_STK_Q, BGT_CF_STK_Q
            NET_SSNL_CL_STK_Q → next OP_STK
            NEG_PO_RAISED → carry-forward accumulation");

            AddHeading(body, "All PURCHASE_PLAN Output Columns", 13, true, "8e44ad");
            var ppCols = new[]
            {
                new[] { "Column", "Formula", "Type" },
                new[] { "RDC_CD, RDC_NM, MAJ_CAT, SSN, SEG, DIV, SUB_DIV, MAJ_CAT_NM", "From ST_MASTER + PRODUCT_HIERARCHY", "Input" },
                new[] { "DC_STK_Q", "WK1: DC_STOCK | WK2+: prev BGT_DC_CL_STK_Q", "Chain" },
                new[] { "GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q", "From DC_STOCK", "Input" },
                new[] { "BIN_CAP_DC_TEAM, BIN_CAP", "From MASTER_BIN_CAPACITY", "Input" },
                new[] { "CW_BGT_SALE_Q .. CW5_BGT_SALE_Q", "Aggregated from TRF_IN_PLAN at week offsets", "Derived" },
                new[] { "BGT_DC_OP_STK_Q", "WK1: DC_STK_Q | WK2+: prev BGT_DC_CL_STK_Q", "Chain" },
                new[] { "BGT_CF_STK_Q", "WK1: DC_STK_Q | WK2+: MAX(prev BGT_DC_CL_STK_Q, 0)", "Chain" },
                new[] { "OP_STK", "WK1: GRT_STK_Q | WK2+: prev NET_SSNL_CL_STK_Q", "Chain" },
                new[] { "GRT_CONS_PCT", "UNPIVOT from MASTER_GRT_CONTRIBUTION by SSN+Week", "Input" },
                new[] { "DEL_PEND_Q", "From DEL_PENDING table (latest by DATE)", "Input" },
                new[] { "CW_TRF_OUT_Q .. CW4_TRF_OUT_Q", "SUM(TRF_IN_STK_Q) from TRF_IN_PLAN by RDC", "Derived" },
                new[] { "TTL_TRF_OUT_Q", "CW + CW1 + CW2 + CW3 + CW4", "Derived" },
                new[] { "BGT_DC_MBQ_SALE", "CW1 + CW2 + CW3 + CW4 SALE", "Derived" },
                new[] { "BGT_DC_CL_MBQ", "MIN(CW1+CW2+CW3+CW4 TRF_OUT, BGT_DC_MBQ_SALE)", "Derived" },
                new[] { "GRT_CONS_Q", "MIN(TTL*0.30, MAX(OP-NT,0), MAX(TTL-MAX(CF-MBQ,0),0), MAX(TTL-NT,0)*PCT)", "Derived" },
                new[] { "PP_NET_BGT_CF_STK_Q", "BGT_CF_STK + GRT_CONS + DEL_PEND", "Derived" },
                new[] { "NET_SSNL_CL_STK_Q", "MAX(OP_STK - GRT_CONS_Q, 0) — chains to next OP_STK", "Chain" },
                new[] { "POS_PO_RAISED", "MAX(BGT_DC_CL_MBQ + CW_TRF_OUT - PP_NET, 0)", "Derived" },
                new[] { "BGT_PUR_Q_INIT", "= POS_PO_RAISED", "Derived" },
                new[] { "NEG_PO_RAISED", "WK1: MIN(0,PUR-DEL) | WK2+: MIN(0,PUR-DEL+prev_NEG)", "Chain" },
                new[] { "BGT_DC_CL_STK_Q", "MAX(PUR + PP_NET - CW_TRF_OUT, 0) — chains to next week", "Chain" },
                new[] { "DC_STK_EXCESS_Q / DC_STK_SHORT_Q", "MAX(CL-MBQ,0) / MAX(MBQ-CL,0)", "Derived" },
                new[] { "CO_STK_EXCESS / SHORT", "ST + DC combined excess/short", "Derived" },
                new[] { "FRESH_BIN_REQ / GRT_BIN_REQ", "BGT_DC_CL_STK/BIN_CAP  |  OP_STK/BIN_CAP", "Derived" },
            };
            AddTable(body, ppCols);
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 5. SUB-LEVEL PLANS
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "5. SUB-LEVEL PLANS (TRF + PP PER LEVEL)");

            AddHeading(body, "Overview", 13, true, "e67e22");
            AddParagraph(body, "Sub-level plans run the EXACT SAME algorithm as main plans but at a finer granularity. Instead of ST_CD + MAJ_CAT + WEEK, they add a SUB_VALUE dimension representing one of 4 levels:");
            AddParagraph(body, "  - MVGR: DISP_MVGR_MATRIX (from ST_MAJ_CAT_MACRO_MVGR_PLAN)");
            AddParagraph(body, "  - Size (SZ): SZ (from ST_MAJ_CAT_SZ_PLAN)");
            AddParagraph(body, "  - Segment (SEG): SEG (from ST_MAJ_CAT_SEG_PLAN)");
            AddParagraph(body, "  - Vendor (VND): M_VND_CD (from ST_MAJ_CAT_VND_PLAN)");
            AddParagraph(body, "");
            AddParagraph(body, "IMPORTANT: Sub-level plans run INDEPENDENTLY — NOT derived from main plan output. They run the full algorithm from scratch using level-specific stock and contribution percentages.", 10, true);

            AddHeading(body, "What Changes vs Main Plan", 13, true, "e67e22");
            var subDiff = new[]
            {
                new[] { "Aspect", "Main Plan", "Sub-Level Plan" },
                new[] { "Granularity", "ST_CD + MAJ_CAT + WEEK", "ST_CD + MAJ_CAT + SUB_VALUE + WEEK" },
                new[] { "Sale Qty", "Direct from QTY_SALE_QTY", "SALE_QTY x CONT_PCT from contribution table" },
                new[] { "Display Qty", "Direct from QTY_DISP_QTY", "DISP_QTY x CONT_PCT" },
                new[] { "Store Stock (TRF)", "From STORE_STOCK", "From SUB_ST_STK_{MVGR|SZ|SEG|VND}" },
                new[] { "DC Stock (PP)", "From DC_STOCK", "From SUB_DC_STK_{MVGR|SZ|SEG|VND}" },
                new[] { "TRF Algorithm", "Full chain", "Identical (shrinkage, NET_CF, TRF_IN, CL_STK)" },
                new[] { "PP GRT_CONS", "MIN of 4 values using GRT_CONS_PCT", "GRT_CONS_Q = 0 (simplified)" },
                new[] { "PP reads from", "TRF_IN_PLAN", "SUB_LEVEL_TRF_PLAN (same level)" },
                new[] { "Output", "TRF_IN_PLAN, PURCHASE_PLAN", "SUB_LEVEL_TRF_PLAN, SUB_LEVEL_PP_PLAN" },
                new[] { "Bin Rounding", "TRF_IN rounded to BIN_CAP multiples", "No bin rounding (raw quantity)" },
            };
            AddTable(body, subDiff);

            AddHeading(body, "Sub-Level TRF Algorithm (SP_GENERATE_SUB_LEVEL_TRF)", 13, true, "27ae60");
            AddCodeBlock(body, @"INPUT PREPARATION:
  1. Get contribution % from ST_MAJ_CAT_{LEVEL}_PLAN (dynamic table name)
  2. UNPIVOT QTY_SALE_QTY (48 WK cols → rows), multiply by CONT_PCT
  3. UNPIVOT QTY_DISP_QTY, multiply by CONT_PCT
  4. Load store stock from SUB_ST_STK_{LEVEL} (latest by DATE)

CHAIN TABLE (per ST_CD + MAJ_CAT + SUB_VALUE + WEEK):
  MBQ   = (DISP_QTY * CONT_PCT) + (SALE_QTY_next * CONT_PCT)
  SALE  = SALE_QTY_current * CONT_PCT
  OP_STK = WK1: SUB_ST_STK | WK2+: prev CL_STK

SAME FORMULAS AS MAIN TRF:
  SHRINKAGE = ROUND(ROUND(OP*0.08,0) * SSN_FACTOR, 0)
  NET_CF    = MAX(OP - SHRINKAGE, 0)
  TRF_IN    = IF(MBQ=0 AND SALE=0) 0 ELSE MAX(MBQ+SALE-NET_CF, 0)
  CL_STK    = closing stock logic (identical)
  DC_MBQ    = SQ1+SQ2+SQ3+SQ4 (next 4 weeks sale * CONT_PCT)

OUTPUT → SUB_LEVEL_TRF_PLAN WHERE LEVEL = @Level");

            AddHeading(body, "Sub-Level PP Algorithm (SP_GENERATE_SUB_LEVEL_PP)", 13, true, "8e44ad");
            AddCodeBlock(body, @"INPUT: Reads from SUB_LEVEL_TRF_PLAN (must run TRF first)
  Aggregates by RDC_CD + MAJ_CAT + SUB_VALUE + WEEK
  DC stock from SUB_DC_STK_{LEVEL} (latest by DATE)

WEEK 1:
  OP_STK = GRT_STK_Q from SUB_DC_STK
  GRT_CONS_Q = 0 (simplified)
  PP_NET = BGT_CF_STK + GRT_CONS
  NET_SSNL_CL_STK_Q = MAX(OP - GRT_CONS, 0)
  POS_PO_RAISED = MAX(BGT_DC_CL_MBQ + CW_TRF_OUT - PP_NET, 0)
  BGT_PUR_Q_INIT = POS_PO_RAISED
  NEG_PO_RAISED = MIN(0, PUR - DEL_PEND)
  BGT_DC_CL_STK_Q = MAX(PUR + PP_NET - CW_TRF_OUT, 0)

WEEK 2+:
  OP_STK = prev NET_SSNL_CL_STK_Q
  DC_STK = prev BGT_DC_CL_STK_Q
  NEG_PO = MIN(0, PUR - DEL_PEND + prev_NEG_PO)
  ... all other formulas identical

OUTPUT → SUB_LEVEL_PP_PLAN WHERE LEVEL = @Level");

            AddHeading(body, "Execution Flow", 13, true, "e67e22");
            AddCodeBlock(body, @"FOR EACH SELECTED LEVEL (MVGR, SZ, SEG, VND):
  STEP 1: Run SP_GENERATE_SUB_LEVEL_TRF(@Level, @StartWeek, @EndWeek)
  STEP 2: Run SP_GENERATE_SUB_LEVEL_PP(@Level, @StartWeek, @EndWeek)

FULL RUN (Background): SubLevelJobService runs all 4 levels sequentially
  Progress: Phase (TRF/PP), CurrentLevel, LevelsCompleted/TotalLevels
  Polled via SubJobStatus endpoint every 5 seconds");

            AddHeading(body, "Sub-Level Input Data (18 Tables)", 13, true, "e67e22");
            var subTables = new[]
            {
                new[] { "Category", "SQL Table Name", "Purpose" },
                new[] { "--- REFERENCE (shared) ---", "", "" },
                new[] { "Sale", "QTY_SALE_QTY", "Weekly sale (48 cols) x CONT_PCT" },
                new[] { "Display", "QTY_DISP_QTY", "Display qty (48 cols) x CONT_PCT" },
                new[] { "Week", "WEEK_CALENDAR", "Fiscal week definitions" },
                new[] { "Store", "MASTER_ST_MASTER", "Store-RDC mapping" },
                new[] { "Bin", "MASTER_BIN_CAPACITY", "MBQ + bin capacity" },
                new[] { "Product", "MASTER_PRODUCT_HIERARCHY", "SSN for shrinkage" },
                new[] { "--- CONTRIBUTION (4) ---", "", "" },
                new[] { "MVGR", "ST_MAJ_CAT_MACRO_MVGR_PLAN", "MVGR contribution % (ST_CD, MAJ_CAT_CD, DISP_MVGR_MATRIX, CONT_PCT)" },
                new[] { "Size", "ST_MAJ_CAT_SZ_PLAN", "Size contribution % (ST_CD, MAJ_CAT_CD, SZ, CONT_PCT)" },
                new[] { "Segment", "ST_MAJ_CAT_SEG_PLAN", "Segment contribution %" },
                new[] { "Vendor", "ST_MAJ_CAT_VND_PLAN", "Vendor contribution %" },
                new[] { "--- STORE STOCK (4) ---", "", "" },
                new[] { "MVGR", "SUB_ST_STK_MVGR", "Store stock at MVGR level (ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY, DATE)" },
                new[] { "Size", "SUB_ST_STK_SZ", "Store stock at Size level" },
                new[] { "Segment", "SUB_ST_STK_SEG", "Store stock at Segment level" },
                new[] { "Vendor", "SUB_ST_STK_VND", "Store stock at Vendor level" },
                new[] { "--- DC STOCK (4) ---", "", "" },
                new[] { "MVGR", "SUB_DC_STK_MVGR", "DC stock at MVGR level (RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, DATE)" },
                new[] { "Size", "SUB_DC_STK_SZ", "DC stock at Size level" },
                new[] { "Segment", "SUB_DC_STK_SEG", "DC stock at Segment level" },
                new[] { "Vendor", "SUB_DC_STK_VND", "DC stock at Vendor level" },
            };
            AddTable(body, subTables);
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 6. DATABASE SCHEMA
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "6. DATABASE SCHEMA & TABLES");

            AddHeading(body, "Output Tables (4)", 13, true, "1a3a5c");
            var outTables = new[]
            {
                new[] { "Table", "Granularity", "Generated By" },
                new[] { "TRF_IN_PLAN", "ST_CD + MAJ_CAT + WEEK", "SP_GENERATE_TRF_IN_PLAN / SP_RUN_ALL_PLANS" },
                new[] { "PURCHASE_PLAN", "RDC_CD + MAJ_CAT + WEEK", "SP_GENERATE_PURCHASE_PLAN" },
                new[] { "SUB_LEVEL_TRF_PLAN", "LEVEL + ST_CD + MAJ_CAT + SUB_VALUE + WEEK", "SP_GENERATE_SUB_LEVEL_TRF" },
                new[] { "SUB_LEVEL_PP_PLAN", "LEVEL + RDC_CD + MAJ_CAT + SUB_VALUE + WEEK", "SP_GENERATE_SUB_LEVEL_PP" },
            };
            AddTable(body, outTables);

            AddHeading(body, "Reference Tables (10)", 13, true, "1a3a5c");
            var refTables = new[]
            {
                new[] { "SQL Table", "Purpose", "Key Columns" },
                new[] { "WEEK_CALENDAR", "Fiscal week definitions", "WEEK_ID, FY_YEAR, FY_WEEK, WK_ST_DT, WK_END_DT, WEEK_SEQ" },
                new[] { "MASTER_ST_MASTER", "Store master + RDC mapping", "ST CD, ST NM, RDC_CD, RDC_NM, HUB_CD, AREA" },
                new[] { "MASTER_BIN_CAPACITY", "Bin capacity + MBQ per category", "ST-CD, MAJ-CAT, BIN CAP, BIN CAP DC TEAM, MBQ" },
                new[] { "QTY_SALE_QTY", "Weekly sale forecast (48 columns)", "ST-CD, MAJ-CAT, WK-1..WK-48" },
                new[] { "QTY_DISP_QTY", "Weekly display qty (48 columns)", "ST-CD, MAJ-CAT, WK-1..WK-48" },
                new[] { "STORE_STOCK", "Store stock (with DATE)", "ST_CD, MAJ_CAT, STK_QTY, DATE" },
                new[] { "DC_STOCK", "DC stock per RDC", "RDC_CD, MAJ_CAT, DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q" },
                new[] { "DEL_PENDING", "Delivery pending at DC", "RDC_CD, MAJ_CAT, DEL_PEND_Q, DATE" },
                new[] { "MASTER_GRT_CONTRIBUTION", "GRT % by SSN by week (48 cols)", "SSN, WK_1..WK_48" },
                new[] { "MASTER_PRODUCT_HIERARCHY", "Product classification → SSN", "SEG, DIV, SUB_DIV, MAJ_CAT_NM, SSN" },
            };
            AddTable(body, refTables);

            AddHeading(body, "Contribution + Sub-Level Stock Tables (12)", 13, true, "1a3a5c");
            var contTables = new[]
            {
                new[] { "Category", "Table", "Sub-Value Column", "Key Columns" },
                new[] { "MVGR Contribution", "ST_MAJ_CAT_MACRO_MVGR_PLAN", "DISP_MVGR_MATRIX", "ST_CD, MAJ_CAT_CD, sub-value, CONT_PCT" },
                new[] { "Size Contribution", "ST_MAJ_CAT_SZ_PLAN", "SZ", "ST_CD, MAJ_CAT_CD, SZ, CONT_PCT" },
                new[] { "Segment Contribution", "ST_MAJ_CAT_SEG_PLAN", "SEG", "ST_CD, MAJ_CAT_CD, SEG, CONT_PCT" },
                new[] { "Vendor Contribution", "ST_MAJ_CAT_VND_PLAN", "M_VND_CD", "ST_CD, MAJ_CAT_CD, M_VND_CD, CONT_PCT" },
                new[] { "Store Stk MVGR/SZ/SEG/VND", "SUB_ST_STK_{level}", "SUB_VALUE", "ST_CD, MAJ_CAT, SUB_VALUE, STK_QTY, DATE" },
                new[] { "DC Stk MVGR/SZ/SEG/VND", "SUB_DC_STK_{level}", "SUB_VALUE", "RDC_CD, MAJ_CAT, SUB_VALUE, DC_STK_Q, GRT_STK_Q, DATE" },
            };
            AddTable(body, contTables);

            AddHeading(body, "Database Patterns & Conventions", 13, true, "1a3a5c");
            AddParagraph(body, "  - Column Naming: SQL uses [ST-CD], [MAJ-CAT], [BIN CAP] (hyphens/spaces). EF models use [Column(\"ST-CD\")] mapping.");
            AddParagraph(body, "  - NOLOCK: All read queries use WITH (NOLOCK) hint.");
            AddParagraph(body, "  - UNPIVOT: Sale/Display 48 WK columns are unpivoted to rows. After UNPIVOT, original alias is invalid.");
            AddParagraph(body, "  - Dynamic SQL: Sub-level SPs use sp_executesql for dynamic table names. Temp tables must be CREATE'd in parent scope.");
            AddParagraph(body, "  - SqlBulkCopy: Bulk Upload uses SqlBulkCopy for 20L+ row performance.");
            AddParagraph(body, "  - Nullable: Most decimal columns are decimal? in EF models. Always use ?? 0 when summing.");
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 7. SYSTEM FEATURES
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "7. SYSTEM FEATURES");

            AddHeading(body, "Dashboard (Operations Command Center)", 13, true, "1a3a5c");
            AddParagraph(body, "  - 4 Alert Cards: Critical Short Stores, Excess Stores, DC Short, Zero Stock + Demand");
            AddParagraph(body, "  - 10 KPI Cards: Stores, Categories, RDCs, Transfer In Qty, Purchase Qty, DC Short/Excess, TRF Out, PO Raised, Del Pending");
            AddParagraph(body, "  - 6 Charts: TRF by Category, Weekly Trend, RDC Bar, Short vs Excess, PP by Category, PP Weekly Trend");
            AddParagraph(body, "  - RDC Inventory Health Table, Top 10 Short/Excess Tables, Risk Categories");
            AddParagraph(body, "  - Sub-Level Status: Per-level TRF/PP row counts + timestamps");
            AddParagraph(body, "  - Data Health: 14 reference table row counts with Ready/Empty status");
            AddParagraph(body, "  - Drill-Down Filters: RDC, Category, FY Week (multi-select with search). Cached 60s per filter.");

            AddHeading(body, "Bulk Upload", 13, true, "1a3a5c");
            AddParagraph(body, "  - 22 Tables Supported: 10 Reference + 4 Contribution + 8 Sub-Level Stock");
            AddParagraph(body, "  - Multi-Sheet Excel: Reads ALL sheets from .xlsx and merges rows");
            AddParagraph(body, "  - Replace or Append mode");
            AddParagraph(body, "  - SqlBulkCopy for performance (20 lakh+ rows in seconds)");
            AddParagraph(body, "  - 500 MB file limit, Download Sample, Download Data, Column Validation");

            AddHeading(body, "Data Export", 13, true, "1a3a5c");
            AddParagraph(body, "  - CSV Export: All output + reference pages. Streamed for 13M+ rows.");
            AddParagraph(body, "  - Pivot CSV (24 metrics): TRF Output, PP Output — weeks as columns.");
            AddParagraph(body, "  - Sub-Level Pivot CSV: Per level, one row per ST_CD+MAJ_CAT+SUB_VALUE.");
            AddParagraph(body, "  - Dashboard CSV: Full TRF_IN_PLAN or PURCHASE_PLAN dump.");

            AddHeading(body, "Reset / Clear Data", 13, true, "c0392b");
            AddParagraph(body, "  - Reset Transfer In: TRUNCATE TABLE TRF_IN_PLAN");
            AddParagraph(body, "  - Reset Purchase Plan: TRUNCATE TABLE PURCHASE_PLAN");
            AddParagraph(body, "  - Reset Sub-Level (per level): DELETE WHERE LEVEL='X'");
            AddParagraph(body, "  - Reset Sub-Level (all): TRUNCATE both SUB_LEVEL tables");
            AddParagraph(body, "  All require JavaScript confirmation. Irreversible.");

            AddHeading(body, "CRUD Operations (22 Table Pages)", 13, true, "1a3a5c");
            AddParagraph(body, "Every reference, contribution, and sub-stock table has: Index (DataTable with search/sort/filter/analytics/CSV), Create, Edit, Delete.");

            AddHeading(body, "Technology Stack", 13, true, "1a3a5c");
            AddParagraph(body, "Backend: ASP.NET Core 8.0 MVC, EF Core 8, EPPlus 7, IMemoryCache, Singleton background jobs");
            AddParagraph(body, "Frontend: Bootstrap 5.3, Chart.js 4.4, DataTables 1.13, Inter font, Custom multi-select JS");
            AddParagraph(body, "Database: SQL Server ('planning'), 6 SPs, 22 reference tables, 4 output tables, NOLOCK, temp tables");
            AddPageBreak(body);

            // ══════════════════════════════════════════════
            // 8. CHANGE LOG
            // ══════════════════════════════════════════════
            AddSectionTitle(body, "8. CHANGE LOG");

            var changes = new[]
            {
                new[] { "#", "Area", "Change", "Before", "After" },
                new[] { "1", "TRF", "DC_MBQ formula", "Sale/30*15", "SQ1+SQ2+SQ3+SQ4" },
                new[] { "2", "PP", "BGT_DC_MBQ_SALE", "SUM(DC_MBQ)", "CW1+CW2+CW3+CW4 SALE" },
                new[] { "3", "PP", "BGT_DC_CL_MBQ", "MIN(TTL_TRF, SALE)", "MIN(CW1-4 TRF, SALE)" },
                new[] { "4", "PP", "CW2-CW5 SALE sourcing from TRF", "Separate calc", "From TRF output" },
                new[] { "5", "PP", "CW2-CW4 TRF_OUT from TRF", "N/A", "From TRF by RDC" },
                new[] { "6", "PP", "DC_STK_Q week chaining WK2+", "Static", "Chained" },
                new[] { "7", "PP", "OP_STK chaining WK2+ = prev NET_SSNL", "Static", "Chained" },
                new[] { "8", "PP", "POS_PO = MAX(MBQ+TRF-NET, 0)", "MAX(PUR-DEL,0)", "New formula" },
                new[] { "9", "PP", "BGT_PUR_Q_INIT = POS_PO_RAISED", "TTL+MBQ-NET-DEL", "= POS_PO" },
                new[] { "10", "PP", "BGT_DC_CL_STK = MAX(PUR+NET-TRF, 0)", "NET+POS-TTL", "New formula" },
                new[] { "11", "PP", "NEG_PO carry-forward WK2+", "No carry", "+prev_NEG" },
                new[] { "12", "PP", "GRT_CONS_PCT dynamic UNPIVOT", "Hardcoded", "Dynamic SSN+Week" },
                new[] { "13", "Ref", "Broader_Menu → PRODUCT_HIERARCHY", "Old table", "New table" },
                new[] { "14", "Ref", "GRT_CONS_pct → GRT_CONTRIBUTION", "Old table", "UNPIVOT" },
                new[] { "15", "System", "SP_RUN_ALL_PLANS V3 bulk", "~27 min", "~1-2 min" },
                new[] { "16", "System", "Dashboard optimization", "30+ queries", "2 SQL + cache" },
                new[] { "17", "Sub", "4 Contribution tables + CRUD", "N/A", "Full CRUD" },
                new[] { "18", "Sub", "Sub-Level TRF SP", "N/A", "Full chain per level" },
                new[] { "19", "Sub", "Sub-Level PP SP", "N/A", "Full DC chain per level" },
                new[] { "20", "Sub", "8 Sub-Level Stock tables", "N/A", "CRUD + Upload" },
                new[] { "21", "Sub", "Background Full Run (4 levels)", "Sync only", "Background job" },
                new[] { "22", "Sub", "Pivot CSV for sub-levels", "N/A", "Per-level pivot" },
                new[] { "23", "Sub", "Input Data Status (18 tables)", "6 tables", "18 + logic" },
                new[] { "24", "System", "Multi-select filters", "Single", "Multi + search" },
                new[] { "25", "System", "Multi-sheet Excel upload", "1 sheet", "All sheets" },
                new[] { "26", "System", "Reset buttons (TRF, PP, Sub)", "N/A", "TRUNCATE/DELETE" },
                new[] { "27", "System", "UI Redesign (Design System v2)", "Basic", "Enterprise" },
                new[] { "28", "System", "Dashboard Sub-Level + Data Health", "N/A", "Status cards" },
            };
            AddTable(body, changes);

            // Footer
            AddParagraph(body, "");
            AddHorizontalLine(body);
            AddParagraph(body, $"End of Document — Generated {DateTime.Now:dd-MMM-yyyy HH:mm} — Transfer In Plan Management System", 9, false, "808080");
        }

        ms.Position = 0;
        return File(ms.ToArray(),
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            $"TRF_IN_PLAN_Knowledge_Base_{DateTime.Now:yyyyMMdd}.docx");
    }

    // ═══════════════════════════════════════════════════
    // HELPER METHODS
    // ═══════════════════════════════════════════════════

    private void AddSectionTitle(Body body, string text)
    {
        var p = body.AppendChild(new Paragraph());
        var ppr = p.AppendChild(new ParagraphProperties(
            new SpacingBetweenLines { Before = "200", After = "100" },
            new ParagraphBorders(new BottomBorder { Val = BorderValues.Single, Size = 6, Color = "4F46E5", Space = 4 })
        ));
        var run = p.AppendChild(new Run());
        run.AppendChild(new RunProperties(
            new Bold(),
            new FontSize { Val = "28" },
            new Color { Val = "2F5496" },
            new RunFonts { Ascii = "Calibri", HighAnsi = "Calibri" }
        ));
        run.AppendChild(new Text(text));
    }

    private void AddHeading(Body body, string text, int sizePt, bool bold, string? color = null)
    {
        var p = body.AppendChild(new Paragraph());
        p.AppendChild(new ParagraphProperties(new SpacingBetweenLines { Before = "120", After = "60" }));
        var run = p.AppendChild(new Run());
        var rp = new RunProperties(
            new FontSize { Val = (sizePt * 2).ToString() },
            new RunFonts { Ascii = "Calibri", HighAnsi = "Calibri" }
        );
        if (bold) rp.AppendChild(new Bold());
        if (color != null) rp.AppendChild(new Color { Val = color });
        run.AppendChild(rp);
        run.AppendChild(new Text(text));
    }

    private void AddParagraph(Body body, string text, int sizePt = 10, bool bold = false, string? color = null)
    {
        var p = body.AppendChild(new Paragraph());
        p.AppendChild(new ParagraphProperties(new SpacingBetweenLines { Before = "20", After = "20" }));
        var run = p.AppendChild(new Run());
        var rp = new RunProperties(
            new FontSize { Val = (sizePt * 2).ToString() },
            new RunFonts { Ascii = "Calibri", HighAnsi = "Calibri" }
        );
        if (bold) rp.AppendChild(new Bold());
        if (color != null) rp.AppendChild(new Color { Val = color });
        run.AppendChild(rp);
        run.AppendChild(new Text(text) { Space = SpaceProcessingModeValues.Preserve });
    }

    private void AddCodeBlock(Body body, string code)
    {
        foreach (var line in code.Split('\n'))
        {
            var p = body.AppendChild(new Paragraph());
            p.AppendChild(new ParagraphProperties(
                new SpacingBetweenLines { Before = "0", After = "0" },
                new Shading { Fill = "F2F2F2", Val = ShadingPatternValues.Clear }
            ));
            var run = p.AppendChild(new Run());
            run.AppendChild(new RunProperties(
                new FontSize { Val = "18" },
                new RunFonts { Ascii = "Consolas", HighAnsi = "Consolas" },
                new Color { Val = "1a3a5c" }
            ));
            run.AppendChild(new Text(line) { Space = SpaceProcessingModeValues.Preserve });
        }
    }

    private void AddTable(Body body, string[][] rows)
    {
        var table = body.AppendChild(new Table());
        table.AppendChild(new TableProperties(
            new TableWidth { Width = "5000", Type = TableWidthUnitValues.Pct },
            new TableBorders(
                new TopBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" },
                new BottomBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" },
                new LeftBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" },
                new RightBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" },
                new InsideHorizontalBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" },
                new InsideVerticalBorder { Val = BorderValues.Single, Size = 4, Color = "CCCCCC" }
            ),
            new TableCellMarginDefault(
                new TopMargin { Width = "40", Type = TableWidthUnitValues.Dxa },
                new TableCellLeftMargin { Width = 60, Type = TableWidthValues.Dxa },
                new BottomMargin { Width = "40", Type = TableWidthUnitValues.Dxa },
                new TableCellRightMargin { Width = 60, Type = TableWidthValues.Dxa }
            )
        ));

        for (int i = 0; i < rows.Length; i++)
        {
            var tr = table.AppendChild(new TableRow());
            bool isHeader = (i == 0);
            foreach (var cellText in rows[i])
            {
                var tc = tr.AppendChild(new TableCell());
                if (isHeader)
                {
                    tc.AppendChild(new TableCellProperties(
                        new Shading { Fill = "2F5496", Val = ShadingPatternValues.Clear }
                    ));
                }
                else if (cellText.StartsWith("---"))
                {
                    tc.AppendChild(new TableCellProperties(
                        new Shading { Fill = "E8E8E8", Val = ShadingPatternValues.Clear }
                    ));
                }
                var p = tc.AppendChild(new Paragraph());
                p.AppendChild(new ParagraphProperties(new SpacingBetweenLines { Before = "0", After = "0" }));
                var run = p.AppendChild(new Run());
                var rp = new RunProperties(
                    new FontSize { Val = "18" },
                    new RunFonts { Ascii = "Calibri", HighAnsi = "Calibri" }
                );
                if (isHeader) { rp.AppendChild(new Bold()); rp.AppendChild(new Color { Val = "FFFFFF" }); }
                else { rp.AppendChild(new Color { Val = "1a3a5c" }); }
                run.AppendChild(rp);
                run.AppendChild(new Text(cellText) { Space = SpaceProcessingModeValues.Preserve });
            }
        }
        body.AppendChild(new Paragraph(new ParagraphProperties(new SpacingBetweenLines { Before = "60", After = "60" })));
    }

    private void AddHorizontalLine(Body body)
    {
        var p = body.AppendChild(new Paragraph());
        p.AppendChild(new ParagraphProperties(
            new ParagraphBorders(new BottomBorder { Val = BorderValues.Single, Size = 6, Color = "4F46E5", Space = 4 })
        ));
    }

    private void AddPageBreak(Body body)
    {
        var p = body.AppendChild(new Paragraph());
        p.AppendChild(new Run(new Break { Type = BreakValues.Page }));
    }
}
