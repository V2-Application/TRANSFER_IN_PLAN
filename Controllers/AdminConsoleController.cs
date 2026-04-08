using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Text.Json;
using TRANSFER_IN_PLAN.Data;

namespace TRANSFER_IN_PLAN.Controllers;

public class AdminConsoleController : Controller
{
    private readonly PlanningDbContext _context;
    private readonly ILogger<AdminConsoleController> _logger;

    public AdminConsoleController(PlanningDbContext context, ILogger<AdminConsoleController> logger)
    {
        _context = context;
        _logger = logger;
    }

    public IActionResult Index() => View();

    // ═══════════════════════════════════════════════════
    // EXECUTE SQL QUERY
    // ═══════════════════════════════════════════════════
    [HttpPost]
    public async Task<IActionResult> ExecuteQuery([FromBody] QueryRequest request)
    {
        if (string.IsNullOrWhiteSpace(request?.Sql))
            return Json(new { success = false, error = "SQL query cannot be empty." });

        var sql = request.Sql.Trim();
        var maxRows = request.MaxRows > 0 ? request.MaxRows : 500;

        try
        {
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = 120;

            // Detect if it's a SELECT/read query or a write query
            var upperSql = sql.ToUpperInvariant().TrimStart();
            bool isSelect = upperSql.StartsWith("SELECT") || upperSql.StartsWith("WITH")
                         || upperSql.StartsWith("EXEC") || upperSql.StartsWith("SP_");

            if (isSelect)
            {
                using var reader = await cmd.ExecuteReaderAsync();
                var columns = new List<string>();
                for (int i = 0; i < reader.FieldCount; i++)
                    columns.Add(reader.GetName(i));

                var rows = new List<Dictionary<string, object?>>();
                int count = 0;
                while (await reader.ReadAsync() && count < maxRows)
                {
                    var row = new Dictionary<string, object?>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var val = reader.GetValue(i);
                        row[columns[i]] = val == DBNull.Value ? null : val;
                    }
                    rows.Add(row);
                    count++;
                }

                bool hasMore = await reader.ReadAsync();
                return Json(new
                {
                    success = true,
                    type = "select",
                    columns,
                    rows,
                    rowCount = rows.Count,
                    hasMore,
                    maxRows
                });
            }
            else
            {
                int affected = await cmd.ExecuteNonQueryAsync();
                return Json(new
                {
                    success = true,
                    type = "execute",
                    message = $"Query executed successfully. {affected} row(s) affected.",
                    rowsAffected = affected
                });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Admin Console SQL error");
            return Json(new { success = false, error = ex.Message });
        }
    }

    // ═══════════════════════════════════════════════════
    // GET ALL TABLES + ROW COUNTS
    // ═══════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> GetTables()
    {
        try
        {
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            var sql = @"
                SELECT t.TABLE_NAME,
                       p.rows AS ROW_COUNT,
                       CASE WHEN t.TABLE_NAME LIKE 'TRF_IN_PLAN%' OR t.TABLE_NAME LIKE 'PURCHASE_PLAN%'
                                 OR t.TABLE_NAME LIKE 'SUB_LEVEL%' THEN 'Output'
                            WHEN t.TABLE_NAME LIKE 'ST_MAJ_CAT%' THEN 'Contribution'
                            WHEN t.TABLE_NAME LIKE 'SUB_ST_STK%' OR t.TABLE_NAME LIKE 'SUB_DC_STK%' THEN 'Sub Stock'
                            ELSE 'Reference' END AS CATEGORY
                FROM INFORMATION_SCHEMA.TABLES t
                LEFT JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                    AND p.index_id IN (0,1)
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                ORDER BY CATEGORY, t.TABLE_NAME";

            using var cmd = new SqlCommand(sql, conn);
            using var reader = await cmd.ExecuteReaderAsync();
            var tables = new List<object>();
            while (await reader.ReadAsync())
            {
                tables.Add(new
                {
                    name = reader.GetString(0),
                    rows = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                    category = reader.GetString(2)
                });
            }
            return Json(new { success = true, tables });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    // ═══════════════════════════════════════════════════
    // GET TABLE SCHEMA (columns)
    // ═══════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> GetTableSchema(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return Json(new { success = false, error = "Table name required." });

        try
        {
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            var sql = @"
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COLUMN_DEFAULT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @t
                ORDER BY ORDINAL_POSITION";

            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@t", tableName);
            using var reader = await cmd.ExecuteReaderAsync();
            var columns = new List<object>();
            while (await reader.ReadAsync())
            {
                columns.Add(new
                {
                    name = reader.GetString(0),
                    type = reader.GetString(1),
                    maxLength = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                    nullable = reader.GetString(3),
                    defaultVal = reader.IsDBNull(4) ? null : reader.GetString(4)
                });
            }
            return Json(new { success = true, tableName, columns });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    // ═══════════════════════════════════════════════════
    // GET TABLE PREVIEW (TOP 50 rows)
    // ═══════════════════════════════════════════════════
    [HttpGet]
    public async Task<IActionResult> PreviewTable(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return Json(new { success = false, error = "Table name required." });

        // Sanitize table name — only allow alphanumeric + underscore
        if (!System.Text.RegularExpressions.Regex.IsMatch(tableName, @"^[A-Za-z0-9_]+$"))
            return Json(new { success = false, error = "Invalid table name." });

        try
        {
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            using var cmd = new SqlCommand($"SELECT TOP 50 * FROM [{tableName}] WITH (NOLOCK)", conn);
            cmd.CommandTimeout = 30;
            using var reader = await cmd.ExecuteReaderAsync();

            var columns = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            var rows = new List<Dictionary<string, object?>>();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    row[columns[i]] = val == DBNull.Value ? null : val;
                }
                rows.Add(row);
            }

            return Json(new { success = true, columns, rows, rowCount = rows.Count });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    // ═══════════════════════════════════════════════════
    // QUICK ACTIONS
    // ═══════════════════════════════════════════════════
    [HttpPost]
    public async Task<IActionResult> QuickAction([FromBody] QuickActionRequest request)
    {
        try
        {
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();

            string sql = request.Action switch
            {
                "plan_status" => @"
                    SELECT 'TRF_IN_PLAN' AS [Table], COUNT(*) AS Rows, COUNT(DISTINCT [ST_CD]) AS Stores,
                           COUNT(DISTINCT [MAJ_CAT]) AS Categories, MIN([FY_WEEK]) AS MinWeek, MAX([FY_WEEK]) AS MaxWeek
                    FROM TRF_IN_PLAN WITH (NOLOCK)
                    UNION ALL
                    SELECT 'PURCHASE_PLAN', COUNT(*), COUNT(DISTINCT [RDC_CD]),
                           COUNT(DISTINCT [MAJ_CAT]), MIN([FY_WEEK]), MAX([FY_WEEK])
                    FROM PURCHASE_PLAN WITH (NOLOCK)",

                "sub_level_status" => @"
                    SELECT [LEVEL] AS [Level], 'TRF' AS [Type], COUNT(*) AS Rows,
                           COUNT(DISTINCT SUB_VALUE) AS SubValues, MAX(CREATED_DT) AS LastRun
                    FROM SUB_LEVEL_TRF_PLAN WITH (NOLOCK) GROUP BY [LEVEL]
                    UNION ALL
                    SELECT [LEVEL], 'PP', COUNT(*), COUNT(DISTINCT SUB_VALUE), MAX(CREATED_DT)
                    FROM SUB_LEVEL_PP_PLAN WITH (NOLOCK) GROUP BY [LEVEL]
                    ORDER BY [Level], [Type]",

                "table_health" => @"
                    SELECT t.TABLE_NAME, p.rows AS ROW_COUNT
                    FROM INFORMATION_SCHEMA.TABLES t
                    LEFT JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA + '.' + t.TABLE_NAME)
                        AND p.index_id IN (0,1)
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                    ORDER BY t.TABLE_NAME",

                "db_size" => @"
                    SELECT DB_NAME() AS DatabaseName,
                           CAST(SUM(size * 8.0 / 1024) AS DECIMAL(10,2)) AS SizeMB,
                           CAST(SUM(CASE WHEN type = 0 THEN size * 8.0 / 1024 END) AS DECIMAL(10,2)) AS DataMB,
                           CAST(SUM(CASE WHEN type = 1 THEN size * 8.0 / 1024 END) AS DECIMAL(10,2)) AS LogMB
                    FROM sys.database_files",

                "sp_list" => @"
                    SELECT name AS SP_Name,
                           CAST(create_date AS DATE) AS Created,
                           CAST(modify_date AS DATE) AS LastModified
                    FROM sys.procedures ORDER BY name",

                "active_connections" => @"
                    SELECT DB_NAME(dbid) AS DatabaseName, COUNT(*) AS Connections,
                           loginame AS LoginName
                    FROM sys.sysprocesses
                    WHERE dbid > 0
                    GROUP BY dbid, loginame
                    ORDER BY Connections DESC",

                "index_usage" => @"
                    SELECT TOP 20 OBJECT_NAME(i.object_id) AS TableName, i.name AS IndexName,
                           s.user_seeks, s.user_scans, s.user_lookups, s.user_updates
                    FROM sys.indexes i
                    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
                    WHERE OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1 AND i.name IS NOT NULL
                    ORDER BY (ISNULL(s.user_seeks,0) + ISNULL(s.user_scans,0)) DESC",

                _ => throw new ArgumentException($"Unknown action: {request.Action}")
            };

            using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = 30;
            using var reader = await cmd.ExecuteReaderAsync();

            var columns = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            var rows = new List<Dictionary<string, object?>>();
            while (await reader.ReadAsync())
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    row[columns[i]] = val == DBNull.Value ? null : val;
                }
                rows.Add(row);
            }

            return Json(new { success = true, type = "select", columns, rows, rowCount = rows.Count });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = ex.Message });
        }
    }

    // ═══════════════════════════════════════════════════
    // AI PROMPT — Natural Language to SQL/Knowledge
    // ═══════════════════════════════════════════════════
    [HttpPost]
    public async Task<IActionResult> AskAI([FromBody] AskRequest request)
    {
        if (string.IsNullOrWhiteSpace(request?.Prompt))
            return Json(new { success = false, error = "Please type a question." });

        var prompt = request.Prompt.Trim();
        var lower = prompt.ToLowerInvariant();

        try
        {
            // ── 1. Knowledge Base Responses ──
            var knowledge = MatchKnowledge(lower);
            if (knowledge != null)
                return Json(new { success = true, type = "knowledge", answer = knowledge, sql = (string?)null });

            // ── 2. Data Queries — generate SQL from natural language ──
            var (sql, explanation) = GenerateSQL(lower, prompt);
            if (sql == null)
                return Json(new { success = true, type = "knowledge",
                    answer = "I'm not sure how to answer that. Try asking about:\n" +
                             "- **Plan status** / **table health** / **database size**\n" +
                             "- **Show data** from any table (e.g., \"show top 10 from TRF_IN_PLAN\")\n" +
                             "- **Formulas** (e.g., \"what is TRF_IN formula\", \"how is POS_PO_RAISED calculated\")\n" +
                             "- **Architecture** (e.g., \"list all controllers\", \"what stored procedures exist\")\n" +
                             "- **Count/Sum** queries (e.g., \"total purchase quantity by RDC\")\n" +
                             "- **Short/excess** analysis\n" +
                             "- Or type raw SQL directly.",
                    sql = (string?)null });

            // Execute the generated SQL
            var connStr = _context.Database.GetConnectionString()!;
            using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            using var cmd = new SqlCommand(sql, conn);
            cmd.CommandTimeout = 60;
            using var reader = await cmd.ExecuteReaderAsync();

            var columns = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            var rows = new List<Dictionary<string, object?>>();
            while (await reader.ReadAsync() && rows.Count < 500)
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var val = reader.GetValue(i);
                    row[columns[i]] = val == DBNull.Value ? null : val;
                }
                rows.Add(row);
            }

            return Json(new { success = true, type = "data", answer = explanation, sql, columns, rows, rowCount = rows.Count });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, error = $"Error: {ex.Message}" });
        }
    }

    // ═══════════════════════════════════════════════════
    // KNOWLEDGE BASE — Formula & Architecture answers
    // ═══════════════════════════════════════════════════
    private string? MatchKnowledge(string q)
    {
        // Normalize: remove special chars, extra spaces
        q = System.Text.RegularExpressions.Regex.Replace(q, @"[^\w\s]", " ");
        q = System.Text.RegularExpressions.Regex.Replace(q, @"\s+", " ").Trim();
        bool isTrf = q.Contains("trf") || q.Contains("transfer");
        bool isPP = q.Contains("pp") || q.Contains("purchase");
        bool isFormula = q.Contains("formula") || q.Contains("column") || q.Contains("calculate") || q.Contains("how is") || q.Contains("what is");
        bool isAll = q.Contains("all") || q.Contains("column wise") || q.Contains("columnwise") || q.Contains("every") || q.Contains("complete") || q.Contains("full");

        // ── TRANSFER IN PLAN — ALL COLUMNS/FORMULAS ──
        if (isTrf && !isPP && (isAll || (isFormula && !MatchesSpecificColumn(q))))
            return "## Transfer In Plan — All Column Formulas\n\n" +
                   "**SP:** `SP_GENERATE_TRF_IN_PLAN` | **Granularity:** ST_CD + MAJ_CAT + WEEK | **Output:** `TRF_IN_PLAN`\n\n" +
                   "| Column | Formula | Type |\n|---|---|---|\n" +
                   "| **BGT_TTL_CF_OP_STK_Q** | WK1: STORE_STOCK.STK_QTY / WK2+: prev CL_STK | Chain |\n" +
                   "| **NT_ACT_Q** (Shrinkage) | ROUND(ROUND(OP_STK × 0.08, 0) × SSN_FACTOR, 0) — S,PS=1.0 others=0.5 | Derived |\n" +
                   "| **NET_BGT_CF_STK_Q** | MAX(OP_STK − Shrinkage, 0) | Derived |\n" +
                   "| **BGT_ST_CL_MBQ** (MBQ) | Display Qty (current wk) + Sale Qty (next wk) | Derived |\n" +
                   "| **CM_BGT_SALE_Q** | Current week sale from QTY_SALE_QTY (UNPIVOT) | Input |\n" +
                   "| **CM1_BGT_SALE_Q** | Next week sale | Input |\n" +
                   "| **CM2_BGT_SALE_Q** | Week+2 sale | Input |\n" +
                   "| **BGT_DISP_CL_Q** | Display qty from QTY_DISP_QTY (UNPIVOT) | Input |\n" +
                   "| **BGT_DISP_CL_OPT** | ROUND(DISP_QTY × 1000 / BIN_CAP, 0) | Derived |\n" +
                   "| **BGT_DISP_CL_OPT_MBQ** | ROUND(MBQ × 1000 / BGT_DISP_CL_OPT, 0) | Derived |\n" +
                   "| **TRF_IN_STK_Q** | IF(MBQ=0 AND SALE=0) 0 ELSE MAX(MBQ + SALE − NET_CF, 0) | **Chain** |\n" +
                   "| **TRF_IN_OPT_CNT** | ROUND(TRF_IN × 1000 / OPT_MBQ, 0) | Derived |\n" +
                   "| **TRF_IN_OPT_MBQ** | TRF_IN × 1000 / TRF_IN_OPT_CNT | Derived |\n" +
                   "| **DC_MBQ** | SQ1 + SQ2 + SQ3 + SQ4 (next 4 weeks sale) | Derived |\n" +
                   "| **BGT_TTL_CF_CL_STK_Q** | IF(no demand) NET_CF / IF(demand>stock) MBQ / ELSE MAX(NET_CF−SALE,0) | **Chain** |\n" +
                   "| **NET_ST_CL_STK_Q** | = CL_STK | Derived |\n" +
                   "| **COVER_SALE_QTY** | Next week sale | Input |\n" +
                   "| **ST_CL_EXCESS_Q** | MAX(CL_STK − MBQ, 0) | Derived |\n" +
                   "| **ST_CL_SHORT_Q** | MAX(MBQ − CL_STK, 0) | Derived |\n" +
                   "| **W_GRT_STK_Q** | IF SSN IN (W,PW) THEN prev_CL_STK ELSE 0 | Derived |\n\n" +
                   "**Week Chaining:** CL_STK(Week N) → OP_STK(Week N+1). Week 1 uses STORE_STOCK, Week 2+ chains from previous closing.";

        // ── PURCHASE PLAN — ALL COLUMNS/FORMULAS ──
        if (isPP && !isTrf && (isAll || (isFormula && !MatchesSpecificColumn(q))))
            return "## Purchase Plan — All Column Formulas\n\n" +
                   "**SP:** `SP_GENERATE_PURCHASE_PLAN` | **Granularity:** RDC_CD + MAJ_CAT + WEEK | **Output:** `PURCHASE_PLAN`\n\n" +
                   "| Column | Formula | Type |\n|---|---|---|\n" +
                   "| **DC_STK_Q** | WK1: DC_STOCK table / WK2+: prev BGT_DC_CL_STK_Q | Chain |\n" +
                   "| **BGT_DC_OP_STK_Q** | WK1: DC_STK_Q / WK2+: prev BGT_DC_CL_STK_Q | Chain |\n" +
                   "| **BGT_CF_STK_Q** | WK1: DC_STK_Q / WK2+: MAX(prev BGT_DC_CL_STK_Q, 0) | Chain |\n" +
                   "| **OP_STK** | WK1: GRT_STK_Q / WK2+: prev NET_SSNL_CL_STK_Q | Chain |\n" +
                   "| **NT_ACT_STK** | IF SSN IN (S,OC,A) THEN GRT_STK × 0.10 ELSE 0 | Derived |\n" +
                   "| **GRT_CONS_PCT** | UNPIVOT from MASTER_GRT_CONTRIBUTION by SSN+Week | Input |\n" +
                   "| **CW_TRF_OUT_Q** | SUM(TRF_IN_STK_Q) from TRF_IN_PLAN by RDC | Derived |\n" +
                   "| **CW1..CW4_TRF_OUT_Q** | TRF_OUT at week offsets +1..+4 | Derived |\n" +
                   "| **TTL_TRF_OUT_Q** | CW + CW1 + CW2 + CW3 + CW4 | Derived |\n" +
                   "| **BGT_DC_MBQ_SALE** | CW1_SALE + CW2_SALE + CW3_SALE + CW4_SALE | Derived |\n" +
                   "| **BGT_DC_CL_MBQ** | MIN(CW1+CW2+CW3+CW4 TRF_OUT, BGT_DC_MBQ_SALE) | Derived |\n" +
                   "| **GRT_CONS_Q** | MIN(TTL×0.30, MAX(OP−NT,0), MAX(TTL−MAX(CF−MBQ,0),0), MAX(TTL−NT,0)×PCT) | Derived |\n" +
                   "| **DEL_PEND_Q** | From DEL_PENDING table | Input |\n" +
                   "| **PP_NET_BGT_CF_STK_Q** | BGT_CF_STK + GRT_CONS + DEL_PEND | Derived |\n" +
                   "| **NET_SSNL_CL_STK_Q** | MAX(OP_STK − GRT_CONS_Q, 0) → chains to next OP_STK | **Chain** |\n" +
                   "| **POS_PO_RAISED** | MAX(BGT_DC_CL_MBQ + CW_TRF_OUT − PP_NET, 0) | Derived |\n" +
                   "| **BGT_PUR_Q_INIT** | = POS_PO_RAISED | Derived |\n" +
                   "| **NEG_PO_RAISED** | WK1: MIN(0, PUR−DEL) / WK2+: MIN(0, PUR−DEL+prev_NEG) | **Chain** |\n" +
                   "| **BGT_DC_CL_STK_Q** | MAX(PUR + PP_NET − CW_TRF_OUT, 0) → chains to next week | **Chain** |\n" +
                   "| **DC_STK_EXCESS_Q** | MAX(BGT_DC_CL_STK − BGT_DC_CL_MBQ, 0) | Derived |\n" +
                   "| **DC_STK_SHORT_Q** | MAX(BGT_DC_CL_MBQ − BGT_DC_CL_STK, 0) | Derived |\n" +
                   "| **CO_STK_EXCESS/SHORT** | ST excess/short + DC excess/short | Derived |\n" +
                   "| **BGT_CO_CL_STK_Q** | NET_BGT_ST_CL + NET_SSNL_CL + BGT_DC_CL | Derived |\n" +
                   "| **FRESH_BIN_REQ** | BGT_DC_CL_STK / BIN_CAP | Derived |\n" +
                   "| **GRT_BIN_REQ** | OP_STK / BIN_CAP | Derived |\n\n" +
                   "**3 Week Chains:** DC_CL_STK→DC_STK, NET_SSNL→OP_STK, NEG_PO carry-forward.";

        // ── BOTH TRF + PP formulas ──
        if ((isTrf && isPP && isFormula) || (isAll && isFormula && !isTrf && !isPP))
            return "## All Plan Formulas\n\n" +
                   "### Transfer In Plan (Store Level)\n" +
                   "| Column | Formula |\n|---|---|\n" +
                   "| **OP_STK** | WK1: STORE_STOCK / WK2+: prev CL_STK |\n" +
                   "| **Shrinkage** | ROUND(ROUND(OP×0.08,0) × SSN_FACTOR, 0) — S,PS=1.0 else 0.5 |\n" +
                   "| **NET_CF** | MAX(OP_STK − Shrinkage, 0) |\n" +
                   "| **MBQ** | Display_Qty + Next_Week_Sale |\n" +
                   "| **TRF_IN** | IF(MBQ=0 AND SALE=0) 0 ELSE MAX(MBQ+SALE−NET_CF, 0) |\n" +
                   "| **CL_STK** | IF(no demand) NET_CF / IF(demand>stock) MBQ / ELSE MAX(NET_CF−SALE,0) |\n" +
                   "| **DC_MBQ** | Sum of next 4 weeks sale |\n\n" +
                   "### Purchase Plan (DC Level)\n" +
                   "| Column | Formula |\n|---|---|\n" +
                   "| **BGT_DC_CL_MBQ** | MIN(next 4wk TRF_OUT, next 4wk SALE) |\n" +
                   "| **GRT_CONS_Q** | MIN(TTL×0.30, OP−NT, TTL−(CF−MBQ), (TTL−NT)×PCT) |\n" +
                   "| **PP_NET** | BGT_CF_STK + GRT_CONS + DEL_PEND |\n" +
                   "| **POS_PO** | MAX(MBQ + CW_TRF_OUT − PP_NET, 0) |\n" +
                   "| **BGT_PUR_Q** | = POS_PO_RAISED |\n" +
                   "| **NEG_PO** | WK1: MIN(0,PUR−DEL) / WK2+: MIN(0,PUR−DEL+prev_NEG) |\n" +
                   "| **DC_CL_STK** | MAX(PUR + PP_NET − CW_TRF_OUT, 0) |";

        // ── SPECIFIC COLUMN MATCHES (more patterns) ──

        // TRF_IN
        if (Match(q, "trf_in formula", "trf in formula", "transfer in formula", "trf_in_stk_q", "how is trf_in", "how is transfer in"))
            return "## Transfer In Formula (TRF_IN_STK_Q)\n\n```\nIF MBQ = 0 AND SALE = 0 THEN TRF_IN = 0\nELSE IF MBQ + SALE - NET_CF > 0 THEN TRF_IN = MBQ + SALE - NET_CF\nELSE TRF_IN = 0\n```\n\n" +
                   "**Where:**\n- **MBQ** = Display Qty (current week) + Sale Qty (next week)\n- **SALE** = Current week sale\n- **NET_CF** = MAX(OP_STK - Shrinkage, 0)\n\n**Week Chain:** WK1 OP_STK = STORE_STOCK, WK2+ OP_STK = prev CL_STK";

        if (Match(q, "shrinkage", "nt_act_q", "nt act"))
            return "## Shrinkage Formula (NT_ACT_Q)\n\n```\nSHRINKAGE = ROUND(ROUND(OP_STK * 0.08, 0) * SSN_FACTOR, 0)\n```\n\n**SSN Factor:** S,PS = **1.0** (full 8%) | Others = **0.5** (half)\n\nApplied in: NET_CF = MAX(OP_STK - SHRINKAGE, 0)";

        if (Match(q, "closing stock", "cl_stk", "cl stk"))
            return "## Closing Stock (BGT_TTL_CF_CL_STK_Q)\n\n```\nIF MBQ=0 AND SALE=0 → CL_STK = NET_CF\nIF MBQ+SALE > NET_CF → CL_STK = MBQ\nELSE → CL_STK = MAX(NET_CF - SALE, 0)\n```\n\nCL_STK chains to next week as OP_STK.";

        if (Match(q, "mbq", "minimum batch"))
            return "## MBQ (Minimum Batch Quantity)\n\n```\nMBQ = Display Qty (current week) + Sale Qty (next week)\n```\n\nSource: `QTY_DISP_QTY` + `QTY_SALE_QTY` (UNPIVOT)";

        if (Match(q, "dc_mbq", "dc mbq"))
            return "## DC_MBQ\n\n```\nDC_MBQ = SQ1 + SQ2 + SQ3 + SQ4 (sum of next 4 weeks sale)\n```";

        if (Match(q, "pos_po", "po raised", "pos po"))
            return "## POS_PO_RAISED\n\n```\nPOS_PO_RAISED = MAX(BGT_DC_CL_MBQ + CW_TRF_OUT_Q - PP_NET_BGT_CF_STK_Q, 0)\n```\n\n- **BGT_DC_CL_MBQ** = MIN(next 4wk TRF_OUT, next 4wk SALE)\n- **CW_TRF_OUT_Q** = Current week transfer out\n- **PP_NET** = BGT_CF_STK + GRT_CONS + DEL_PEND";

        if (Match(q, "bgt_pur", "purchase quantity", "purchase formula", "pur q init"))
            return "## BGT_PUR_Q_INIT (Purchase Quantity)\n\n```\nBGT_PUR_Q_INIT = POS_PO_RAISED\n```\n\nWhere POS_PO = MAX(BGT_DC_CL_MBQ + CW_TRF_OUT - PP_NET, 0)";

        if (Match(q, "neg_po", "negative po", "neg po"))
            return "## NEG_PO_RAISED\n\n```\nWK1: MIN(0, BGT_PUR_Q_INIT - DEL_PEND_Q)\nWK2+: MIN(0, BGT_PUR_Q_INIT - DEL_PEND_Q + prev_NEG_PO)\n```\n\nCarry-forward: previous week's NEG_PO accumulates.";

        if (Match(q, "bgt_dc_cl_stk", "dc closing", "dc cl stk"))
            return "## BGT_DC_CL_STK_Q (DC Closing Stock)\n\n```\nBGT_DC_CL_STK_Q = MAX(BGT_PUR_Q_INIT + PP_NET_BGT_CF_STK_Q - CW_TRF_OUT_Q, 0)\n```\n\nChains to next week: DC_STK_Q, BGT_DC_OP_STK_Q, BGT_CF_STK_Q";

        if (Match(q, "grt_cons", "grt consumption", "grt formula"))
            return "## GRT_CONS_Q\n\n```\nIF TTL_TRF_OUT = 0 → 0\nELSE MIN of:\n  1. TTL_TRF_OUT × 0.30\n  2. MAX(OP_STK - NT_ACT_STK, 0)\n  3. MAX(TTL_TRF - MAX(BGT_CF - MBQ, 0), 0)\n  4. MAX(TTL_STK - NT_ACT, 0) × GRT_CONS_PCT\n```\n\nGRT_CONS_PCT from `MASTER_GRT_CONTRIBUTION` (UNPIVOT by SSN+Week). Sub-level: GRT_CONS=0.";

        if (Match(q, "pp_net", "net bgt cf", "net cf stk"))
            return "## PP_NET_BGT_CF_STK_Q\n\n```\n= BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q\n```";

        if (Match(q, "net_ssnl", "seasonal cl", "ssnl cl"))
            return "## NET_SSNL_CL_STK_Q\n\n```\n= MAX(OP_STK - GRT_CONS_Q, 0)\n```\n\nChains to next week as OP_STK (GRT stock).";

        if (Match(q, "bgt_dc_cl_mbq", "dc cl mbq", "dc mbq sale"))
            return "## BGT_DC_CL_MBQ\n\n```\n= MIN(CW1+CW2+CW3+CW4 TRF_OUT, BGT_DC_MBQ_SALE)\n```\n\nBGT_DC_MBQ_SALE = CW1_SALE + CW2_SALE + CW3_SALE + CW4_SALE";

        if (Match(q, "excess", "short") && isFormula)
            return "## Excess & Short Formulas\n\n**Store Level (TRF):**\n- `ST_CL_EXCESS_Q` = MAX(CL_STK − MBQ, 0)\n- `ST_CL_SHORT_Q` = MAX(MBQ − CL_STK, 0)\n\n**DC Level (PP):**\n- `DC_STK_EXCESS_Q` = MAX(BGT_DC_CL_STK − BGT_DC_CL_MBQ, 0)\n- `DC_STK_SHORT_Q` = MAX(BGT_DC_CL_MBQ − BGT_DC_CL_STK, 0)\n\n**Company:**\n- `CO_STK_EXCESS` = ST_EXCESS + DC_EXCESS\n- `CO_STK_SHORT` = ST_SHORT + DC_SHORT";

        // Architecture
        if (Match(q, "controller", "all controller"))
            return "## Controllers (30)\n\n**Core:** Home, Plan, PurchasePlan, SubLevel, BulkUpload, Info, AdminConsole, Error\n\n**CRUD (22):** WeekCalendar, StoreMaster, BinCapacity, SaleQty, DispQty, StoreStock, DcStock, DelPending, GrtContribution, ProductHierarchy, ContMacroMvgr, ContSz, ContSeg, ContVnd, SubStStkMvgr/Sz/Seg/Vnd, SubDcStkMvgr/Sz/Seg/Vnd";

        if (Match(q, "service", "background job", "singleton"))
            return "## Services (3)\n\n| Service | Lifetime | Purpose |\n|---|---|---|\n| **PlanService** | Scoped | SP execution helper |\n| **PlanJobService** | Singleton | Background TRF+PP full run |\n| **SubLevelJobService** | Singleton | Background sub-level (4 levels) |";

        if (Match(q, "stored procedure", "sp list", "what sp", "list sp"))
            return "## Stored Procedures (6)\n\n| SP | Purpose | Output |\n|---|---|---|\n| **SP_GENERATE_TRF_IN_PLAN** | Store-level TRF | TRF_IN_PLAN |\n| **SP_GENERATE_PURCHASE_PLAN** | DC-level PP | PURCHASE_PLAN |\n| **SP_RUN_ALL_PLANS** | Full run | Both |\n| **SP_GENERATE_SUB_LEVEL_TRF** | Sub TRF | SUB_LEVEL_TRF_PLAN |\n| **SP_GENERATE_SUB_LEVEL_PP** | Sub PP | SUB_LEVEL_PP_PLAN |";

        if (Match(q, "tech stack", "technology", "framework", "packages"))
            return "## Technology Stack\n\n**Backend:** ASP.NET Core 8.0, EF Core 8, EPPlus 7, IMemoryCache, Singleton jobs\n**Frontend:** Bootstrap 5.3, Chart.js 4.4, DataTables 1.13, Inter font\n**Database:** SQL Server (`planning`), 6 SPs, 22 ref tables, 4 output tables\n**Config:** localhost:5005, 500MB uploads, 300s SQL timeout";

        if (Match(q, "sub level", "sub-level", "sublevel", "4 level", "four level", "level wise"))
            return "## Sub-Level Plans\n\nSame algorithm at finer granularity: **ST_CD + MAJ_CAT + SUB_VALUE + WEEK**\n\n| Level | Sub-Value | Contribution Table |\n|---|---|---|\n| MVGR | DISP_MVGR_MATRIX | ST_MAJ_CAT_MACRO_MVGR_PLAN |\n| Size | SZ | ST_MAJ_CAT_SZ_PLAN |\n| Segment | SEG | ST_MAJ_CAT_SEG_PLAN |\n| Vendor | M_VND_CD | ST_MAJ_CAT_VND_PLAN |\n\n**Differences:** Sale×CONT_PCT, Display×CONT_PCT, level-specific stock (SUB_ST_STK_*, SUB_DC_STK_*), GRT_CONS=0 in sub PP.\n\n**Flow:** TRF first → PP reads TRF output per level.";

        if (Match(q, "week chain", "chaining", "chain work", "what chain"))
            return "## Week Chaining\n\n**TRF:** CL_STK(N) → OP_STK(N+1)\n\n**PP (3 chains):**\n1. BGT_DC_CL_STK → next DC_STK, DC_OP, CF_STK\n2. NET_SSNL_CL_STK → next OP_STK\n3. NEG_PO → carry-forward accumulation\n\nImplemented as WHILE loops, set-based per week.";

        if (Match(q, "workflow", "how does app", "execution flow", "end to end", "how app work"))
            return "## Application Workflow\n\n1. **Upload Reference Data** (10 tables)\n2. **Execute TRF Plan** → TRF_IN_PLAN\n3. **Execute Purchase Plan** (reads TRF) → PURCHASE_PLAN\n4. **Review + Export** CSV/Pivot\n5. **Upload Sub-Level Data** (12 tables)\n6. **Execute Sub-Level Plans** → SUB_LEVEL_TRF + PP\n7. **Dashboard Monitoring**\n\n**Critical:** TRF must run BEFORE PP.";

        // Generic formula question without specific plan
        if (isFormula && !isTrf && !isPP && !isAll)
            return "## Which plan's formulas do you need?\n\nTry asking:\n- **\"Transfer In Plan column wise formula\"** — all TRF columns\n- **\"Purchase Plan column wise formula\"** — all PP columns\n- **\"All formulas\"** — both TRF + PP overview\n- Or ask about a specific column: `TRF_IN_STK_Q`, `POS_PO_RAISED`, `BGT_PUR_Q_INIT`, `shrinkage`, etc.";

        return null;
    }

    private static bool MatchesSpecificColumn(string q)
    {
        var specific = new[] { "trf_in_stk", "nt_act", "shrinkage", "cl_stk", "closing stock", "mbq", "dc_mbq",
            "pos_po", "bgt_pur", "neg_po", "bgt_dc_cl_stk", "grt_cons", "pp_net", "net_ssnl", "bgt_dc_cl_mbq",
            "excess", "short", "op_stk", "net_cf", "opening stock" };
        return specific.Any(s => q.Contains(s));
    }

    // ═══════════════════════════════════════════════════
    // SQL GENERATOR — Natural Language to SQL
    // ═══════════════════════════════════════════════════
    private (string? sql, string explanation) GenerateSQL(string q, string original)
    {
        // Direct SQL passthrough
        if (q.StartsWith("select ") || q.StartsWith("with ") || q.StartsWith("exec ") || q.StartsWith("update ") ||
            q.StartsWith("delete ") || q.StartsWith("insert ") || q.StartsWith("truncate ") || q.StartsWith("alter ") ||
            q.StartsWith("create ") || q.StartsWith("drop ") || q.StartsWith("sp_"))
            return (original, "Running your SQL directly:");

        // Plan status
        if (Match(q, "plan status", "plan count", "how many plan rows", "trf and pp status"))
            return (@"SELECT 'TRF_IN_PLAN' AS [Plan], COUNT(*) AS Rows, COUNT(DISTINCT [ST_CD]) AS Entities,
                COUNT(DISTINCT [MAJ_CAT]) AS Categories, MIN(FY_WEEK) AS MinWk, MAX(FY_WEEK) AS MaxWk
                FROM TRF_IN_PLAN WITH (NOLOCK)
                UNION ALL
                SELECT 'PURCHASE_PLAN', COUNT(*), COUNT(DISTINCT [RDC_CD]),
                COUNT(DISTINCT [MAJ_CAT]), MIN(FY_WEEK), MAX(FY_WEEK)
                FROM PURCHASE_PLAN WITH (NOLOCK)", "Here's the current plan status:");

        // Sub-level status
        if (Match(q, "sub level status", "sub-level status", "level status", "sub level count"))
            return (@"SELECT [LEVEL], 'TRF' AS [Type], COUNT(*) AS Rows,
                COUNT(DISTINCT SUB_VALUE) AS SubValues, MAX(CREATED_DT) AS LastRun
                FROM SUB_LEVEL_TRF_PLAN WITH (NOLOCK) GROUP BY [LEVEL]
                UNION ALL
                SELECT [LEVEL], 'PP', COUNT(*), COUNT(DISTINCT SUB_VALUE), MAX(CREATED_DT)
                FROM SUB_LEVEL_PP_PLAN WITH (NOLOCK) GROUP BY [LEVEL]
                ORDER BY [Level], [Type]", "Sub-Level plan status by level:");

        // Table health / row counts
        if (Match(q, "table health", "row count", "all table count", "table status", "how many rows"))
            return (@"SELECT t.TABLE_NAME, p.rows AS ROW_COUNT,
                CASE WHEN p.rows > 0 THEN 'Ready' ELSE 'Empty' END AS Status
                FROM INFORMATION_SCHEMA.TABLES t
                LEFT JOIN sys.partitions p ON p.object_id = OBJECT_ID(t.TABLE_SCHEMA+'.'+t.TABLE_NAME) AND p.index_id IN (0,1)
                WHERE t.TABLE_TYPE='BASE TABLE' ORDER BY t.TABLE_NAME", "All tables with row counts:");

        // DB size
        if (Match(q, "database size", "db size", "how big", "storage"))
            return (@"SELECT DB_NAME() AS [Database],
                CAST(SUM(size*8.0/1024) AS DECIMAL(10,2)) AS TotalMB,
                CAST(SUM(CASE WHEN type=0 THEN size*8.0/1024 END) AS DECIMAL(10,2)) AS DataMB,
                CAST(SUM(CASE WHEN type=1 THEN size*8.0/1024 END) AS DECIMAL(10,2)) AS LogMB
                FROM sys.database_files", "Database storage usage:");

        // Table sizes
        if (Match(q, "table size", "biggest table", "table space", "which table largest"))
            return (@"SELECT t.TABLE_NAME, p.rows AS RowCount,
                CAST(SUM(a.total_pages)*8.0/1024 AS DECIMAL(10,2)) AS SizeMB
                FROM INFORMATION_SCHEMA.TABLES t
                JOIN sys.indexes i ON i.object_id=OBJECT_ID(t.TABLE_SCHEMA+'.'+t.TABLE_NAME)
                JOIN sys.partitions p ON i.object_id=p.object_id AND i.index_id=p.index_id
                JOIN sys.allocation_units a ON p.partition_id=a.container_id
                WHERE t.TABLE_TYPE='BASE TABLE' AND i.index_id<=1
                GROUP BY t.TABLE_NAME, p.rows ORDER BY SizeMB DESC", "Table sizes (largest first):");

        // Short stores
        if (Match(q, "short store", "which store short", "store with short", "critical short"))
            return (@"SELECT TOP 20 ST_CD, MAJ_CAT, RDC_CD, FY_WEEK,
                ST_CL_SHORT_Q AS ShortQty, BGT_ST_CL_MBQ AS MBQ, BGT_TTL_CF_CL_STK_Q AS ClosingStk
                FROM TRF_IN_PLAN WITH (NOLOCK) WHERE ST_CL_SHORT_Q > 0
                ORDER BY ST_CL_SHORT_Q DESC", "Top 20 stores with stock shortage:");

        // Excess stores
        if (Match(q, "excess store", "which store excess", "store with excess", "overstocked"))
            return (@"SELECT TOP 20 ST_CD, MAJ_CAT, RDC_CD, FY_WEEK,
                ST_CL_EXCESS_Q AS ExcessQty, BGT_ST_CL_MBQ AS MBQ, BGT_TTL_CF_CL_STK_Q AS ClosingStk
                FROM TRF_IN_PLAN WITH (NOLOCK) WHERE ST_CL_EXCESS_Q > 0
                ORDER BY ST_CL_EXCESS_Q DESC", "Top 20 overstocked stores:");

        // DC short
        if (Match(q, "dc short", "which dc short", "rdc short", "dc with short"))
            return (@"SELECT TOP 20 RDC_CD, MAJ_CAT, FY_WEEK,
                DC_STK_SHORT_Q AS DCShort, BGT_DC_CL_MBQ AS DC_MBQ, BGT_DC_CL_STK_Q AS DC_ClosingStk
                FROM PURCHASE_PLAN WITH (NOLOCK) WHERE DC_STK_SHORT_Q > 0
                ORDER BY DC_STK_SHORT_Q DESC", "Top 20 DC short positions:");

        // TRF summary by RDC
        if (Match(q, "trf by rdc", "transfer by rdc", "trf summary", "transfer summary"))
            return (@"SELECT RDC_CD, COUNT(DISTINCT ST_CD) AS Stores, COUNT(DISTINCT MAJ_CAT) AS Cats,
                SUM(TRF_IN_STK_Q) AS TotalTrfIn, SUM(ST_CL_SHORT_Q) AS TotalShort,
                SUM(ST_CL_EXCESS_Q) AS TotalExcess
                FROM TRF_IN_PLAN WITH (NOLOCK) GROUP BY RDC_CD ORDER BY TotalTrfIn DESC", "Transfer In summary by RDC:");

        // PP summary by RDC
        if (Match(q, "pp by rdc", "purchase by rdc", "pp summary", "purchase summary"))
            return (@"SELECT RDC_CD, COUNT(DISTINCT MAJ_CAT) AS Cats,
                SUM(BGT_PUR_Q_INIT) AS TotalPurchase, SUM(POS_PO_RAISED) AS TotalPO,
                SUM(DC_STK_SHORT_Q) AS DCShort, SUM(DC_STK_EXCESS_Q) AS DCExcess
                FROM PURCHASE_PLAN WITH (NOLOCK) GROUP BY RDC_CD ORDER BY TotalPurchase DESC", "Purchase Plan summary by RDC:");

        // Category analysis
        if (Match(q, "by category", "category summary", "category wise", "maj cat"))
            return (@"SELECT MAJ_CAT, COUNT(DISTINCT ST_CD) AS Stores,
                SUM(TRF_IN_STK_Q) AS TotalTrfIn, SUM(CM_BGT_SALE_Q) AS TotalSale,
                SUM(ST_CL_SHORT_Q) AS TotalShort, SUM(ST_CL_EXCESS_Q) AS TotalExcess
                FROM TRF_IN_PLAN WITH (NOLOCK) GROUP BY MAJ_CAT ORDER BY TotalTrfIn DESC", "Category-wise analysis:");

        // Week trend
        if (Match(q, "week trend", "weekly trend", "week wise", "by week"))
            return (@"SELECT FY_WEEK, SUM(TRF_IN_STK_Q) AS TrfIn, SUM(CM_BGT_SALE_Q) AS Sale,
                SUM(ST_CL_SHORT_Q) AS Short, SUM(ST_CL_EXCESS_Q) AS Excess
                FROM TRF_IN_PLAN WITH (NOLOCK) GROUP BY FY_WEEK ORDER BY FY_WEEK", "Weekly trend analysis:");

        // Store detail
        if (Match(q, "store detail", "show store", "store data") && ExtractCode(q) is string stCode)
            return ($@"SELECT ST_CD, MAJ_CAT, FY_WEEK, BGT_TTL_CF_OP_STK_Q AS OP_STK, NT_ACT_Q AS Shrinkage,
                NET_BGT_CF_STK_Q AS NET_CF, CM_BGT_SALE_Q AS Sale, TRF_IN_STK_Q AS TrfIn,
                BGT_TTL_CF_CL_STK_Q AS CL_STK, ST_CL_SHORT_Q AS Short, ST_CL_EXCESS_Q AS Excess
                FROM TRF_IN_PLAN WITH (NOLOCK) WHERE ST_CD = '{stCode}'
                ORDER BY MAJ_CAT, FY_WEEK", $"Store {stCode} — TRF chain detail:");

        // RDC detail
        if (Match(q, "rdc detail", "show rdc") && ExtractCode(q) is string rdcCode)
            return ($@"SELECT RDC_CD, MAJ_CAT, FY_WEEK, BGT_DC_OP_STK_Q AS DC_OP,
                BGT_PUR_Q_INIT AS Purchase, POS_PO_RAISED AS PO, BGT_DC_CL_STK_Q AS DC_CL,
                DC_STK_SHORT_Q AS Short, DC_STK_EXCESS_Q AS Excess
                FROM PURCHASE_PLAN WITH (NOLOCK) WHERE RDC_CD = '{rdcCode}'
                ORDER BY MAJ_CAT, FY_WEEK", $"RDC {rdcCode} — Purchase Plan detail:");

        // Show / top from table
        if (q.Contains("show") || q.Contains("top") || q.Contains("from") || q.Contains("display"))
        {
            var tableName = FindTableName(q);
            if (tableName != null)
            {
                int n = ExtractNumber(q) ?? 50;
                return ($"SELECT TOP {n} * FROM [{tableName}] WITH (NOLOCK)",
                    $"Showing top {n} rows from **{tableName}**:");
            }
        }

        // Count from table
        if (q.Contains("count") || q.Contains("how many"))
        {
            var tableName = FindTableName(q);
            if (tableName != null)
                return ($"SELECT COUNT(*) AS TotalRows FROM [{tableName}] WITH (NOLOCK)",
                    $"Row count for **{tableName}**:");
        }

        // Distinct values
        if (q.Contains("distinct") || q.Contains("unique"))
        {
            var tableName = FindTableName(q);
            if (tableName != null)
            {
                if (q.Contains("rdc")) return ($"SELECT DISTINCT RDC_CD FROM [{tableName}] WITH (NOLOCK) ORDER BY RDC_CD", $"Distinct RDC codes in {tableName}:");
                if (q.Contains("store") || q.Contains("st_cd")) return ($"SELECT DISTINCT ST_CD FROM [{tableName}] WITH (NOLOCK) ORDER BY ST_CD", $"Distinct store codes in {tableName}:");
                if (q.Contains("cat") || q.Contains("maj")) return ($"SELECT DISTINCT MAJ_CAT FROM [{tableName}] WITH (NOLOCK) ORDER BY MAJ_CAT", $"Distinct categories in {tableName}:");
            }
        }

        // Schema
        if (Match(q, "schema", "columns of", "structure of", "describe"))
        {
            var tableName = FindTableName(q);
            if (tableName != null)
                return ($"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{tableName}' ORDER BY ORDINAL_POSITION",
                    $"Schema of **{tableName}**:");
        }

        return (null, "");
    }

    // ═══════════════════════════════════════════════════
    // HELPER METHODS
    // ═══════════════════════════════════════════════════
    private static bool Match(string q, params string[] patterns)
        => patterns.Any(p => q.Contains(p));

    private static string? ExtractCode(string q)
    {
        var match = System.Text.RegularExpressions.Regex.Match(q, @"\b([A-Z0-9]{3,10})\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        // Skip common words
        var skip = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "show", "store", "detail", "rdc", "from", "top", "the", "for", "and", "all", "with" };
        foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(q, @"\b([A-Z0-9]{3,10})\b", System.Text.RegularExpressions.RegexOptions.IgnoreCase))
            if (!skip.Contains(m.Value)) return m.Value;
        return null;
    }

    private static int? ExtractNumber(string q)
    {
        var match = System.Text.RegularExpressions.Regex.Match(q, @"\b(\d+)\b");
        return match.Success ? int.Parse(match.Value) : null;
    }

    private static readonly string[] KnownTables = {
        "TRF_IN_PLAN", "PURCHASE_PLAN", "SUB_LEVEL_TRF_PLAN", "SUB_LEVEL_PP_PLAN",
        "WEEK_CALENDAR", "MASTER_ST_MASTER", "MASTER_BIN_CAPACITY", "QTY_SALE_QTY", "QTY_DISP_QTY",
        "STORE_STOCK", "DC_STOCK", "DEL_PENDING", "MASTER_GRT_CONTRIBUTION", "MASTER_PRODUCT_HIERARCHY",
        "ST_MAJ_CAT_MACRO_MVGR_PLAN", "ST_MAJ_CAT_SZ_PLAN", "ST_MAJ_CAT_SEG_PLAN", "ST_MAJ_CAT_VND_PLAN",
        "SUB_ST_STK_MVGR", "SUB_ST_STK_SZ", "SUB_ST_STK_SEG", "SUB_ST_STK_VND",
        "SUB_DC_STK_MVGR", "SUB_DC_STK_SZ", "SUB_DC_STK_SEG", "SUB_DC_STK_VND",
        "QTY_MSA_AND_GRT", "QTY_DEL_PENDING"
    };

    private static string? FindTableName(string q)
    {
        // Exact match first
        foreach (var t in KnownTables)
            if (q.Contains(t.ToLowerInvariant())) return t;
        // Partial match
        foreach (var t in KnownTables)
        {
            var parts = t.ToLowerInvariant().Split('_');
            if (parts.Length >= 2 && parts.Skip(1).All(p => q.Contains(p))) return t;
        }
        // Common aliases
        if (q.Contains("trf") && q.Contains("plan") && !q.Contains("sub")) return "TRF_IN_PLAN";
        if (q.Contains("purchase") && q.Contains("plan") && !q.Contains("sub")) return "PURCHASE_PLAN";
        if (q.Contains("sale")) return "QTY_SALE_QTY";
        if (q.Contains("disp")) return "QTY_DISP_QTY";
        if (q.Contains("store") && q.Contains("stock")) return "STORE_STOCK";
        if (q.Contains("dc") && q.Contains("stock")) return "DC_STOCK";
        if (q.Contains("store") && q.Contains("master")) return "MASTER_ST_MASTER";
        if (q.Contains("bin") && q.Contains("cap")) return "MASTER_BIN_CAPACITY";
        if (q.Contains("week") && q.Contains("cal")) return "WEEK_CALENDAR";
        if (q.Contains("product") && q.Contains("hier")) return "MASTER_PRODUCT_HIERARCHY";
        if (q.Contains("grt") && q.Contains("cont")) return "MASTER_GRT_CONTRIBUTION";
        if (q.Contains("del") && q.Contains("pend")) return "DEL_PENDING";
        return null;
    }

    public class QueryRequest
    {
        public string Sql { get; set; } = "";
        public int MaxRows { get; set; } = 500;
    }

    public class QuickActionRequest
    {
        public string Action { get; set; } = "";
    }

    public class AskRequest
    {
        public string Prompt { get; set; } = "";
    }
}
