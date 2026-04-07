# PURCHASE PLAN SQL SCRIPTS - Complete Documentation

## Overview
This directory contains four complete MSSQL scripts for the **Purchase Plan system**, which aggregates store-level Transfer In Plan data at the **RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK** level to generate purchase planning metrics.

---

## Files & Execution Order

### File 1: `08_CREATE_PURCHASE_PLAN_REF_TABLES.sql`
**Purpose:** Create reference tables needed by the Purchase Plan system

**Contents:**
- **QTY_DEL_PENDING** table: Tracks pending delivery quantities by RDC ÃƒÂ— Category ÃƒÂ— Date
  - Columns: ID (PK), RDC_CD, MAJ_CAT, DEL_PEND_Q, DATE
  - Indexes: (RDC_CD, MAJ_CAT, DATE), (DATE)
  - Allows lookup of latest delivery pending qty for any RDCÃƒÂ—Category

**Dependencies:**
- Requires the [planning] database to exist
- Assumes existing reference tables: WEEK_CALENDAR, MASTER_ST_MASTER, MASTER_BIN_CAPACITY, MASTER_GRT_CONS_percentage, QTY_MSA_AND_GRT, TRF_IN_PLAN

**Execution:**
```sql
SQLCMD -S <server> -d planning -i 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql
```

---

### File 2: `09_CREATE_PURCHASE_PLAN_TABLE.sql`
**Purpose:** Create the PURCHASE_PLAN output table at RDC ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK granularity

**Table Structure:**
- **PURCHASE_PLAN** (grain: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID)

**Column Groups:**

1. **Identifiers & Keys:**
   - ID (identity), RDC_CD, RDC_NM, MAJ_CAT, SSN, WEEK_ID, FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT

2. **Reference Stock Data:**
   - DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q (from QTY_MSA_AND_GRT)
   - BIN_CAP_DC_TEAM, BIN_CAP (from MASTER_BIN_CAPACITY)

3. **Store-Level Planning (from TRF_IN_PLAN aggregation):**
   - BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q, CW2_BGT_SALE_Q
   - CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, CW5_BGT_SALE_Q (via week offset joins)
   - BGT_ST_OP_MBQ, NET_ST_OP_STK_Q, CW_TRF_OUT_Q, CW1_TRF_OUT_Q
   - BGT_ST_CL_MBQ, NET_BGT_ST_CL_STK_Q, ST_STK_EXCESS_Q, ST_STK_SHORT_Q

4. **DC Opening Stock:**
   - BGT_DC_OP_STK_Q (=MAX(DC_STK_Q, 0))
   - PP_NT_ACT_Q, BGT_CF_STK_Q

5. **Total Stock Calculations:**
   - TTL_STK, OP_STK, NT_ACT_STK (no-touch/seasonal active stock)

6. **GRT Consumption:**
   - GRT_CONS_PCT (from MASTER_GRT_CONS_percentage by SSN + week)
   - GRT_CONS_Q (calculated)

7. **Delivery Pending & DC Confirm Stock:**
   - DEL_PEND_Q (from QTY_DEL_PENDING)
   - PP_NET_BGT_CF_STK_Q (purchase plan net budget confirm stock)

8. **Purchase Plan Logic:**
   - TTL_TRF_OUT_Q, BGT_DC_MBQ_SALE, BGT_DC_CL_MBQ, NET_SSNL_CL_STK_Q
   - BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED
   - BGT_DC_CL_STK_Q, BGT_CO_CL_STK_Q

9. **Stock Excess/Shortage:**
   - DC_STK_EXCESS_Q, DC_STK_SHORT_Q
   - ST_STK_EXCESS_Q, ST_STK_SHORT_Q
   - CO_STK_EXCESS_Q, CO_STK_SHORT_Q

10. **Bin Requirements:**
    - FRESH_BIN_REQ = BGT_DC_CL_STK_Q / BIN_CAP
    - GRT_BIN_REQ = OP_STK / BIN_CAP

11. **Audit Columns:**
    - CREATED_DT, CREATED_BY, MODIFIED_DT, MODIFIED_BY

**Indexes:**
- Clustered: ID
- Non-clustered indexes for optimal query performance:
  - (RDC_CD, MAJ_CAT, WEEK_ID) Ã¢Â€Â” primary search
  - (WEEK_ID, FY_YEAR) Ã¢Â€Â” temporal queries
  - (RDC_CD, MAJ_CAT) Ã¢Â€Â” dimensional queries
  - Alert queries index for SHORT_Q and POS_PO_RAISED metrics

**Execution:**
```sql
SQLCMD -S <server> -d planning -i 09_CREATE_PURCHASE_PLAN_TABLE.sql
```

---

### File 3: `10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql`
**Purpose:** Create stored procedure SP_GENERATE_PURCHASE_PLAN to generate purchase plan metrics

**Procedure Signature:**
```sql
EXEC dbo.SP_GENERATE_PURCHASE_PLAN
    @StartWeekID INT,
    @EndWeekID INT,
    @RdcCode VARCHAR(20) = NULL,
    @MajCat VARCHAR(50) = NULL,
    @Debug BIT = 0
```

**Parameters:**
- `@StartWeekID`: Week ID to start processing (inclusive)
- `@EndWeekID`: Week ID to end processing (inclusive)
- `@RdcCode`: Optional filter to a single RDC (NULL = all RDCs)
- `@MajCat`: Optional filter to a single category (NULL = all categories)
- `@Debug`: Set to 1 to print step-by-step progress messages

**Algorithm (7 Steps):**

**STEP 1: Build Working Weeks**
- Query WEEK_CALENDAR for WEEK_ID range
- Derive Season (SSN) from FY_WEEK:
  - FY_WEEK 1-13: 'S' (Spring)
  - FY_WEEK 14-26: 'OC' (Open & Close)
  - FY_WEEK 27-39: 'A' (Autumn/Fall)
  - FY_WEEK 40+: 'H' (Holiday)

**STEP 2: Build RDC ÃƒÂ— MAJ_CAT Matrix**
- CROSS JOIN distinct RDC_CD values from MASTER_ST_MASTER
- With distinct MAJ_CAT values from MASTER_BIN_CAPACITY
- Apply filters (@RdcCode, @MajCat) if provided

**STEP 3: Aggregate from TRF_IN_PLAN**
- Group by RDC_CD, MAJ_CAT, WEEK_ID
- Aggregate current week metrics: S_GRT_STK_Q, W_GRT_STK_Q, BGT_DISP_CL_Q, etc.
- Join with future weeks (WEEK_ID+1, +2, +3, +4, +5) via WEEK_SEQ offset to get:
  - CW3_BGT_SALE_Q, CW4_BGT_SALE_Q, CW5_BGT_SALE_Q
  - CW1_TRF_OUT_Q (next week transfer out)
- Join with prior week (WEEK_ID-1) to get BGT_ST_OP_MBQ

**STEP 4: Load Reference Data**
- For each RDC ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK:
  - DC_STK_Q, GRT_STK_Q: Latest from QTY_MSA_AND_GRT by DATE Ã¢Â‰Â¤ WK_END_DT
  - BIN_CAP_DC_TEAM, BIN_CAP: From MASTER_BIN_CAPACITY
  - GRT_CONS_PCT: From MASTER_GRT_CONS_percentage by SSN + matching WK column
  - DEL_PEND_Q: Latest from QTY_DEL_PENDING by DATE Ã¢Â‰Â¤ WK_END_DT

**STEP 5: Calculate Purchase Plan Metrics (Initial)**
- BGT_DC_OP_STK_Q = MAX(DC_STK_Q, 0)
- PP_NT_ACT_Q = 0
- BGT_CF_STK_Q = MAX(BGT_DC_OP_STK_Q - PP_NT_ACT_Q, 0)
- TTL_STK = GRT_STK_Q
- OP_STK = TTL_STK
- NT_ACT_STK = TTL_STK ÃƒÂ— (0.10 if SSN in ('S','OC','A') else 0)
- TTL_TRF_OUT_Q = CW_TRF_OUT_Q
- BGT_DC_CL_MBQ = MIN(CW1_TRF_OUT_Q, BGT_DC_MBQ_SALE)

Then calculate dependent columns in order:
```
GRT_CONS_Q = MIN(
    TTL_TRF_OUT_Q ÃƒÂ— 0.30,
    MAX(OP_STK - NT_ACT_STK, 0),
    MAX(TTL_TRF_OUT_Q - MAX(BGT_CF_STK_Q - TTL_TRF_OUT_QÃƒÂ—0 - BGT_DC_CL_MBQ, 0), 0),
    MAX(TTL_STK - NT_ACT_STK, 0) ÃƒÂ— GRT_CONS_PCT
)

PP_NET_BGT_CF_STK_Q = BGT_CF_STK_Q + GRT_CONS_Q + DEL_PEND_Q

NET_SSNL_CL_STK_Q = MAX(OP_STK - GRT_CONS_Q, 0)

BGT_PUR_Q_INIT = MAX(TTL_TRF_OUT_Q + BGT_DC_CL_MBQ - PP_NET_BGT_CF_STK_Q - DEL_PEND_Q, 0)

POS_PO_RAISED = MAX(BGT_PUR_Q_INIT - DEL_PEND_Q, 0)
NEG_PO_RAISED = MIN(BGT_PUR_Q_INIT - DEL_PEND_Q, 0)

BGT_DC_CL_STK_Q = MAX(PP_NET_BGT_CF_STK_Q + POS_PO_RAISED - TTL_TRF_OUT_Q, 0)

DC_STK_EXCESS_Q = MAX(BGT_DC_CL_STK_Q - BGT_DC_CL_MBQ, 0)
DC_STK_SHORT_Q = MAX(BGT_DC_CL_MBQ - BGT_DC_CL_STK_Q, 0)

CO_STK_EXCESS_Q = ST_STK_EXCESS_Q + DC_STK_EXCESS_Q
CO_STK_SHORT_Q = ST_STK_SHORT_Q + DC_STK_SHORT_Q

BGT_CO_CL_STK_Q = NET_BGT_ST_CL_STK_Q + NET_SSNL_CL_STK_Q + BGT_DC_CL_STK_Q

FRESH_BIN_REQ = BGT_DC_CL_STK_Q / NULLIF(BIN_CAP, 0)
GRT_BIN_REQ = OP_STK / NULLIF(BIN_CAP, 0)
```

**STEP 6: Week Chaining via Cursor**
- For each RDC ÃƒÂ— MAJ_CAT combination:
  - Iterate weeks in WEEK_SEQ order
  - For week N > 1: BGT_DC_OP_STK_Q(N) = BGT_DC_CL_STK_Q(N-1)
  - Recalculate: BGT_CF_STK_Q, GRT_CONS_Q, PP_NET_BGT_CF_STK_Q, BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED, BGT_DC_CL_STK_Q, etc.
  - This creates a rolling forecast where each week's opening stock is the prior week's closing stock

**STEP 7: Insert into PURCHASE_PLAN**
- Delete prior data for this period (optional cleanup)
- Insert all calculated rows
- Return summary with row count and execution time

**Usage Examples:**

```sql
-- Generate for entire year
EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID = 1, @EndWeekID = 52, @Debug = 0;

-- Generate for single RDC, first 13 weeks only
EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID = 1, @EndWeekID = 13, @RdcCode = 'RDC001';

-- Generate for specific category across all RDCs
EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID = 1, @EndWeekID = 52, @MajCat = 'DELI', @Debug = 1;

-- Debug run with progress messages
EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID = 1, @EndWeekID = 13, @Debug = 1;
```

**Performance Notes:**
- Recommended to run by fiscal year (FY_YEAR) rather than full calendar year for optimal performance
- With ~100 RDCs and ~50 categories, expect ~5000-6000 rows per week
- Execution time typically 30-60 seconds for 52-week period

**Execution:**
```sql
SQLCMD -S <server> -d planning -i 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql
```

---

### File 4: `11_CREATE_PURCHASE_PLAN_VIEWS.sql`
**Purpose:** Create reporting views for Purchase Plan analysis

**Views Created:**

#### 1. VW_PURCHASE_PLAN_DETAIL
**Grain:** RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID (all rows)

**Contents:** All PURCHASE_PLAN columns with:
- Column aliases using hyphens (e.g., 'RDC-CD', 'MAJ-CAT') for reporting
- Self-joins to show next 2 weeks' budget sales forecasts (NXT-WK-BGT-SALE-Q, NXT2-WK-BGT-SALE-Q)

**Usage:**
```sql
SELECT * FROM dbo.VW_PURCHASE_PLAN_DETAIL
WHERE [RDC-CD] = 'RDC001' AND [MAJ-CAT] = 'DELI'
ORDER BY [WEEK_ID];
```

---

#### 2. VW_PURCHASE_PLAN_SUMMARY
**Grain:** RDC_CD ÃƒÂ— WEEK_ID (aggregated across all categories)

**Key Metrics (SUM by RDCÃƒÂ—WEEK):**
- TTL-STK-SUM, GRT-STK-Q-SUM, OP-STK-SUM
- GRT-CONS-Q-SUM, GRT-CONS-PCT-AVG
- CW-BGT-SALE-Q-SUM, CW1-BGT-SALE-Q-SUM, CW2-BGT-SALE-Q-SUM
- TTL-TRF-OUT-Q-SUM, CW-TRF-OUT-Q-SUM
- BGT-PUR-Q-INIT-SUM, POS-PO-RAISED-SUM, NEG-PO-RAISED-SUM
- DC-STK-SHORT-Q-SUM, CO-STK-SHORT-Q-SUM (alerts)
- FRESH-BIN-REQ-SUM, GRT-BIN-REQ-SUM

**Usage (find RDCs with shortages):**
```sql
SELECT * FROM dbo.VW_PURCHASE_PLAN_SUMMARY
WHERE [DC-STK-SHORT-Q-SUM] > 0 OR [CO-STK-SHORT-Q-SUM] > 0
ORDER BY [RDC-CD], [WEEK_ID];
```

---

#### 3. VW_PURCHASE_PLAN_ALERTS
**Grain:** RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID (filtered: only rows with alerts)

**Alert Conditions:**
- DC-STK-SHORT-Q > 0
- CO-STK-SHORT-Q > 0
- POS-PO-RAISED > 0

**Usage:**
```sql
SELECT [ALERT-TYPE], COUNT(*) AS AlertCount
FROM dbo.VW_PURCHASE_PLAN_ALERTS
WHERE [WEEK_ID] BETWEEN 1 AND 13
GROUP BY [ALERT-TYPE];
```

---

#### 4. VW_PURCHASE_PLAN_CATEGORY_SUMMARY
**Grain:** MAJ_CAT ÃƒÂ— WEEK_ID (aggregated across all RDCs)

**Key Metrics (SUM by CATEGORYÃƒÂ—WEEK):**
- TTL-STK-SUM, TTL-STK-AVG, TTL-STK-MIN, TTL-STK-MAX (distribution)
- GRT-STK-Q-SUM, GRT-CONS-Q-SUM, GRT-CONS-PCT-AVG
- CW-BGT-SALE-Q-SUM, CW1-BGT-SALE-Q-SUM, CW2-BGT-SALE-Q-SUM
- BGT-PUR-Q-INIT-SUM, POS-PO-RAISED-SUM
- RDC-WITH-POS-PO (count of RDCs needing to raise POs)
- RDC-WITH-DC-SHORT, RDC-WITH-CO-SHORT (count of RDCs with shortages)
- DEL-PEND-Q-SUM (total pending deliveries by category)

**Usage (find categories with widespread shortages):**
```sql
SELECT [MAJ-CAT], [WEEK_ID],
       [RDC-WITH-DC-SHORT], [RDC-WITH-CO-SHORT],
       [DC-STK-SHORT-Q-SUM], [CO-STK-SHORT-Q-SUM]
FROM dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY
WHERE [RDC-WITH-DC-SHORT] > 3  -- 3+ RDCs short
ORDER BY [MAJ-CAT], [WEEK_ID];
```

---

#### 5. VW_WEEK_REFERENCE (Helper)
**Grain:** WEEK_ID (one row per week)

**Contents:**
- WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK
- WK-ST-DT, WK-END-DT
- WEEK-DAY-COUNT (days in week, typically 7)

**Usage:**
```sql
SELECT * FROM dbo.VW_WEEK_REFERENCE
WHERE [FY_YEAR] = 2024 AND [FY_WEEK] BETWEEN 1 AND 13
ORDER BY [WEEK_SEQ];
```

---

**Execution:**
```sql
SQLCMD -S <server> -d planning -i 11_CREATE_PURCHASE_PLAN_VIEWS.sql
```

---

## Complete Setup Script

To run all four scripts in order (recommended approach):

```bash
# Execute sequentially
sqlcmd -S <server> -d planning -i 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql
sqlcmd -S <server> -d planning -i 09_CREATE_PURCHASE_PLAN_TABLE.sql
sqlcmd -S <server> -d planning -i 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql
sqlcmd -S <server> -d planning -i 11_CREATE_PURCHASE_PLAN_VIEWS.sql

# Or as a single batch file:
for %%f in (08_CREATE_PURCHASE_PLAN_REF_TABLES.sql 09_CREATE_PURCHASE_PLAN_TABLE.sql 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql 11_CREATE_PURCHASE_PLAN_VIEWS.sql) do (
    sqlcmd -S <server> -d planning -i %%f
)
```

---

## Data Dictionary

### Column Naming Convention
- **Table columns:** Use underscores (e.g., `BGT_DC_CL_STK_Q`, `DC_STK_SHORT_Q`)
- **View aliases:** Use hyphens for display (e.g., `'BGT-DC-CL-STK-Q'`, `'DC-STK-SHORT-Q'`)

### Metric Types

**Stock Quantities (ending in _Q):**
- Measured in units (count of items)
- All DECIMAL(18,2) for fractional quantities

**Percentages (ending in _PCT):**
- Decimal format (0.10 = 10%)
- DECIMAL(10,4) for precision

**Bin Requirements (ending in _REQ):**
- Decimal (18,4) for fractional bins
- Calculated as Quantity / Bin Capacity

**Currencies/Bulk (ending in _MBQ):**
- MD/Bulk quantities
- DECIMAL(18,2)

---

## Key Relationships

```
TRF_IN_PLAN (store-level, grain: ST_CD ÃƒÂ— RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID)
    Ã¢Â†Â“
    GROUP BY RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID
    Ã¢Â†Â“
PURCHASE_PLAN (RDC-level, grain: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID)
    Ã¢Â†Â“
Views (summarized for reporting)
    Ã¢Â”ÂœÃ¢Â”Â€ VW_PURCHASE_PLAN_DETAIL (all dimensions)
    Ã¢Â”ÂœÃ¢Â”Â€ VW_PURCHASE_PLAN_SUMMARY (by RDC)
    Ã¢Â”ÂœÃ¢Â”Â€ VW_PURCHASE_PLAN_CATEGORY_SUMMARY (by Category)
    Ã¢Â”Â”Ã¢Â”Â€ VW_PURCHASE_PLAN_ALERTS (exceptions only)
```

---

## Testing & Validation

After running all scripts:

```sql
-- Verify table created
SELECT COUNT(*) FROM dbo.PURCHASE_PLAN;  -- Should be 0 initially

-- Verify procedure created
SELECT * FROM sys.procedures WHERE name = 'SP_GENERATE_PURCHASE_PLAN';

-- Verify views created
SELECT name FROM sys.views WHERE name LIKE 'VW_PURCHASE_PLAN%' OR name = 'VW_WEEK_REFERENCE';

-- Test procedure execution
EXEC dbo.SP_GENERATE_PURCHASE_PLAN
    @StartWeekID = 1,
    @EndWeekID = 13,
    @Debug = 1;

-- Verify data inserted
SELECT COUNT(*) FROM dbo.PURCHASE_PLAN;

-- Sample query
SELECT TOP 10 * FROM dbo.VW_PURCHASE_PLAN_DETAIL ORDER BY [RDC-CD], [WEEK_ID];

-- Check for alerts
SELECT [ALERT-TYPE], COUNT(*) FROM dbo.VW_PURCHASE_PLAN_ALERTS GROUP BY [ALERT-TYPE];
```

---

## Support & Troubleshooting

**Issue: "Column not found" errors in views**
- Ensure all hyphenated column names in reference tables are bracketed: `[MAJ-CAT]`, `[DC-STK-Q]`

**Issue: Week offset joins failing**
- Verify WEEK_CALENDAR has sequential WEEK_SEQ values with no gaps
- Check that @StartWeekID and @EndWeekID are valid WEEK_ID values

**Issue: NULL values in reference data**
- Check QTY_MSA_AND_GRT and QTY_DEL_PENDING have current dates Ã¢Â‰Â¤ WK_END_DT
- Verify MASTER_GRT_CONS_percentage has all SSN values and WK columns

**Issue: Cursor performance slow**
- Consider disabling indexes before large runs, re-enabling after
- Run procedure separately by RDC_CD for better parallelization

---

## Notes
- All scripts use `USE [planning]` to ensure correct database context
- Each script is self-contained and can be re-run (includes DROP statements)
- Comments explain purpose and dependencies at the top of each file
- Indexes are created for typical query patterns in Purchase Planning
- Views use brackets for all hyphenated identifiers to ensure portability

---

Generated: 2026-04-03
