# TRF_IN_PLAN SQL Scripts Directory

## Complete File Listing

### TRANSFER IN PLAN SCRIPTS (Files 01-07)
Baseline setup for store-level Transfer In Plan system

| File | Size | Purpose | Type |
|------|------|---------|------|
| `01_CREATE_REFERENCE_TABLES.sql` | 11K | Create master reference tables | DDL |
| `02_CREATE_TRF_IN_PLAN_TABLE.sql` | 3.6K | Create TRF_IN_PLAN output table | DDL |
| `03_CREATE_SP_GENERATE_TRF_IN_PLAN.sql` | 23K | Stored procedure for TRF_IN_PLAN generation | DML |
| `04_INSERT_SAMPLE_DATA.sql` | 14K | Insert sample master data | DML |
| `05_VIEWS_AND_EXECUTION.sql` | 5.8K | Create TRF_IN views and test queries | DDL |
| `06_CREATE_VW_TRF_IN_DETAIL.sql` | 3.5K | Detailed view of TRF_IN_PLAN | DDL |
| `07_CREATE_VW_TRF_IN_PIVOT.sql` | 55K | Pivot view for TRF_IN_PLAN analysis | DDL |

### PURCHASE PLAN SCRIPTS (Files 08-11) - NEW
Aggregation layer that rolls up TRF_IN_PLAN to RDC ÃƒÂ— Category ÃƒÂ— Week

| File | Size | Objects | Purpose |
|------|------|---------|---------|
| **`08_CREATE_PURCHASE_PLAN_REF_TABLES.sql`** | 2.9K | 1 table + 2 indexes | Reference tables for Purchase Plan |
| **`09_CREATE_PURCHASE_PLAN_TABLE.sql`** | 7.1K | 1 table + 4 indexes | PURCHASE_PLAN output table (56 columns) |
| **`10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql`** | 37K | 1 procedure | Stored procedure with 7-step algorithm |
| **`11_CREATE_PURCHASE_PLAN_VIEWS.sql`** | 13K | 5 views | Reporting and analysis views |

### DOCUMENTATION

| File | Size | Purpose |
|------|------|---------|
| **`README_PURCHASE_PLAN.md`** | 15K | Complete technical documentation with examples |
| **`EXECUTION_GUIDE.txt`** | 12K | Step-by-step execution instructions |
| **`INDEX.md`** | This file | Directory and quick reference |

### DATA FILES

| File | Size | Purpose |
|------|------|---------|
| `TransferIn_Plan.xlsx` | 627K | Excel workbook with TRF_IN_PLAN data model |
| `PurchasePlan.xlsx` | 279K | Excel workbook with PURCHASE_PLAN data model |

### CONSOLIDATED SETUP

| File | Size | Purpose |
|------|------|---------|
| `FULL_TRF_IN_PLAN_SETUP.sql` | 53K | Complete all-in-one script (01-07) |

---

## Quick Start

### First Time Setup (All Systems)

Execute in order:
```bash
sqlcmd -S <server> -d planning -i 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql
sqlcmd -S <server> -d planning -i 09_CREATE_PURCHASE_PLAN_TABLE.sql
sqlcmd -S <server> -d planning -i 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql
sqlcmd -S <server> -d planning -i 11_CREATE_PURCHASE_PLAN_VIEWS.sql
```

### Generate Purchase Plan Data

```sql
EXEC dbo.SP_GENERATE_PURCHASE_PLAN
  @StartWeekID = 1,
  @EndWeekID = 52,
  @Debug = 1;
```

### Query Results

```sql
-- All detail with calculations
SELECT TOP 100 * FROM dbo.VW_PURCHASE_PLAN_DETAIL
ORDER BY [RDC-CD], [MAJ-CAT], [WEEK_ID];

-- Summary by RDC
SELECT * FROM dbo.VW_PURCHASE_PLAN_SUMMARY
WHERE [CO-STK-SHORT-Q-SUM] > 0;

-- Alerts only
SELECT * FROM dbo.VW_PURCHASE_PLAN_ALERTS
WHERE [ALERT-TYPE] IN ('DC-STOCK-SHORT', 'COMPANY-STOCK-SHORT');

-- By Category
SELECT * FROM dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY
WHERE [RDC-WITH-POS-PO] > 0
ORDER BY [MAJ-CAT], [WEEK_ID];
```

---

## Database Objects Created

### Tables
- `QTY_DEL_PENDING` - Delivery pending tracking
- `PURCHASE_PLAN` - Main output table (56 columns, RDC ÃƒÂ— CAT ÃƒÂ— WEEK grain)

### Stored Procedures
- `SP_GENERATE_PURCHASE_PLAN` - Main calculation engine (7-step algorithm)

### Views
- `VW_PURCHASE_PLAN_DETAIL` - Full detail with all columns
- `VW_PURCHASE_PLAN_SUMMARY` - By RDC aggregation
- `VW_PURCHASE_PLAN_ALERTS` - Exception reporting
- `VW_PURCHASE_PLAN_CATEGORY_SUMMARY` - By Category aggregation
- `VW_WEEK_REFERENCE` - Week calendar helper

### Indexes
- Primary: `PK_PURCHASE_PLAN` on ID
- Search: (RDC_CD, MAJ_CAT, WEEK_ID)
- Temporal: (WEEK_ID, FY_YEAR)
- Dimensional: (RDC_CD, MAJ_CAT)
- Alerts: Filtered index on SHORT_Q > 0

---

## Data Model Grain

### TRF_IN_PLAN (Store Level)
```
Grain: ST_CD ÃƒÂ— RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID
Rows: Typically 1,000s (stores ÃƒÂ— categories ÃƒÂ— weeks)
```

### PURCHASE_PLAN (RDC Level) - OUTPUT
```
Grain: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID
Rows: Typically 100s-1000s (RDCs ÃƒÂ— categories ÃƒÂ— weeks)
Example: 100 RDCs ÃƒÂ— 50 categories ÃƒÂ— 52 weeks = 260,000 rows
```

---

## Key Metrics Calculated

### Stock Levels
- **TTL_STK**: Total available stock
- **OP_STK**: Operating stock
- **NT_ACT_STK**: No-touch active stock
- **GRT_STK_Q**: GRT opening position
- **DC_STK_Q**: DC opening position

### Consumption
- **GRT_CONS_PCT**: Percentage (from master)
- **GRT_CONS_Q**: Calculated consumption quantity

### Transfer & Sales
- **CW_BGT_SALE_Q**: Current week forecast
- **CW1-5_BGT_SALE_Q**: Forward weeks (via joins)
- **TTL_TRF_OUT_Q**: Total transfer to stores

### Purchase Planning
- **BGT_PUR_Q_INIT**: Initial purchase quantity
- **POS_PO_RAISED**: Positive PO amount
- **NEG_PO_RAISED**: Negative PO (returns)
- **PP_NET_BGT_CF_STK_Q**: Confirmed stock

### Alerts
- **DC_STK_SHORT_Q**: Shortage at DC
- **DC_STK_EXCESS_Q**: Excess at DC
- **CO_STK_SHORT_Q**: Company-level shortage
- **CO_STK_EXCESS_Q**: Company-level excess

### Bin Requirements
- **FRESH_BIN_REQ**: Bins for fresh stock
- **GRT_BIN_REQ**: Bins for GRT stock

---

## Dependencies

### Required Tables (must pre-exist)
- `WEEK_CALENDAR`
- `MASTER_ST_MASTER`
- `MASTER_BIN_CAPACITY`
- `MASTER_GRT_CONS_percentage`
- `QTY_MSA_AND_GRT`
- `TRF_IN_PLAN` (must be populated)

### Created by Script 08
- `QTY_DEL_PENDING`

---

## Execution Sequence

### Scenario 1: New Installation
```
Step 1: Run 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql
Step 2: Run 09_CREATE_PURCHASE_PLAN_TABLE.sql
Step 3: Run 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql
Step 4: Run 11_CREATE_PURCHASE_PLAN_VIEWS.sql
Step 5: EXEC dbo.SP_GENERATE_PURCHASE_PLAN @StartWeekID=1, @EndWeekID=52
Step 6: Query views for reporting
```

### Scenario 2: Weekly Refresh
```
Step 1: Populate current week's TRF_IN_PLAN data (from stores)
Step 2: Update QTY_DEL_PENDING with current pending deliveries
Step 3: EXEC SP_GENERATE_PURCHASE_PLAN @StartWeekID=<current>, @EndWeekID=<current+12>
Step 4: Publish reports from views
```

### Scenario 3: Category Analysis
```
EXEC SP_GENERATE_PURCHASE_PLAN 
  @StartWeekID=1, @EndWeekID=52, 
  @MajCat='DELI'
SELECT * FROM VW_PURCHASE_PLAN_CATEGORY_SUMMARY
WHERE [MAJ-CAT]='DELI'
```

---

## Expected Performance

| Scenario | Rows | Time |
|----------|------|------|
| Full year (52 weeks) | ~260K | 30-60 sec |
| Quarter (13 weeks) | ~65K | 8-15 sec |
| Single RDC (52 weeks) | ~2.6K | 2-5 sec |
| Single RDC (13 weeks) | ~650 | <1 sec |

*Times vary based on database size and server performance*

---

## Documentation References

### Complete Details
See `README_PURCHASE_PLAN.md` for:
- Complete algorithm walkthrough
- Column definitions and derivations
- Advanced query examples
- Troubleshooting guide
- Performance tuning tips

### Quick Execution
See `EXECUTION_GUIDE.txt` for:
- Step-by-step setup instructions
- Validation queries
- Usage examples
- Common errors and solutions

### Excel Data Models
- `TransferIn_Plan.xlsx` - TRF_IN_PLAN structure
- `PurchasePlan.xlsx` - PURCHASE_PLAN structure

---

## File Sizes Summary

```
Purchase Plan Scripts (Total: 60K)
Ã¢Â”ÂœÃ¢Â”Â€ 08_CREATE_PURCHASE_PLAN_REF_TABLES.sql    2.9K
Ã¢Â”ÂœÃ¢Â”Â€ 09_CREATE_PURCHASE_PLAN_TABLE.sql         7.1K
Ã¢Â”ÂœÃ¢Â”Â€ 10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql  37.0K
Ã¢Â”Â”Ã¢Â”Â€ 11_CREATE_PURCHASE_PLAN_VIEWS.sql        13.0K

Documentation (Total: 27K)
Ã¢Â”ÂœÃ¢Â”Â€ README_PURCHASE_PLAN.md                  15.0K
Ã¢Â”Â”Ã¢Â”Â€ EXECUTION_GUIDE.txt                      12.0K

Total Scripts in Directory: ~225K
```

---

## Column Count

### PURCHASE_PLAN Table: 56 Columns
```
Identifiers (5):      ID, RDC_CD, RDC_NM, MAJ_CAT, SSN, WEEK_ID, 
                      FY_WEEK, FY_YEAR, WK_ST_DT, WK_END_DT

Stock Reference (4):  DC_STK_Q, GRT_STK_Q, S_GRT_STK_Q, W_GRT_STK_Q

Bin Capacity (2):     BIN_CAP_DC_TEAM, BIN_CAP

Store Planning (13):  BGT_DISP_CL_Q, CW_BGT_SALE_Q, CW1_BGT_SALE_Q,
                      CW2_BGT_SALE_Q, CW3_BGT_SALE_Q, CW4_BGT_SALE_Q,
                      CW5_BGT_SALE_Q, BGT_ST_OP_MBQ, NET_ST_OP_STK_Q,
                      CW_TRF_OUT_Q, CW1_TRF_OUT_Q, TTL_TRF_OUT_Q,
                      BGT_ST_CL_MBQ, NET_BGT_ST_CL_STK_Q

DC Opening (3):       BGT_DC_OP_STK_Q, PP_NT_ACT_Q, BGT_CF_STK_Q

Stock Calc (3):       TTL_STK, OP_STK, NT_ACT_STK

GRT (2):              GRT_CONS_PCT, GRT_CONS_Q

Delivery (1):         DEL_PEND_Q

Purchase Logic (15):  PP_NET_BGT_CF_STK_Q, NET_SSNL_CL_STK_Q,
                      BGT_DC_MBQ_SALE, BGT_DC_CL_MBQ, BGT_DC_CL_STK_Q,
                      BGT_PUR_Q_INIT, POS_PO_RAISED, NEG_PO_RAISED,
                      BGT_CO_CL_STK_Q, DC_STK_EXCESS_Q, DC_STK_SHORT_Q,
                      ST_STK_EXCESS_Q, ST_STK_SHORT_Q,
                      CO_STK_EXCESS_Q, CO_STK_SHORT_Q

Bins (2):             FRESH_BIN_REQ, GRT_BIN_REQ

Audit (4):            CREATED_DT, CREATED_BY, MODIFIED_DT, MODIFIED_BY
```

---

## Support

For detailed information:
- Algorithm details Ã¢Â†Â’ See `10_CREATE_SP_GENERATE_PURCHASE_PLAN.sql` (lines 35-200)
- Column definitions Ã¢Â†Â’ See `README_PURCHASE_PLAN.md` (Data Dictionary section)
- Query examples Ã¢Â†Â’ See `EXECUTION_GUIDE.txt` (POST-EXECUTION VALIDATION)
- Troubleshooting Ã¢Â†Â’ See `README_PURCHASE_PLAN.md` (Support & Troubleshooting)

---

**Generated:** 2026-04-03  
**Status:** Complete - Ready for Implementation
