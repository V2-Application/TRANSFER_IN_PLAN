# V2 Retail - Unified Planning & Replenishment Architecture
## Snowflake + Azure End-to-End Design

> **For:** Akash Agarwal, Director - Technology & Data
> **Scope:** Complete automation of budget creation, planning, allocation, and replenishment for 320+ stores, 500+ major categories
> **Date:** April 2026

---

## 1. Current State Map

### 1.1 What Exists Today

```
+------------------------------------------------------------------+
|                     ALREADY BUILT (Production)                    |
+------------------------------------------------------------------+
|                                                                    |
|  [ARS] Auto Replenishment System                                  |
|  - Python FastAPI on Azure App Service                            |
|  - 5 engines: Budget Cascade > Article Scoring > Greedy Filler    |
|    > Size Allocator > DO Generator                                |
|  - Processes 1M store-article pairs in <6 min                    |
|  - API: ars-v2retail-api.azurewebsites.net                       |
|                                                                    |
|  [Planning App] ASP.NET Core 8 on localhost:5005                  |
|  - 29 controllers, 6 stored procedures                           |
|  - Weekly TRF-IN + Purchase Plan + Sub-level plans               |
|  - SQL Server database "planning" (26 tables)                    |
|                                                                    |
|  [Supabase] PostgreSQL (project pymdqnnwwxrgeolvgvgv)            |
|  - co_budget_store_major_category: 387K rows                     |
|  - co_budget_company_major_category_size: 1.5K rows              |
|  - Budget data shared between ARS + Planning                     |
|                                                                    |
|  [Snowflake] Analytics & Scoring                                  |
|  - ARS scoring visualization                                     |
|  - Currently analytics only                                       |
|                                                                    |
|  [Azure Fabric] Data Warehouse                                    |
|  - Gold Layer with 8B+ rows of sales data                        |
|  - V2Retail_Gold lakehouse                                        |
|                                                                    |
|  [Cloudflare] Workers                                             |
|  - replen.v2retail.net (contribution refresh cron)               |
|  - nubo-ads-bot (marketing)                                      |
|                                                                    |
|  [n8n] Workflow Orchestration                                     |
|  - Existing automation workflows                                  |
|                                                                    |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
|                     STILL IN EXCEL (Manual)                       |
+------------------------------------------------------------------+
|                                                                    |
|  [V-0063] Rolling Budget Plan (685 MB, 1006 cols)                |
|  - LYSP + growth rates + fill rate algo + festivals              |
|  - Produces: BGT SALE-QTY, BGT SALE-VAL per store x MAJCAT      |
|  - THE SOURCE OF ALL BUDGET NUMBERS                              |
|                                                                    |
|  [V-0081] Fixture & Density Model (371 cols)                     |
|  - State model > National model > Store fitment > ROI scoring    |
|  - Produces: ACC DENSITY, DISP QTY per store x MAJCAT           |
|  - THE SOURCE OF ALL DISPLAY TARGETS                             |
|                                                                    |
|  [VER.0061] Monthly TRF-IN & Purchase Plan                       |
|  - Store-level transfer-in calculation                           |
|  - DC-level purchase plan                                        |
|  - Being replaced by ASP.NET Planning App                        |
|                                                                    |
|  [Monthly->Weekly] Budget Disaggregation                          |
|  - Manual split of monthly budgets into 48 weekly columns        |
|  - Feeds into ASP.NET Planning App                               |
|                                                                    |
|  [Allocation Excel] 29-step VBA macro (896 cols)                 |
|  - Being replaced by ARS                                          |
|  - Still used for some MAJCATs not yet migrated                  |
|                                                                    |
+------------------------------------------------------------------+
```

### 1.2 The Gap

The **only manual steps** remaining are at the TOP of the pipeline:
1. **Budget creation** (V-0063) - LYSP-based sales budget with growth rates
2. **Fixture planning** (V-0081) - Display density and fixture allocation
3. **Monthly-to-weekly disaggregation** - Splitting monthly into weekly buckets

Everything downstream (TRF-IN, Purchase Plan, Article Allocation, Size Allocation, Delivery Orders) is already automated. **The highest ROI is automating the budget creation.**

---

## 2. Target Architecture

### 2.1 Design Principles

1. **Snowflake as the single computation engine** - All planning, budgeting, and analytics in one place
2. **Azure as the application layer** - APIs, UIs, SAP integration
3. **Supabase as the operational data store** - Real-time reads for ARS and Planning App
4. **No Excel in the loop** - Every step automated, with human review via dashboards

### 2.2 Architecture Diagram

```
+========================================================================+
|                    V2 RETAIL UNIFIED ARCHITECTURE                       |
+========================================================================+
|                                                                          |
|  LAYER 1: DATA INGESTION (Azure Data Factory + Fabric)                  |
|  +-----------------------------------------------------------------+    |
|  | SAP HANA -----> Stock (DC + Store), Article Master, PO Status   |    |
|  | SQL Server ---> 8B+ rows historical sales (via Fabric Gold)     |    |
|  | HHT/POS -----> Real-time store stock updates                    |    |
|  | Manual -------> Season calendar, fixture vetted inputs          |    |
|  +-----------------------------------------------------------------+    |
|         |                                                                |
|         v                                                                |
|  LAYER 2: SNOWFLAKE (Compute + Storage + Analytics)                     |
|  +-----------------------------------------------------------------+    |
|  |                                                                   |    |
|  |  BRONZE (Raw)        SILVER (Clean)        GOLD (Business)       |    |
|  |  +-------------+    +---------------+    +------------------+    |    |
|  |  | sap_dc_stock|    | dim_store     |    | fact_budget_     |    |    |
|  |  | sap_st_stock|--->| dim_article   |--->|   store_majcat   |    |    |
|  |  | sap_sales   |    | dim_majcat    |    | fact_trf_in_plan |    |    |
|  |  | sap_po      |    | dim_season    |    | fact_purchase_   |    |    |
|  |  | fabric_gold |    | dim_week      |    |   plan           |    |    |
|  |  +-------------+    | dim_festival  |    | fact_allocation  |    |    |
|  |                      +---------------+    | fact_fixture_    |    |    |
|  |                                           |   plan           |    |    |
|  |                                           +------------------+    |    |
|  |                                                                   |    |
|  |  COMPUTE LAYER (Stored Procedures + Tasks)                       |    |
|  |  +-----------------------------------------------------------+  |    |
|  |  |                                                             |  |    |
|  |  |  [1] BUDGET ENGINE (replaces V-0063)                       |  |    |
|  |  |      LYSP lookup > Growth rates > Fill rate algo >         |  |    |
|  |  |      Festival adjust > Min/Max caps > Bottom-up reconcile  |  |    |
|  |  |                                                             |  |    |
|  |  |  [2] FIXTURE ENGINE (replaces V-0081)                      |  |    |
|  |  |      State model > National model > ROI scoring >          |  |    |
|  |  |      Incremental fix > Floor balance > Density output      |  |    |
|  |  |                                                             |  |    |
|  |  |  [3] DISAGGREGATION ENGINE (replaces manual weekly split)  |  |    |
|  |  |      Monthly budget > Season curves > Weekly allocation    |  |    |
|  |  |                                                             |  |    |
|  |  |  [4] PLANNING ENGINE (replaces VER.0061 + ASP.NET SPs)    |  |    |
|  |  |      Store TRF-IN > DC Purchase > Sub-level plans          |  |    |
|  |  |                                                             |  |    |
|  |  |  [5] ANALYTICS ENGINE                                      |  |    |
|  |  |      Budget vs Actual > Forecast accuracy > ROI tracking   |  |    |
|  |  |                                                             |  |    |
|  |  +-----------------------------------------------------------+  |    |
|  +-----------------------------------------------------------------+    |
|         |                                                                |
|         v                                                                |
|  LAYER 3: OPERATIONAL (Supabase + Azure SQL)                            |
|  +-----------------------------------------------------------------+    |
|  | Supabase: co_budget_store_major_category (auto-synced from SF)  |    |
|  | Azure SQL: ALLOCATION_MRDC_RAW_DATA (article master for ARS)    |    |
|  | Azure SQL: planning database (26 tables for ASP.NET app)        |    |
|  +-----------------------------------------------------------------+    |
|         |                                                                |
|         v                                                                |
|  LAYER 4: APPLICATION (Azure App Service)                               |
|  +-----------------------------------------------------------------+    |
|  |  [ARS API] Article-level allocation (5 engines, <6 min)        |    |
|  |  [Planning App] Weekly TRF-IN + Purchase Plan (ASP.NET)        |    |
|  |  [Budget UI] New - Budget review & approval dashboard          |    |
|  |  [Fixture UI] New - Fixture planning review dashboard          |    |
|  +-----------------------------------------------------------------+    |
|         |                                                                |
|         v                                                                |
|  LAYER 5: OUTPUT & INTEGRATION                                          |
|  +-----------------------------------------------------------------+    |
|  |  SAP RFC: Delivery Orders, Purchase Orders                      |    |
|  |  Power BI: Dashboards (via Fabric/Snowflake)                   |    |
|  |  Telegram: Alerts & approvals                                   |    |
|  |  n8n: Orchestration & scheduling                               |    |
|  +-----------------------------------------------------------------+    |
|                                                                          |
+========================================================================+
```

---

## 3. Snowflake Data Model

### 3.1 Bronze Layer (Raw Ingestion)

```sql
-- SAP stock snapshots (daily)
CREATE TABLE bronze.sap_dc_stock (
    rdc_cd STRING, maj_cat STRING, dc_stk_q NUMBER, grt_stk_q NUMBER,
    s_grt_stk_q NUMBER, w_grt_stk_q NUMBER, pw_grt_stk_q NUMBER,
    msa_stk_q NUMBER, ssnl_32_stk_q NUMBER, ssnl_37_stk_q NUMBER,
    ssnl_42_stk_q NUMBER, c_art_stk NUMBER, st_to_dc_stk_q NUMBER,
    snapshot_date DATE, loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE bronze.sap_st_stock (
    st_cd STRING, maj_cat STRING, stk_qty NUMBER, st0001_stk NUMBER,
    st0002_stk NUMBER, int_qty NUMBER, prd_qty NUMBER,
    snapshot_date DATE, loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Sales history (incremental from Fabric Gold)
CREATE TABLE bronze.fabric_sales_history (
    st_cd STRING, maj_cat STRING, sale_date DATE,
    sale_qty NUMBER, sale_val NUMBER(18,2), gm_val NUMBER(18,2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- SAP article master (weekly refresh)
CREATE TABLE bronze.sap_article_master (
    gen_art_number STRING, color STRING, size STRING, mrp NUMBER(10,2),
    cost NUMBER(10,2), seg STRING, div STRING, sub_div STRING,
    maj_cat STRING, mvgr STRING, vendor_cd STRING, vendor_nm STRING,
    fabric STRING, season STRING, aging_days INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Pending POs from SAP
CREATE TABLE bronze.sap_pending_po (
    rdc_cd STRING, maj_cat STRING, ssn STRING,
    po_qty NUMBER, delivery_month DATE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### 3.2 Silver Layer (Dimension Tables)

```sql
CREATE TABLE silver.dim_store (
    st_cd STRING PRIMARY KEY,
    st_nm STRING, state STRING, status STRING, op_date DATE,
    area NUMBER, flr INT, rdc_cd STRING, rdc_nm STRING,
    hub_cd STRING, hub_nm STRING,
    dc_to_hub_frkh INT, hub_to_st_frkh INT,
    dc_to_hub_kol INT, hub_to_st_kol INT,
    ref_st_cd STRING, ref_st_nm STRING,
    ref_grp_new STRING, ref_grp_old STRING,
    -- Festival flags
    flag_d_puja BOOLEAN, flag_diwali BOOLEAN, flag_chhat BOOLEAN,
    flag_eid BOOLEAN, flag_b_eid BOOLEAN, flag_ugadi BOOLEAN,
    flag_holi BOOLEAN, flag_g_puja BOOLEAN, flag_s_puja BOOLEAN,
    flag_p_asthmi BOOLEAN, flag_pongal BOOLEAN, flag_rajjo BOOLEAN,
    flag_bihu BOOLEAN,
    -- Cover days per month (APR-MAR)
    cover_apr INT, cover_may INT, cover_jun INT, cover_jul INT,
    cover_aug INT, cover_sep INT, cover_oct INT, cover_nov INT,
    cover_dec INT, cover_jan INT, cover_feb INT, cover_mar INT,
    sale_cover INT, int_cover INT, prd_cover INT
);

CREATE TABLE silver.dim_majcat (
    maj_cat STRING PRIMARY KEY,
    seg STRING,          -- APP/GM/FW (business segment)
    div STRING,          -- KIDS/MENS/LADIES/etc
    sub_div STRING,      -- KB-U, MS-L, etc
    ssn STRING,          -- A/S/W/PW/SSNL
    ref_maj_cat STRING,  -- Reference MAJCAT (for new categories)
    manual_gr_pct NUMBER(5,2) DEFAULT 1.1,  -- Growth multiplier from Sheet3
    -- Price tier segment (from allocation)
    price_seg STRING,    -- E/V/P (Economy/Value/Premium)
    -- Fixture constraints
    min_fix NUMBER(8,2), max_fix NUMBER(8,2),
    -- Bin capacity
    carton_l NUMBER(6,2), carton_b NUMBER(6,2), carton_h NUMBER(6,2),
    per_carton_qty INT
);

CREATE TABLE silver.dim_week (
    week_id INT PRIMARY KEY,
    fy_year INT, fy_week INT,
    wk_start_date DATE, wk_end_date DATE,
    month_date DATE,    -- First of month
    days_in_week INT,
    week_seq INT        -- Sequential week number
);

CREATE TABLE silver.dim_festival_calendar (
    festival STRING, state STRING, month INT,
    impact_pct NUMBER(5,2),  -- % sales uplift during festival
    duration_days INT
);
```

### 3.3 Gold Layer (Fact Tables - Computation Outputs)

```sql
-- THE MASTER BUDGET TABLE (replaces V-0063 output + Supabase)
CREATE TABLE gold.fact_budget_store_majcat (
    run_id STRING,
    run_date DATE,
    st_cd STRING REFERENCES silver.dim_store(st_cd),
    maj_cat STRING REFERENCES silver.dim_majcat(maj_cat),
    plan_month DATE,

    -- Budget outputs (THE KEY NUMBERS)
    bgt_sale_qty NUMBER(12,2),
    bgt_sale_val NUMBER(14,2),
    bgt_disp_qty NUMBER(12,2),    -- Display stock target
    bgt_disp_val NUMBER(14,2),
    bgt_gm_val NUMBER(14,2),      -- Gross margin target

    -- Inputs used
    lysp_sale_qty NUMBER(12,2),   -- Last year same period
    lysp_sale_val NUMBER(14,2),
    growth_rate_st_majcat NUMBER(6,4),  -- Store x MAJCAT growth %
    growth_rate_majcat NUMBER(6,4),     -- Category growth %
    growth_rate_store NUMBER(6,4),      -- Store growth %
    fill_rate_adj NUMBER(6,4),          -- Fill rate adjustment
    festival_adj NUMBER(6,4),           -- Festival impact adjustment

    -- Fixture data
    fix_count NUMBER(8,2),        -- Fixture count
    area NUMBER(8,2),             -- Display area sq ft
    density NUMBER(8,2),          -- ACC DENSITY (articles per fixture)
    disp_pct NUMBER(6,4),         -- Display % of next month sales

    -- Algo tracking
    algo_sale_qty_1 NUMBER(12,2), -- Algorithm intermediate
    algo_sale_qty_2 NUMBER(12,2),
    rxl NUMBER(8,2),              -- Range execution level
    rxl_gr_pct NUMBER(6,4),       -- RXL growth %

    -- Status
    is_approved BOOLEAN DEFAULT FALSE,
    approved_by STRING,
    approved_at TIMESTAMP,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- FIXTURE PLAN (replaces V-0081 output)
CREATE TABLE gold.fact_fixture_plan (
    run_id STRING,
    run_date DATE,
    st_cd STRING,
    maj_cat STRING,

    -- Fixture allocation
    state_model_fix NUMBER(8,2),
    nat_model_fix NUMBER(8,2),
    roi_auto_fix NUMBER(8,2),
    final_fix_pur NUMBER(8,2),    -- For purchase planning
    final_fix_alc NUMBER(8,2),    -- For allocation
    final_density NUMBER(8,2),    -- ACC DENSITY output

    -- ROI metrics
    gp_psf NUMBER(10,2),          -- Gross profit per sq ft
    flr_gp_psf NUMBER(10,2),     -- Floor GP PSF
    psf_ach_pct NUMBER(6,4),     -- PSF achievement %

    -- Floor balance
    st_flr_fix NUMBER(8,2),      -- Store floor fixtures
    flr_balance NUMBER(8,2),     -- Floor balance after allocation

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- STORE TRANSFER-IN PLAN (replaces VER.0061 ST-TRF-IN + ASP.NET TRF_IN_PLAN)
CREATE TABLE gold.fact_trf_in_plan (
    run_id STRING,
    run_date DATE,
    st_cd STRING,
    maj_cat STRING,
    plan_week_id INT,             -- For weekly granularity
    plan_month DATE,              -- For monthly granularity

    -- Opening position
    op_stk_q NUMBER(12,2),
    net_op_stk_q NUMBER(12,2),
    nt_act_q NUMBER(12,2),

    -- Budget inputs
    bgt_disp_cl_q NUMBER(12,2),
    bgt_sale_q NUMBER(12,2),
    bgt_cover_sale_q NUMBER(12,2),
    bgt_st_cl_mbq NUMBER(12,2),

    -- OUTPUTS
    trf_in_stk_q NUMBER(12,2),   -- Transfer-in quantity
    trf_in_opt_cnt NUMBER(10,2), -- Transfer-in option count
    dc_mbq NUMBER(12,2),

    -- Closing position
    cl_stk_q NUMBER(12,2),
    excess_q NUMBER(12,2),
    short_q NUMBER(12,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- DC PURCHASE PLAN (replaces VER.0061 DC-TRF OUT + ASP.NET PURCHASE_PLAN)
CREATE TABLE gold.fact_purchase_plan (
    run_id STRING,
    run_date DATE,
    rdc_cd STRING,
    maj_cat STRING,
    plan_month DATE,

    -- DC position
    dc_op_stk_q NUMBER(12,2),
    grt_stk_q NUMBER(12,2),
    grt_cons_q NUMBER(12,2),

    -- Aggregated store demand
    trf_out_total NUMBER(12,2),
    bgt_st_cl_mbq NUMBER(12,2),
    net_st_cl_stk NUMBER(12,2),

    -- DC calculations
    dc_cl_mbq NUMBER(12,2),
    dc_cl_stk_q NUMBER(12,2),

    -- OUTPUTS
    bgt_pur_q NUMBER(12,2),      -- Purchase quantity
    pos_po NUMBER(12,2),          -- POs to raise
    neg_po NUMBER(12,2),          -- POs to cancel
    fresh_bin_req NUMBER(10,2),   -- Bin space needed
    grt_bin_req NUMBER(10,2),

    -- Company level
    co_cl_stk_q NUMBER(12,2),
    dc_excess_q NUMBER(12,2),
    dc_short_q NUMBER(12,2),
    st_excess_q NUMBER(12,2),
    st_short_q NUMBER(12,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ALLOCATION RESULTS (from ARS)
CREATE TABLE gold.fact_allocation (
    run_id STRING,
    run_date DATE,
    st_cd STRING,
    maj_cat STRING,
    gen_art_color STRING,
    size STRING,

    -- Allocation
    opt_no INT,
    score NUMBER(8,2),
    alloc_qty INT,
    mrp NUMBER(10,2),
    cost NUMBER(10,2),
    total_value NUMBER(14,2),

    -- Source
    rdc_cd STRING,
    engine_version STRING,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 4. The 5 Compute Engines in Snowflake

### 4.1 Engine 1: Budget Creation (replaces V-0063)

This is the **highest value automation** - replaces the 685 MB Excel with 1,006 columns.

```sql
-- Snowflake Stored Procedure: Budget Engine
CREATE OR REPLACE PROCEDURE gold.sp_generate_budget(
    plan_months ARRAY,  -- e.g., ['2026-04-01', '2026-05-01', ...]
    run_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- STEP 1: Get LYSP (Last Year Same Period) actuals
    CREATE OR REPLACE TEMP TABLE tmp_lysp AS
    SELECT st_cd, maj_cat, plan_month,
        SUM(sale_qty) AS lysp_sale_qty,
        SUM(sale_val) AS lysp_sale_val,
        SUM(gm_val) AS lysp_gm_val
    FROM bronze.fabric_sales_history
    WHERE sale_date BETWEEN DATEADD(YEAR, -1, plan_month)
                        AND DATEADD(YEAR, -1, LAST_DAY(plan_month))
    GROUP BY 1, 2, 3;

    -- STEP 2: Calculate growth rates
    CREATE OR REPLACE TEMP TABLE tmp_growth AS
    SELECT
        s.st_cd, m.maj_cat, p.plan_month,
        -- Store x MAJCAT growth (from last 3 months trend)
        COALESCE(recent.gr_pct, 0) AS growth_rate_st_majcat,
        -- Category growth (national)
        COALESCE(cat_gr.gr_pct, m.manual_gr_pct - 1, 0.10) AS growth_rate_majcat,
        -- Store growth (all categories)
        COALESCE(st_gr.gr_pct, 0.10) AS growth_rate_store
    FROM silver.dim_store s
    CROSS JOIN silver.dim_majcat m
    CROSS JOIN (SELECT column1 AS plan_month FROM VALUES ... ) p
    LEFT JOIN ... ;  -- Growth calculation joins

    -- STEP 3: Apply fill rate adjustment (ALGO from Sheet2)
    -- If LW fill rate < 70%: increase stock
    -- If LW fill rate > 130%: decrease stock
    CREATE OR REPLACE TEMP TABLE tmp_fill_adj AS
    SELECT st_cd, maj_cat,
        CASE
            WHEN lw_fill_rate < 0.70 THEN
                lw_str * (1 + (0.70 / lw_fill_rate - 1) * 0.50)
            WHEN lw_fill_rate > 1.30 THEN
                lw_str * (1 - (1 - 1.30 / lw_fill_rate) * 0.50)
            ELSE lw_str
        END AS adj_str,
        CASE
            WHEN lw_str = 0 AND lysp_str = 0 THEN
                CASE WHEN seg = 'APP' THEN 45 ELSE 60 END  -- Default days
            ELSE NULL
        END AS default_days
    FROM ...;

    -- STEP 4: Apply festival adjustments
    CREATE OR REPLACE TEMP TABLE tmp_festival AS
    SELECT b.st_cd, b.maj_cat, b.plan_month,
        COALESCE(f.impact_pct, 0) AS festival_uplift
    FROM tmp_lysp b
    LEFT JOIN silver.dim_festival_calendar f
        ON b.plan_month = f.month
        AND EXISTS (
            SELECT 1 FROM silver.dim_store s
            WHERE s.st_cd = b.st_cd
            AND s.state = f.state
            AND CASE f.festival
                WHEN 'DIWALI' THEN s.flag_diwali
                WHEN 'CHHAT' THEN s.flag_chhat
                -- ... all 13 festivals
                END = TRUE
        );

    -- STEP 5: Calculate budget
    -- BGT = LYSP * (1 + growth) * (1 + festival) * fill_rate_adj
    -- Then apply MIN/MAX caps from dim_majcat
    INSERT INTO gold.fact_budget_store_majcat (...)
    SELECT
        :run_id,
        CURRENT_DATE(),
        l.st_cd, l.maj_cat, l.plan_month,

        -- Core budget calculation
        GREATEST(
            m.min_fix,
            LEAST(
                m.max_fix,
                l.lysp_sale_qty
                * (1 + g.growth_rate_st_majcat)
                * (1 + COALESCE(fst.festival_uplift, 0))
                * COALESCE(fa.adj_factor, 1.0)
            )
        ) AS bgt_sale_qty,

        -- ... similar for bgt_sale_val, bgt_disp_qty etc.
    FROM tmp_lysp l
    JOIN tmp_growth g USING (st_cd, maj_cat, plan_month)
    LEFT JOIN tmp_fill_adj fa USING (st_cd, maj_cat)
    LEFT JOIN tmp_festival fst USING (st_cd, maj_cat, plan_month)
    JOIN silver.dim_majcat m ON l.maj_cat = m.maj_cat;

    -- STEP 6: Bottom-up reconciliation
    -- Ensure store-level budgets sum to company targets
    -- Adjust proportionally if bottom-up total differs from top-down

    RETURN 'Budget generated: ' || :run_id;
END;
$$;
```

### 4.2 Engine 2: Fixture Planning (replaces V-0081)

```sql
CREATE OR REPLACE PROCEDURE gold.sp_generate_fixture_plan(run_id STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'numpy')
HANDLER = 'run'
AS
$$
def run(session, run_id):
    """
    Fixture planning algorithm (371 Excel columns -> Python logic):
    1. State model: base fixtures per state x MAJCAT
    2. National model: min/max caps
    3. ROI scoring: GP PSF ranking
    4. 4 rounds of incremental fix (APP x2, GM, MODEL)
    5. Floor balance enforcement
    6. Output: FINAL_FIX and ACC_DENSITY
    """
    import pandas as pd
    import numpy as np

    # Load inputs
    stores = session.table("silver.dim_store").to_pandas()
    majcats = session.table("silver.dim_majcat").to_pandas()
    sales = session.sql("""
        SELECT st_cd, maj_cat, SUM(sale_val) as sale_val, SUM(gm_val) as gm_val
        FROM bronze.fabric_sales_history
        WHERE sale_date >= DATEADD(MONTH, -3, CURRENT_DATE())
        GROUP BY 1, 2
    """).to_pandas()

    # Step 1: State model
    # Each state has base fixture per MAJCAT (from historical patterns)

    # Step 2: National model with min/max
    # NAT_MODEL = CLAMP(state_model, min_fix, max_fix)

    # Step 3: ROI scoring
    # GP_PSF = GM_Value / (Fix_Count * Area_Per_Fix)
    # Rank categories by GP_PSF within each store

    # Step 4: Incremental fix (4 rounds)
    # Round 1-2: APP additions (high GP PSF categories get more)
    # Round 3: GM additions
    # Round 4: Model corrections
    # Each round: bottom-up, floor balance check, cap enforcement

    # Step 5: Output
    results = pd.DataFrame(...)  # Final fixture plan
    session.write_pandas(results, "gold.fact_fixture_plan", auto_create_table=False)

    return f"Fixture plan generated: {run_id}"
$$;
```

### 4.3 Engine 3: Weekly Disaggregation (replaces manual split)

```sql
CREATE OR REPLACE PROCEDURE gold.sp_disaggregate_to_weekly(
    plan_month DATE, run_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Use season curves + historical weekly patterns to split monthly into weekly
    INSERT INTO gold.fact_trf_in_plan (
        run_id, run_date, st_cd, maj_cat, plan_week_id, plan_month,
        bgt_sale_q, bgt_disp_cl_q
    )
    SELECT
        :run_id, CURRENT_DATE(),
        b.st_cd, b.maj_cat, w.week_id, b.plan_month,
        -- Distribute monthly qty across weeks using historical pattern
        b.bgt_sale_qty * (
            COALESCE(hw.week_pct, 1.0 / COUNT(*) OVER (PARTITION BY b.st_cd, b.maj_cat))
        ) AS weekly_sale_qty,
        b.bgt_disp_qty * (
            COALESCE(hw.week_pct, 1.0 / COUNT(*) OVER (PARTITION BY b.st_cd, b.maj_cat))
        ) AS weekly_disp_qty
    FROM gold.fact_budget_store_majcat b
    JOIN silver.dim_week w ON w.month_date = b.plan_month
    LEFT JOIN (
        -- Historical weekly distribution pattern (LYSP)
        SELECT st_cd, maj_cat, fy_week,
            sale_qty / NULLIF(SUM(sale_qty) OVER (PARTITION BY st_cd, maj_cat, month_date), 0) AS week_pct
        FROM bronze.fabric_sales_history h
        JOIN silver.dim_week w ON h.sale_date BETWEEN w.wk_start_date AND w.wk_end_date
        WHERE h.sale_date BETWEEN DATEADD(YEAR, -1, :plan_month) AND DATEADD(YEAR, -1, LAST_DAY(:plan_month))
    ) hw ON b.st_cd = hw.st_cd AND b.maj_cat = hw.maj_cat AND w.fy_week = hw.fy_week
    WHERE b.plan_month = :plan_month;

    RETURN 'Weekly disaggregation complete';
END;
$$;
```

### 4.4 Engine 4: TRF-IN & Purchase Plan (replaces VER.0061 + ASP.NET SPs)

```sql
-- This replicates SP_GENERATE_TRF_IN_PLAN and SP_GENERATE_PURCHASE_PLAN
-- but runs in Snowflake with full monthly chaining

CREATE OR REPLACE PROCEDURE gold.sp_generate_plans(
    start_month DATE, months_forward INT, run_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- CHAIN: Month by month, closing -> next opening
    FOR m IN 0 TO :months_forward - 1 DO
        LET current_month := DATEADD(MONTH, m, :start_month);

        -- STORE TRF-IN
        MERGE INTO gold.fact_trf_in_plan t
        USING (
            SELECT
                :run_id AS run_id, :current_month AS plan_month,
                b.st_cd, b.maj_cat,
                -- Opening stock = previous month closing, or current SAP stock for M1
                CASE WHEN m = 0 THEN ss.stk_qty
                     ELSE prev.cl_stk_q END AS op_stk_q,
                b.bgt_disp_qty AS bgt_disp_cl_q,
                b.bgt_sale_qty AS bgt_sale_q,
                -- Cover sales = next month sale / 30 * cover days
                (next_b.bgt_sale_qty / 30 * s.cover_days) AS bgt_cover_sale_q,
                -- MBQ = display + cover
                (b.bgt_disp_qty + next_b.bgt_sale_qty / 30 * s.cover_days) AS bgt_st_cl_mbq,
                -- TRF-IN = MAX(MBQ + SALE - OP_STK, 0)
                GREATEST(0,
                    (b.bgt_disp_qty + next_b.bgt_sale_qty / 30 * s.cover_days)
                    + b.bgt_sale_qty
                    - CASE WHEN m = 0 THEN ss.stk_qty ELSE prev.cl_stk_q END
                ) AS trf_in_stk_q
            FROM gold.fact_budget_store_majcat b
            JOIN silver.dim_store s ON b.st_cd = s.st_cd
            LEFT JOIN bronze.sap_st_stock ss ON b.st_cd = ss.st_cd AND b.maj_cat = ss.maj_cat
            LEFT JOIN gold.fact_trf_in_plan prev
                ON b.st_cd = prev.st_cd AND b.maj_cat = prev.maj_cat
                AND prev.plan_month = DATEADD(MONTH, -1, :current_month)
            LEFT JOIN gold.fact_budget_store_majcat next_b
                ON b.st_cd = next_b.st_cd AND b.maj_cat = next_b.maj_cat
                AND next_b.plan_month = DATEADD(MONTH, 1, :current_month)
            WHERE b.plan_month = :current_month
        ) s ON t.st_cd = s.st_cd AND t.maj_cat = s.maj_cat AND t.plan_month = s.plan_month
        WHEN NOT MATCHED THEN INSERT (...) VALUES (...);

        -- Update closing stock
        UPDATE gold.fact_trf_in_plan
        SET cl_stk_q = net_op_stk_q + trf_in_stk_q - bgt_sale_q,
            excess_q = GREATEST(cl_stk_q - bgt_st_cl_mbq, 0),
            short_q = GREATEST(bgt_st_cl_mbq - cl_stk_q, 0)
        WHERE plan_month = :current_month AND run_id = :run_id;

        -- DC PURCHASE PLAN
        INSERT INTO gold.fact_purchase_plan (...)
        SELECT
            :run_id, :current_month, s.rdc_cd, t.maj_cat,
            -- DC opening = previous month closing or current SAP
            CASE WHEN m = 0 THEN dc.dc_stk_q ELSE prev_dc.dc_cl_stk_q END AS dc_op_stk_q,
            -- TRF-OUT = SUM of all store TRF-INs for this DC
            SUM(t.trf_in_stk_q) AS trf_out_total,
            -- DC MBQ = next month total sales / 2 (15-day cover)
            SUM(next_b.bgt_sale_qty) / 2 AS dc_cl_mbq,
            -- PUR = MAX(DC_MBQ + TRF_OUT - DC_OP, 0)
            GREATEST(0,
                SUM(next_b.bgt_sale_qty) / 2
                + SUM(t.trf_in_stk_q)
                - CASE WHEN m = 0 THEN dc.dc_stk_q ELSE prev_dc.dc_cl_stk_q END
            ) AS bgt_pur_q
        FROM gold.fact_trf_in_plan t
        JOIN silver.dim_store s ON t.st_cd = s.st_cd
        LEFT JOIN bronze.sap_dc_stock dc ON s.rdc_cd = dc.rdc_cd AND t.maj_cat = dc.maj_cat
        LEFT JOIN gold.fact_purchase_plan prev_dc ...
        LEFT JOIN gold.fact_budget_store_majcat next_b ...
        WHERE t.plan_month = :current_month AND t.run_id = :run_id
        GROUP BY s.rdc_cd, t.maj_cat;

    END FOR;

    RETURN 'Plans generated for ' || :months_forward || ' months';
END;
$$;
```

### 4.5 Engine 5: Analytics & Monitoring

```sql
-- Budget vs Actual view
CREATE VIEW gold.v_budget_vs_actual AS
SELECT
    b.st_cd, b.maj_cat, b.plan_month,
    b.bgt_sale_qty, b.bgt_sale_val,
    COALESCE(a.actual_sale_qty, 0) AS actual_sale_qty,
    COALESCE(a.actual_sale_val, 0) AS actual_sale_val,
    COALESCE(a.actual_sale_qty, 0) / NULLIF(b.bgt_sale_qty, 0) AS achievement_pct,
    b.bgt_disp_qty,
    COALESCE(s.stk_qty, 0) AS current_stock,
    COALESCE(s.stk_qty, 0) / NULLIF(b.bgt_disp_qty, 0) AS fill_rate
FROM gold.fact_budget_store_majcat b
LEFT JOIN (
    SELECT st_cd, maj_cat, DATE_TRUNC('MONTH', sale_date) AS month,
        SUM(sale_qty) AS actual_sale_qty, SUM(sale_val) AS actual_sale_val
    FROM bronze.fabric_sales_history
    GROUP BY 1, 2, 3
) a ON b.st_cd = a.st_cd AND b.maj_cat = a.maj_cat AND b.plan_month = a.month
LEFT JOIN bronze.sap_st_stock s ON b.st_cd = s.st_cd AND b.maj_cat = s.maj_cat;

-- Forecast accuracy tracking
CREATE VIEW gold.v_forecast_accuracy AS
SELECT
    plan_month, maj_cat,
    AVG(achievement_pct) AS avg_achievement,
    STDDEV(achievement_pct) AS achievement_stddev,
    COUNT(CASE WHEN achievement_pct BETWEEN 0.8 AND 1.2 THEN 1 END) / COUNT(*) AS within_20pct_rate
FROM gold.v_budget_vs_actual
GROUP BY 1, 2;
```

---

## 5. Orchestration & Scheduling

### 5.1 Snowflake Tasks (Monthly Planning Cycle)

```sql
-- Monthly budget generation (runs 1st of each month)
CREATE TASK gold.task_monthly_budget
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 6 1 * * Asia/Kolkata'  -- 6 AM on 1st
AS
    CALL gold.sp_generate_budget(
        ARRAY_CONSTRUCT(CURRENT_DATE(), DATEADD(MONTH, 1, CURRENT_DATE()), ...),
        UUID_STRING()
    );

-- Fixture plan refresh (runs weekly)
CREATE TASK gold.task_weekly_fixture
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 7 * * MON Asia/Kolkata'  -- 7 AM Monday
    AFTER gold.task_monthly_budget
AS
    CALL gold.sp_generate_fixture_plan(UUID_STRING());

-- Weekly disaggregation + TRF/PP plans
CREATE TASK gold.task_weekly_plans
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 8 * * MON Asia/Kolkata'
    AFTER gold.task_weekly_fixture
AS
    CALL gold.sp_generate_plans(
        DATE_TRUNC('MONTH', CURRENT_DATE()), 6, UUID_STRING()
    );

-- Sync to Supabase (after plans complete)
CREATE TASK gold.task_sync_supabase
    WAREHOUSE = 'COMPUTE_WH'
    AFTER gold.task_weekly_plans
AS
    -- Uses Snowflake External Function to call Supabase API
    CALL gold.sp_sync_to_supabase();
```

### 5.2 Daily Operations

```
06:00  SAP stock refresh (DC + Store) -> Bronze layer
06:30  Budget vs Actual refresh -> Analytics views
07:00  Anomaly detection (stock-outs, overstocks)
07:30  Telegram alerts for exceptions
08:00  ARS allocation run (triggered by n8n or Cloudflare cron)
21:00  Contribution refresh (existing Cloudflare cron)
```

### 5.3 n8n Workflow Integration

```
[n8n] Monthly Planning Workflow:
  1. Trigger: 1st of month at 5 AM
  2. Call Snowflake API: sp_generate_budget
  3. Wait for completion
  4. Send Telegram: "Budget generated for {month}. Review at dashboard."
  5. Wait for approval (Telegram callback)
  6. On approve: Call sp_generate_fixture_plan + sp_generate_plans
  7. Sync to Supabase
  8. Trigger ARS allocation
  9. Send Telegram: "Full planning cycle complete"
```

---

## 6. Integration Points

### 6.1 Snowflake -> Supabase Sync

```python
# Snowflake External Function or Azure Function
def sync_budget_to_supabase():
    """Sync fact_budget_store_majcat to co_budget_store_major_category"""
    # Read from Snowflake
    budgets = snowflake.execute("""
        SELECT st_cd, maj_cat, plan_month,
            bgt_sale_qty, bgt_sale_val, bgt_disp_qty, bgt_disp_val
        FROM gold.fact_budget_store_majcat
        WHERE is_approved = TRUE AND run_date = CURRENT_DATE()
    """)

    # Pivot months into columns and upsert to Supabase
    for row in budgets:
        month_col = f"sale_q_{row.plan_month.strftime('%b_%Y').lower()}"
        supabase.table('co_budget_store_major_category').upsert({
            'store_code': row.st_cd,
            'major_category': row.maj_cat,
            month_col: row.bgt_sale_qty,
            # ... other columns
        })
```

### 6.2 Snowflake -> ASP.NET Planning App Sync

```python
def sync_weekly_to_planning_app():
    """Sync weekly disaggregated data to QTY_SALE_QTY and QTY_DISP_QTY"""
    # Read from Snowflake (already disaggregated)
    weekly = snowflake.execute("""
        SELECT st_cd, maj_cat, plan_week_id, bgt_sale_q, bgt_disp_cl_q
        FROM gold.fact_trf_in_plan
        WHERE run_date = CURRENT_DATE()
        PIVOT (...)  -- Convert rows to WK-1..WK-48 columns
    """)
    # Bulk insert to SQL Server planning database
    sql_server.bulk_insert('QTY_SALE_QTY', weekly)
    sql_server.bulk_insert('QTY_DISP_QTY', weekly)
```

### 6.3 Snowflake -> ARS Trigger

```python
def trigger_ars_allocation():
    """After budget sync, trigger ARS for all MAJCATs"""
    majcats = supabase.table('co_budget_store_major_category') \
        .select('major_category').execute()

    unique_cats = list(set(r['major_category'] for r in majcats.data))

    # Batch ARS runs (5 at a time for parallel)
    for batch in chunks(unique_cats, 5):
        requests.post('https://ars-v2retail-api.azurewebsites.net/api/v1/allocation-engine/run', json={
            'majcats': batch,
            'rdc_code': 'DH24',
            'current_month': datetime.now().month
        })
```

---

## 7. Migration Path

### Phase 1: Snowflake Foundation (Weeks 1-4)
- Set up Snowflake account with Bronze/Silver/Gold layers
- Build data ingestion: SAP -> Bronze (stock, articles, POs)
- Build data ingestion: Fabric Gold -> Bronze (sales history)
- Build Silver dimension tables
- Validate: compare Snowflake data vs current Excel inputs

### Phase 2: Budget Engine (Weeks 5-8)
- Implement sp_generate_budget (V-0063 replacement)
- Validate against actual V-0063 output for 10 MAJCATs
- Build budget review dashboard (Power BI or Streamlit)
- Add Telegram approval workflow via n8n

### Phase 3: Fixture Engine (Weeks 9-11)
- Implement sp_generate_fixture_plan (V-0081 replacement)
- Validate against actual V-0081 output for 5 stores
- Connect fixture output to budget (density -> disp_qty)

### Phase 4: Integration (Weeks 12-14)
- Build Snowflake -> Supabase sync
- Build Snowflake -> ASP.NET Planning App sync
- Build weekly disaggregation engine
- End-to-end test: Snowflake budget -> Supabase -> ARS -> DOs

### Phase 5: Planning Engine Migration (Weeks 15-17)
- Move TRF-IN and Purchase Plan calculations to Snowflake
- Run parallel: ASP.NET app vs Snowflake for 2 weeks
- Validate <1% variance
- ASP.NET app becomes UI-only (reads from Snowflake)

### Phase 6: Full Automation (Weeks 18-20)
- Enable Snowflake Tasks for monthly/weekly automation
- Deprecate V-0063, V-0081, VER.0061 Excel files
- Training for planning team (Mehfuj)
- Monitoring, alerting, and runbook documentation

---

## 8. Cost Estimate

| Component | Monthly Cost | Notes |
|-----------|-------------|-------|
| Snowflake Compute | ~Rs 50K-80K | Standard WH, ~2hrs daily compute |
| Snowflake Storage | ~Rs 10K | ~500 GB (sales history + plans) |
| Azure App Service (ARS) | Already running | No change |
| Azure App Service (Planning) | Already running | No change |
| Supabase | Already running | No change |
| Azure Data Factory | ~Rs 15K | Daily SAP ingestion |
| **Total incremental** | **~Rs 75K-1L/month** | vs 20 machines x 14 hrs of Excel |

---

## 9. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Budget algorithm accuracy | 2-week parallel run comparing Snowflake vs V-0063 Excel output |
| Fixture model complexity (371 cols) | Implement in Snowpark Python (not SQL) for easier debugging |
| SAP integration reliability | Retry logic + Telegram alerts on failure |
| Planning team adoption | Keep Excel export capability during transition |
| Snowflake cost overrun | Set resource monitors, auto-suspend warehouse |
| Data freshness | Real-time stock via SAP RFC every 2 hours |

---

*Document generated: 2026-04-07*
*Architecture designed for V2 Retail Limited*
*Integrates: ARS (production), ASP.NET Planning (production), Snowflake (new), Azure (existing)*
