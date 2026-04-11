# V2 Retail - Central Planning System: Complete Technical Architecture

> **Purpose:** This document fully describes V2 Retail's Excel-based central planning system - the process that determines how much stock to buy (Purchase Plan), how much to transfer to stores (Transfer-In Plan), and how to balance inventory across DC and stores at the Major Category level. Every file, sheet, column, formula logic, and algorithm step is documented so anyone can build an automated replacement on Azure/Snowflake.

---

## TABLE OF CONTENTS

1. [What This System Does (Plain English)](#1-what-this-system-does)
2. [Glossary of Planning Terms](#2-glossary)
3. [System Files Overview](#3-system-files)
4. [Complete Schema: Sheet-by-Sheet](#4-complete-schema)
5. [The Planning Algorithm (Step-by-Step)](#5-planning-algorithm)
6. [Data Flow Diagram](#6-data-flow)
7. [Relationship with Allocation System](#7-relationship-with-allocation)
8. [Automation Architecture (Azure + Snowflake)](#8-automation-architecture)
9. [Database Schema for Replacement](#9-database-schema)
10. [Implementation Plan](#10-implementation-plan)

---

## 1. What This System Does

V2 Retail's Central Planning team answers three questions every month for each Major Category across 320+ stores and 2 DCs:

> 1. **How much stock should each store have on display?** (Store Transfer-In Plan)
> 2. **How much stock does the DC need to fulfill store demand?** (DC Stock Plan)
> 3. **How much do we need to purchase from vendors?** (Purchase Plan)

The system works by **rolling forward month-by-month**: starting with today's stock position (opening stock), subtracting projected sales, adding planned transfers/purchases, and calculating closing stock. If closing stock falls below MBQ (minimum required), it triggers a transfer or purchase.

**Key difference from the Allocation system:** Planning works at the **Major Category level** (e.g., "Men's Jeans" as a whole), while Allocation works at the **individual article/SKU level**. Planning happens first, then Allocation executes the plan.

**Operated by:** Mehfuj (Central Planning team), file path: `\\file\0-V2\04-DEPARTMENT\04-PLANNING\01-CENTRAL PLANNING\10-MY FOLDER\04-MEHFUJ\`

---

## 2. Glossary of Planning Terms

| Term | Full Form | Meaning |
|------|-----------|---------|
| **ST-TRF-IN** | Store Transfer-In | Quantity transferred from DC to store |
| **DC-TRF-OUT** | DC Transfer-Out | Quantity transferred from DC to stores (same as TRF-IN from store perspective) |
| **PUR** | Purchase | Quantity to be bought from vendors |
| **PO** | Purchase Order | SAP document to buy from vendors |
| **BGT-DISP CL-Q** | Budget Display Closing Quantity | Target display stock at end of period |
| **BGT-DISP CL-OPT** | Budget Display Closing Options | Target number of article-options on display |
| **BGT-COVER SALE-Q** | Budget Cover Sales Quantity | Stock needed to cover projected sales |
| **BGT-ST CL-MBQ** | Budget Store Closing MBQ | Minimum stock store should have at month end |
| **BGT-ST OP-MBQ** | Budget Store Opening MBQ | Minimum stock store should have at month start |
| **BGT-DC OP-STK-Q** | Budget DC Opening Stock Qty | DC stock at month start |
| **BGT-DC CL-STK-Q** | Budget DC Closing Stock Qty | DC stock at month end |
| **BGT-DC CL-MBQ** | Budget DC Closing MBQ | Minimum DC stock at month end |
| **BGT-DC MBQ-SALE** | Budget DC MBQ based on Sales | DC MBQ calculated from sales rate |
| **BGT-CO CL-STK-Q** | Budget Company Closing Stock | Total company stock (DC + all stores) |
| **BGT-PUR-Q** | Budget Purchase Quantity | Quantity to purchase from vendors |
| **BGT-TTL-CF OP-STK-Q** | Budget Total Carry-Forward Opening Stock | Opening stock carried from previous month |
| **BGT-TTL-CF CL-STK-Q** | Budget Total Carry-Forward Closing Stock | Closing stock to carry to next month |
| **NET-BGT CF-STK-Q** | Net Budget Carry-Forward Stock | Opening stock after deducting non-active stock |
| **NT ACT-Q** | Non-Active Quantity | Stock that can't be sold (damaged, blocked, etc.) |
| **TRF-IN STK-Q** | Transfer-In Stock Quantity | Stock to be transferred to store in the month |
| **TRF-IN OPT CNT** | Transfer-In Option Count | Number of article-options to transfer |
| **TRF-IN OPT MBQ** | Transfer-In Option MBQ | MBQ worth of options to transfer |
| **DC-MBQ** | DC Minimum Buy Quantity | Minimum stock DC needs for allocation |
| **BGT-NT ACT-Q** | Budget Non-Active Quantity | Budget for non-active (dead) stock |
| **ST-CL EXCESS-Q** | Store Closing Excess Quantity | Store overstocked vs MBQ |
| **ST-CL SHORT-Q** | Store Closing Short Quantity | Store understocked vs MBQ |
| **DC-STK EXCESS-Q** | DC Stock Excess Quantity | DC overstocked vs MBQ |
| **DC-STK SHORT-Q** | DC Stock Short Quantity | DC understocked vs MBQ |
| **CO-STK EXCESS-Q** | Company Stock Excess | Total company excess |
| **CO-STK SHORT-Q** | Company Stock Short | Total company shortage |
| **GRT** | Gratis | Free/promotional goods |
| **GRT-STK-Q** | Gratis Stock Quantity | Gratis stock at DC |
| **GRT CONS%** | Gratis Consumption % | Expected gratis usage rate |
| **GRT CONS-Q** | Gratis Consumption Qty | Expected gratis usage quantity |
| **SSNL** | Seasonal | Season-specific stock |
| **SSNL-32/37/42** | Seasonal Stock aged 32/37/42 weeks | Aging buckets for seasonal stock |
| **MSA** | Minimum Saleable Assortment | Minimum size assortment to sell |
| **MSA (6-7-8)** | MSA for sizes 6, 7, 8 | MSA for specific size range |
| **BIN CAP** | Bin Capacity | DC warehouse bin capacity for this category |
| **FRESH BIN REQ** | Fresh Bin Required | New bin space needed for fresh stock |
| **GRT BIN REQ** | Gratis Bin Required | Bin space needed for gratis stock |
| **C-ART STK** | Closed Article Stock | Stock of articles that have been closed/discontinued |
| **S-GRT / W-GRT / PW-GRT** | Summer/Winter/Pre-Winter Gratis | Seasonal gratis stock |
| **ACC DENSITY** | Account Density | Number of article-slots per store for this category |
| **AVG DENSITY** | Average Density | Average article-slots across stores |
| **SALES COVER** | Sales Cover Days | Days of sales the stock should cover |
| **INT+PRD** | In-Transit + Production Days | Lead time for stock replenishment |
| **DEL PEND-Q** | Delivery Pending Quantity | Pending deliveries not yet received |
| **NET-SSNL CL-STK-Q** | Net Seasonal Closing Stock | Seasonal stock at period end |
| **CM+1 / CM+2** | Current Month +1 / +2 | Sales cover for next 1 or 2 months |
| **RDC** | Regional Distribution Center | DH24 (FRKH=Faridabad), DW01 (KOL=Kolkata) |
| **FRKH** | Faridabad (Haryana) | Delhi DC location |
| **KOL** | Kolkata | East DC location |
| **HUB** | Hub warehouse | Intermediate warehouse between DC and stores |
| **UPC-ST-OP-DT** | Store Opening Date (UPC format) | When the store opened |

---

## 3. System Files Overview

| File | Size | Purpose |
|------|------|---------|
| **VER.0061 MAJ-CAT-ST-TRF-IN & PUR REPORT** | 7 MB | **Current template** - New RDC-wise format with 15-day MBQ |
| **VER.0049 MAJ-CAT-ST-TRF-IN & PUR REPORT** | 21 MB | **Older template** - RDC week-wise format |
| **SALE MONTH WISE.xlsx** | 141 MB | Monthly sales history by store x major category |
| **ST_MAJ_CAT_PLAN - Copy.xlsx** | 141 MB | Store x Major Category plan (copy of sales data) |

The VER.0061 is the **active production file**. VER.0049 is the previous version kept for reference.

---

## 4. Complete Schema: Sheet-by-Sheet (VER.0061 - Current Template)

### 4.1 Sheet: `ST-TRF-IN PLAN` (Store Transfer-In Plan)
**Rows:** 776 | **Cols:** 493
**Purpose:** For each Store x Major Category combination, calculates how much stock to transfer from DC to store each month. This is the **demand side** of the plan.

**Row layout:**
- Rows 0-2: Title and config
- Row 3: Sort/sequence numbers for monthly columns
- Row 4: Date references
- Row 5: Month start dates (Excel serial numbers: 46082=Mar-26, 46142=Apr-26, etc.)
- Row 6: Month labels (MAR-26, APR-26, MAY-26, etc.)
- Row 7: Column headers
- Row 8: Sub-headers (month labels for LISTING and SALES COVER)
- Row 9+: Data rows (one per store x major category)

#### SECTION A: Store & Category Identity (Cols 0-19)

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | ST-CD | Store code | HB05 |
| 1 | RDC_CD | Regional DC code | DH24 |
| 2 | RDC_NM | Regional DC name | FRKH |
| 3 | HUB_CD | Hub warehouse code | DB03 |
| 4 | HUB_NM | Hub warehouse name | PATNA |
| 5 | ST-NM | Store name (short) | PTN |
| 6 | SEG | Segment (APP/GM/FW) | APP |
| 7 | DIV | Division | MENS |
| 8 | SUB-DIV | Sub-division | MS-U |
| 9 | MAJ-CAT | Major category | M_TIGHTS_FS |
| 10 | UPC-ST-OP-DT | Store opening date | 41028 (=2012-05-01) |
| 11 | REF-CD | Reference code (same as ST-CD) | HB05 |
| 12 | ST STS | Store status | OLD / NEW |
| 13 | MAJ STS | Major category status | ACT / INACT |
| 14 | SSN | Season | PW / S / A / W |
| 16 | ST-COMB | Store-MAJCAT composite key | HB05M_TIGHTS_FS |
| 17 | NAT-COMB | National (company) MAJCAT key | M_TIGHTS_FS |
| 18 | LIST ROW | Listing row reference | |

#### SECTION B: Monthly Listing Flags (Cols 20-32)

| Col | Name | Months | Description |
|-----|------|--------|-------------|
| 20-31 | MONTH-LISTING | MAR through FEB | 1=Store is listed for this MAJCAT in this month, 0=Not listed |

This determines whether a store receives stock for a category in each month. Based on season and store eligibility.

#### SECTION C: Sales Cover Days (Cols 33-46)

| Col | Name | Months | Description |
|-----|------|--------|-------------|
| 33-46 | SALES COVER | APR through MAY (next year) | Number of days of sales the store's stock should cover |

Typically 8 days for most stores. Varies by store priority and category.

#### SECTION D: Current Stock Position (Cols 48-60)

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 48 | ST-OP STK-Q | Store opening stock quantity | 319.46 |
| 49 | C-ART STK | Closed article stock | 0 |
| 50 | SUMMER GRT STK-Q | Summer gratis stock at store | 17.38 |
| 51 | HUB INT PRD | Hub in-transit/production stock | 0 |
| 52 | ST-EXCESS (V02) | Store excess (V02 location) | 0 |
| 53 | WINTER GRT STK-Q | Winter gratis stock at store | 26.51 |
| 54 | ST-OP-V2S STK-Q | V2S opening stock | 0 |
| 55 | ST0002 COVER DAYS | Store location 2 cover days | |
| 56 | AVG DENSITY | Average display density (articles/category) | 110 |
| 57 | ACC DENSITY | Actual account density | 22 |
| 58 | INT+PRD | In-transit + production lead time days | |
| 59 | ALLOC COVER DAYS | Allocation cover days | |

#### SECTION E: Monthly Planning Block (REPEATS for each month)

Starting with MAR-26 (col 61), then APR-26 (col 85), MAY-26 (col 108), JUN-26 (col 131), and so on through APR-27. Each block is 22 data columns + 1 empty separator column = 23 col spacing.

**IMPORTANT: The MAR-26 (first) block is different from subsequent blocks.** Col 61 = `MTD B-SALE-Q` (month-to-date budget sales - unique to first block only). The standard `BGT-DISP CL-Q` starts at col 62 for MAR, col 85 for APR, col 108 for MAY, etc. All subsequent blocks start directly with BGT-DISP CL-Q.

Block start positions: MAR=62, APR=85, MAY=108, JUN=131, JUL=154, AUG=177, SEP=200, OCT=223, NOV=246, DEC=269, JAN=292, FEB=315, MAR=338, APR=361. Empty separator cols at 84, 107, 130, 153, etc.

**The repeating block structure (using APR-26 as example, cols 85-106):**

| Col | Name | Description | Formula Logic |
|-----|------|-------------|--------------|
| 85 | BGT-DISP CL-Q | Budget display closing quantity | From `co_budget_store_major_category.disp_q_{month}` |
| 86 | BGT-DISP CL-OPT | Budget display closing options | = DISP-Q / density factor |
| 87 | {CM+1} BGT SALE-DAYS | Current+1 month budget sale days | From SALES COVER lookup |
| 88 | {CM+2} BGT SALE-DAYS | Current+2 month budget sale days | From SALES COVER lookup |
| 89 | BGT-COVER SALE-Q | Budget cover sales quantity | **= BGT-SALE-Q(CM+1) / 30 * SALE-DAYS(CM+1) + BGT-SALE-Q(CM+2) / 30 * SALE-DAYS(CM+2)** |
| 90 | BGT-ST CL-MBQ | Budget store closing MBQ | **= BGT-DISP CL-Q + BGT-COVER SALE-Q** |
| 91 | BGT-DISP CL-OPT MBQ | MBQ in option terms | = MBQ / density |
| 92 | BGT-TTL-CF OP-STK-Q | Total carry-forward opening stock | = Previous month's closing stock |
| 93 | NT ACT-Q | Non-active (dead) stock qty | Deducted from usable stock |
| 94 | NET-BGT CF-STK-Q | Net budget carry-forward stock | **= OP-STK-Q - NT-ACT-Q** |
| 95 | {CM} BGT SALE-Q | Current month budget sales qty | From `co_budget_store_major_category.sale_q_{month}` |
| 96 | {CM+1} BGT SALE-Q | Next month budget sales qty | From budget table |
| 97 | {CM+2} BGT SALE-Q | Month+2 budget sales qty | From budget table |
| 98 | TRF-IN STK-Q | **Transfer-in stock quantity** | **KEY OUTPUT: = MAX(MBQ + SALE-Q - OP-STK, 0)** |
| 99 | TRF-IN OPT CNT | Transfer-in option count | = TRF-IN-Q / density |
| 100 | TRF-IN OPT MBQ | Transfer-in MBQ worth | |
| 101 | DC-MBQ | DC MBQ for this store-category | |
| 102 | BGT-TTL-CF CL-STK-Q | Total carry-forward closing stock | **= OP-STK + TRF-IN - SALE-Q** |
| 103 | BGT-NT ACT-Q | Budget non-active quantity | |
| 104 | NET-ST CL-STK-Q | Net store closing stock | **= CL-STK - NT-ACT** |
| 105 | ST-CL EXCESS-Q | Store closing excess | **= MAX(CL-STK - MBQ, 0)** |
| 106 | ST-CL SHORT-Q | Store closing short | **= MAX(MBQ - CL-STK, 0)** |

**The core formula chain (per store per month):**
```
MBQ = Display_Budget + Cover_Sales
     = disp_q + (sale_q_next_month/30 * cover_days_1 + sale_q_month_after/30 * cover_days_2)

TRF-IN = MAX(MBQ + Current_Month_Sales - Opening_Stock, 0)

Closing_Stock = Opening_Stock + TRF-IN - Current_Month_Sales
Next_Month_Opening = This_Month_Closing
```

#### SECTION F: Trailing Columns (Cols 385-493)

Columns 385-398 contain variance/delta calculations.
Columns 417-478 contain column sequence numbers and additional sort keys.

---

### 4.2 Sheet: `DC-TRF OUT PLAN (PUR)-WO-PO` (DC Transfer-Out / Purchase Plan WITHOUT Pending POs)
**Rows:** 785 | **Cols:** 625
**Purpose:** For each DC x Major Category, calculates how much stock to transfer to stores and how much to purchase from vendors. This is the **supply side** of the plan. "WO-PO" means **without** considering pending Purchase Orders.

#### SECTION A: DC & Category Identity (Cols 0-12)

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | DC | DC code | FRKH |
| 1 | SEG | Segment | APP |
| 2 | DIV | Division | MENS |
| 3 | SUB-DIV | Sub-division | MS-U |
| 4 | MAJ-CAT | Major category | M_TIGHTS_FS |
| 5 | MAJ-STAT | Major category status | |
| 6 | RNG SEG | Range segment | |
| 7 | RNG | Range | |
| 8 | MAJ STAT | Major category active status | ACT |
| 9 | SSN | Season | PW |
| 11 | COMB | MAJCAT combo key | M_TIGHTS_FS |
| 12 | (DC-COMB) | DC + MAJCAT key | FRKHM_TIGHTS_FS |

#### SECTION B: DC Current Stock (Cols 14-40)

**Per-DC stock breakdown (repeated for each DC: DH24/FRKH cols 17-24, DW01/KOL cols 26-33):**

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 14 | DC-STK-Q | Total DC stock (both DCs combined) | 11,697.70 |
| 15 | GRT-STK-Q | Total gratis stock | 5,579.16 |
| 17 | MSA STK-Q | MSA stock at DC (FRKH) | 6,215.11 |
| 18 | GRT STK-Q | Gratis stock at DC (FRKH) | 3,235.60 |
| 19 | ST TO DC STK-Q | Store-returned stock at DC | 318.59 |
| 20 | PRO-GRT STK-Q | Promotional gratis stock | 841.58 |
| 21 | SSNL-32 STK-Q | Seasonal stock aged 32 weeks | 2,058.33 |
| 22 | SSNL-37 STK-Q | Seasonal stock aged 37 weeks | 0 |
| 23 | MSA (6-7-8) | MSA for sizes 6-7-8 | 17.10 |
| 24 | SSNL-42 STK-Q | Seasonal stock aged 42 weeks | 0 |
| 26-33 | (same for DW01/KOL) | Same stock breakdown for Kolkata DC | |
| 35 | C-ART STK | Closed article stock | 0 |
| 36 | S-GRT STK-Q | Summer gratis stock | 17.38 |
| 37 | W-GRT STK-Q | Winter gratis stock | 26.51 |
| 38 | PW-GRT STK-Q | Pre-winter gratis stock | 0 |
| 39 | BIN CAP DC TEAM | Bin capacity (DC team input) | |
| 40 | BIN CAP | Calculated bin capacity | |

#### SECTION C: Sales Cover (Cols 42-69)

| Col | Name | Description |
|-----|------|-------------|
| 42 | MAR (CM+1) | March sales cover - current month + 1 month |
| 44 | APR (CM+1) | April sales cover |
| ... | ... | (every 2 cols: month + CM+1 and CM+2) |
| 68 | APR (CM+1) | April next year |

Each month has 2 columns: CM+1 and CM+2 sales cover days.

#### SECTION D: Monthly Purchase Plan Block (REPEATS 13 times: APR-26 through APR-27)

Starting at col 71 (APR-26), then col 111 (MAY-26), col 149 (JUN-26), etc.

**IMPORTANT: The APR-26 (first) block is 40 columns wide (71-110), while subsequent blocks are 38 columns.** The first block has an extra previous-month sales column (MAR BGT SALE-Q at col 72) and 4 BGT SALE-Q columns (MAR/APR/MAY/JUN) vs 3 in subsequent blocks. Also, the first block's GRT column is `BGT GRT OP-STK-Q` (col 81) while subsequent blocks use `BGT SSNL OP-STK-Q` (seasonal stock instead of gratis).

The TRF-OUT column (col 88 in APR) is actually a **4-column compound**: col 88=TTL-TRF OUT-Q (total), col 89=MAR (previous month), col 90=APR (current), col 91=MAY (next). Subsequent blocks have 3 TRF-OUT sub-columns (no previous month).

**The repeating purchase plan block (using APR-26 as example, cols 71-110):**

| Col | Name | Description | Formula Logic |
|-----|------|-------------|--------------|
| 71 | BGT-DISP CL-Q | Budget display closing quantity (all stores combined) | = SUM of all store BGT-DISP from ST-TRF-IN |
| 72 | MAR BGT SALE-Q | Previous month budget sales | From budget table |
| 73 | APR BGT SALE-Q | Current month budget sales | From budget table |
| 74 | MAY BGT SALE-Q | Next month budget sales | From budget table |
| 75 | JUN BGT SALE-Q | Month+2 budget sales | From budget table |
| 76 | BGT-ST OP-MBQ | Budget store opening MBQ (all stores) | Sum of all store MBQs |
| 77 | NET ST-OP STK-Q | Net store opening stock | Sum of all store stocks |
| 78 | BGT-DC OP-STK-Q | Budget DC opening stock | DC stock at start of month |
| 79 | NT ACT-Q | Non-active quantity | |
| 80 | BGT CF-STK-Q | Budget carry-forward stock | = DC-OP-STK - NT-ACT |
| 81 | BGT GRT OP-STK-Q | Budget gratis opening stock | Gratis stock available |
| 82 | (OP-STK) | Opening gratis stock | |
| 83 | (NT-ACT-STK) | Non-active gratis stock | |
| 84 | GRT CONS% | Gratis consumption percentage | Expected usage rate |
| 85 | GRT CONS-Q | Gratis consumption quantity | = GRT-STK * CONS% |
| 86 | DEL PEND-Q | Delivery pending quantity | Pending deliveries |
| 87 | NET-BGT CF-STK-Q | Net budget carry-forward stock | **= BGT-CF-STK + GRT-CONS - DEL-PEND** |
| 88 | TRF-OUT (TTL) | **Total transfer-out quantity** | **= SUM of all store TRF-IN for this DC-MAJCAT** |
| 89 | (month-1 TRF-OUT) | Previous month transfer-out | |
| 90 | (current month TRF-OUT) | Current month transfer-out | |
| 91 | (next month TRF-OUT) | Next month transfer-out | |
| 92 | BGT ST-CL MBQ | Budget store closing MBQ | Sum of all store MBQs at month end |
| 93 | NET BGT ST-CL STK-Q | Net budget store closing stock | Sum of all store closing stocks |
| 94 | NET-SSNL CL-STK-Q | Net seasonal closing stock | Seasonal stock remaining |
| 95 | BGT-DC MBQ-SALE | DC MBQ based on sales | **= half-month sales rate * cover days** |
| 96 | BGT-DC CL-MBQ | Budget DC closing MBQ | **= MAX(DC-MBQ-SALE, safety stock)** |
| 97 | BGT-DC CL-STK-Q | Budget DC closing stock | **= DC-OP-STK + PUR-Q - TRF-OUT** |
| 98 | **BGT PUR-Q (INITIAL)** | **Budget purchase quantity** | **KEY OUTPUT: = MAX(DC-MBQ + TRF-OUT - DC-OP-STK, 0)** |
| 99 | POS PO TO BE RAISED | Positive PO to raise | = MAX(PUR-Q, 0) |
| 100 | NEG PO TO BE RAISED | Negative PO (cancel) | = MIN(PUR-Q, 0) |
| 101 | BGT-CO CL-STK-Q | Budget company closing stock | **= ST-CL-STK + DC-CL-STK** |
| 102 | DC-STK EXCESS-Q | DC stock excess | = MAX(DC-CL-STK - DC-MBQ, 0) |
| 103 | DC-STK SHORT-Q | DC stock short | = MAX(DC-MBQ - DC-CL-STK, 0) |
| 104 | ST-STK EXCESS-Q | Store stock excess | Sum of all store excesses |
| 105 | ST-STK SHORT-Q | Store stock short | Sum of all store shortages |
| 106 | CO-STK EXCESS-Q | Company stock excess | = DC-EXCESS + ST-EXCESS |
| 107 | CO-STK SHORT-Q | Company stock short | = DC-SHORT + ST-SHORT |
| 108 | FRESH BIN REQ | Fresh stock bin requirement | = PUR-Q / carton_density |
| 109 | GRT BIN REQ | Gratis bin requirement | = GRT-STK / carton_density |

**The core DC-level formula chain:**
```
DC_Opening = Previous_Month_DC_Closing
TRF_OUT = SUM(all store TRF-IN for this DC-MAJCAT for current + next months)
DC_MBQ = BGT_Sales_Next_Month / 30 * Cover_Days
PUR_Q = MAX(DC_MBQ + TRF_OUT - DC_Opening - GRT_Available, 0)
DC_Closing = DC_Opening + PUR_Q - TRF_OUT
Company_Closing = DC_Closing + SUM(all store closings)
```

---

### 4.3 Sheet: `DC-TRF OUT PLAN (PUR)-W-PO` (WITH Pending POs)
**Rows:** 785 | **Cols:** 625
**Purpose:** Same as WO-PO but **includes pending Purchase Orders** already raised. This shows the net additional POs needed.

Same structure as WO-PO. The difference is:
- PUR-Q here = PUR-Q(WO-PO) - existing PO quantities
- Shows actual additional POs to raise vs the gross requirement

---

### 4.4 Sheet: `CO-STK-PLAN (W-O PO)` (Company Stock Plan)
**Rows:** 741 | **Cols:** 329
**Purpose:** Older-format company stock plan. Monthly purchase plan blocks from OCT-24 through JUN-25.

Similar structure to DC-TRF OUT PLAN but with additional columns:

| Col | Name | Description |
|-----|------|-------------|
| 8 | MVGR-1 | Merchandise group level 1 |
| 16 | NT-ACT GRT-STK-Q | Non-active gratis stock |
| 17 | DH25 STK-Q | DH25 DC stock |
| 26 | IN-ACT MSA-Q | Inactive MSA quantity |
| 27 | IN-ACT ST STK-Q | Inactive store stock |
| 28 | XXXX STK-Q | Misc stock |
| 29 | V2S CONT% | V2S contribution % |
| 30 | ST-STK-V2S | V2S store stock |
| 31 | V2S MSA | V2S MSA |

Monthly blocks (cols 33+) include PO-Q and PO-V (value) columns in addition to the standard purchase plan fields.

---

### 4.5 Sheet: `STORE COVER DY` (Store Cover Days Master)
**Rows:** 664 | **Cols:** 45
**Purpose:** Master table defining sales cover days for each store, per month, per DC.

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | ST_CD | Store code | HB05 |
| 1 | ST_NM | Store name | PTN |
| 2 | STATE | State | BIHAR |
| 3 | STATUS | Store status | OLD |
| 4 | OP DATE | Opening date | 41028 |
| 5 | RDC_CD | RDC code | DW01 |
| 6 | RDC_NM | RDC name | KOL |
| 7 | HUB_CD | Hub code | DB03 |
| 8 | HUB_NM | Hub name | PATNA |
| 9 | DC TO HUB INTRA (FRKH) | Transit days DC to Hub (FRKH) | 2 |
| 10 | HUB TO ST INTRA (FRKH) | Transit days Hub to Store (FRKH) | 1 |
| 11 | DC TO HUB INTRA (KOL) | Transit days DC to Hub (KOL) | 2 |
| 12 | HUB TO ST INTRA (KOL) | Transit days Hub to Store (KOL) | 1 |
| 14 | SALE (MBQ cover) | Base sales cover days | 2 |
| 15 | INT (MBQ cover) | In-transit cover days | 3 |
| 16 | PRD (MBQ cover) | Production cover days | 3 |
| 17-30 | APR through MAY (monthly) | Monthly cover days | 8 |
| 32-44 | APR through APR (old) | Previous cover days | 8 |

**Cover days formula:**
```
Total_Cover = SALE + INT + PRD + Monthly_Cover
MBQ = Daily_Sales * Total_Cover
```

---

### 4.6 Sheet: `PEND-PO-Q` (Pending Purchase Orders)
**Rows:** 766 | **Cols:** 20
**Purpose:** Lists all pending (not yet received) Purchase Orders by DC x Major Category x Month.

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | RDC | Regional DC | FRKH |
| 1 | SEG | Segment | APP |
| 2 | DIV | Division | KIDS |
| 3 | SUB_DIV | Sub-division | KB-U |
| 4 | MAJ | Major category | JB_K_SHIRT_HS |
| 5 | SSN | Season | S |
| 6 | COMB | Composite key | JB_K_SHIRT_HSFRKH |
| 7-19 | Monthly columns | Pending PO qty per month | 15.237 (Apr), 19.799 (May), etc. |

Monthly columns use Excel serial dates: 46142=Apr-26, 46173=May-26, 46203=Jun-26, etc.

---

### 4.7 Sheet: `MAJ CAT BIN CAPACITY` (DC Bin Capacity)
**Rows:** 667 | **Cols:** 9
**Purpose:** Defines how many pieces fit in one carton/bin for each major category. Used to calculate bin requirements.

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | S.NO | Serial number | 1 |
| 1 | DIV | Division | |
| 2 | SEASON | Season | |
| 3 | SUB-DIV | Sub-division | KB |
| 4 | MAJ-CAT | Major category | JB_BABA_SUIT_SET_FS |
| 5 | CARTON SIZE (L) | Length cm | 26 |
| 6 | CARTON SIZE (B) | Breadth cm | 18 |
| 7 | CARTON SIZE (H) | Height cm | 12 |
| 8 | PER CARTON QTY | Pieces per carton | 72 |

**Bin requirement formula:**
```
Fresh_Bins = PUR_Q / Per_Carton_Qty
GRT_Bins = GRT_STK / Per_Carton_Qty
```

---

## 5. The Planning Algorithm (Step-by-Step)

### 5.1 High-Level Flow

```
START
  |
  v
[1] LOAD INPUTS
  |  - Store master (STORE COVER DY: 664 stores with cover days, hub routes)
  |  - Current stock (DC-STK, ST-STK from SAP)
  |  - Budget data (sale_q, disp_q per store x majcat x month from Supabase)
  |  - Pending POs (PEND-PO-Q)
  |  - Bin capacity (MAJ CAT BIN CAPACITY)
  |
  v
[2] FOR EACH MONTH (rolling 13 months: APR-26 to APR-27):
  |
  |  [2a] STORE LEVEL (ST-TRF-IN PLAN):
  |    For each store x majcat:
  |      1. Get opening stock (= previous month closing, or current stock for M1)
  |      2. Get budget display qty (disp_q) and sales qty (sale_q)
  |      3. Calculate cover sales = next_month_sales/30 * cover_days
  |      4. Calculate MBQ = display_qty + cover_sales
  |      5. Calculate TRF-IN = MAX(MBQ + current_month_sales - opening_stock, 0)
  |      6. Calculate closing stock = opening + TRF-IN - sales
  |      7. Calculate excess/short = closing vs MBQ
  |
  |  [2b] DC LEVEL (DC-TRF OUT PLAN):
  |    For each DC x majcat:
  |      1. Get DC opening stock (= previous month closing, or current DC stock for M1)
  |      2. Sum all store TRF-INs = TRF-OUT for this DC-majcat
  |      3. Calculate gratis consumption
  |      4. Calculate DC MBQ = next_month_sales_rate * cover_days
  |      5. Calculate PUR-Q = MAX(DC-MBQ + TRF-OUT - DC-OP-STK - GRT, 0)
  |      6. Calculate DC closing = DC-OP + PUR - TRF-OUT
  |      7. Calculate bin requirements = PUR-Q / carton_capacity
  |      8. Calculate company closing = DC-closing + all-store-closing
  |      9. Calculate excess/short at DC, store, and company level
  |
  v
[3] OUTPUT: Monthly purchase plan, transfer plan, stock projections
  |
  v
END
```

### 5.2 The Store Transfer-In Calculation (Detailed)

```python
def calculate_store_trf_in(store, majcat, month):
    # INPUTS
    op_stk = get_opening_stock(store, majcat, month)  # From SAP or previous month
    disp_q = get_budget_display(store, majcat, month)  # From co_budget_store_major_category
    sale_q_cm = get_budget_sales(store, majcat, month)  # Current month sales
    sale_q_cm1 = get_budget_sales(store, majcat, month+1)  # Next month sales
    sale_q_cm2 = get_budget_sales(store, majcat, month+2)  # Month+2 sales
    cover_days = get_cover_days(store, month)  # From STORE COVER DY (typically 8)
    nt_act = get_non_active_stock(store, majcat)  # Blocked/damaged stock
    
    # CALCULATIONS
    net_op_stk = op_stk - nt_act
    
    # Cover sales = next month's daily sales * cover days
    # NOTE: In practice, cover_days_2 is always 0 (15-day MBQ = half-month cover only)
    # The formula structurally supports 2-month cover but currently only uses 1 month
    cover_sale_q = (sale_q_cm1 / 30 * cover_days) + (sale_q_cm2 / 30 * cover_days_2)  # cover_days_2 = 0
    
    # MBQ = what store MUST have = display stock + safety buffer
    mbq = disp_q + cover_sale_q
    
    # Transfer-in = what we need to send
    trf_in = max(mbq + sale_q_cm - net_op_stk, 0)
    
    # Closing stock = what remains
    cl_stk = net_op_stk + trf_in - sale_q_cm
    
    # Excess/Short
    excess = max(cl_stk - mbq, 0)
    short = max(mbq - cl_stk, 0)
    
    return {
        'trf_in': trf_in,
        'cl_stk': cl_stk,
        'mbq': mbq,
        'excess': excess,
        'short': short
    }
```

### 5.3 The DC Purchase Calculation (Detailed)

```python
def calculate_dc_purchase(dc, majcat, month):
    # INPUTS
    dc_op_stk = get_dc_opening_stock(dc, majcat, month)
    grt_stk = get_gratis_stock(dc, majcat)
    grt_cons_pct = get_gratis_consumption_pct(dc, majcat, month)
    pend_po = get_pending_po(dc, majcat, month)
    nt_act = get_dc_non_active(dc, majcat)
    bin_capacity = get_bin_capacity(majcat)
    
    # Sum all store transfer-ins from this DC
    trf_out = sum(
        get_store_trf_in(store, majcat, month)
        for store in get_stores_for_dc(dc)
    )
    # Include next month's transfers too (for look-ahead)
    trf_out_next = sum(
        get_store_trf_in(store, majcat, month+1)
        for store in get_stores_for_dc(dc)
    )
    
    # Net DC opening stock
    net_dc_op = dc_op_stk - nt_act
    grt_consumed = grt_stk * grt_cons_pct / 100
    net_dc_cf = net_dc_op + grt_consumed - pend_po
    
    # All-store MBQ (sum)
    st_cl_mbq = sum(
        get_store_mbq(store, majcat, month)
        for store in get_stores_for_dc(dc)
    )
    
    # DC MBQ = half-month worth of sales
    sale_q_next = get_total_sales(dc, majcat, month+1)
    dc_mbq_sale = sale_q_next / 2  # ~15 day cover
    dc_cl_mbq = max(dc_mbq_sale, dc_op_stk * 0.1)  # Floor at 10% of opening
    
    # PURCHASE QUANTITY
    pur_q = max(dc_cl_mbq + trf_out + trf_out_next - net_dc_cf, 0)
    
    # DC Closing
    dc_cl_stk = net_dc_cf + pur_q - trf_out
    
    # Company closing
    st_cl_stk = sum(get_store_closing(store, majcat, month) for store in get_stores_for_dc(dc))
    co_cl_stk = dc_cl_stk + st_cl_stk
    
    # Bin requirements
    fresh_bins = pur_q / bin_capacity if bin_capacity > 0 else 0
    grt_bins = grt_stk / bin_capacity if bin_capacity > 0 else 0
    
    return {
        'pur_q': pur_q,
        'trf_out': trf_out,
        'dc_cl_stk': dc_cl_stk,
        'co_cl_stk': co_cl_stk,
        'dc_excess': max(dc_cl_stk - dc_cl_mbq, 0),
        'dc_short': max(dc_cl_mbq - dc_cl_stk, 0),
        'fresh_bins': fresh_bins,
        'grt_bins': grt_bins,
        'pos_po': max(pur_q, 0),
        'neg_po': min(pur_q, 0)
    }
```

### 5.4 Rolling Forward Logic

```
Month N Closing Stock --> Month N+1 Opening Stock

For stores:
  CL-STK(APR) = OP-STK(APR) + TRF-IN(APR) - SALE(APR)
  OP-STK(MAY) = CL-STK(APR)
  CL-STK(MAY) = OP-STK(MAY) + TRF-IN(MAY) - SALE(MAY)
  ... and so on for 13 months

For DC:
  DC-CL(APR) = DC-OP(APR) + PUR(APR) - TRF-OUT(APR)
  DC-OP(MAY) = DC-CL(APR)
  ... and so on

This creates a chain where any change in Month 1 ripples through all subsequent months.
```

---

## 6. Data Flow Diagram

```
+-------------------+    +-------------------+    +-------------------+
|    SAP HANA       |    |    Supabase       |    |    Manual Input   |
|    (Stock Data)   |    |    (V2SRM)        |    |    (Cover Days,   |
|                   |    |                   |    |     Bin Capacity) |
|  DC Stock (14 DCs)|    | co_budget_store_  |    |                   |
|  Store Stock      |    | major_category    |    | STORE COVER DY    |
|  Pending POs      |    | (387K rows)       |    | MAJ CAT BIN CAP   |
+--------+----------+    +--------+----------+    +--------+----------+
         |                         |                        |
         +----------+--------------+------------------------+
                    |
                    v
    +---------------------------------------------+
    |         PLANNING ENGINE                      |
    |                                              |
    |  Step 1: Load all inputs                     |
    |  Step 2: For each of 13 months:              |
    |    - Store TRF-IN calculation (776 rows)     |
    |    - DC PUR calculation (785 rows)           |
    |    - Rolling stock forward                   |
    |  Step 3: Calculate excess/short/bins         |
    +---------------------------------------------+
                    |
         +----------+----------+
         |                     |
         v                     v
+------------------+  +------------------+
| ST-TRF-IN PLAN   |  | DC-TRF OUT PLAN  |
| (776 x 493)      |  | (785 x 625)      |
|                   |  |                   |
| Per store x MC:   |  | Per DC x MC:     |
| - Monthly TRF-IN  |  | - Monthly PUR-Q  |
| - MBQ             |  | - TRF-OUT        |
| - Closing stock   |  | - DC closing     |
| - Excess/Short    |  | - Bin needs      |
+------------------+  +------------------+
         |                     |
         v                     v
+------------------+  +------------------+
| Allocation System |  | SAP Purchase     |
| (Article-level    |  | Orders           |
|  allocation)      |  | (Vendor POs)     |
+------------------+  +------------------+
```

---

## 7. Relationship with Allocation System

```
PLANNING (This Document)          ALLOCATION (Previous Document)
========================          ============================
Level: Major Category             Level: Article / SKU
Grain: Store x MAJCAT x Month    Grain: Store x Article x Option x Month
Output: TRF-IN quantities         Output: Specific articles + sizes to send
        PUR quantities                    Delivery orders to SAP
Timing: Monthly planning          Timing: Daily execution

Flow:
  Planning decides "Send 237 pcs of M_JEANS to store HB05 in April"
                              |
                              v
  Allocation decides "Send 15 pcs of Article 1130140482 (Size 30, BLK) 
                      + 12 pcs of Article 1130140483 (Size 32, NVY) 
                      + ... to store HB05"
```

The `co_budget_store_major_category` table in Supabase is the bridge:
- Planning **reads** `sale_q` and `disp_q` from it
- Planning **produces** TRF-IN and PUR-Q (which feed into allocation as BGT-DISP-Q)
- Allocation **reads** BGT-DISP-Q from `BASE-DATA.xlsb` (which should match planning output)

---

## 8. Automation Architecture (Azure + Snowflake)

### 8.1 Target Architecture

```
+================================================================+
|                   PLANNING ENGINE v2.0                           |
+================================================================+
|                                                                  |
|  +------------------+  +--------------------+  +---------------+ |
|  | DATA LAYER       |  | COMPUTE LAYER      |  | OUTPUT LAYER  | |
|  | (Snowflake)      |  | (Azure Functions)  |  |               | |
|  |                  |  |                    |  | Power BI Dash | |
|  | Budget Tables    |->| Store TRF-IN Calc  |->| SAP PO API    | |
|  | Stock Snapshots  |->| DC Purchase Calc   |->| Allocation    | |
|  | Cover Days       |->| Rolling Forward    |->| Telegram Alert| |
|  | Bin Capacity     |->| Excess/Short Calc  |->| Excel Export  | |
|  +------------------+  +--------------------+  +---------------+ |
|                                                                  |
+================================================================+
```

### 8.2 Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| **Data Warehouse** | Snowflake | Handles rolling 13-month calculations, time-travel for audit |
| **Hot Data** | Supabase (PostgreSQL) | Already has budget data (387K rows), real-time API |
| **Compute** | Azure Functions (Python) | Serverless, integrates with existing Azure Fabric |
| **Orchestration** | n8n / Azure Data Factory | Already in V2 stack |
| **SAP Integration** | Python + pyrfc | Stock reads, PO creation |
| **Dashboard** | Power BI (Azure Fabric) | Already in V2 stack |
| **Alerts** | Telegram Bot | Existing V2 channel |

### 8.3 Snowflake Schema

```sql
-- Planning run output: Store Transfer-In
CREATE TABLE planning_store_trf_in (
    run_id UUID,
    run_date DATE,
    plan_month DATE,              -- The month being planned
    st_cd VARCHAR(10),
    rdc_cd VARCHAR(10),
    hub_cd VARCHAR(10),
    seg VARCHAR(5),
    div VARCHAR(10),
    sub_div VARCHAR(10),
    maj_cat VARCHAR(50),
    ssn CHAR(5),
    st_status VARCHAR(5),
    maj_status VARCHAR(5),
    listing_flag BOOLEAN,
    
    -- Opening position
    op_stk_q DECIMAL(12,2),
    grt_stk_q DECIMAL(12,2),
    nt_act_q DECIMAL(12,2),
    net_op_stk_q DECIMAL(12,2),
    
    -- Budget inputs
    bgt_disp_cl_q DECIMAL(12,2),
    bgt_disp_cl_opt DECIMAL(12,2),
    bgt_sale_q_cm DECIMAL(12,2),
    bgt_sale_q_cm1 DECIMAL(12,2),
    bgt_sale_q_cm2 DECIMAL(12,2),
    sale_days_cm1 INT,
    sale_days_cm2 INT,
    avg_density DECIMAL(10,2),
    acc_density DECIMAL(10,2),
    
    -- Calculated outputs
    bgt_cover_sale_q DECIMAL(12,2),
    bgt_st_cl_mbq DECIMAL(12,2),
    trf_in_stk_q DECIMAL(12,2),   -- KEY OUTPUT
    trf_in_opt_cnt DECIMAL(10,2),
    trf_in_opt_mbq DECIMAL(12,2),
    dc_mbq DECIMAL(12,2),
    
    -- Closing position
    bgt_cl_stk_q DECIMAL(12,2),
    net_st_cl_stk_q DECIMAL(12,2),
    st_cl_excess_q DECIMAL(12,2),
    st_cl_short_q DECIMAL(12,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Planning run output: DC Purchase Plan
CREATE TABLE planning_dc_purchase (
    run_id UUID,
    run_date DATE,
    plan_month DATE,
    dc VARCHAR(10),
    seg VARCHAR(5),
    div VARCHAR(10),
    sub_div VARCHAR(10),
    maj_cat VARCHAR(50),
    ssn CHAR(5),
    maj_status VARCHAR(5),
    
    -- DC Stock position
    dc_stk_q DECIMAL(12,2),
    grt_stk_q DECIMAL(12,2),
    msa_stk_frkh DECIMAL(12,2),
    msa_stk_kol DECIMAL(12,2),
    c_art_stk DECIMAL(12,2),
    
    -- Budget inputs
    bgt_disp_cl_q DECIMAL(12,2),
    bgt_sale_q_cm DECIMAL(12,2),
    bgt_sale_q_cm1 DECIMAL(12,2),
    bgt_sale_q_cm2 DECIMAL(12,2),
    bgt_sale_q_cm3 DECIMAL(12,2),
    bgt_st_op_mbq DECIMAL(12,2),
    net_st_op_stk_q DECIMAL(12,2),
    
    -- DC calculations
    bgt_dc_op_stk_q DECIMAL(12,2),
    bgt_cf_stk_q DECIMAL(12,2),
    grt_cons_pct DECIMAL(8,4),
    grt_cons_q DECIMAL(12,2),
    del_pend_q DECIMAL(12,2),
    net_bgt_cf_stk_q DECIMAL(12,2),
    
    -- Transfer-out (to stores)
    trf_out_total DECIMAL(12,2),
    trf_out_m1 DECIMAL(12,2),
    trf_out_m2 DECIMAL(12,2),
    trf_out_m3 DECIMAL(12,2),
    
    -- Purchase plan
    bgt_st_cl_mbq DECIMAL(12,2),
    net_bgt_st_cl_stk DECIMAL(12,2),
    bgt_dc_mbq_sale DECIMAL(12,2),
    bgt_dc_cl_mbq DECIMAL(12,2),
    bgt_dc_cl_stk_q DECIMAL(12,2),
    bgt_pur_q DECIMAL(12,2),       -- KEY OUTPUT
    pos_po DECIMAL(12,2),
    neg_po DECIMAL(12,2),
    
    -- Company level
    bgt_co_cl_stk_q DECIMAL(12,2),
    dc_stk_excess_q DECIMAL(12,2),
    dc_stk_short_q DECIMAL(12,2),
    st_stk_excess_q DECIMAL(12,2),
    st_stk_short_q DECIMAL(12,2),
    co_stk_excess_q DECIMAL(12,2),
    co_stk_short_q DECIMAL(12,2),
    
    -- Bin requirements
    fresh_bin_req DECIMAL(10,2),
    grt_bin_req DECIMAL(10,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Store cover days master
CREATE TABLE store_cover_days (
    st_cd VARCHAR(10),
    st_nm VARCHAR(50),
    state VARCHAR(30),
    status VARCHAR(5),
    op_date DATE,
    rdc_cd VARCHAR(10),
    rdc_nm VARCHAR(20),
    hub_cd VARCHAR(10),
    hub_nm VARCHAR(20),
    dc_to_hub_frkh INT,
    hub_to_st_frkh INT,
    dc_to_hub_kol INT,
    hub_to_st_kol INT,
    sale_cover INT,
    int_cover INT,
    prd_cover INT,
    cover_apr INT, cover_may INT, cover_jun INT,
    cover_jul INT, cover_aug INT, cover_sep INT,
    cover_oct INT, cover_nov INT, cover_dec INT,
    cover_jan INT, cover_feb INT, cover_mar INT,
    PRIMARY KEY (st_cd)
);

-- Major category bin capacity
CREATE TABLE majcat_bin_capacity (
    sub_div VARCHAR(10),
    maj_cat VARCHAR(50),
    season CHAR(5),
    carton_l DECIMAL(6,2),
    carton_b DECIMAL(6,2),
    carton_h DECIMAL(6,2),
    per_carton_qty INT,
    PRIMARY KEY (maj_cat, season)
);
```

---

## 9. Implementation Plan

### Phase 1: Data Foundation (Weeks 1-3)
- Sync SAP stock data (DC + store) into Snowflake via Azure Fabric
- Map Supabase `co_budget_store_major_category` into Snowflake
- Migrate STORE COVER DY and MAJ CAT BIN CAPACITY to Snowflake
- Migrate PEND-PO-Q data feed from SAP
- Validate: compare Snowflake data vs Excel for 5 major categories

### Phase 2: Core Calculation Engine (Weeks 4-7)
- Implement store TRF-IN calculation in Python/SQL
- Implement DC purchase calculation in Python/SQL
- Implement rolling forward logic (13-month chain)
- Unit test each calculation against current Excel outputs
- Parallel run: Excel vs automated for 2 weeks

### Phase 3: Output & Integration (Weeks 8-10)
- Build Power BI dashboard showing plan vs actual
- Build SAP PO creation API (for approved purchase quantities)
- Build Telegram alerts for excess/short flags
- Connect output to allocation system (feed BGT-DISP-Q)
- Build Excel export for team review during transition

### Phase 4: AI Enhancement (Weeks 11-14)
- Replace static budget sales with ML demand forecast
- Dynamic cover days based on actual sell-through rates
- Anomaly detection on stock positions
- Auto-suggest PO quantities with confidence intervals
- Season transition predictor

---

## 10. Key Differences: VER.0049 vs VER.0061

| Feature | VER.0049 (Old) | VER.0061 (Current) |
|---------|---------------|-------------------|
| Format | RDC Week-wise | RDC-wise with 15-day MBQ |
| Time period | Feb 2025 onwards | Mar 2026 onwards |
| MBQ calculation | Standard | 15-day MBQ (shorter cover) |
| File size | 21 MB | 7 MB (leaner) |
| DC layout | Week-wise breakdown | Monthly aggregated |

### Detailed VER.0049 vs VER.0061 Comparison

**Sheet-level differences:**

| Sheet | VER.0049 | VER.0061 |
|-------|----------|----------|
| ST-TRF-IN PLAN | 778 x **1,331 cols** (53 weekly blocks) | 776 x **493 cols** (13 monthly blocks) |
| DC-TRF OUT | 1 sheet: `FRKH-DC-TRF OUT PLAN (PR)-WO-PO` (779 x **2,373 cols**) | 2 sheets: WO-PO + W-PO (785 x 625 each) |
| LISTING | **237,255 rows** x 22 cols (Store x MAJCAT monthly flags) | Removed (merged into ST-TRF-IN PLAN) |
| DC MBQ COVER | 14 rows (SSN-level MBQ cover: 15 days for S/GM) | Removed (embedded in calculations) |
| DISP CONS | 34 rows (Week-to-month mapping) | Removed (not needed in monthly mode) |
| PEND-PO-Q | 3 rows, 48 cols, **weekly**, all zeros | 766 rows, 20 cols, **monthly**, actual data |
| STORE COVER DY | 665 x 31 cols | 664 x 45 cols (+OLD cover section) |

**Column-level differences in ST-TRF-IN PLAN:**

| Feature | VER.0049 | VER.0061 |
|---------|----------|----------|
| Planning granularity | **Weekly** (WK-48, WK-49, etc.) | **Monthly / 15-day** |
| Repeating block size | ~18 cols per week x 53 weeks | ~23 cols per month x 13 months |
| Block includes | BGT-DISP OP-Q, OP-MBQ SALE-Q, BGT-ST OP-MBQ, CL-MBQ SALE-Q, REM SALEQ | **NEW:** BGT-DISP CL-OPT, BGT-DISP CL-OPT MBQ, TRF-IN OPT CNT, TRF-IN OPT MBQ, DC-MBQ, BGT-COVER SALE-Q |
| Density columns | Not present | **NEW:** AVG DENSITY (col 56), ACC DENSITY (col 57) |
| Sales days | Not present | **NEW:** APR BGT SALE-DAYS, MAY BGT SALE-DAYS |
| Store attributes | Has REGION (col 15), CLUSTER (col 16) | Removed |
| GRT stock | Single S-GRT STK-Q | Split: SUMMER GRT STK-Q + WINTER GRT STK-Q |
| Hub info | Not present | **NEW:** HUB INT PRD (col 51) |

**Column-level differences in DC-TRF OUT PLAN:**

| Feature | VER.0049 | VER.0061 |
|---------|----------|----------|
| Planning granularity | **Weekly** purchase blocks | **Monthly** purchase blocks |
| Bin capacity | Not present | **NEW:** BIN CAP, BIN CAP DC TEAM, FRESH BIN REQ, GRT BIN REQ |
| GRT stock | Single GRT column | Split: S-GRT, W-GRT, PW-GRT |
| V2S columns | Has ST-STK-V2S, V2S MSA, V2S CONT% | Removed |
| Store excess | ST-EXCESS, ST-EXCESS (V02) | Removed |
| Purchase calc | BGT PUR-Q | **NEW:** BGT PUR-Q (INITIAL) with POS/NEG PO split |
| DC MBQ | BGT-DC CL-MBQ only | **NEW:** BGT-DC MBQ-SALE (sales-based MBQ) + BGT-DC CL-MBQ |
| Seasonal stock | Separate seasonal stock cols | **NEW:** BGT SSNL OP-STK-Q, NET-SSNL CL-STK-Q |

**Key architectural shifts:**
1. **Weekly -> Monthly:** Reduced complexity from 53 weekly blocks to 13 monthly blocks (1,331 -> 493 cols for stores, 2,373 -> 625 for DC)
2. **Option-level metrics added:** VER.0061 adds TRF-IN OPT CNT and density metrics, connecting planning directly to the allocation system's option-based model
3. **Bin capacity integrated:** Warehouse capacity planning built into the purchase plan calculation
4. **Dual DC sheets:** Separate WO-PO and W-PO views instead of one combined sheet
5. **LISTING sheet eliminated:** 237K-row listing table merged into ST-TRF-IN flags

---

## 11. SALE MONTH WISE.xlsx (Store Sales Plan Input)

**File:** `SALE MONTH WIES.xlsx` (0.6 MB actual, despite 141 MB on disk)
**Purpose:** Store x Major Category monthly sales plan - the **input** that feeds into `co_budget_store_major_category` in Supabase.

### Sheet 1: `ST_MAJ_CAT_SALE_V_PLAN` (Sales Value Plan)
**Rows:** 765 data rows | **Cols:** 42

| Col | Name | Description |
|-----|------|-------------|
| A | ST_CD | Store code |
| B | MAJ_CAT_CD | Major category code |
| C-N | Apr 2026 - Mar 2027 | Monthly planned sales **value** (Rs) |
| O-AP | Apr 2027 - Jul 2029 | Future months (empty, placeholder) |

### Sheet 2: `ST_MAJ_CAT_SALE_Q_PLAN` (Sales Quantity Plan)
**Rows:** 765 data rows | **Cols:** 42

| Col | Name | Description |
|-----|------|-------------|
| A | ST_CD | Store code |
| B | MAJ_CAT_CD | Major category code |
| C-N | Apr 2026 - Mar 2027 | Monthly planned sales **quantity** (pcs) |
| O-AP | Apr 2027 - Jul 2029 | Future months (empty, placeholder) |

**Key observations:**
- This file currently contains only **2 stores** (HB05, HJ08) with **382 major categories each** - appears to be a draft/test, not the full 320+ store plan
- Seasonal patterns visible: winter categories (tights, sweaters) peak Oct-Dec; summer categories (shirts, tees) peak Apr-Jun
- Only FY2026-27 data populated (Apr 2026 - Mar 2027)
- These values are the same as `sale_v_{month}` and `sale_q_{month}` columns in `co_budget_store_major_category` Supabase table
- The full 320+ store plan exists in Supabase (387K rows) but this file appears to be the upload template for 2 specific stores

---

## 12. Input Data Files (Budget Upload Templates)

These xlsx files are the **input templates** used to upload budget data into the planning system. Currently contain 2 test stores (HB05, HJ08) with 382-383 major categories.

### 12.1 Monthly Files

| File | Sheets | Rows | Cols | Content |
|------|--------|------|------|---------|
| **SALE MONTH WISE.xlsx** | 2 (V + Q) | 6,410 | 42 | Monthly sale plan (value Rs + quantity) |
| **DISP MONTH WISE.xlsx** | 2 (V + Q) | 6,898 | 42 | Monthly display stock plan (value Rs + quantity) |

**Structure (both files identical layout):**
- Col A: `ST_CD` (Store code)
- Col B: `MAJ_CAT_CD` (Major category)
- Cols C-N: Monthly values Apr 2026 - Mar 2027 (**populated**)
- Cols O-AP: Monthly values Apr 2027 - Jul 2029 (**empty**, placeholder for future years)

Sheet 1 = `_V_PLAN` (Value in Rs), Sheet 2 = `_Q_PLAN` (Quantity in pcs)

These feed into Supabase `co_budget_store_major_category` as `sale_v_{month}`, `sale_q_{month}`, `disp_v_{month}`, `disp_q_{month}`.

### 12.2 Weekly Files

| File | Sheets | Rows | Cols | Content |
|------|--------|------|------|---------|
| **ST_MAJ_WEEK_WISE_SALE-QTY.xlsx** | 1 (SALE-Q) | 766 | 50 | Weekly sale quantity plan |
| **ST_MAJ_WEEK_WISE_DISP-QTY.xlsx** | 1 (DISP-Q) | 5,755 | 50 | Weekly display stock quantity plan |

**Structure:**
- Row 1: `START DT` + week start dates (2026-04-01, 2026-04-06, ...)
- Row 2: `END DT` + week end dates
- Row 3: `ST-CD | MAJ-CAT | WK-1 | WK-2 | ... | WK-48`
- Row 4+: Data

48 weeks covering Apr 2026 to Feb 2027. Weekly files have **quantity only** (no value sheet).

The DISP weekly file has 5,755 rows vs 766 in SALE - likely contains multiple entries per store-category (possibly by warehouse source or sub-level splits).

These feed into the ASP.NET planning app's `QTY_SALE_QTY` and `QTY_DISP_QTY` tables (48 weekly columns WK-1 to WK-48).

---

## 13. What's Already Built vs What's Still in Excel

### 13.1 ARS (Auto Replenishment System) - BUILT

The ARS is a **production-ready Azure-hosted allocation engine** that replaces the Excel-based allocation system documented in `V2-Allocation-AI-Architecture.docx`.

**Architecture:** 5-engine pipeline
1. **Engine 1: Budget Cascade** - Reads `co_budget_store_major_category` from Supabase, calculates MBQ per store x MAJCAT
2. **Engine 2: Article Scoring** - Scores every DC article against every store using 14 weighted factors
3. **Engine 3: Greedy Filler** - Fills option slots with highest-scoring articles, equitable distribution (lowest fill-rate first)
4. **Engine 4: Size Allocator** - Breaks options into size-level quantities using `co_budget_company_major_category_size`
5. **Engine 5: DO Generator** - Compiles delivery orders with full article details

**Performance:** Processes ~1M store-article pairs, fills 31K+ option slots, generates 145K+ variant allocations in **<6 minutes** (vs 14 hours on 20 Excel machines = 88% improvement).

**API:** `https://ars-v2retail-api.azurewebsites.net` with Swagger at `/docs`

**Stack:** Azure App Service, Azure SQL, Supabase, Snowflake (analytics), JWT auth with RBAC + RLS

### 13.2 Transfer-In Planning App - BUILT

An **ASP.NET Core 8.0 MVC web application** at `localhost:5005` that automates store transfer-in and purchase planning.

**Architecture:**
- 29 controllers, 95+ Razor views, 3 services
- SQL Server database `planning` with 26 tables
- 6 stored procedures for plan generation
- Background job services for bulk runs

**Key stored procedures:**
| SP | Purpose |
|----|---------|
| `SP_GENERATE_TRF_IN_PLAN` | Store transfer-in for specific store/week range |
| `SP_GENERATE_PURCHASE_PLAN` | DC purchase plan for week range |
| `SP_RUN_ALL_PLANS` | Full bulk run: truncate + TRF + PP for all stores |
| `SP_GENERATE_SUB_LEVEL_TRF` | Sub-level TRF (by MVGR/Size/Segment/Vendor) |
| `SP_GENERATE_SUB_LEVEL_PP` | Sub-level PP from sub-level TRF |

**Algorithm (week chaining):**
```
TRF-IN per store x majcat x week:
  Week 1: OP_STK = Store Stock
  Shrinkage = OP_STK * 8% * (seasonal factor)
  NET_CF = OP_STK - Shrinkage
  MBQ = Display + Next_Week_Sale
  TRF_IN = MAX(0, MBQ + Sale - NET_CF)
  CL_STK = NET_CF + TRF_IN
  Week 2+: OP_STK = previous CL_STK -> chain continues

PP per DC x majcat x week:
  Aggregates TRF_OUT = SUM(all store TRF_IN for this DC)
  DC_MBQ = SUM(next 4 weeks sale)
  PUR_Q = MAX(0, TRF_OUT + CL_MBQ - CF_STK)
  DC_CL = CF_STK + PUR - TRF_OUT -> chain continues
```

**Sub-level plans:** Same algorithm but splits by CONT_PCT (contribution %) across 4 dimensions: Macro MVGR, Size, Segment, Vendor. Uses level-specific stock tables.

**Data inputs (22 uploadable tables):**
- `QTY_SALE_QTY` - Weekly sale forecast (48 weeks) <- from ST_MAJ_WEEK_WISE_SALE-QTY.xlsx
- `QTY_DISP_QTY` - Weekly display qty (48 weeks) <- from ST_MAJ_WEEK_WISE_DISP-QTY.xlsx
- `STORE_STOCK` - Current store stock <- from SAP
- `DC_STOCK` - DC stock per RDC <- from SAP
- `DEL_PENDING` - Pending deliveries <- from SAP
- `MASTER_ST_MASTER` - Store master with RDC/Hub mapping
- `MASTER_BIN_CAPACITY` - Bin capacity per major category
- `MASTER_GRT_CONTRIBUTION` - GRT consumption % per week
- `MASTER_PRODUCT_HIERARCHY` - Product -> Season mapping
- `WEEK_CALENDAR` - Fiscal week definitions
- 4 contribution tables (MVGR, Size, Segment, Vendor)
- 8 sub-level stock tables (4 store + 4 DC)

### 13.3 What's Still in Excel

| Component | Status | Where |
|-----------|--------|-------|
| **Monthly budget creation** | Manual Excel (SALE/DISP MONTH WISE) | Upload to Supabase via bulk |
| **Weekly budget disaggregation** | Manual Excel (WEEK WISE files) | Upload to ASP.NET app |
| **Rolling plan (V-0063)** | Excel only (685 MB!) | Not yet automated |
| **ROI-based density model (V-0081)** | Excel only | Used to set fixture density |
| **Store cover days** | Excel (STORE COVER DY sheet) | Manual maintenance |
| **Bin capacity** | Excel (MAJ CAT BIN CAPACITY) | Upload to app |
| **Budget cascade (store-level CONT%)** | In ARS Engine 1 | Automated |
| **Article scoring & allocation** | In ARS Engines 2-5 | Automated |
| **TRF-IN calculation** | In ASP.NET app | Automated |
| **Purchase plan** | In ASP.NET app | Automated |
| **Sub-level plans** | In ASP.NET app | Automated |

### 13.4 End-to-End Flow (Current State)

```
[MANUAL] Monthly Budget Creation (Excel)
    |
    v
[MANUAL] Upload to Supabase (co_budget_store_major_category)
    |
    +-> [AUTO] ARS Engine 1: Budget Cascade -> MBQ per store
    |   [AUTO] ARS Engines 2-5: Score -> Fill -> Size -> DO
    |   Output: Article-level delivery orders
    |
    +-> [MANUAL] Disaggregate to weekly (Excel WEEK WISE files)
        |
        v
    [MANUAL] Upload to ASP.NET app (QTY_SALE_QTY, QTY_DISP_QTY)
        |
        v
    [AUTO] SP_RUN_ALL_PLANS -> TRF_IN_PLAN + PURCHASE_PLAN
        |
        v
    [AUTO] SP_GENERATE_SUB_LEVEL_TRF/PP -> Sub-level plans
```

### 13.5 Target: Full Automation

```
[AUTO] Demand Forecast ML -> Monthly budgets
    |
    v
[AUTO] Write to Supabase + Disaggregate to weekly (Snowflake)
    |
    +-> [AUTO] ARS Pipeline -> Article-level DOs
    |
    +-> [AUTO] Planning App API -> TRF + PP + Sub-level
    |
    v
[AUTO] SAP PO Creation + Telegram Alerts + Dashboard
```

The gap to close: **budget creation** (currently manual Excel) and **weekly disaggregation** (currently manual Excel). Everything downstream is already automated.

---

## 14. V-0063: Rolling Plan (685 MB)

**File:** `V-0063 ST & CO-MAJ_CAT-ROLLING-PLAN (APR-26-SEP-26) 31-MAR-2025 (APR-26 TO SEP-26).xlsb`
**Purpose:** Master 6-month rolling plan for all stores x major categories. This is the **largest planning file** and contains the complete forward stock projection.

**6 sheets:**

| Index | Sheet | Purpose |
|-------|-------|---------|
| 0 | MRST_MAJ_CAT | **Main data** - Store x MAJCAT rolling plan (analysis pending - huge sheet) |
| 1 | Sheet1 | Supporting data (analysis pending) |
| 2 | RXL | Supporting data (analysis pending) |
| 3 | Sheet3 | Category remapping + manual growth multipliers |
| 4 | REF-LIST | Store reference master (650+ stores, festivals, areas) |
| 5 | Sheet2 | Algorithm documentation (STR adjustment rules) |

### 14.1 Sheet: `Sheet3` (Category Remapping + Growth)
**Rows:** 25 | **Cols:** 7

**Section 1: New MAJCAT to Reference MAJCAT mapping (22 entries)**
Maps new category codes to reference categories for budget estimation:
- JB_T_PYJAMA -> JB_H_PYJAMA
- JB_D_JOGGER -> JB_JEANS
- L_CARGO -> L_JEANS
- etc.

**Section 2: Manual Growth % (cols 4-5)**
Growth multipliers applied to budget forecasts:
- Default: 1.1x (10% growth)
- M_BRIEF: 1.5x (50% growth - high growth category)
- M_PRW_SHIRT_FS: 1.2x (20% growth)

### 14.2 Sheet: `REF-LIST` (Store Reference Master)
**Rows:** 661 | **Cols:** 24

| Col | Name | Description |
|-----|------|-------------|
| 0 | ST CD | Store code |
| 1 | ST NM | Store name |
| 2 | STATUS (L2M STAT) | Last 2 month status |
| 3 | STATUS (L7M STAT) | Last 7 month status |
| 4 | OP-DATE | Opening date |
| 5 | AREA | Store area (sq ft) |
| 6 | STATE | State |
| 7 | REF_ST CD | Reference store code (for new stores) |
| 8 | REF_ST NM | Reference store name |
| 9 | REF-GRP-NEW | New reference group |
| 10 | REF-GRP-OLD | Old reference group |
| 11-23 | Festival flags | YES/NO for 13 festivals: D.Puja, Diwali, Chhat, Eid, B.Eid, Ugadi, Holi, G.Puja, S.Puja, P.Asthmi, Pongal, Rajjo, Bihu |

**Key insight:** Festival flags are used to adjust sales budgets during festival periods. Different regions have different festival patterns (e.g., Chhat in Bihar, Pongal in Tamil Nadu, Bihu in Assam).

### 14.3 Sheet: `Sheet2` (STR Adjustment Algorithm)
**Rows:** 8 | **Cols:** 3
**Purpose:** Documents the stock replenishment algorithm rules for adjusting planned stock levels based on fill rate (FR%).

| Rule | Condition | Formula |
|------|-----------|---------|
| ALGO-1 (LW) | If Last Week FR% < 70% | `LW_STR * (1 + (70%/LW_FR% - 1) * 50%)` |
| ALGO-2 (LW) | If Last Week FR% > 130% | `LW_STR * (1 - (1 - 130%/LW_FR%) * 50%)` |
| ALGO-3 (LW) | If LW revised STR < 7 days | Floor at 7 days stock |
| ALGO-1 (LYSP) | If Last Year Same Period FR% < 70% | `LYSP_STR * (1 + (70%/LYSP_FR% - 1) * 50%)` |
| ALGO-2 (LYSP) | If Last Year Same Period FR% > 130% | `LYSP_STR * (1 - (1 - 130%/LYSP_FR%) * 50%)` |
| ALGO-3 (LYSP) | If LYSP revised STR < 7 days | Floor at 7 days stock |
| ALGO-3 (default) | If LW or LYSP STR = 0 | 45 days for APP, 60 days for GM |

**Logic:** If actual sales far exceeded stock (FR% < 70% = understocked), increase planned stock. If stock far exceeded sales (FR% > 130% = overstocked), decrease. The 50% dampening factor prevents over-correction. Minimum 7 days cover enforced.

### 14.4 Sheet: `MRST_MAJ_CAT` (Main Rolling Plan - THE CORE ENGINE)
**Rows:** 100,000+ | **Cols:** 1,006
**Purpose:** The **master budget/sales plan** for every store x major category x season. One row per combination. Contains actual sales history, forecast algorithms, and 6-month forward sales plan. This is where the budget numbers originate before flowing to Supabase and the allocation/planning systems.

**Row layout:**
- Rows 0-11: Config/index rows (serial numbers, column mapping)
- Row 12: Section headers with monthly date references
- Row 13: Column headers (289 non-empty)
- Row 14: Sub-headers (890 non-empty)
- Row 15+: Data rows

**Monthly planning blocks start at:** APR=col 222, MAY=col 344, JUN=col 466, JUL=col 588, AUG=col 710, SEP=col 832. Each block = ~122 columns.

#### SECTION A: Store Hierarchy (Cols 0-55)

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| 0 | ST-CD | Store code | HB05 |
| 1 | ST-NM | Store name | PTN |
| 2 | FLR | Floor number | 2 |
| 3-4 | OP DATE | Opening date | 41028 |
| 5 | MTD DAYS | Month-to-date days | |
| 6 | LM DAYS | Last month days | |
| 7 | LLM DAYS | Last-last month days | |
| 8 | OP DAYS | Operating days since opening | 5084 |
| 9 | STAT 2M | Status (last 2 months) | OLD |
| 10 | STAT L-7M | Status (last 7 months) | OLD |
| 11 | SEG | Segment (APP/GM) | APP |
| 12 | DIV | Division | KIDS |
| 13-14 | NEW SUB_DIV | New sub-division | IB |
| 15-16 | DIV/SUB-DIV SSN COUNT | Season count per division/sub-div | |
| 17 | MAJ CAT | Major category | IB_H_DNGR_SUIT |
| 18 | REF MAJ_CAT | Reference MAJCAT (for new categories) | |
| 19 | B&M CAT | Business & Merchandise category | |
| 20 | SSN | Season | S |
| 21 | MAJ STAT | Major category status | ACT |
| 22-23 | REF-ST GRP NEW/OLD | Reference store group | |
| 24-25 | REF ST-CD / ST-NM | Reference store (for new stores) | |
| 26 | INPUT | Input flag | |
| 27 | NON KIDS STORE | Flag for non-kids stores | |
| 29-39 | Festival flags | D.PUJA, DIWALI, CHHAT, EID, B.EID, UGADI, HOLI, G.PUJA, S.PUJA, P.ASTHMI, PONGAL | YES/NO |
| 41-49 | Store metrics | APF, ASP, ST ASP, ACP, PER PC AREA, COST%, FLR AREA, 7 FT DENSITY | |
| 51-55 | Composite keys | MAJ CAT, ST DIV SSN, ST-SUB-DIV SSN, ST MAJ CAT, ST FLR | |

#### SECTION B: Actual Reference Data (Cols 57-142)

5 time-window blocks, each with the same 14-16 metrics:

| Window | Cols | Period |
|--------|------|--------|
| LW (Last Week) | 57-73 | Current week actuals |
| MTD (Month-to-Date) | 75-91 | Current month actuals |
| LM (Last Month) | 93-109 | Previous month actuals |
| L-30D (Last 30 Days) | 111-126 | Rolling 30 days |
| L-3M (Last 3 Months) | 128-142 | Rolling 3 months |

Each window contains: VAL, QTY, FIX (fixtures), SALE, ACT SALE (PD AVG), GM (gross margin), ASP (avg selling price), AREA, STR (stock turn), MAJ PSF (major cat per sq ft), FLR PSF (floor per sq ft), ACH% (achievement %), AUTO FIX.

#### SECTION C: Historical Sales (Cols 144-195)

26 months of monthly sales history (MAR-24 through MAR-26):
- Cols 144-169: SALE_V TTL (total sale value per month)
- Cols 170-195: SALE_Q TTL (total sale quantity per month)

#### SECTION D: Base Month Averages (Cols 196-220)

| Period | Content |
|--------|---------|
| MAR-OCT avg | SALE_V AVG, SALE_Q AVG for 2024 and 2025 |
| FEB-MAR avg | Same structure, shorter window |

#### SECTION E: Monthly Planning Block (REPEATS 6 times: APR-SEP 2026)

Each monthly block = ~122 columns. Structure per block:

| Offset | Name | Description |
|--------|------|-------------|
| +0 | FIX | Fixture count for this month |
| +1 | AREA | Display area (sq ft) |
| +2 | DISP QTY | Display quantity budget |
| +3 | SALE LISTING | Sale listing flag |
| +4 | BGT SALE QTY (PD) | **Budget sale quantity per day** |
| +5 | LYSP-25 SALE-Q (PD) | Last year same period sale qty/day |
| +6 | REV LYSP-25 SALE-Q (PD) | Revised LYSP |
| +7 | DIFF% | Difference % (budget vs LYSP) |
| +8 | BGT SALE VAL (PD) | Budget sale value per day |
| +9 | LYSP-25 SALE-V (PD) | LYSP sale value/day |
| +10 | DIFF% | Value difference % |
| +11 | BGT GM VAL (PD) | Budget gross margin value/day |
| +12 | LYSP ASP | LYSP average selling price |
| +14 | BGT STR | Budget stock turn ratio |
| +15 | SALE PSF | Sales per sq ft |
| +16 | FLR PSF | Floor sales per sq ft |
| +17 | PSF ACH% | PSF achievement % |
| +19-27 | SALE-BGT | Budget QTY/VAL, LYSP 2025, LYSM 2025 for QTY/VAL/GM |
| +31-34 | SALE QTY/VAL (PD) | Current and prior period daily sales |
| +35 | OLD/NEW | Store status flag |
| +36 | FILL RATE% | Fill rate percentage |
| +37-45 | LYSP metrics | LYSP AVG QTY, ASP GR%, AVG ASP, REV LYSP, AVG QTY/VAL, TTL QTY/VAL |
| +46-60 | Growth rates | ST MAJ-CAT GR%, MAJ-CAT GR%, STORE GR%, MAJ GR% (multiple period variants) |
| +62-79 | BASE TO CM GR% | Growth % from base period to current month, with variants for old/new stores |
| +81-89 | RXL | RXL (range execution), LYSP RXL, RXL GR%, MIN RXL GR%, MIN LYSP SALE QTY/VAL |
| +90-93 | ALGO SALES | ALGO SALES QTY-1, ALGO SALES QTY-2, BGT SALES AVG-QTY, BGT SALES AVG-VAL |
| +94-99 | Contribution | ST-MAJ VAL/CONT%, BGT SALES AVG, SALE WITH STR, SALE WITH MAXI CAP, LM BGT ALGO |
| +100-103 | BGT outputs | **BGT SALE-QTY, BGT SALE-VAL** (the final budget numbers), FINAL QTY/ASP/VAL |
| +106-110 | BTM-UP | BTM-UP RXL, RXL, RXL DIFF, FINAL SALE-VAL, FINAL SALE-QTY |
| +111-120 | Purchase & Allocation | RXL (purchase), FINAL SALE-VAL/QTY (purchase + allocation variants), OLD VS NEW comparison |

**This is where the budget numbers are born.** The `BGT SALE-QTY` and `BGT SALE-VAL` at offset +100-103 are the final outputs that flow to:
- `co_budget_store_major_category.sale_q_{month}` and `sale_v_{month}` in Supabase
- `QTY_SALE_QTY` in the ASP.NET planning app
- `BGT ART-SALES` in the Excel allocation system's BASE-DATA

**The budget calculation algorithm (reverse-engineered from column structure):**
```
1. Start with LYSP (Last Year Same Period) actual sales
2. Apply growth rates:
   - ST MAJ-CAT GR%: Store x MAJCAT specific growth
   - MAJ-CAT GR%: Category-level growth
   - STORE GR%: Store-level growth
   - MANUAL GR% from Sheet3 (1.1x default, up to 1.5x)
3. Adjust for:
   - Fill rate (if FR% < 70%: increase using ALGO-1 formula)
   - RXL (range execution level) changes
   - Store status (OLD vs NEW different growth paths)
   - Festival impact (13 festival flags)
4. Apply min/max caps from MIN-MAX table
5. Bottom-up reconciliation (BTM-UP RXL vs top-down)
6. Final output: BGT SALE-QTY, BGT SALE-VAL
```

### 14.5 Sheet: `Sheet1` (Company-Level Summary)
**Rows:** 398 | **Cols:** 77
**Purpose:** Company-level MAJCAT summary comparing NEW vs OLD plan.

| Col | Name | Description |
|-----|------|-------------|
| 0 | SEG | Segment |
| 1 | DIV | Division |
| 2 | NEW SUB_DIV | Sub-division |
| 3 | MAJ CAT | Major category |
| 4 | SSN | Season |

Then per month (5 cols each x 12 months = 60 cols): NEW SALE_V, NEW SALE_Q, OLD SALE_V, OLD SALE_Q, DIFF.

Row 5 = company totals.

### 14.6 Sheet: `RXL` (Store-Level Range Execution)
**Rows:** 663 | **Cols:** 73
**Purpose:** Store-level RXL (range execution level) plan with current year, LYSP, old plan, and difference.

| Cols | Block | Period |
|------|-------|--------|
| 0-12 | Store identity | ST CD, ST NM, OP DATE, STATUS, OLD ST, MAJ, RNG SEG, SSN, COMB |
| 14-27 | FY 2025-2026 (RETAIL) | Monthly MAR-27 back to FEB-26 |
| 29-42 | LYSP RXL 2024-2025 | Same months, last year |
| 44-57 | RXL-OLD | Previous plan |
| 59-72 | DIFF | New vs old difference |

---

## 16. Cross-System Verification & Critical Notes

The following findings come from cross-checking the architecture documents against the actual built systems (ARS + ASP.NET Planning App):

### 16.1 Three Separate Systems, Not One

| System | Level | Granularity | Technology | Status |
|--------|-------|-------------|------------|--------|
| **Planning (Excel + ASP.NET)** | MAJCAT | Store x MAJCAT x Week | Excel + ASP.NET Core 8 + SQL Server | Partially automated |
| **Allocation (Excel)** | Article | Store x Article x Size x Option | Excel VBA (29 steps, 896 cols) | Legacy - being replaced |
| **ARS (Allocation replacement)** | Article | Store x Article x Size | Python FastAPI + Azure SQL + Supabase | Production |

These solve different problems:
- **Planning** answers: "How many total pieces of Jeans should Store HB05 receive this week?"
- **Allocation** answers: "Which specific jean articles (by color, size) should go to Store HB05?"

### 16.2 ARS vs Excel Allocation - Key Differences

The ARS is **NOT a faithful reimplementation** of the Excel system. It is a **redesigned** system:

| Feature | Excel (29-step VBA) | ARS (5-engine) |
|---------|--------------------|--------------| 
| Approach | Budget-cascade, hierarchical option tagging | Score-based greedy filling |
| MBQ types | 7 types (DISP, B_MTH, SSN, DISP+B_MTH, etc.) | 1 type (DISP only) |
| Option planning | 8-step algo at 5 hierarchy levels (SEG, MVGR, MC, SEG(M), MAJCAT) | Flat score ranking, fill lowest fill-rate first |
| X-ART handling | Dedicated steps with stock caps based on X-MBQ | Not implemented |
| Multi-option tagging | 4-level cascade allowing articles to take multiple slots | Not implemented |
| Fallback options | Safety net for unfilled slots | Not implemented |
| Hold quantities | HOLD-Q, HOLD-MBQ, REV_HOLD_MBQ passes | Not implemented |
| PR-Q generation | Production requirements calculated | Not implemented |
| GRT allocation | Step 6 handles gratis stock | Not implemented |
| Life cycle tracking | L-ART LC dates, aging | Not implemented |
| Store accounts | ST-ACC sheet (3,195 rows) tracking article slots | Not implemented |
| Performance | 14 hours on 20 Excel machines | 6 minutes on 1 Azure server |
| Rounding | Conservative: only round up if fractional > 0.7 | Same rule in MBQ calc only |

### 16.3 Budget Source Evolution

| Era | Budget Source | Format |
|-----|-------------|--------|
| Legacy | BASE-DATA.xlsb (20 Excel sheets: BGT-MAJCAT, BGT-SEG, BGT-MVGR, etc.) | Excel with VLOOKUP/SUMIFS |
| Current | `co_budget_store_major_category` in Supabase (387K rows) | PostgreSQL API |
| Both use | `sale_q_{month}`, `disp_q_{month}`, `pur_q_{month}` per store x MAJCAT | Monthly columns |

The allocation doc describes the legacy Excel budget source. The ARS and ASP.NET app use Supabase. The data should be identical but in different formats.

### 16.4 SEG Definition Note

**Important:** "SEG" means different things in different contexts:
- In **Planning** files: SEG = APP (Apparel) / GM (General Merchandise) / FW (Footwear) - i.e., the **segment of business**
- In **Allocation** files: SEG = E (Economy) / V (Value) / P (Premium) - i.e., the **price segment**
- The ARS doc conflates these two meanings. The correct mapping is: Planning SEG = Division, Allocation SEG = Price tier.

## 15. V-0081: ROI-Based Fixture & Density Model (2 MB)

**File:** `V-0081.3.5 APR-26 ST MAJ MODEL FIX WORKING RETAIL-ROI-01-APR-2026-ALC WITH OLD DNSTY-ALLOC.xlsb`
**Purpose:** Determines how many **fixtures** (display shelves/racks) and **article-option slots** each store gets for each major category. This is the **fixture planning model** that drives display density, which in turn drives MBQ and allocation.

**5 sheets, 3 test stores (HB05, HL05, HM22)**

### 15.1 Sheet: `ST-FLR-FIX-WITH STR CAP` (Store Floor Fixtures with Store Capacity)
**Rows:** 1,014 | **Cols:** 70
**Purpose:** Store-level floor fixture allocation with floor area constraints.

| Col | Name | Description | Example |
|-----|------|-------------|---------|
| A | COMB | Composite key | |
| B | ST-CD | Store code | HX53 |
| C | ST-NM | Store name | WAGHOLI |
| D | OP DATE | Opening date | |
| E | STAT | Status | |
| F | FL | Floor number | |
| G | INSTALLED FLR FIX | Installed floor fixtures | 230 |
| H | ST FLR AREA | Store floor area (sq ft) | 15,927 |
| I | STORE AREA | Total store area | |
| J | ST TTL FIX | Store total fixtures | 800 |
| K | APF | Area per fixture (sq ft) | 19.91 |
| L-W | ST-FLR TAG/BAL FIX | Tagged vs balance fixtures (with/without floor adjust) | |
| Y-AO | NEW FIX@ST-FLR | New fixture plan by division (MENS/LADIES/KIDS/GM) with FIX CONT% and SALE CONT% | |
| AQ | OLD FLR FIX | Previous fixture count | |
| AR | DIFF | Old vs new difference | |
| AW-AY | APP-FIX | Festival fixtures (PUJA/DIWALI/CHHAT) | |
| BA-BR | Monthly SALE-V | Sales value OCT-25 through MAR-27 (18 months) | |

**Totals:** 207,257 installed floor fixtures across all stores, 45.46 total floor area.

### 15.2 Sheet: `ST-MAJCAT` (THE CORE MODEL - 371 Columns!)
**Rows:** 1,156 data rows | **Cols:** 371
**Purpose:** The master fixture/density calculation engine. One row per store x major category. This is where **every store's display capacity** is calculated.

**Column sections (371 columns across 15+ calculation stages):**

| Cols | Section | Key Columns |
|------|---------|-------------|
| A-I | Store Identity | ST-CD, STATUS, ST-NM, FINAL FLR, OP DATE, STATE |
| J-R | Category | SEG, DIV, SUB-DIV, MAJ CAT, SSN |
| S-T | Pricing | NEW ACP (avg cost), NEW ASP (avg selling price) |
| U-AB | Store-Floor | ST-FIX, ST AREA, ST-FLR FIX, ST-FLR AREA, APF |
| AC-AS | Constraints | MIN fix, MAX fix, MAJ STAT, store type flags (NON-GM, MO-ONLY, DIMAPUR, HIGH-ROI, BOOK-FOLD) |
| AU-BD | **Density** | OLD ACC DENSITY, NEW ACC DENSITY, DNSTY BOOK FOLD, DNSTY NRML, BASE PSF ACH%, FINAL DNSTY, PER FIX STK-PSF, MIN FLR-PSF ACH% |
| BF-CC | **State Model** | 22 Indian states (MP, Bihar, UK, J&K, Delhi, HP, Tripura, Arunachal, Assam, Karnataka, Goa, UP, Jharkhand, WB, Meghalaya, Odisha, AP, Chhattisgarh, Haryana, Maharashtra, Punjab, Rajasthan) + DIMAPUR |
| CD-DO | **National Model & Algo** | NAT MODEL, MIN/MAX caps, WINTER FLR DIV FIX, 10+ algorithm steps (ALGO-1, ALGO-2, INC-1, INC-2, FINAL-FIX) |
| DQ-DX | **Final Fix** | FINAL ST-MODEL FIX, MAX MODEL CAP, OLD vs NEW comparison |
| DZ-EQ | **ROI Metrics** | STK-Q, FIX, SALE-Q AVG, SALE-V AVG, GM-V AVG, GM%, GP PSF, FLR GP-PSF, FLR GP-PSF ACH%, AUTO FIX, AUTO FIX CONT% (repeated for FEB-APR 2025 and FEB-APR 2026) |
| FJ-FW | **Sub-Div Fitment** | SUB-DIV FIX VETTED, FLR BTM UP, FINAL FIX (INT), FINAL FIX (ROUND), FLR BLNC |
| FY-GX | **Allocation Algo** | TTL SALES-Q (NEXT 4 MTH), DISP%, STR FIX, ALGO FIX, ALGO FIX W MDL CAP, ALGO FIX WITH STR CAP, FIX DEC/INC, MODEL %, MIN ST-FITMENT, FINAL ALGO FIX |
| GY-JN | **Incremental Fix** | 4 rounds of ADD FIX (APP, APP, GM, MODEL) with bottom-up algo, floor balance tracking |
| JP-LC | **Purchase & Allocation** | FINAL FIX WITH INC (PUR), FINAL FIX WITH INC (ALC), DISP-Q, DENSITY CONS%, FINAL DENSITY CONS%, REV AREA, REV SALES PSF, GAP DETAILS |
| LF-NG | **Monthly Sales** | LYSP SALE-V (5 months), MAX BGT SALE-Q (18 months JUL-25 to MAR-27), MAX BGT SALE-V (18 months) |

**The fixture planning algorithm:**
```
1. STATE MODEL: Each state has a model fixture count per MAJCAT
   (e.g., Bihar gets 2 fixtures for FW_M_SLIPPER, Delhi gets 5)

2. NATIONAL MODEL: Baseline fixture count (typically higher than state)
   NAT_MODEL with MIN/MAX caps per MAJCAT

3. STORE FITMENT: Adjust national model to fit store's floor area
   ALGO: MIN(NAT_MODEL, MAX_CAP) constrained by ST-FLR available
   Multiple iterations (ALGO-1, ALGO-2) to balance floor across divisions

4. ROI SCORING: Calculate GP PSF (Gross Profit Per Sq Ft) for each MAJCAT
   GP_PSF = GM_Value / (Fix_Count * Area_Per_Fix)
   FLR_GP_PSF_ACH% = Actual GP PSF / Target GP PSF
   
5. AUTO FIX: ROI-based fixture recommendation
   High GP PSF -> more fixtures, Low GP PSF -> fewer fixtures

6. INCREMENTAL FIX: 4 rounds of adjustment
   Round 1-2: APP (Apparel) additions
   Round 3: GM (General Merchandise) additions  
   Round 4: MODEL-based corrections
   Each round: bottom-up calculation, floor balance check, cap enforcement

7. FINAL OUTPUT:
   FINAL FIX WITH INC (PUR) -> Fixture count for purchase planning
   FINAL FIX WITH INC (ALC) -> Fixture count for allocation
   FINAL DENSITY CONS% -> Density contribution % 
   FINAL DISP QTY -> Display quantity budget
```

**This is the source of ACC DENSITY** used in ST-TRF-IN PLAN (col 57) and the ARS Budget Cascade engine.

### 15.3 Sheet: `ST-SUB INPUT FIX` (Sub-Division Fixtures)
**Rows:** 11,334 | **Cols:** 12
**Purpose:** Sub-division level fixture assignments per store.

| Col | Name | Description |
|-----|------|-------------|
| A | ST-CD | Store code |
| B | ST-NM | Store name |
| C | STAT | Status |
| D | SEG | Segment |
| E | DIV | Division |
| F | REV DIV | Revised division |
| G | SUB DIV | Sub-division |
| H | FLR | Floor |
| I | COMB | Store + Sub-div key |
| J | FINAL SUB-DIV FIX | Final sub-division fixtures |
| L | OLD | Previous fixture count |

### 15.4 Sheet: `MIN-MAX` (Fixture Min/Max per MAJCAT)
**Rows:** 636 | **Cols:** 14
**Purpose:** Minimum and maximum fixture constraints per major category.

| Col | Name | Example |
|-----|------|---------|
| A | DIV | KIDS |
| B | SUB DIV | KB-L |
| C | MAJ CAT | JB_CRG |
| D | SSN | A |
| E | MIN | 0.25 |
| F | MAX | 8 |
| G | REMARKS | |
| L-N | NEW STORES | List of upcoming stores (HN31, HL06, HX52, etc.) |

### 15.5 Sheet: `ST MODEL CONS` (Store Model Consolidation)
**Rows:** 2,535 | **Cols:** 9
**Purpose:** Model consolidation percentage per store x season. Currently **all stores = 0.5 (50%)**.

**How this connects to planning:** The fixture model produces `ACC DENSITY` and `FINAL DISP QTY` which feed directly into:
- `co_budget_store_major_category.disp_q_{month}` in Supabase
- `QTY_DISP_QTY` in the ASP.NET planning app
- ARS Engine 1 Budget Cascade (MBQ calculation)
- ST-TRF-IN PLAN column 57 (ACC DENSITY) and column 85 (BGT-DISP CL-Q)

---

---

## 17. Verification Results

The following verifications were performed against the actual VER.0061 xlsb file:

### Formula Verification (PASSED)

Tested 3 store x MAJCAT rows (HB05/M_TEES_HS, HB05/L_H_TOP_HS, HJ08/M_TEES_HS) for APR-26 block:

| Formula | Expected | Actual | Status |
|---------|----------|--------|--------|
| COVER = SALE_CM+1/30 * DAYS | Calculated | Matched within floating-point precision | PASS |
| MBQ = DISP + COVER | Calculated | Matched exactly | PASS |
| TRF-IN = MAX(MBQ + SALE - NET_OP, 0) | Calculated | Matched exactly | PASS |
| CL-STK = NET_OP + TRF-IN - SALE | Calculated | Matched exactly | PASS |
| NET-OP = OP-STK - NT_ACT | Calculated | Matched (NT_ACT=0 in test rows) | PASS |

### Column Position Verification

| Check | Status | Notes |
|-------|--------|-------|
| APR block starts col 85 | CORRECT | |
| MAY block starts col 108 | CORRECT | |
| JUN block starts col 131 | CORRECT | |
| MAR block col 61 = MTD B-SALE-Q | CORRECTED | Doc initially said BGT-DISP; fixed |
| DC APR block starts col 71 | CORRECT | |
| DC MAY block starts col 111 | CORRECT | |
| DC first block = 40 cols (not 38) | CORRECTED | Extra MAR SALE-Q column; fixed |
| STORE COVER DY 664 rows | CORRECT | |
| PEND-PO-Q 766 rows, 20 cols | CORRECT | |
| Serial dates = month-end | NOTED | 46142 = Apr 30, not Apr 1 |

### Known Limitations

1. **pyxlsb cannot extract Excel formulas** from .xlsb binary format - only cached computed values. Formula logic was reverse-engineered from data patterns and column relationships.
2. **15-day MBQ**: The CM+2 SALE-DAYS column is always 0 in current data, meaning the cover calculation effectively uses only 1 month of forward sales (not 2). The "15 D MBQ" in the filename confirms this is intentional.
3. **REMARKS sheet** exists in VER.0061 but is empty (25 rows, 8 cols, no content).

---

*Document generated: 2026-04-07*
*Based on analysis of 8 planning files + CLAUDE.md + V2_Retail_ARS_Architecture.docx*
*Data source: \\file\0-V2\04-DEPARTMENT\04-PLANNING\01-CENTRAL PLANNING\10-MY FOLDER\04-MEHFUJ\07-04-2026*
*Supabase: co_budget_store_major_category (387K rows, project pymdqnnwwxrgeolvgvgv)*
*ARS API: https://ars-v2retail-api.azurewebsites.net*
*Planning App: ASP.NET Core 8.0, database `planning`, 26 tables, 6 SPs*
