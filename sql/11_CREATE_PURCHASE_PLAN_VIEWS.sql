-- =====================================================
-- FILE: 11_CREATE_PURCHASE_PLAN_VIEWS.sql
-- PURPOSE: Create views for Purchase Plan reporting and analysis
-- DATABASE: [planning]
-- =====================================================

USE [planning];
GO

-- =====================================================
-- VIEW 1: VW_PURCHASE_PLAN_DETAIL
-- DESCRIPTION: Detailed view with all columns including future week forecasts
-- GRAIN: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID (one row per combination)
-- =====================================================

IF OBJECT_ID('dbo.VW_PURCHASE_PLAN_DETAIL', 'V') IS NOT NULL
    DROP VIEW dbo.VW_PURCHASE_PLAN_DETAIL;
GO

CREATE VIEW dbo.VW_PURCHASE_PLAN_DETAIL
AS
SELECT
    pp.[ID],
    pp.[RDC_CD] AS 'RDC-CD',
    pp.[RDC_NM] AS 'RDC-NM',
    pp.[MAJ_CAT] AS 'MAJ-CAT',
    pp.[SSN],
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT] AS 'WK-ST-DT',
    pp.[WK_END_DT] AS 'WK-END-DT',

    -- Reference Stock Data
    pp.[DC_STK_Q] AS 'DC-STK-Q',
    pp.[GRT_STK_Q] AS 'GRT-STK-Q',
    pp.[S_GRT_STK_Q] AS 'S-GRT-STK-Q',
    pp.[W_GRT_STK_Q] AS 'W-GRT-STK-Q',

    -- Bin Capacity
    pp.[BIN_CAP_DC_TEAM] AS 'BIN-CAP-DC-TEAM',
    pp.[BIN_CAP],

    -- Budget Display & Store Sales
    pp.[BGT_DISP_CL_Q] AS 'BGT-DISP-CL-Q',
    pp.[CW_BGT_SALE_Q] AS 'CW-BGT-SALE-Q',
    pp.[CW1_BGT_SALE_Q] AS 'CW1-BGT-SALE-Q',
    pp.[CW2_BGT_SALE_Q] AS 'CW2-BGT-SALE-Q',
    pp.[CW3_BGT_SALE_Q] AS 'CW3-BGT-SALE-Q',
    pp.[CW4_BGT_SALE_Q] AS 'CW4-BGT-SALE-Q',
    pp.[CW5_BGT_SALE_Q] AS 'CW5-BGT-SALE-Q',

    -- Store Opening
    pp.[BGT_ST_OP_MBQ] AS 'BGT-ST-OP-MBQ',
    pp.[NET_ST_OP_STK_Q] AS 'NET-ST-OP-STK-Q',

    -- DC Opening Stock
    pp.[BGT_DC_OP_STK_Q] AS 'BGT-DC-OP-STK-Q',
    pp.[PP_NT_ACT_Q] AS 'PP-NT-ACT-Q',
    pp.[BGT_CF_STK_Q] AS 'BGT-CF-STK-Q',

    -- Total Stock
    pp.[TTL_STK] AS 'TTL-STK',
    pp.[OP_STK] AS 'OP-STK',
    pp.[NT_ACT_STK] AS 'NT-ACT-STK',

    -- GRT Consumption
    pp.[GRT_CONS_PCT] AS 'GRT-CONS-PCT',
    pp.[GRT_CONS_Q] AS 'GRT-CONS-Q',

    -- Delivery Pending
    pp.[DEL_PEND_Q] AS 'DEL-PEND-Q',

    -- Purchase Plan Confirm Stock
    pp.[PP_NET_BGT_CF_STK_Q] AS 'PP-NET-BGT-CF-STK-Q',

    -- Transfer Out
    pp.[CW_TRF_OUT_Q] AS 'CW-TRF-OUT-Q',
    pp.[CW1_TRF_OUT_Q] AS 'CW1-TRF-OUT-Q',
    pp.[TTL_TRF_OUT_Q] AS 'TTL-TRF-OUT-Q',

    -- Store Closing
    pp.[BGT_ST_CL_MBQ] AS 'BGT-ST-CL-MBQ',
    pp.[NET_BGT_ST_CL_STK_Q] AS 'NET-BGT-ST-CL-STK-Q',
    pp.[NET_SSNL_CL_STK_Q] AS 'NET-SSNL-CL-STK-Q',

    -- DC Sales & Closing
    pp.[BGT_DC_MBQ_SALE] AS 'BGT-DC-MBQ-SALE',
    pp.[BGT_DC_CL_MBQ] AS 'BGT-DC-CL-MBQ',
    pp.[BGT_DC_CL_STK_Q] AS 'BGT-DC-CL-STK-Q',

    -- Purchase Orders
    pp.[BGT_PUR_Q_INIT] AS 'BGT-PUR-Q-INIT',
    pp.[POS_PO_RAISED] AS 'POS-PO-RAISED',
    pp.[NEG_PO_RAISED] AS 'NEG-PO-RAISED',

    -- Company Closing
    pp.[BGT_CO_CL_STK_Q] AS 'BGT-CO-CL-STK-Q',

    -- Stock Excess/Shortage
    pp.[DC_STK_EXCESS_Q] AS 'DC-STK-EXCESS-Q',
    pp.[DC_STK_SHORT_Q] AS 'DC-STK-SHORT-Q',
    pp.[ST_STK_EXCESS_Q] AS 'ST-STK-EXCESS-Q',
    pp.[ST_STK_SHORT_Q] AS 'ST-STK-SHORT-Q',
    pp.[CO_STK_EXCESS_Q] AS 'CO-STK-EXCESS-Q',
    pp.[CO_STK_SHORT_Q] AS 'CO-STK-SHORT-Q',

    -- Bin Requirements
    pp.[FRESH_BIN_REQ],
    pp.[GRT_BIN_REQ],

    -- Audit
    pp.[CREATED_DT],
    pp.[CREATED_BY],
    pp.[MODIFIED_DT],
    pp.[MODIFIED_BY],

    -- Future week sales (self-join for context)
    ISNULL(ppn1.[CW_BGT_SALE_Q], 0) AS 'NXT-WK-BGT-SALE-Q',
    ISNULL(ppn2.[CW_BGT_SALE_Q], 0) AS 'NXT2-WK-BGT-SALE-Q'
FROM dbo.PURCHASE_PLAN pp
LEFT JOIN dbo.PURCHASE_PLAN ppn1 ON ppn1.[RDC_CD] = pp.[RDC_CD]
                                   AND ppn1.[MAJ_CAT] = pp.[MAJ_CAT]
                                   AND ppn1.[WEEK_ID] = (SELECT MAX([WEEK_ID])
                                                          FROM dbo.WEEK_CALENDAR wc1
                                                          WHERE wc1.[WEEK_SEQ] = (SELECT wc2.[WEEK_SEQ] + 1
                                                                                  FROM dbo.WEEK_CALENDAR wc2
                                                                                  WHERE wc2.[WEEK_ID] = pp.[WEEK_ID]))
LEFT JOIN dbo.PURCHASE_PLAN ppn2 ON ppn2.[RDC_CD] = pp.[RDC_CD]
                                   AND ppn2.[MAJ_CAT] = pp.[MAJ_CAT]
                                   AND ppn2.[WEEK_ID] = (SELECT MAX([WEEK_ID])
                                                          FROM dbo.WEEK_CALENDAR wc1
                                                          WHERE wc1.[WEEK_SEQ] = (SELECT wc2.[WEEK_SEQ] + 2
                                                                                  FROM dbo.WEEK_CALENDAR wc2
                                                                                  WHERE wc2.[WEEK_ID] = pp.[WEEK_ID]));

GO

-- =====================================================
-- VIEW 2: VW_PURCHASE_PLAN_SUMMARY
-- DESCRIPTION: Aggregated summary by RDC across all categories
-- GRAIN: RDC_CD ÃƒÂ— WEEK_ID (sums across all categories)
-- =====================================================

IF OBJECT_ID('dbo.VW_PURCHASE_PLAN_SUMMARY', 'V') IS NOT NULL
    DROP VIEW dbo.VW_PURCHASE_PLAN_SUMMARY;
GO

CREATE VIEW dbo.VW_PURCHASE_PLAN_SUMMARY
AS
SELECT
    pp.[RDC_CD] AS 'RDC-CD',
    pp.[RDC_NM] AS 'RDC-NM',
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT] AS 'WK-ST-DT',
    pp.[WK_END_DT] AS 'WK-END-DT',
    COUNT(DISTINCT pp.[MAJ_CAT]) AS 'CATEGORY-COUNT',

    -- Totals
    SUM(pp.[TTL_STK]) AS 'TTL-STK-SUM',
    SUM(pp.[OP_STK]) AS 'OP-STK-SUM',
    SUM(pp.[GRT_STK_Q]) AS 'GRT-STK-Q-SUM',

    -- GRT Consumption
    SUM(pp.[GRT_CONS_Q]) AS 'GRT-CONS-Q-SUM',
    AVG(pp.[GRT_CONS_PCT]) AS 'GRT-CONS-PCT-AVG',

    -- Sales
    SUM(pp.[CW_BGT_SALE_Q]) AS 'CW-BGT-SALE-Q-SUM',
    SUM(pp.[CW1_BGT_SALE_Q]) AS 'CW1-BGT-SALE-Q-SUM',
    SUM(pp.[CW2_BGT_SALE_Q]) AS 'CW2-BGT-SALE-Q-SUM',

    -- Transfer Out
    SUM(pp.[TTL_TRF_OUT_Q]) AS 'TTL-TRF-OUT-Q-SUM',
    SUM(pp.[CW_TRF_OUT_Q]) AS 'CW-TRF-OUT-Q-SUM',

    -- DC Opening/Closing
    SUM(pp.[BGT_DC_OP_STK_Q]) AS 'BGT-DC-OP-STK-Q-SUM',
    SUM(pp.[BGT_DC_CL_STK_Q]) AS 'BGT-DC-CL-STK-Q-SUM',

    -- Purchase Orders
    SUM(pp.[BGT_PUR_Q_INIT]) AS 'BGT-PUR-Q-INIT-SUM',
    SUM(pp.[POS_PO_RAISED]) AS 'POS-PO-RAISED-SUM',
    SUM(ISNULL(pp.[NEG_PO_RAISED], 0)) AS 'NEG-PO-RAISED-SUM',

    -- Confirm Stock
    SUM(pp.[PP_NET_BGT_CF_STK_Q]) AS 'PP-NET-BGT-CF-STK-Q-SUM',

    -- Delivery Pending
    SUM(pp.[DEL_PEND_Q]) AS 'DEL-PEND-Q-SUM',

    -- Stock Excess/Shortage
    SUM(pp.[DC_STK_EXCESS_Q]) AS 'DC-STK-EXCESS-Q-SUM',
    SUM(pp.[DC_STK_SHORT_Q]) AS 'DC-STK-SHORT-Q-SUM',
    SUM(pp.[ST_STK_EXCESS_Q]) AS 'ST-STK-EXCESS-Q-SUM',
    SUM(pp.[ST_STK_SHORT_Q]) AS 'ST-STK-SHORT-Q-SUM',
    SUM(pp.[CO_STK_EXCESS_Q]) AS 'CO-STK-EXCESS-Q-SUM',
    SUM(pp.[CO_STK_SHORT_Q]) AS 'CO-STK-SHORT-Q-SUM',

    -- Bin Requirements
    SUM(pp.[FRESH_BIN_REQ]) AS 'FRESH-BIN-REQ-SUM',
    SUM(pp.[GRT_BIN_REQ]) AS 'GRT-BIN-REQ-SUM'

FROM dbo.PURCHASE_PLAN pp
GROUP BY
    pp.[RDC_CD],
    pp.[RDC_NM],
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT],
    pp.[WK_END_DT];

GO

-- =====================================================
-- VIEW 3: VW_PURCHASE_PLAN_ALERTS
-- DESCRIPTION: Alerts for potential issues (shortages, overages, or active POs)
-- GRAIN: RDC_CD ÃƒÂ— MAJ_CAT ÃƒÂ— WEEK_ID (only rows with alerts)
-- =====================================================

IF OBJECT_ID('dbo.VW_PURCHASE_PLAN_ALERTS', 'V') IS NOT NULL
    DROP VIEW dbo.VW_PURCHASE_PLAN_ALERTS;
GO

CREATE VIEW dbo.VW_PURCHASE_PLAN_ALERTS
AS
SELECT
    pp.[ID],
    pp.[RDC_CD] AS 'RDC-CD',
    pp.[RDC_NM] AS 'RDC-NM',
    pp.[MAJ_CAT] AS 'MAJ-CAT',
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[WK_ST_DT] AS 'WK-ST-DT',
    pp.[WK_END_DT] AS 'WK-END-DT',

    -- Alert Reasons
    CASE WHEN pp.[DC_STK_SHORT_Q] > 0 THEN 'DC-STOCK-SHORT'
         WHEN pp.[CO_STK_SHORT_Q] > 0 THEN 'COMPANY-STOCK-SHORT'
         WHEN pp.[POS_PO_RAISED] > 0 THEN 'POSITIVE-PO'
         ELSE 'OTHER'
    END AS 'ALERT-TYPE',

    -- Relevant Metrics
    pp.[BGT_DC_CL_MBQ] AS 'BGT-DC-CL-MBQ',
    pp.[BGT_DC_CL_STK_Q] AS 'BGT-DC-CL-STK-Q',
    pp.[DC_STK_SHORT_Q] AS 'DC-STK-SHORT-Q',
    pp.[CO_STK_SHORT_Q] AS 'CO-STK-SHORT-Q',
    pp.[POS_PO_RAISED] AS 'POS-PO-RAISED',
    pp.[TTL_STK] AS 'TTL-STK',
    pp.[TTL_TRF_OUT_Q] AS 'TTL-TRF-OUT-Q',
    pp.[PP_NET_BGT_CF_STK_Q] AS 'PP-NET-BGT-CF-STK-Q',

    -- Context
    pp.[GRT_STK_Q] AS 'GRT-STK-Q',
    pp.[DEL_PEND_Q] AS 'DEL-PEND-Q',
    pp.[CREATED_DT]

FROM dbo.PURCHASE_PLAN pp
WHERE pp.[DC_STK_SHORT_Q] > 0
   OR pp.[CO_STK_SHORT_Q] > 0
   OR pp.[POS_PO_RAISED] > 0;

GO

-- =====================================================
-- VIEW 4: VW_PURCHASE_PLAN_CATEGORY_SUMMARY
-- DESCRIPTION: Aggregated summary by category across all RDCs
-- GRAIN: MAJ_CAT ÃƒÂ— WEEK_ID (sums across all RDCs)
-- =====================================================

IF OBJECT_ID('dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY', 'V') IS NOT NULL
    DROP VIEW dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY;
GO

CREATE VIEW dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY
AS
SELECT
    pp.[MAJ_CAT] AS 'MAJ-CAT',
    pp.[SSN],
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT] AS 'WK-ST-DT',
    pp.[WK_END_DT] AS 'WK-END-DT',
    COUNT(DISTINCT pp.[RDC_CD]) AS 'RDC-COUNT',

    -- Stock Levels
    SUM(pp.[TTL_STK]) AS 'TTL-STK-SUM',
    SUM(pp.[OP_STK]) AS 'OP-STK-SUM',
    AVG(pp.[TTL_STK]) AS 'TTL-STK-AVG',
    MIN(pp.[TTL_STK]) AS 'TTL-STK-MIN',
    MAX(pp.[TTL_STK]) AS 'TTL-STK-MAX',

    -- GRT Metrics
    SUM(pp.[GRT_STK_Q]) AS 'GRT-STK-Q-SUM',
    SUM(pp.[GRT_CONS_Q]) AS 'GRT-CONS-Q-SUM',
    AVG(pp.[GRT_CONS_PCT]) AS 'GRT-CONS-PCT-AVG',

    -- Sales (across all RDCs)
    SUM(pp.[CW_BGT_SALE_Q]) AS 'CW-BGT-SALE-Q-SUM',
    SUM(pp.[CW1_BGT_SALE_Q]) AS 'CW1-BGT-SALE-Q-SUM',
    SUM(pp.[CW2_BGT_SALE_Q]) AS 'CW2-BGT-SALE-Q-SUM',

    -- Transfer Out
    SUM(pp.[TTL_TRF_OUT_Q]) AS 'TTL-TRF-OUT-Q-SUM',

    -- DC Operations
    SUM(pp.[BGT_DC_OP_STK_Q]) AS 'BGT-DC-OP-STK-Q-SUM',
    SUM(pp.[BGT_DC_CL_STK_Q]) AS 'BGT-DC-CL-STK-Q-SUM',

    -- Purchase Orders
    SUM(pp.[BGT_PUR_Q_INIT]) AS 'BGT-PUR-Q-INIT-SUM',
    SUM(pp.[POS_PO_RAISED]) AS 'POS-PO-RAISED-SUM',
    COUNT(CASE WHEN pp.[POS_PO_RAISED] > 0 THEN 1 END) AS 'RDC-WITH-POS-PO',

    -- Shortages
    SUM(pp.[DC_STK_SHORT_Q]) AS 'DC-STK-SHORT-Q-SUM',
    SUM(pp.[CO_STK_SHORT_Q]) AS 'CO-STK-SHORT-Q-SUM',
    COUNT(CASE WHEN pp.[DC_STK_SHORT_Q] > 0 THEN 1 END) AS 'RDC-WITH-DC-SHORT',
    COUNT(CASE WHEN pp.[CO_STK_SHORT_Q] > 0 THEN 1 END) AS 'RDC-WITH-CO-SHORT',

    -- Excess
    SUM(pp.[DC_STK_EXCESS_Q]) AS 'DC-STK-EXCESS-Q-SUM',
    SUM(pp.[CO_STK_EXCESS_Q]) AS 'CO-STK-EXCESS-Q-SUM',

    -- Deliveries
    SUM(pp.[DEL_PEND_Q]) AS 'DEL-PEND-Q-SUM',

    -- Bin Requirements
    SUM(pp.[FRESH_BIN_REQ]) AS 'FRESH-BIN-REQ-SUM',
    SUM(pp.[GRT_BIN_REQ]) AS 'GRT-BIN-REQ-SUM'

FROM dbo.PURCHASE_PLAN pp
GROUP BY
    pp.[MAJ_CAT],
    pp.[SSN],
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT],
    pp.[WK_END_DT];

GO

-- =====================================================
-- HELPER VIEW: VW_WEEK_REFERENCE
-- DESCRIPTION: Provides week calendar context
-- =====================================================

IF OBJECT_ID('dbo.VW_WEEK_REFERENCE', 'V') IS NOT NULL
    DROP VIEW dbo.VW_WEEK_REFERENCE;
GO

CREATE VIEW dbo.VW_WEEK_REFERENCE
AS
SELECT
    [WEEK_ID],
    [WEEK_SEQ],
    [FY_WEEK],
    [FY_YEAR],
    [CAL_YEAR],
    [YEAR_WEEK],
    [WK_ST_DT] AS 'WK-ST-DT',
    [WK_END_DT] AS 'WK-END-DT',
    DATEDIFF(DAY, [WK_ST_DT], [WK_END_DT]) + 1 AS 'WEEK-DAY-COUNT'
FROM dbo.WEEK_CALENDAR;

GO

PRINT '';
PRINT '========================================';
PRINT 'PURCHASE PLAN VIEWS CREATED';
PRINT '========================================';
PRINT 'VW_PURCHASE_PLAN_DETAIL';
PRINT 'VW_PURCHASE_PLAN_SUMMARY';
PRINT 'VW_PURCHASE_PLAN_ALERTS';
PRINT 'VW_PURCHASE_PLAN_CATEGORY_SUMMARY';
PRINT 'VW_WEEK_REFERENCE';
PRINT '========================================';
GO

-- =====================================================
-- TEST QUERIES (uncomment to use)
-- =====================================================

-- SELECT TOP 100 * FROM dbo.VW_PURCHASE_PLAN_DETAIL
-- ORDER BY [RDC-CD], [MAJ-CAT], [WEEK_ID];
-- GO

-- SELECT TOP 50 * FROM dbo.VW_PURCHASE_PLAN_SUMMARY
-- WHERE [CO-STK-SHORT-Q-SUM] > 0 OR [POS-PO-RAISED-SUM] > 0
-- ORDER BY [RDC-CD], [WEEK_ID];
-- GO

-- SELECT TOP 100 * FROM dbo.VW_PURCHASE_PLAN_ALERTS
-- ORDER BY [ALERT-TYPE], [RDC-CD], [MAJ-CAT], [WEEK_ID];
-- GO

-- SELECT TOP 50 * FROM dbo.VW_PURCHASE_PLAN_CATEGORY_SUMMARY
-- WHERE [POS-PO-RAISED-SUM] > 0
-- ORDER BY [MAJ-CAT], [WEEK_ID];
-- GO

PRINT 'All Purchase Plan views are ready for use.';
GO
