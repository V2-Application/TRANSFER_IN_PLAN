-- =====================================================
-- FILE: 14_RECREATE_VW_PURCHASE_PLAN_DETAIL.sql
-- PURPOSE: Recreate VW_PURCHASE_PLAN_DETAIL with all 42 columns
--          matching PurchasePlan.xlsx column headers exactly
-- DATABASE: [planning]
-- PREREQUISITE: Run 13_PATCH_ADD_MISSING_PP_COLUMNS.sql first
--
-- ALL 42 COLUMNS:
--  1. BGT-DISP CL-Q          22. TRF CW+1
--  2. CW BGT-SALE-Q           23. TRF CW+2
--  3. CW+1 BGT-SALE-Q         24. TRF CW+3
--  4. CW+2 BGT-SALE-Q         25. TRF CW+4
--  5. CW+3 BGT-SALE-Q         26. BGT ST-CL MBQ
--  6. CW+4 BGT-SALE-Q         27. NET BGT ST-CL STK-Q
--  7. CW+5 BGT-SALE-Q         28. NET-SSNL CL-STK-Q
--  8. BGT-ST OP-MBQ           29. BGT-DC CL-MBQ
--  9. NET ST-OP STK-Q         30. BGT-DC CL-STK-Q
-- 10. BGT-DC OP-STK-Q         31. BGT PUR-Q (INITIAL)
-- 11. NT ACT-Q                32. POS PO TO BE RAISED
-- 12. BGT CF-STK-Q            33. NEG PO TO BE RAISED
-- 13. TTL-GRT-STK-Q           34. BGT-CO CL-STK-Q
-- 14. OP-GRT-STK-Q            35. DC-STK EXCESS-Q
-- 15. NT-ACT-GRT-STK          36. DC-STK SHORT-Q
-- 16. GRT CONS%               37. ST-STK EXCESS-Q
-- 17. GRT CONS-Q              38. ST-STK SHORT-Q
-- 18. DEL PEND-Q              39. CO-STK EXCESS-Q
-- 19. NET-BGT CF-STK-Q        40. CO-STK SHORT-Q
-- 20. TRF-OUT                 41. FRESH BIN REQ
-- 21. TRF CW                  42. GRT BIN REQ
-- =====================================================

USE [planning];
GO

IF OBJECT_ID('dbo.VW_PURCHASE_PLAN_DETAIL', 'V') IS NOT NULL
    DROP VIEW dbo.VW_PURCHASE_PLAN_DETAIL;
GO

CREATE VIEW dbo.VW_PURCHASE_PLAN_DETAIL
AS
SELECT
    -- ===== IDENTIFIERS =====
    pp.[ID],
    pp.[RDC_CD]                             AS [RDC-CD],
    pp.[RDC_NM]                             AS [RDC-NM],
    pp.[MAJ_CAT]                            AS [MAJ-CAT],
    pp.[SSN],
    pp.[WEEK_ID],
    pp.[FY_WEEK],
    pp.[FY_YEAR],
    pp.[WK_ST_DT]                           AS [WK-ST-DT],
    pp.[WK_END_DT]                          AS [WK-END-DT],

    -- Week label with date range: "WK-01 (3-4 TO 9-4)"
    'WK-' + RIGHT('0' + CAST(pp.[FY_WEEK] AS VARCHAR), 2)
        + ' (' + FORMAT(pp.[WK_ST_DT], 'd-M')
        + ' TO ' + FORMAT(pp.[WK_END_DT], 'd-M') + ')'
                                            AS [WEEK_LABEL],

    -- ===== REFERENCE STOCK =====
    pp.[DC_STK_Q]                           AS [DC-STK-Q],
    pp.[GRT_STK_Q]                          AS [GRT-STK-Q],
    pp.[S_GRT_STK_Q]                        AS [S-GRT-STK-Q],
    pp.[W_GRT_STK_Q]                        AS [W-GRT-STK-Q],
    pp.[BIN_CAP_DC_TEAM]                    AS [BIN-CAP-DC-TEAM],
    pp.[BIN_CAP],

    -- ===== 42 PLAN COLUMNS (matching Excel headers) =====

    -- 1. BGT-DISP CL-Q
    pp.[BGT_DISP_CL_Q]                     AS [BGT-DISP CL-Q],

    -- 2-7. BGT SALE-Q (CW through CW+5) Ã¢Â€Â” week labels are dynamic per row
    pp.[CW_BGT_SALE_Q]                      AS [CW BGT-SALE-Q],
    pp.[CW1_BGT_SALE_Q]                     AS [CW+1 BGT-SALE-Q],
    pp.[CW2_BGT_SALE_Q]                     AS [CW+2 BGT-SALE-Q],
    pp.[CW3_BGT_SALE_Q]                     AS [CW+3 BGT-SALE-Q],
    pp.[CW4_BGT_SALE_Q]                     AS [CW+4 BGT-SALE-Q],
    pp.[CW5_BGT_SALE_Q]                     AS [CW+5 BGT-SALE-Q],

    -- 8. BGT-ST OP-MBQ
    pp.[BGT_ST_OP_MBQ]                      AS [BGT-ST OP-MBQ],

    -- 9. NET ST-OP STK-Q
    pp.[NET_ST_OP_STK_Q]                    AS [NET ST-OP STK-Q],

    -- 10. BGT-DC OP-STK-Q
    pp.[BGT_DC_OP_STK_Q]                    AS [BGT-DC OP-STK-Q],

    -- 11. NT ACT-Q
    pp.[PP_NT_ACT_Q]                        AS [NT ACT-Q],

    -- 12. BGT CF-STK-Q
    pp.[BGT_CF_STK_Q]                       AS [BGT CF-STK-Q],

    -- 13. TTL-GRT-STK-Q
    pp.[TTL_STK]                            AS [TTL-GRT-STK-Q],

    -- 14. OP-GRT-STK-Q
    pp.[OP_STK]                             AS [OP-GRT-STK-Q],

    -- 15. NT-ACT-GRT-STK
    pp.[NT_ACT_STK]                         AS [NT-ACT-GRT-STK],

    -- 16. GRT CONS%
    pp.[GRT_CONS_PCT]                       AS [GRT CONS%],

    -- 17. GRT CONS-Q
    pp.[GRT_CONS_Q]                         AS [GRT CONS-Q],

    -- 18. DEL PEND-Q
    pp.[DEL_PEND_Q]                         AS [DEL PEND-Q],

    -- 19. NET-BGT CF-STK-Q
    pp.[PP_NET_BGT_CF_STK_Q]               AS [NET-BGT CF-STK-Q],

    -- 20. TRF-OUT (total across all weeks)
    pp.[TTL_TRF_OUT_Q]                      AS [TRF-OUT],

    -- 21. TRF CW (current week transfer out)
    pp.[CW_TRF_OUT_Q]                       AS [TRF CW],

    -- 22. TRF CW+1
    pp.[CW1_TRF_OUT_Q]                      AS [TRF CW+1],

    -- 23. TRF CW+2  (NEW Ã¢Â€Â” from patch)
    pp.[CW2_TRF_OUT_Q]                      AS [TRF CW+2],

    -- 24. TRF CW+3  (NEW Ã¢Â€Â” from patch)
    pp.[CW3_TRF_OUT_Q]                      AS [TRF CW+3],

    -- 25. TRF CW+4  (NEW Ã¢Â€Â” from patch)
    pp.[CW4_TRF_OUT_Q]                      AS [TRF CW+4],

    -- 26. BGT ST-CL MBQ
    pp.[BGT_ST_CL_MBQ]                      AS [BGT ST-CL MBQ],

    -- 27. NET BGT ST-CL STK-Q
    pp.[NET_BGT_ST_CL_STK_Q]               AS [NET BGT ST-CL STK-Q],

    -- 28. NET-SSNL CL-STK-Q
    pp.[NET_SSNL_CL_STK_Q]                 AS [NET-SSNL CL-STK-Q],

    -- 29. BGT-DC CL-MBQ
    pp.[BGT_DC_CL_MBQ]                      AS [BGT-DC CL-MBQ],

    -- 30. BGT-DC CL-STK-Q
    pp.[BGT_DC_CL_STK_Q]                    AS [BGT-DC CL-STK-Q],

    -- 31. BGT PUR-Q (INITIAL)
    pp.[BGT_PUR_Q_INIT]                     AS [BGT PUR-Q (INITIAL)],

    -- 32. POS PO TO BE RAISED
    pp.[POS_PO_RAISED]                      AS [POS PO TO BE RAISED],

    -- 33. NEG PO TO BE RAISED
    pp.[NEG_PO_RAISED]                      AS [NEG PO TO BE RAISED],

    -- 34. BGT-CO CL-STK-Q
    pp.[BGT_CO_CL_STK_Q]                    AS [BGT-CO CL-STK-Q],

    -- 35. DC-STK EXCESS-Q
    pp.[DC_STK_EXCESS_Q]                    AS [DC-STK EXCESS-Q],

    -- 36. DC-STK SHORT-Q
    pp.[DC_STK_SHORT_Q]                     AS [DC-STK SHORT-Q],

    -- 37. ST-STK EXCESS-Q
    pp.[ST_STK_EXCESS_Q]                    AS [ST-STK EXCESS-Q],

    -- 38. ST-STK SHORT-Q
    pp.[ST_STK_SHORT_Q]                     AS [ST-STK SHORT-Q],

    -- 39. CO-STK EXCESS-Q
    pp.[CO_STK_EXCESS_Q]                    AS [CO-STK EXCESS-Q],

    -- 40. CO-STK SHORT-Q
    pp.[CO_STK_SHORT_Q]                     AS [CO-STK SHORT-Q],

    -- 41. FRESH BIN REQ
    pp.[FRESH_BIN_REQ]                      AS [FRESH BIN REQ],

    -- 42. GRT BIN REQ
    pp.[GRT_BIN_REQ]                        AS [GRT BIN REQ],

    -- ===== AUDIT =====
    pp.[CREATED_DT],
    pp.[CREATED_BY]

FROM dbo.PURCHASE_PLAN pp;
GO

PRINT '>> VW_PURCHASE_PLAN_DETAIL recreated with all 42 plan columns.';
PRINT '>> Usage: SELECT * FROM dbo.VW_PURCHASE_PLAN_DETAIL ORDER BY [RDC-CD], [MAJ-CAT], [WEEK_ID];';
GO
