/*==============================================================
  TRANSFER IN PLAN - MSSQL
  Script 6: VIEW - VW_TRF_IN_DETAIL

  BGT-ST-CL-MBQ = BGT-DISP-CL-Q (closing display)
                 + Next Week BGT-SALE-Q

  One row per ST-CD x MAJ-CAT x WEEK
  Includes current week + next 2 weeks BGT-SALE-Q columns
==============================================================*/

USE [planning];
GO

IF OBJECT_ID('dbo.VW_TRF_IN_DETAIL','V') IS NOT NULL
    DROP VIEW dbo.VW_TRF_IN_DETAIL;
GO

CREATE VIEW dbo.VW_TRF_IN_DETAIL
AS
SELECT
    T.ST_CD                                 AS [ST-CD],
    T.RDC_CD                                AS [RDC_CD],
    T.RDC_NM                                AS [RDC_NM],
    T.HUB_CD                                AS [HUB_CD],
    T.HUB_NM                                AS [HUB_NM],
    T.ST_NM                                 AS [ST-NM],

    ISNULL(T.SEG,     'NA')                 AS [SEG],
    ISNULL(T.DIV,     'NA')                 AS [DIV],
    ISNULL(T.SUB_DIV, 'NA')                 AS [SUB-DIV],

    T.MAJ_CAT                               AS [MAJ-CAT],

    SM.[OP-DATE]                            AS [UPC-ST-OP-DT],
    SM.[REF_ST CD]                          AS [REF-CD],
    SM.[STATUS]                             AS [ST STS],
    CAST(NULL AS VARCHAR(20))               AS [MAJ STS],

    T.SSN                                   AS [SSN],

    T.WEEK_ID                               AS [WEEK_ID],
    T.FY_WEEK                               AS [FY_WEEK],
    T.WK_ST_DT                              AS [WK_ST_DT],
    T.WK_END_DT                             AS [WK_END_DT],
    'WK-' + RIGHT('0' + CAST(T.FY_WEEK AS VARCHAR), 2)
        + ' (' + FORMAT(T.WK_ST_DT, 'd-M')
        + ' TO ' + FORMAT(T.WK_END_DT, 'd-M') + ')'
                                            AS [WEEK_LABEL],

    T.BGT_TTL_CF_OP_STK_Q                  AS [st-op-stk-q],
    T.BGT_DISP_CL_Q                        AS [BGT-DISP-CL-Q],

    -- BGT-ST-CL-MBQ = Closing Display + Next Week Sale
    T.BGT_ST_CL_MBQ                        AS [BGT-ST-CL-MBQ],

    T.BGT_TTL_CF_OP_STK_Q                  AS [BGT-TTL-CF-OP-STK-Q],
    T.NT_ACT_Q                              AS [NT-ACT-Q],
    T.NET_BGT_CF_STK_Q                     AS [NET-BGT-CF-STK-Q],

    -- Current week BGT-SALE-Q
    T.CM_BGT_SALE_Q                         AS [CW BGT-SALE-Q],

    -- Next week (CW+1) BGT-SALE-Q
    ISNULL(NW1.CM_BGT_SALE_Q, 0)           AS [CW+1 BGT-SALE-Q],

    -- Week after next (CW+2) BGT-SALE-Q
    ISNULL(NW2.CM_BGT_SALE_Q, 0)           AS [CW+2 BGT-SALE-Q],

    T.TRF_IN_STK_Q                         AS [TRF-IN-STK-Q],

    T.BGT_TTL_CF_CL_STK_Q                  AS [BGT-TTL-CF-CL-STK-Q],
    T.BGT_NT_ACT_Q                          AS [BGT-NT-ACT-Q],
    T.NET_ST_CL_STK_Q                      AS [NET-ST-CL-STK-Q],

    T.ST_CL_EXCESS_Q                       AS [ST-CL-EXCESS-Q],
    T.ST_CL_SHORT_Q                        AS [ST-CL-SHORT-Q]

FROM dbo.TRF_IN_PLAN T

LEFT JOIN dbo.MASTER_ST_MASTER SM
    ON SM.[ST CD] = T.ST_CD

-- Self-join: next week (WEEK_ID + 1) for same store x category
LEFT JOIN dbo.TRF_IN_PLAN NW1
    ON NW1.ST_CD   = T.ST_CD
    AND NW1.MAJ_CAT = T.MAJ_CAT
    AND NW1.WEEK_ID = T.WEEK_ID + 1

-- Self-join: week after next (WEEK_ID + 2) for same store x category
LEFT JOIN dbo.TRF_IN_PLAN NW2
    ON NW2.ST_CD   = T.ST_CD
    AND NW2.MAJ_CAT = T.MAJ_CAT
    AND NW2.WEEK_ID = T.WEEK_ID + 2;
GO

PRINT '>> VW_TRF_IN_DETAIL created successfully.';
GO
