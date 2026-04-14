-- ============================================================
-- ARS ALLOCATION MODULE — Article Auto Sale View with PD Calculations
-- Database: datav2 (Server 192.168.151.28)
-- Base Table: ARS_ST_ART_AUTO_SALE
-- ============================================================
USE datav2;
GO

IF OBJECT_ID('dbo.VW_ARS_ST_ART_AUTO_SALE', 'V') IS NOT NULL
    DROP VIEW dbo.VW_ARS_ST_ART_AUTO_SALE;
GO

CREATE VIEW dbo.VW_ARS_ST_ART_AUTO_SALE
AS
SELECT
    ID, ST, [GEN-ART], CLR, MJ,
    [CM-REM-DAYS],
    [NM-DAYS],
    [CM-AUTO-SALE-Q],
    [NM-AUTO-SALE-Q],

    -- CM Per Day Sale = CM-AUTO-SALE-Q / CM-REM-DAYS
    CASE WHEN [CM-REM-DAYS] > 0
         THEN [CM-AUTO-SALE-Q] / [CM-REM-DAYS]
         ELSE 0 END                                         AS CM_PD_SALE_Q,

    -- NM Per Day Sale = NM-AUTO-SALE-Q / NM-DAYS
    CASE WHEN [NM-DAYS] > 0
         THEN [NM-AUTO-SALE-Q] / [NM-DAYS]
         ELSE 0 END                                         AS NM_PD_SALE_Q,

    -- ST_ART_ALC_SALE_Q_PD = MIN(ALC_DAYS+HOLD_DAYS, CM_REM_DAYS) * CM_PD
    --                       + ((ALC_DAYS+HOLD_DAYS) - MIN(ALC_DAYS+HOLD_DAYS, CM_REM_DAYS)) * NM_PD
    -- Note: ALC_DAYS and HOLD_DAYS injected at query time from Store Master / Hold Days Master.
    --       This view exposes CM_PD and NM_PD for downstream computation.
    CASE WHEN [CM-REM-DAYS] > 0
         THEN [CM-AUTO-SALE-Q] / [CM-REM-DAYS]
         ELSE 0 END                                         AS ST_ART_ALC_SALE_Q_PD

FROM dbo.ARS_ST_ART_AUTO_SALE;
GO

PRINT '>> VW_ARS_ST_ART_AUTO_SALE created on datav2.';
GO
