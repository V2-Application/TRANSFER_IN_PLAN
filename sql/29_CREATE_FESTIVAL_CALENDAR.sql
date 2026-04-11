-- ============================================================
-- FESTIVAL CALENDAR — State x Month uplift % for budget adjustment
-- Based on V-0063 REF-LIST: 13 festivals mapped to states
-- Run against 'planning' database
-- ============================================================
USE planning;
GO

IF OBJECT_ID('dbo.FESTIVAL_CALENDAR', 'U') IS NULL
CREATE TABLE dbo.FESTIVAL_CALENDAR (
    ID              INT IDENTITY(1,1) NOT NULL,
    FESTIVAL        VARCHAR(50)     NOT NULL,
    STATE           VARCHAR(50)     NOT NULL,
    MONTH_NUM       INT             NOT NULL,       -- 1-12
    IMPACT_PCT      DECIMAL(8,4)    NOT NULL,       -- e.g. 0.15 = 15% uplift
    DURATION_DAYS   INT             NULL DEFAULT 7,
    CONSTRAINT PK_FESTIVAL_CALENDAR PRIMARY KEY (ID)
);
GO

-- ── Seed: Major Indian festivals mapped to states + months ──
-- Source: V-0063 REF-LIST sheet (13 festival flags per store by state)
IF NOT EXISTS (SELECT 1 FROM dbo.FESTIVAL_CALENDAR)
BEGIN
    -- DIWALI (Oct-Nov) — pan-India, biggest retail event
    INSERT INTO dbo.FESTIVAL_CALENDAR (FESTIVAL, STATE, MONTH_NUM, IMPACT_PCT, DURATION_DAYS) VALUES
    ('DIWALI', 'UP', 10, 0.25, 15), ('DIWALI', 'UP', 11, 0.15, 10),
    ('DIWALI', 'BIHAR', 10, 0.25, 15), ('DIWALI', 'BIHAR', 11, 0.15, 10),
    ('DIWALI', 'JHARKHAND', 10, 0.25, 15), ('DIWALI', 'JHARKHAND', 11, 0.15, 10),
    ('DIWALI', 'RAJASTHAN', 10, 0.25, 15), ('DIWALI', 'MADHYA PRADESH', 10, 0.25, 15),
    ('DIWALI', 'DELHI', 10, 0.20, 15), ('DIWALI', 'HARYANA', 10, 0.20, 15),
    ('DIWALI', 'PUNJAB', 10, 0.20, 15), ('DIWALI', 'UTTARAKHAND', 10, 0.20, 15),
    ('DIWALI', 'WEST BENGAL', 10, 0.15, 10), ('DIWALI', 'ODISHA', 10, 0.15, 10),
    ('DIWALI', 'KARNATAKA', 10, 0.15, 10), ('DIWALI', 'TAMIL NADU', 10, 0.10, 10),
    ('DIWALI', 'ASSAM', 10, 0.15, 10), ('DIWALI', 'TELANGANA', 10, 0.15, 10),
    ('DIWALI', 'MAHARASHTRA', 10, 0.20, 15), ('DIWALI', 'GUJARAT', 10, 0.20, 15),

    -- DURGA PUJA / NAVRATRI (Oct) — East + North
    ('D_PUJA', 'WEST BENGAL', 10, 0.30, 10), ('D_PUJA', 'ODISHA', 10, 0.20, 10),
    ('D_PUJA', 'JHARKHAND', 10, 0.20, 10), ('D_PUJA', 'ASSAM', 10, 0.25, 10),
    ('D_PUJA', 'BIHAR', 10, 0.15, 10), ('D_PUJA', 'TRIPURA', 10, 0.20, 10),

    -- CHHAT PUJA (Nov) — Bihar, Jharkhand, UP
    ('CHHAT', 'BIHAR', 11, 0.25, 7), ('CHHAT', 'JHARKHAND', 11, 0.20, 7),
    ('CHHAT', 'UP', 11, 0.15, 7), ('CHHAT', 'DELHI', 11, 0.08, 5),

    -- EID (month varies by year — approximate Apr/May)
    ('EID', 'UP', 4, 0.15, 5), ('EID', 'WEST BENGAL', 4, 0.12, 5),
    ('EID', 'BIHAR', 4, 0.12, 5), ('EID', 'JHARKHAND', 4, 0.10, 5),
    ('EID', 'DELHI', 4, 0.10, 5), ('EID', 'ASSAM', 4, 0.12, 5),
    ('EID', 'KARNATAKA', 4, 0.08, 5), ('EID', 'TELANGANA', 4, 0.08, 5),

    -- BAKRI EID (month varies — approximate Jun/Jul)
    ('B_EID', 'UP', 6, 0.10, 3), ('B_EID', 'WEST BENGAL', 6, 0.08, 3),
    ('B_EID', 'BIHAR', 6, 0.08, 3), ('B_EID', 'DELHI', 6, 0.06, 3),

    -- HOLI (Mar) — North India
    ('HOLI', 'UP', 3, 0.15, 5), ('HOLI', 'BIHAR', 3, 0.15, 5),
    ('HOLI', 'RAJASTHAN', 3, 0.15, 5), ('HOLI', 'MADHYA PRADESH', 3, 0.12, 5),
    ('HOLI', 'DELHI', 3, 0.12, 5), ('HOLI', 'HARYANA', 3, 0.12, 5),
    ('HOLI', 'JHARKHAND', 3, 0.12, 5), ('HOLI', 'UTTARAKHAND', 3, 0.12, 5),
    ('HOLI', 'WEST BENGAL', 3, 0.08, 3), ('HOLI', 'PUNJAB', 3, 0.12, 5),

    -- PONGAL (Jan) — Tamil Nadu, Karnataka
    ('PONGAL', 'TAMIL NADU', 1, 0.20, 5), ('PONGAL', 'KARNATAKA', 1, 0.10, 3),

    -- UGADI (Mar/Apr) — South India
    ('UGADI', 'KARNATAKA', 3, 0.12, 3), ('UGADI', 'TELANGANA', 3, 0.12, 3),
    ('UGADI', 'ANDHRA PRADESH', 3, 0.12, 3),

    -- BIHU (Apr) — Assam
    ('BIHU', 'ASSAM', 4, 0.20, 7),

    -- GANESH PUJA (Sep) — Maharashtra, Karnataka
    ('G_PUJA', 'MAHARASHTRA', 9, 0.15, 10), ('G_PUJA', 'KARNATAKA', 9, 0.10, 5),

    -- SARASWATI PUJA (Feb) — East
    ('S_PUJA', 'WEST BENGAL', 2, 0.08, 3), ('S_PUJA', 'BIHAR', 2, 0.06, 3),
    ('S_PUJA', 'ODISHA', 2, 0.06, 3), ('S_PUJA', 'ASSAM', 2, 0.06, 3),

    -- KRISHNA JANMASHTAMI / P_ASTHMI (Aug) — North
    ('P_ASTHMI', 'UP', 8, 0.10, 3), ('P_ASTHMI', 'RAJASTHAN', 8, 0.10, 3),
    ('P_ASTHMI', 'MADHYA PRADESH', 8, 0.08, 3), ('P_ASTHMI', 'DELHI', 8, 0.06, 3),
    ('P_ASTHMI', 'HARYANA', 8, 0.08, 3), ('P_ASTHMI', 'MAHARASHTRA', 8, 0.08, 3),

    -- RAJJO (local — specific states)
    ('RAJJO', 'GUJARAT', 9, 0.10, 5), ('RAJJO', 'RAJASTHAN', 9, 0.08, 5);
END
GO

-- ── Min/Max caps per category (add to SALE_BUDGET_CONFIG) ──
IF NOT EXISTS (SELECT 1 FROM dbo.SALE_BUDGET_CONFIG WHERE CONFIG_KEY = 'MIN_BGT_QTY_PER_STORE')
BEGIN
    INSERT INTO dbo.SALE_BUDGET_CONFIG (CONFIG_KEY, CONFIG_VALUE, DESCRIPTION) VALUES
    ('MIN_BGT_QTY_PER_STORE', '5', 'Minimum budget qty per store x category x month'),
    ('MAX_BGT_GROWTH_CAP', '3.00', 'Max allowed growth multiplier (3x = 200% growth)'),
    ('RECONCILIATION_ENABLED', '1', 'Enable bottom-up reconciliation (1=yes, 0=no)'),
    ('RECONCILIATION_TOLERANCE', '0.05', 'Allow 5% variance before reconciling');
END
GO

PRINT 'Festival calendar + config enhancements created.';
GO
