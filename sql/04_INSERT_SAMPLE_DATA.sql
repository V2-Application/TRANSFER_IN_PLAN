/*==============================================================
  TRANSFER IN PLAN - MSSQL SETUP
  Script 4 of 5: INSERT SAMPLE DATA
  Populates all reference tables with realistic sample data
  for 8 stores, 5 major categories, 52 weeks.
==============================================================*/

USE [planning];
GO

--------------------------------------------------------------
-- 1. WEEK_CALENDAR - 52 weeks starting 2026-04-03
--------------------------------------------------------------
TRUNCATE TABLE dbo.WEEK_CALENDAR;

DECLARE @i INT = 1;
DECLARE @BaseDate DATE = '2026-04-03';

WHILE @i <= 52
BEGIN
    INSERT INTO dbo.WEEK_CALENDAR (WEEK_ID, WEEK_SEQ, FY_WEEK, FY_YEAR, CAL_YEAR, YEAR_WEEK, WK_ST_DT, WK_END_DT)
    VALUES (
        @i,                                         -- WEEK_ID
        @i,                                         -- WEEK_SEQ
        @i,                                         -- FY_WEEK
        2026,                                       -- FY_YEAR
        YEAR(DATEADD(WEEK, @i-1, @BaseDate)),       -- CAL_YEAR
        CAST(YEAR(DATEADD(WEEK, @i-1, @BaseDate)) AS VARCHAR)
            + '-W' + RIGHT('0' + CAST(@i AS VARCHAR), 2),  -- YEAR_WEEK
        DATEADD(WEEK, @i-1, @BaseDate),             -- WK_ST_DT
        DATEADD(DAY, 6, DATEADD(WEEK, @i-1, @BaseDate))  -- WK_END_DT
    );
    SET @i = @i + 1;
END;
GO

PRINT '>> WEEK_CALENDAR: 52 weeks inserted.';
GO

--------------------------------------------------------------
-- 2. MASTER_ST_MASTER - 8 stores
--------------------------------------------------------------
DELETE FROM dbo.MASTER_ST_MASTER;

INSERT INTO dbo.MASTER_ST_MASTER
    ([ST CD],[ST NM],[RDC_CD],[RDC_NM],[HUB_CD],[HUB_NM],[STATUS],[GRID_ST_STS],[OP-DATE],[AREA],[STATE],[REF STATE],[SALE GRP],[REF_ST CD],[REF_ST NM],[REF-GRP-NEW],[REF-GRP-OLD],[Date])
VALUES
    ('ST001','DELHI STORE',      'RDC01','RDC NORTH','HUB01','HUB DELHI',    'NEW','A','2020-01-15','NORTH','DELHI',     'DELHI',  'GRP-N1','ST001','DELHI',      'NGRP-1','NGRP-OLD1','2026-04-01'),
    ('ST002','MUMBAI STORE',     'RDC02','RDC WEST', 'HUB02','HUB MUMBAI',   'NEW','A','2019-06-01','WEST', 'MAHARASHTRA','MUMBAI', 'GRP-W1','ST002','MUMBAI',     'WGRP-1','WGRP-OLD1','2026-04-01'),
    ('ST003','BANGALORE STORE',  'RDC03','RDC SOUTH','HUB03','HUB BANGALORE','NEW','A','2020-03-10','SOUTH','KARNATAKA', 'BANGALORE','GRP-S1','ST003','BANGALORE','SGRP-1','SGRP-OLD1','2026-04-01'),
    ('ST004','CHENNAI STORE',    'RDC03','RDC SOUTH','HUB04','HUB CHENNAI',  'NEW','A','2021-01-20','SOUTH','TAMILNADU', 'CHENNAI', 'GRP-S2','ST004','CHENNAI',   'SGRP-2','SGRP-OLD2','2026-04-01'),
    ('ST005','KOLKATA STORE',    'RDC04','RDC EAST', 'HUB05','HUB KOLKATA',  'NEW','A','2020-08-15','EAST', 'WESTBENGAL','KOLKATA', 'GRP-E1','ST005','KOLKATA',   'EGRP-1','EGRP-OLD1','2026-04-01'),
    ('ST006','HYDERABAD STORE',  'RDC03','RDC SOUTH','HUB06','HUB HYDERABAD','NEW','A','2021-05-01','SOUTH','TELANGANA', 'HYDERABAD','GRP-S3','ST006','HYDERABAD','SGRP-3','SGRP-OLD3','2026-04-01'),
    ('ST007','PUNE STORE',       'RDC02','RDC WEST', 'HUB07','HUB PUNE',     'NEW','A','2022-02-14','WEST', 'MAHARASHTRA','PUNE',   'GRP-W2','ST007','PUNE',      'WGRP-2','WGRP-OLD2','2026-04-01'),
    ('ST008','JAIPUR STORE',     'RDC01','RDC NORTH','HUB08','HUB JAIPUR',   'NEW','A','2021-11-01','NORTH','RAJASTHAN', 'JAIPUR', 'GRP-N2','ST008','JAIPUR',    'NGRP-2','NGRP-OLD2','2026-04-01');
GO

PRINT '>> MASTER_ST_MASTER: 8 stores inserted.';
GO

--------------------------------------------------------------
-- 3. MASTER_BIN_CAPACITY - 5 major categories
--------------------------------------------------------------
DELETE FROM dbo.MASTER_BIN_CAPACITY;

INSERT INTO dbo.MASTER_BIN_CAPACITY ([MAJ-CAT],[BIN CAP DC TEAM],[BIN CAP])
VALUES
    ('APPAREL',     150.00, 120.00),
    ('FOOTWEAR',    100.00,  80.00),
    ('ACCESSORIES', 250.00, 200.00),
    ('ELECTRONICS',  80.00,  60.00),
    ('HOME',        130.00, 100.00);
GO

PRINT '>> MASTER_BIN_CAPACITY: 5 categories inserted.';
GO

--------------------------------------------------------------
-- 4. MASTER_GRT_CONS_percentage - Season consumption %
--------------------------------------------------------------
DELETE FROM dbo.MASTER_GRT_CONS_percentage;

INSERT INTO dbo.MASTER_GRT_CONS_percentage ([SSN],
    [WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],[WK-9],[WK-10],[WK-11],[WK-12],
    [WK-13],[WK-14],[WK-15],[WK-16],[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],
    [WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],[WK-33],[WK-34],[WK-35],[WK-36],
    [WK-37],[WK-38],[WK-39],[WK-40],[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48],[2])
VALUES
    ('S',
     0.030,0.030,0.025,0.025,0.020,0.020,0.020,0.018,0.018,0.018,0.015,0.015,
     0.015,0.012,0.012,0.012,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,
     0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,
     0.010,0.010,0.012,0.012,0.015,0.015,0.018,0.018,0.020,0.020,0.025,0.025, 0),
    ('W',
     0.008,0.008,0.008,0.008,0.010,0.010,0.012,0.012,0.015,0.015,0.018,0.018,
     0.020,0.020,0.025,0.025,0.030,0.030,0.030,0.030,0.028,0.028,0.025,0.025,
     0.030,0.030,0.028,0.028,0.025,0.025,0.020,0.020,0.018,0.018,0.015,0.015,
     0.012,0.012,0.010,0.010,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008, 0),
    ('PW',
     0.010,0.010,0.010,0.010,0.012,0.012,0.015,0.015,0.020,0.020,0.025,0.025,
     0.030,0.030,0.030,0.028,0.028,0.025,0.025,0.020,0.020,0.018,0.018,0.015,
     0.015,0.012,0.012,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.010,
     0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.010,0.010, 0),
    ('PS',
     0.020,0.020,0.018,0.018,0.015,0.015,0.012,0.012,0.010,0.010,0.010,0.010,
     0.010,0.010,0.010,0.010,0.010,0.010,0.010,0.012,0.012,0.015,0.015,0.018,
     0.018,0.020,0.020,0.025,0.025,0.025,0.025,0.028,0.028,0.030,0.030,0.030,
     0.028,0.025,0.025,0.020,0.020,0.018,0.015,0.015,0.012,0.010,0.010,0.010, 0);
GO

PRINT '>> MASTER_GRT_CONS_percentage: 4 seasons inserted.';
GO

--------------------------------------------------------------
-- 5. QTY_SALE_QTY - Weekly sale plan per store x majcat
--    (generated via loop with realistic seasonal patterns)
--------------------------------------------------------------
DELETE FROM dbo.QTY_SALE_QTY;

DECLARE @stores TABLE (ST_CD VARCHAR(20), BaseSale INT);
INSERT INTO @stores VALUES
    ('ST001',180),('ST002',220),('ST003',160),('ST004',140),
    ('ST005',130),('ST006',150),('ST007',170),('ST008',120);

DECLARE @cats TABLE (MAJ_CAT VARCHAR(50), CatMult DECIMAL(5,2));
INSERT INTO @cats VALUES
    ('APPAREL',1.0),('FOOTWEAR',0.6),('ACCESSORIES',0.8),('ELECTRONICS',0.4),('HOME',0.5);

DECLARE @sql NVARCHAR(MAX);
DECLARE @st VARCHAR(20), @mc VARCHAR(50), @base INT;
DECLARE @cm DECIMAL(5,2);

DECLARE cur CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.ST_CD, s.BaseSale, c.MAJ_CAT, c.CatMult FROM @stores s CROSS JOIN @cats c;
OPEN cur;
FETCH NEXT FROM cur INTO @st, @base, @mc, @cm;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'INSERT INTO dbo.QTY_SALE_QTY ([ST-CD],[MAJ-CAT],'
        + '[WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],'
        + '[WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],'
        + '[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],'
        + '[WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],'
        + '[WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],'
        + '[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]) '
        + 'VALUES (''' + @st + ''',''' + @mc + ''',';

    DECLARE @w INT = 1;
    DECLARE @vals NVARCHAR(MAX) = '';
    WHILE @w <= 48
    BEGIN
        -- Seasonal multiplier: summer peaks WK 1-12, winter peaks WK 25-36
        DECLARE @seasonal DECIMAL(5,2) = CASE
            WHEN @w BETWEEN 1  AND 12 THEN 1.2
            WHEN @w BETWEEN 13 AND 24 THEN 0.9
            WHEN @w BETWEEN 25 AND 36 THEN 1.3
            ELSE 1.0 END;

        DECLARE @val INT = ROUND(@base * @cm * @seasonal + (ABS(CHECKSUM(NEWID())) % 40 - 20), 0);
        IF @val < 10 SET @val = 10;

        SET @vals = @vals + CAST(@val AS VARCHAR);
        IF @w < 48 SET @vals = @vals + ',';
        SET @w = @w + 1;
    END

    SET @sql = @sql + @vals + ')';
    EXEC sp_executesql @sql;

    FETCH NEXT FROM cur INTO @st, @base, @mc, @cm;
END

CLOSE cur;
DEALLOCATE cur;
GO

PRINT '>> QTY_SALE_QTY: 40 rows (8 stores x 5 cats) inserted.';
GO

--------------------------------------------------------------
-- 6. QTY_DISP_QTY - Weekly display plan (similar pattern)
--------------------------------------------------------------
DELETE FROM dbo.QTY_DISP_QTY;

DECLARE @stores2 TABLE (ST_CD VARCHAR(20), BaseDisp INT);
INSERT INTO @stores2 VALUES
    ('ST001',60),('ST002',75),('ST003',50),('ST004',45),
    ('ST005',40),('ST006',55),('ST007',65),('ST008',35);

DECLARE @cats2 TABLE (MAJ_CAT VARCHAR(50), CatMult DECIMAL(5,2));
INSERT INTO @cats2 VALUES
    ('APPAREL',1.0),('FOOTWEAR',0.7),('ACCESSORIES',0.9),('ELECTRONICS',0.5),('HOME',0.6);

DECLARE @sql2 NVARCHAR(MAX);
DECLARE @st2 VARCHAR(20), @mc2 VARCHAR(50), @base2 INT;
DECLARE @cm2x DECIMAL(5,2);

DECLARE cur2 CURSOR LOCAL FAST_FORWARD FOR
    SELECT s.ST_CD, s.BaseDisp, c.MAJ_CAT, c.CatMult FROM @stores2 s CROSS JOIN @cats2 c;
OPEN cur2;
FETCH NEXT FROM cur2 INTO @st2, @base2, @mc2, @cm2x;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql2 = 'INSERT INTO dbo.QTY_DISP_QTY ([ST-CD],[MAJ-CAT],'
        + '[WK-1],[WK-2],[WK-3],[WK-4],[WK-5],[WK-6],[WK-7],[WK-8],'
        + '[WK-9],[WK-10],[WK-11],[WK-12],[WK-13],[WK-14],[WK-15],[WK-16],'
        + '[WK-17],[WK-18],[WK-19],[WK-20],[WK-21],[WK-22],[WK-23],[WK-24],'
        + '[WK-25],[WK-26],[WK-27],[WK-28],[WK-29],[WK-30],[WK-31],[WK-32],'
        + '[WK-33],[WK-34],[WK-35],[WK-36],[WK-37],[WK-38],[WK-39],[WK-40],'
        + '[WK-41],[WK-42],[WK-43],[WK-44],[WK-45],[WK-46],[WK-47],[WK-48]) '
        + 'VALUES (''' + @st2 + ''',''' + @mc2 + ''',';

    DECLARE @w2 INT = 1;
    DECLARE @vals2 NVARCHAR(MAX) = '';
    WHILE @w2 <= 48
    BEGIN
        DECLARE @dval INT = ROUND(@base2 * @cm2x + (ABS(CHECKSUM(NEWID())) % 20 - 10), 0);
        IF @dval < 5 SET @dval = 5;
        SET @vals2 = @vals2 + CAST(@dval AS VARCHAR);
        IF @w2 < 48 SET @vals2 = @vals2 + ',';
        SET @w2 = @w2 + 1;
    END

    SET @sql2 = @sql2 + @vals2 + ')';
    EXEC sp_executesql @sql2;

    FETCH NEXT FROM cur2 INTO @st2, @base2, @mc2, @cm2x;
END

CLOSE cur2;
DEALLOCATE cur2;
GO

PRINT '>> QTY_DISP_QTY: 40 rows inserted.';
GO

--------------------------------------------------------------
-- 7. QTY_ST_STK_Q - Current store stock snapshot
--------------------------------------------------------------
DELETE FROM dbo.QTY_ST_STK_Q;

INSERT INTO dbo.QTY_ST_STK_Q ([ST_CD],[MAJ_CAT],[STK_QTY],[DATE])
VALUES
    ('ST001','APPAREL',520,'2026-04-01'),('ST001','FOOTWEAR',310,'2026-04-01'),
    ('ST001','ACCESSORIES',420,'2026-04-01'),('ST001','ELECTRONICS',180,'2026-04-01'),
    ('ST001','HOME',350,'2026-04-01'),
    ('ST002','APPAREL',680,'2026-04-01'),('ST002','FOOTWEAR',400,'2026-04-01'),
    ('ST002','ACCESSORIES',550,'2026-04-01'),('ST002','ELECTRONICS',220,'2026-04-01'),
    ('ST002','HOME',430,'2026-04-01'),
    ('ST003','APPAREL',450,'2026-04-01'),('ST003','FOOTWEAR',270,'2026-04-01'),
    ('ST003','ACCESSORIES',380,'2026-04-01'),('ST003','ELECTRONICS',150,'2026-04-01'),
    ('ST003','HOME',300,'2026-04-01'),
    ('ST004','APPAREL',380,'2026-04-01'),('ST004','FOOTWEAR',230,'2026-04-01'),
    ('ST004','ACCESSORIES',320,'2026-04-01'),('ST004','ELECTRONICS',120,'2026-04-01'),
    ('ST004','HOME',260,'2026-04-01'),
    ('ST005','APPAREL',340,'2026-04-01'),('ST005','FOOTWEAR',200,'2026-04-01'),
    ('ST005','ACCESSORIES',280,'2026-04-01'),('ST005','ELECTRONICS',110,'2026-04-01'),
    ('ST005','HOME',230,'2026-04-01'),
    ('ST006','APPAREL',410,'2026-04-01'),('ST006','FOOTWEAR',250,'2026-04-01'),
    ('ST006','ACCESSORIES',350,'2026-04-01'),('ST006','ELECTRONICS',140,'2026-04-01'),
    ('ST006','HOME',290,'2026-04-01'),
    ('ST007','APPAREL',490,'2026-04-01'),('ST007','FOOTWEAR',290,'2026-04-01'),
    ('ST007','ACCESSORIES',400,'2026-04-01'),('ST007','ELECTRONICS',170,'2026-04-01'),
    ('ST007','HOME',330,'2026-04-01'),
    ('ST008','APPAREL',300,'2026-04-01'),('ST008','FOOTWEAR',180,'2026-04-01'),
    ('ST008','ACCESSORIES',250,'2026-04-01'),('ST008','ELECTRONICS',100,'2026-04-01'),
    ('ST008','HOME',200,'2026-04-01');
GO

PRINT '>> QTY_ST_STK_Q: 40 stock snapshots inserted.';
GO

--------------------------------------------------------------
-- 8. QTY_MSA_AND_GRT - DC/GRT stock
--------------------------------------------------------------
DELETE FROM dbo.QTY_MSA_AND_GRT;

INSERT INTO dbo.QTY_MSA_AND_GRT ([RDC_CD],[RDC],[MAJ-CAT],[DC-STK-Q],[GRT-STK-Q],[W-GRT-STK-Q],[DATE])
VALUES
    ('RDC01','RDC NORTH','APPAREL',5000,2000,800,'2026-04-01'),
    ('RDC01','RDC NORTH','FOOTWEAR',3000,1200,500,'2026-04-01'),
    ('RDC01','RDC NORTH','ACCESSORIES',4000,1600,600,'2026-04-01'),
    ('RDC01','RDC NORTH','ELECTRONICS',2000,800,300,'2026-04-01'),
    ('RDC01','RDC NORTH','HOME',3500,1400,500,'2026-04-01'),
    ('RDC02','RDC WEST','APPAREL',6000,2500,1000,'2026-04-01'),
    ('RDC02','RDC WEST','FOOTWEAR',3500,1400,600,'2026-04-01'),
    ('RDC02','RDC WEST','ACCESSORIES',4500,1800,700,'2026-04-01'),
    ('RDC02','RDC WEST','ELECTRONICS',2500,1000,400,'2026-04-01'),
    ('RDC02','RDC WEST','HOME',4000,1600,600,'2026-04-01'),
    ('RDC03','RDC SOUTH','APPAREL',7000,2800,1100,'2026-04-01'),
    ('RDC03','RDC SOUTH','FOOTWEAR',4000,1600,650,'2026-04-01'),
    ('RDC03','RDC SOUTH','ACCESSORIES',5500,2200,900,'2026-04-01'),
    ('RDC03','RDC SOUTH','ELECTRONICS',3000,1200,500,'2026-04-01'),
    ('RDC03','RDC SOUTH','HOME',5000,2000,800,'2026-04-01'),
    ('RDC04','RDC EAST','APPAREL',3500,1400,550,'2026-04-01'),
    ('RDC04','RDC EAST','FOOTWEAR',2000,800,300,'2026-04-01'),
    ('RDC04','RDC EAST','ACCESSORIES',2800,1100,450,'2026-04-01'),
    ('RDC04','RDC EAST','ELECTRONICS',1500,600,250,'2026-04-01'),
    ('RDC04','RDC EAST','HOME',2500,1000,400,'2026-04-01');
GO

PRINT '>> QTY_MSA_AND_GRT: 20 DC stock rows inserted.';
PRINT '>> ALL SAMPLE DATA LOADED SUCCESSFULLY.';
GO
