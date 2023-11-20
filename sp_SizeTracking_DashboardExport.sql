CREATE PROCEDURE [dbo].[sp_SizeTracking_DashboardExport]      
AS      
BEGIN  

-- https://www.mssqltips.com/sqlservertip/6158/how-to-check-monthly-growth-of-database-in-sql-server/
-- https://dba.stackexchange.com/questions/102828/how-can-i-determine-which-tables-are-causing-the-database-to-grow-over-time
  
DROP TABLE IF EXISTS ##temp_TableInfo;  
DROP TABLE IF EXISTS ##temp_BackupInfo;  
  
SET NOCOUNT ON;  
  
CREATE TABLE ##temp_BackupInfo ( RecordID INT IDENTITY (1,1) NOT NULL ,  
        [Database] VARCHAR(64) ,   
        Year SMALLINT,   
        Month SMALLINT,   
        Week SMALLINT,   
        Day SMALLINT,   
        BackupSizeGB DECIMAL(10,2) ,   
        PreviousSizeGB DECIMAL(10,2),   
        GBChange DECIMAL(10,2),   
        PercentageChange DECIMAL(20,4) );  
  
;WITH BackupsSize AS  
(  
SELECT  
	[database_name]  
    , [Year]  = DATEPART(year,[backup_start_date])  
    , [Month] = DATEPART(month,[backup_start_date])  
	, [Week] = DATEPART(week,[backup_start_date])  
	, [Day] = DATEPART(day,[backup_start_date])  
    , [Backup Size GB] = CONVERT(DECIMAL(10,2),ROUND(AVG([backup_size]/1024/1024/1024),4))  
    --, [Compressed Backup Size GB] = CONVERT(DECIMAL(10,2),ROUND(AVG([compressed_backup_size]/1024/1024/1024),4))  
FROM   
    msdb.dbo.backupset  
WHERE   
 1=1  
AND   
 [database_name] IN (N'Assess50', N'Assess50Analysis', N'Assess50NC', N'Assess50Online', N'WebPro50', N'MCPAWebsite')  
AND   
 [type] = 'D'  
AND   
 [backup_start_date] BETWEEN DATEADD(mm, -36, GETDATE()) AND GETDATE() -- changed from -24 (back 2 years) to -36 (back 3 years)  
GROUP BY   
    [database_name]  
    , DATEPART(yyyy,[backup_start_date])  
    , DATEPART(mm, [backup_start_date])  
	, DATEPART(ww, [backup_start_date])  
	, DATEPART(dd, [backup_start_date])  
)   
  
  
INSERT INTO ##temp_BackupInfo ( [Database] , [Year] , [Month] , [Week] , [Day] , [BackupSizeGB] , [PreviousSizeGB] , [GBChange] , [PercentageChange] )  
  
SELECT   
   b.[database_name],  
   b.Year,  
   b.Month,  
   b.Week,  
   b.Day,  
   b.[Backup Size GB],  
   LAG(b.[Backup Size GB]) OVER (PARTITION BY b.[database_name] ORDER BY b.[Year] ASC, b.[Month] ASC, b.[Week] ASC, b.[Day] ASC) AS [Previous Size],  
   --b.[Backup Size GB] - d.[Backup Size GB] AS deltaNormal (GB Change),  
   b.[Backup Size GB] - (LAG(b.[Backup Size GB]) OVER (PARTITION BY b.[database_name] ORDER BY b.[Year] ASC, b.[Month] ASC, b.[Week] ASC, b.[Day] ASC)) AS [GB Change],  
   --ISNULL(CAST((b.[Backup Size GB] - d.[Backup Size GB])*100/NULLIF(d.[Backup Size GB],0) AS DECIMAL(20,4)),0) AS [% Change]--,  
   ISNULL(CAST((b.[Backup Size GB] - (LAG(b.[Backup Size GB]) OVER (PARTITION BY b.[database_name] ORDER BY b.[Year] ASC, b.[Month] ASC, b.[Week] ASC, b.[Day] ASC)))*100/NULLIF((LAG(b.[Backup Size GB]) OVER (PARTITION BY b.[database_name] ORDER BY b.[Year] ASC, 
   b.[Month] ASC, b.[Week] ASC, b.[Day] ASC)),0) AS DECIMAL(20,4)),0) AS [% Change]--,  
   --b.[Compressed Backup Size GB],  
   --b.[Compressed Backup Size GB] - d.[Compressed Backup Size GB] AS deltaCompressed  
FROM   
	BackupsSize b  
ORDER BY   
	database_name ASC,  
	Year DESC,   
	Month DESC,   
	Week DESC,   
	Day DESC  
  
  
--SELECT * FROM ##temp_BackupInfo  
--ORDER BY RecordID ASC  
  
  
EXEC dbo.ext_ExportDataToCsv @dbName = N'tempdb',          -- nvarchar(100)      
         @includeHeaders = 1, -- bit      
         @filePath = N'\\filepath\Dashboard_Data',        -- nvarchar(512)      
         @tableName = N'##temp_BackupInfo',       -- nvarchar(100)      
         @reportName = N'BackupInfo.csv',      -- nvarchar(100)      
         @delimiter = N'|'        -- nvarchar(4)      
      
    
DECLARE @excelColumns TABLE (Number SMALLINT, Letter VARCHAR(4));      
INSERT INTO @excelColumns ( Number, Letter )      
VALUES      
  (1,'A'),      
  (2,'B'),      
  (3,'C'),      
  (4,'D'),      
  (5,'E'),      
  (6,'F'),      
  (7,'G'),      
  (8,'H'),      
  (9,'I'),      
  (10,'J'),      
  (11,'K'),      
  (12,'L'),      
  (13,'M'),      
  (14,'N'),      
  (15,'O'),      
  (16,'P'),      
  (17,'Q'),      
  (18,'R'),      
  (19,'S'),      
  (20,'T'),      
  (21,'U'),      
  (22,'V'),      
  (23,'W'),      
  (24,'X'),      
  (25,'Y'),      
  (26,'Z'),      
  (27,'AA'),      
  (28,'AB'),      
  (29,'AC'),      
  (30,'AD'),      
  (31,'AE'),      
  (32,'AF'),      
  (33,'AG'),      
  (34,'AH'),      
  (35,'AI'),      
  (36,'AJ'),      
  (37,'AK'),      
  (38,'AL'),      
  (39,'AM'),      
  (40,'AN'),      
  (41,'AO'),      
  (42,'AP'),      
  (43,'AQ'),      
  (44,'AR'),      
  (45,'AS'),      
  (46,'AT'),      
  (47,'AU'),      
  (48,'AV'),      
  (49,'AW'),      
  (50,'AX'),      
  (51,'AY'),      
  (52,'AZ');      
      
 --SELECT * FROM @excelColumns      
      
DECLARE @columnLetter VARCHAR(4) = (SELECT Letter FROM @excelColumns JOIN (SELECT COUNT(COLUMN_NAME) [c] FROM tempdb.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '##temp_BackupInfo') t ON t.c = [@excelColumns].Number);      
      
DECLARE @recordCount SMALLINT = (SELECT COUNT(*) + 1 FROM ##temp_BackupInfo);      
      
EXEC [Assess50].[dbo].CSVtoXLSXwTable @fullCsvPath = '\\filepath\Dashboard_Data\BackupInfo.csv',  -- varchar(512)      
               @fullXlsxPath = '\\filepath\Dashboard_Data\BackupInfo.xlsx', -- varchar(512)      
               @rowCount = @recordCount,      -- int      
               @colCharacter = @columnLetter  -- varchar(4)  
  
  
/* =================================== */  
  
  
DECLARE @databases TABLE ( [Name] VARCHAR(64), Number SMALLINT );  
  
INSERT INTO @databases  
SELECT   
 name,  
 --database_id,   
 --create_date,  
 ROW_NUMBER() OVER (ORDER BY [database_id] ASC) [RN]   
FROM   
	sys.databases  
WHERE   
	name NOT IN ('master','model','msdb','tempdb')  
  
--SELECT * FROM @databases  
  
/* ================================ */  
  
CREATE TABLE ##temp_TableInfo ( TableID INT IDENTITY (1,1) NOT NULL ,   
        [Database] VARCHAR(64) ,   
        TableName VARCHAR(128),   
        [Rows] INT,   
        ReservedGB DECIMAL(20,5),   
        DataGB DECIMAL(20,5),   
        IndexGB DECIMAL(20,5),   
        UsedGB DECIMAL(20,5),   
        UnusedGB DECIMAL(20,5) );  
  
WHILE (SELECT COUNT([Name]) FROM @databases) > 0  
BEGIN  
  
 DECLARE @db VARCHAR(64) = (SELECT [Name] FROM @databases WHERE [Number] = (SELECT COUNT([Name]) FROM @databases));  
 DECLARE @sql VARCHAR(MAX);  
  
 SET @sql = '  
 USE '+@db+'  
 ;WITH extra AS  
 (   -- Get info for FullText indexes, XML Indexes, etc  
  SELECT  sit.[object_id],  
    sit.[parent_id],  
    ps.[index_id],  
    SUM(ps.reserved_page_count) AS [reserved_page_count],  
    SUM(ps.used_page_count) AS [used_page_count]  
  FROM    sys.dm_db_partition_stats ps  
  INNER JOIN  sys.internal_tables sit  
    ON  sit.[object_id] = ps.[object_id]  
  WHERE   sit.internal_type IN  
       (202, 204, 207, 211, 212, 213, 214, 215, 216, 221, 222, 236)  
  GROUP BY    sit.[object_id],  
     sit.[parent_id],  
     ps.[index_id]  
 ), agg AS  
 (   -- Get info for Tables, Indexed Views, etc (including "extra")  
  SELECT  ps.[object_id] AS [ObjectID],  
    ps.index_id AS [IndexID],  
    SUM(ps.in_row_data_page_count) AS [InRowDataPageCount],  
    SUM(ps.used_page_count) AS [UsedPageCount],  
    SUM(ps.reserved_page_count) AS [ReservedPageCount],  
    SUM(ps.row_count) AS [RowCount],  
    SUM(ps.lob_used_page_count + ps.row_overflow_used_page_count)  
      AS [LobAndRowOverflowUsedPageCount]  
  FROM    sys.dm_db_partition_stats ps  
  GROUP BY    ps.[object_id],  
     ps.[index_id]  
  UNION ALL  
  SELECT  ex.[parent_id] AS [ObjectID],  
    ex.[object_id] AS [IndexID],  
    0 AS [InRowDataPageCount],  
    SUM(ex.used_page_count) AS [UsedPageCount],  
    SUM(ex.reserved_page_count) AS [ReservedPageCount],  
    0 AS [RowCount],  
    0 AS [LobAndRowOverflowUsedPageCount]  
  FROM    extra ex  
  GROUP BY    ex.[parent_id],  
     ex.[object_id]  
 ), spaceused AS  
 (  
 SELECT  agg.[ObjectID],  
   OBJECT_SCHEMA_NAME(agg.[ObjectID]) AS [SchemaName],  
   OBJECT_NAME(agg.[ObjectID]) AS [TableName],  
   SUM(CASE  
     WHEN (agg.IndexID < 2) THEN agg.[RowCount]  
     ELSE 0  
    END) AS [Rows],  
   SUM(agg.ReservedPageCount) * 8 AS [ReservedKB],  
   SUM(agg.LobAndRowOverflowUsedPageCount +  
    CASE  
     WHEN (agg.IndexID < 2) THEN (agg.InRowDataPageCount)  
     ELSE 0  
    END) * 8 AS [DataKB],  
   SUM(agg.UsedPageCount - agg.LobAndRowOverflowUsedPageCount -  
    CASE  
     WHEN (agg.IndexID < 2) THEN agg.InRowDataPageCount  
     ELSE 0  
    END) * 8 AS [IndexKB],  
   SUM(agg.ReservedPageCount - agg.UsedPageCount) * 8 AS [UnusedKB],  
   SUM(agg.UsedPageCount) * 8 AS [UsedKB]  
 FROM    agg  
 GROUP BY    agg.[ObjectID],  
    OBJECT_SCHEMA_NAME(agg.[ObjectID]),  
    OBJECT_NAME(agg.[ObjectID])  
 )  
 INSERT INTO ##temp_TableInfo ( [Database], TableName, [Rows], ReservedGB, DataGB, IndexGB, UsedGB, UnusedGB )  
 SELECT   
     '''+@db+''',  
     --sp.SchemaName,  
     sp.TableName,  
     sp.[Rows],  
     --sp.ReservedKB,  
     CAST((sp.ReservedKB / 1024.0 / 1024.0) AS DECIMAL(20,5)) AS [ReservedGB],  
     --sp.DataKB,  
     CAST((sp.DataKB / 1024.0 / 1024.0) AS DECIMAL(20,5)) AS [DataGB],  
     --sp.IndexKB,  
     CAST((sp.IndexKB / 1024.0 / 1024.0) AS DECIMAL(20,5)) AS [IndexGB],  
     --sp.UsedKB AS [UsedKB],  
     CAST((sp.UsedKB / 1024.0 / 1024.0) AS DECIMAL(20,5)) AS [UsedGB],  
     --sp.UnusedKB,  
     CAST((sp.UnusedKB / 1024.0 / 1024.0) AS DECIMAL(20,5)) AS [UnusedGB]--,  
     --so.[type_desc] AS [ObjectType],  
     --so.[schema_id] AS [SchemaID],  
     --sp.ObjectID  
 FROM   spaceused sp  
 INNER JOIN sys.all_objects so  
   ON so.[object_id] = sp.ObjectID  
 WHERE so.is_ms_shipped = 0  
 ';  
  
 EXEC (@sql);  
  
 DELETE FROM @databases WHERE [Name] = @db;  
  
END  
  
ALTER TABLE ##temp_TableInfo  
ADD RecordID INT;  
  
/*  
SELECT  
 [Database],  
 max(RecordID) [ID]  
FROM ##temp_BackupInfo  
GROUP BY [Database]  
*/  
  
UPDATE ##temp_TableInfo  
SET RecordID = m.ID  
FROM ##temp_TableInfo i  
JOIN  (  
 SELECT  
  [Database],  
  max(RecordID) [ID]  
 FROM ##temp_BackupInfo  
 GROUP BY [Database]  
 ) m ON m.[Database] = i.[Database]  
WHERE   
 i.[Database] = m.[Database]  
  
  
--SELECT * FROM ##temp_TableInfo  
--ORDER BY [Database] ASC, [TableName] ASC  
  
EXEC dbo.ext_ExportDataToCsv @dbName = N'tempdb',          -- nvarchar(100)      
         @includeHeaders = 1, -- bit      
         @filePath = N'\\filepath\Dashboard_Data',        -- nvarchar(512)      
         @tableName = N'##temp_TableInfo',       -- nvarchar(100)      
         @reportName = N'TableInfo.csv',      -- nvarchar(100)      
         @delimiter = N'|'        -- nvarchar(4)      
  
  
  
  
  
SET @columnLetter = (SELECT Letter FROM @excelColumns JOIN (SELECT COUNT(COLUMN_NAME) [c] FROM tempdb.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '##temp_TableInfo') t ON t.c = [@excelColumns].Number);      
      
SET @recordCount = (SELECT COUNT(*) + 1 FROM ##temp_TableInfo);      
      
EXEC [Assess50].[dbo].CSVtoXLSXwTable @fullCsvPath = '\\filepath\Dashboard_Data\TableInfo.csv',  -- varchar(512)      
               @fullXlsxPath = '\\filepath\Dashboard_Data\TableInfo.xlsx', -- varchar(512)      
               @rowCount = @recordCount,      -- int      
               @colCharacter = @columnLetter  -- varchar(4)  
  
  
  
DROP TABLE IF EXISTS ##temp_TableInfo;  
DROP TABLE IF EXISTS ##temp_BackupInfo;  
  
END  