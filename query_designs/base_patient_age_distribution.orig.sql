-- Optimizations applied: Remove Data Model Root, Query Consolidation, Remove Date Cross Joins, Use Optimized DMCs
SET TRANSACTION ISOLATION LEVEL SNAPSHOT

DECLARE @param0003 AS NUMERIC ( 28,10 )  = 5;

DECLARE @param0004 AS NUMERIC ( 28,10 )  = 18;

DECLARE @param0005 AS NUMERIC ( 28,10 )  = 30;

DECLARE @param0006 AS NUMERIC ( 28,10 )  = 40;

DECLARE @param0007 AS NUMERIC ( 28,10 )  = 50;

DECLARE @param0008 AS NUMERIC ( 28,10 )  = 65;

DECLARE @param0009 AS NUMERIC ( 28,10 )  = 75;

DECLARE @param0010 AS NUMERIC ( 28,10 )  = 85;

DECLARE @param0011 AS NUMERIC ( 28,10 )  = 25;

DROP TABLE IF EXISTS #resultSet;

SELECT
* INTO #resultSet
FROM
 ( 

    SELECT
     slice0 , slice1 , slice2 , COUNT_BIG ( DurableKey )  AS count3 , COUNT_BIG ( DurableKey )  AS count4

    FROM
      ( 

        SELECT
         subq0.DurableKey, subq0.slice0 AS slice0 , subq0.slice1 AS slice1 , subq0.slice2 AS slice2 , NULL AS count3, NULL AS count4
        FROM
          ( 
            SELECT
             DISTINCT RootTable0.DurableKey AS DurableKey, DateTable2.DateBucket AS slice0 , NULL AS slice1MaxValue , NULL AS slice2MaxValue , NULL AS count3 , NULL AS count4 , ISNULL ( ConsolidatedTable4.slice1, 9 )  AS slice1 , ISNULL ( ConsolidatedTable5.slice2, 4 )  AS slice2
            FROM
             SlicerDicer.PatientDim_638706495790535755_2pG0IGNtEaYHvhTqWNi1Q AS RootTable0
            INNER JOIN dbo.ActivePatientRegistryDataMartX AS FilterTable1 ON RootTable0.DurableKey = FilterTable1.PatientDurableKey
            INNER JOIN  ( 
                SELECT
                 0 AS DateBucket, '20211213' AS startDate, '20220101' AS endDate UNION ALL
                SELECT
                 1 AS DateBucket, '20220101' AS startDate, '20230101' AS endDate UNION ALL
                SELECT
                 2 AS DateBucket, '20230101' AS startDate, '20240101' AS endDate UNION ALL
                SELECT
                 3 AS DateBucket, '20240101' AS startDate, '20241213' AS endDate )  DateTable2 ON  (  ( FilterTable1.StartDateKey < DateTable2.endDate  ) 
                AND  ( FilterTable1.EndDateKey >= DateTable2.startDate OR FilterTable1.EndDateKey < 0 )  ) 
            INNER JOIN  ( 
                SELECT
                 0 AS DateBucket UNION ALL
                SELECT
                 1 AS DateBucket UNION ALL
                SELECT
                 2 AS DateBucket UNION ALL
                SELECT
                 3 AS DateBucket )  ConsolidatedTable3 ON  (  DateTable2.DateBucket = ConsolidatedTable3.DateBucket  ) 
            LEFT OUTER JOIN  ( 
                SELECT
                 DISTINCT SlicerTable1.PatientDurableKey AS DurableKey,  ( CASE
                    WHEN  (  (  ( SlicerTable1.DateKey < '20220101'  ) 
                            AND  ( SlicerTable1.DateKey >= '20211213' OR SlicerTable1.DateKey < 0 )  )  )  THEN 0
                    WHEN  (  (  ( SlicerTable1.DateKey < '20230101'  ) 
                            AND  ( SlicerTable1.DateKey >= '20220101' OR SlicerTable1.DateKey < 0 )  )  )  THEN 1
                    WHEN  (  (  ( SlicerTable1.DateKey < '20240101'  ) 
                            AND  ( SlicerTable1.DateKey >= '20230101' OR SlicerTable1.DateKey < 0 )  )  )  THEN 2
                    WHEN  (  (  ( SlicerTable1.DateKey < '20241213'  ) 
                            AND  ( SlicerTable1.DateKey >= '20240101' OR SlicerTable1.DateKey < 0 )  )  )  THEN 3
                    ELSE NULL END )  AS slice0 ,  ( CASE
                    WHEN  ( SlicerTable2.Years < @param0003 /* 5 */ )  THEN 0
                    WHEN  (   ( SlicerTable2.Years < @param0004 /* 18 */ ) 
                        AND  ( SlicerTable2.Years >= @param0003 /* 5 */ )   )  THEN 1
                    WHEN  (   ( SlicerTable2.Years < @param0005 /* 30 */ ) 
                        AND  ( SlicerTable2.Years >= @param0004 /* 18 */ )   )  THEN 2
                    WHEN  (   ( SlicerTable2.Years < @param0006 /* 40 */ ) 
                        AND  ( SlicerTable2.Years >= @param0005 /* 30 */ )   )  THEN 3
                    WHEN  (   ( SlicerTable2.Years < @param0007 /* 50 */ ) 
                        AND  ( SlicerTable2.Years >= @param0006 /* 40 */ )   )  THEN 4
                    WHEN  (   ( SlicerTable2.Years < @param0008 /* 65 */ ) 
                        AND  ( SlicerTable2.Years >= @param0007 /* 50 */ )   )  THEN 5
                    WHEN  (   ( SlicerTable2.Years < @param0009 /* 75 */ ) 
                        AND  ( SlicerTable2.Years >= @param0008 /* 65 */ )   )  THEN 6
                    WHEN  (   ( SlicerTable2.Years < @param0010 /* 85 */ ) 
                        AND  ( SlicerTable2.Years >= @param0009 /* 75 */ )   )  THEN 7
                    WHEN  ( SlicerTable2.Years >= @param0010 /* 85 */ )  THEN 8
                    ELSE NULL END )  AS slice1
                FROM
                 SlicerDicer.EncounterFact_638706495790379538_Y9ieLULUuoCKqxv37Jg AS SlicerTable1
                INNER JOIN dbo.DurationDim AS SlicerTable2 ON SlicerTable1.AgeKey = SlicerTable2.DurationKey
                WHERE
                  (  (  (  ( SlicerTable1.DateKey < '20241213' ) 
                            AND NOT  ( SlicerTable1.DateKey < 0 )  ) 
                        AND  (  ( SlicerTable1.DateKey >= '20211213' )  OR  ( SlicerTable1.DateKey < 0 )  )  ) 
                    AND  ( SlicerTable2.Years IS NOT NULL )   )  )  ConsolidatedTable4 ON  ( RootTable0.DurableKey = ConsolidatedTable4.DurableKey ) 
            AND  (  (  ConsolidatedTable3.DateBucket = ConsolidatedTable4.slice0  )  ) 
            LEFT OUTER JOIN  ( 
                SELECT
                 DISTINCT RootTable0.DurableKey AS DurableKey,  ( CASE
                    WHEN  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 < @param0011 /* 25 */ )  THEN 0
                    WHEN  (   ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 < @param0007 /* 50 */ ) 
                        AND  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 >= @param0011 /* 25 */ )   )  THEN 1
                    WHEN  (   ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 < @param0009 /* 75 */ ) 
                        AND  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 >= @param0007 /* 50 */ )   )  THEN 2
                    WHEN  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 >= @param0009 /* 75 */ )  THEN 3
                    ELSE NULL END )  AS slice2
                FROM
                 SlicerDicer.PatientDim_638706495790535755_2pG0IGNtEaYHvhTqWNi1Q AS RootTable0
                INNER JOIN dbo.Svi2020ZipCodeFact AS SlicerTable1 ON  ( RootTable0.PostalCodeKey = SlicerTable1.PostalCodeKey ) 
                AND  (  ( SlicerTable1.PostalCodeKey > 0 )  ) 
                WHERE
                  (  (  ( RootTable0.IsValid = 1 ) 
                        AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                    AND  ( RootTable0.IsCurrent = 1 ) 
                    AND  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 IS NOT NULL )   )  )  ConsolidatedTable5 ON RootTable0.DurableKey = ConsolidatedTable5.DurableKey
            WHERE
              (  (  ( RootTable0.IsValid = 1 ) 
                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                AND  ( RootTable0.IsCurrent = 1 ) 
                AND  ( 1=1 ) 
                AND  (  (  ( FilterTable1.StartDateKey < '20241213' ) 
                        AND NOT  ( FilterTable1.StartDateKey < 0 )  ) 
                    AND  (  ( FilterTable1.EndDateKey >= '20211213' )  OR  ( FilterTable1.EndDateKey < 0 )  )  )  )   )  subq0
        WHERE
          (  (  (  ( subq0.slice1 <> '9' )  OR  ( subq0.slice1MaxValue IS NULL )  ) 
                AND  (  ( subq0.slice2 <> '4' )  OR  ( subq0.slice2MaxValue IS NULL )  )  )  ) 
 )  subq

    WHERE
    
subq.DurableKey > 0

    GROUP BY slice0, slice1, slice2
 )  AS tempResultSet
OPTION ( RECOMPILE, USE HINT ( 'FORCE_DEFAULT_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL' ) , NO_PERFORMANCE_SPOOL ) 

SELECT
DISTINCT mainResultSet.slice0 , mainResultSet.slice1 , mainResultSet.slice2 , mainResultSet.count3 AS count3 , mainResultSet.count4 AS count4
FROM
#resultSet mainResultSet
WHERE
mainResultSet.slice0 IS NOT NULL
AND mainResultSet.slice1 IS NOT NULL
AND mainResultSet.slice2 IS NOT NULL

DROP TABLE #resultSet
