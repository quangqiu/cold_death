
DECLARE @start_date AS nvarchar(MAX) = '20000101'
DECLARE @end_date   AS nvarchar(MAX) = '20250101'

SELECT
* INTO #base_patients
FROM
 ( 

    SELECT
     slice0 , slice1 , COUNT_BIG ( DurableKey )  AS count2 , COUNT_BIG ( DurableKey )  AS count3

    FROM
      ( 

        SELECT
         subq0.DurableKey, subq0.slice0 AS slice0 , subq0.slice1 AS slice1 , NULL AS count2, NULL AS count3
        FROM
          ( 
            SELECT
             DISTINCT RootTable0.DurableKey AS DurableKey, DateTable1.DateBucket AS slice0 , ISNULL ( NotASubquery2.slice1, 19 )  AS slice1 , NULL AS slice1MaxValue , NULL AS count2 , NULL AS count3
            FROM
             PatientDim AS RootTable0
            CROSS JOIN  ( SELECT DateBucket FROM #YearlyDateBuckets where startDate>=@start_date AND startDate<@end_date
	    	  /*
                SELECT 20 AS DateBucket UNION ALL
                SELECT 21 AS DateBucket UNION ALL
                SELECT 22 AS DateBucket UNION ALL
                SELECT 23 AS DateBucket*/ )  DateTable1
            LEFT OUTER JOIN  ( 
                SELECT
                 DISTINCT SlicerTable1.PatientDurableKey AS DurableKey,
		   (SELECT DateBucket from #YearlyDateBuckets where   ( SlicerTable1.DateKey < endDate  ) 
                            AND  ( SlicerTable1.DateKey >= startDate OR SlicerTable1.DateKey < 0 )      )  AS slice0 ,

		   ( CASE
                    WHEN  ( SlicerTable2.Years < 1 )  THEN 0
		    
                    WHEN  (   ( SlicerTable2.Years < 5  ) AND  ( SlicerTable2.Years >= 1  )   )  THEN 1
                    WHEN  (   ( SlicerTable2.Years < 10 ) AND  ( SlicerTable2.Years >= 5  )   )  THEN 2
                    WHEN  (   ( SlicerTable2.Years < 15 ) AND  ( SlicerTable2.Years >= 10 )   )  THEN 3
                    WHEN  (   ( SlicerTable2.Years < 20 ) AND  ( SlicerTable2.Years >= 15 )   )  THEN 4
                    WHEN  (   ( SlicerTable2.Years < 25 ) AND  ( SlicerTable2.Years >= 20 )   )  THEN 5
		    
		    WHEN  (   ( SlicerTable2.Years < 30 ) AND  ( SlicerTable2.Years >= 25 )   )  THEN 6
                    WHEN  (   ( SlicerTable2.Years < 35 ) AND  ( SlicerTable2.Years >= 30 )   )  THEN 7
                    WHEN  (   ( SlicerTable2.Years < 40 ) AND  ( SlicerTable2.Years >= 35 )   )  THEN 8
                    WHEN  (   ( SlicerTable2.Years < 45 ) AND  ( SlicerTable2.Years >= 40 )   )  THEN 9
                    WHEN  (   ( SlicerTable2.Years < 50 ) AND  ( SlicerTable2.Years >= 45 )   )  THEN 10

		    WHEN  (   ( SlicerTable2.Years < 55 ) AND  ( SlicerTable2.Years >= 50 )   )  THEN 11
                    WHEN  (   ( SlicerTable2.Years < 60 ) AND  ( SlicerTable2.Years >= 55 )   )  THEN 12
                    WHEN  (   ( SlicerTable2.Years < 65 ) AND  ( SlicerTable2.Years >= 60 )   )  THEN 13
                    WHEN  (   ( SlicerTable2.Years < 70 ) AND  ( SlicerTable2.Years >= 65 )   )  THEN 14
                    WHEN  (   ( SlicerTable2.Years < 75 ) AND  ( SlicerTable2.Years >= 70 )   )  THEN 15

		    WHEN  (   ( SlicerTable2.Years < 80 ) AND  ( SlicerTable2.Years >= 75 )   )  THEN 16		    
		    WHEN  (   ( SlicerTable2.Years < 85 ) AND  ( SlicerTable2.Years >= 80 )   )  THEN 17
		    
                    WHEN  ( SlicerTable2.Years >= 85 )  THEN 18
                    ELSE NULL END )  AS slice1
                FROM
                 EncounterFact AS SlicerTable1
                INNER JOIN dbo.DurationDim AS SlicerTable2 ON SlicerTable1.AgeKey = SlicerTable2.DurationKey
                WHERE
                  (  (  (  ( SlicerTable1.DateKey < @end_date ) 
                            AND NOT  ( SlicerTable1.DateKey < 0 )  ) 
                        AND  (  ( SlicerTable1.DateKey >= @start_date )  OR  ( SlicerTable1.DateKey < 0 )  )  ) 
                    AND  ( SlicerTable2.Years IS NOT NULL )   )  )  NotASubquery2 ON  ( RootTable0.DurableKey = NotASubquery2.DurableKey ) 
            AND  (  (  DateTable1.DateBucket = NotASubquery2.slice0  )  ) 
            INNER JOIN dbo.ActivePatientRegistryDataMartX AS ConsolidatedTable3 ON RootTable0.DurableKey = ConsolidatedTable3.PatientDurableKey
            INNER JOIN  ( SELECT * from #YearlyDateBuckets where startDate >=@start_date AND startDate < @end_date )  ConsolidatedTable4 ON  (  (  ( ConsolidatedTable3.StartDateKey < ConsolidatedTable4.endDate  ) 
                    AND  ( ConsolidatedTable3.EndDateKey >= ConsolidatedTable4.startDate OR ConsolidatedTable3.EndDateKey < 0 )  )  ) 
            AND  (  (  DateTable1.DateBucket = ConsolidatedTable4.DateBucket  )  ) 
            WHERE
              (  (  ( RootTable0.IsValid = 1 ) 
                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                AND  ( RootTable0.IsCurrent = 1 ) 
                AND  (  (  ( ConsolidatedTable3.StartDateKey < @end_date ) 
                        AND NOT  ( ConsolidatedTable3.StartDateKey < 0 )  ) 
                    AND  (  ( ConsolidatedTable3.EndDateKey >= @start_date )  OR  ( ConsolidatedTable3.EndDateKey < 0 )  )  )  )   )  subq0
        WHERE
          (  (  (  ( subq0.slice1 <> '19' )  OR  ( subq0.slice1MaxValue IS NULL )  )  )  ) 
 )  subq

    WHERE
    
subq.DurableKey > 0

    GROUP BY slice0, slice1
 )  AS tempResultSet
OPTION ( RECOMPILE, USE HINT ( 'FORCE_DEFAULT_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL' ) , NO_PERFORMANCE_SPOOL ) 



SELECT
* INTO  #base_patients_by_age_svi
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
             PatientDim AS RootTable0
            INNER JOIN dbo.ActivePatientRegistryDataMartX AS FilterTable1 ON RootTable0.DurableKey = FilterTable1.PatientDurableKey
            INNER JOIN  ( select * from #YearlyDateBuckets where startDate>=@start_date AND startDate<@end_date ) AS DateTable2 ON  (  ( FilterTable1.StartDateKey < DateTable2.endDate  ) 
                AND  ( FilterTable1.EndDateKey >= DateTable2.startDate OR FilterTable1.EndDateKey < 0 )  ) 
            INNER JOIN  ( SELECT DateBucket FROM #YearlyDateBuckets where startDate>=@start_date AND startDate<@end_date )  ConsolidatedTable3 ON  (  DateTable2.DateBucket = ConsolidatedTable3.DateBucket  ) 
            LEFT OUTER JOIN  (
	    	       
		    CASE
                    WHEN  ( SlicerTable2.Years < 1 )  THEN 0
		    
                    WHEN  (   ( SlicerTable2.Years < 5  ) AND  ( SlicerTable2.Years >= 1  )   )  THEN 1
                    WHEN  (   ( SlicerTable2.Years < 10 ) AND  ( SlicerTable2.Years >= 5  )   )  THEN 2
                    WHEN  (   ( SlicerTable2.Years < 15 ) AND  ( SlicerTable2.Years >= 10 )   )  THEN 3
                    WHEN  (   ( SlicerTable2.Years < 20 ) AND  ( SlicerTable2.Years >= 15 )   )  THEN 4
                    WHEN  (   ( SlicerTable2.Years < 25 ) AND  ( SlicerTable2.Years >= 20 )   )  THEN 5
		    
		    WHEN  (   ( SlicerTable2.Years < 30 ) AND  ( SlicerTable2.Years >= 25 )   )  THEN 6
                    WHEN  (   ( SlicerTable2.Years < 35 ) AND  ( SlicerTable2.Years >= 30 )   )  THEN 7
                    WHEN  (   ( SlicerTable2.Years < 40 ) AND  ( SlicerTable2.Years >= 35 )   )  THEN 8
                    WHEN  (   ( SlicerTable2.Years < 45 ) AND  ( SlicerTable2.Years >= 40 )   )  THEN 9
                    WHEN  (   ( SlicerTable2.Years < 50 ) AND  ( SlicerTable2.Years >= 45 )   )  THEN 10

		    WHEN  (   ( SlicerTable2.Years < 55 ) AND  ( SlicerTable2.Years >= 50 )   )  THEN 11
                    WHEN  (   ( SlicerTable2.Years < 60 ) AND  ( SlicerTable2.Years >= 55 )   )  THEN 12
                    WHEN  (   ( SlicerTable2.Years < 65 ) AND  ( SlicerTable2.Years >= 60 )   )  THEN 13
                    WHEN  (   ( SlicerTable2.Years < 70 ) AND  ( SlicerTable2.Years >= 65 )   )  THEN 14
                    WHEN  (   ( SlicerTable2.Years < 75 ) AND  ( SlicerTable2.Years >= 70 )   )  THEN 15

		    WHEN  (   ( SlicerTable2.Years < 80 ) AND  ( SlicerTable2.Years >= 75 )   )  THEN 16		    
		    WHEN  (   ( SlicerTable2.Years < 85 ) AND  ( SlicerTable2.Years >= 80 )   )  THEN 17
		    
                    WHEN  ( SlicerTable2.Years >= 85 )  THEN 18
                    ELSE NULL END 
	    )  AS slice1
                FROM
                 EncounterFact AS SlicerTable1
                INNER JOIN dbo.DurationDim AS SlicerTable2 ON SlicerTable1.AgeKey = SlicerTable2.DurationKey
                WHERE
                  (  (  (  ( SlicerTable1.DateKey < @end_date ) 
                            AND NOT  ( SlicerTable1.DateKey < 0 )  ) 
                        AND  (  ( SlicerTable1.DateKey >= @start_date )  OR  ( SlicerTable1.DateKey < 0 )  )  ) 
                    AND  ( SlicerTable2.Years IS NOT NULL )   )  )  ConsolidatedTable4 ON  ( RootTable0.DurableKey = ConsolidatedTable4.DurableKey ) 
            AND  (  (  ConsolidatedTable3.DateBucket = ConsolidatedTable4.slice0  )  ) 
            LEFT OUTER JOIN  ( 
                SELECT
                 DISTINCT RootTable0.DurableKey AS DurableKey,  (
		  CASE
                    WHEN  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X *100 <  25  )  THEN 0
                    WHEN  (   (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 <  50  ) 
                        AND  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=  25  )   )  THEN 1
                    WHEN  (   (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 <  75  ) 
                        AND  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=  50  )   )  THEN 2
                    WHEN  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=     75 )  THEN 3
                    ELSE NULL END )
		      AS slice2
                FROM
                 PatientDim AS RootTable0
                WHERE
                  (  (  ( RootTable0.IsValid = 1 ) 
                        AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                    AND  ( RootTable0.IsCurrent = 1 ) 
                    AND  ( SlicerTable1.SocioeconomicThemeSummaryPctlRanking*100 IS NOT NULL )   )  )  ConsolidatedTable5 ON RootTable0.DurableKey = ConsolidatedTable5.DurableKey
            WHERE
              (  (  ( RootTable0.IsValid = 1 ) 
                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                AND  ( RootTable0.IsCurrent = 1 ) 
                AND  (  (  ( FilterTable1.StartDateKey < @end_date ) 
                        AND NOT  ( FilterTable1.StartDateKey < 0 )  ) 
                    AND  (  ( FilterTable1.EndDateKey >= @start_date )  OR  ( FilterTable1.EndDateKey < 0 )  )  )  )   )  subq0
        WHERE
          (  (  (  ( subq0.slice1 <> '19' )  OR  ( subq0.slice1MaxValue IS NULL )  ) 
                AND  (  ( subq0.slice2 <> '4' )  OR  ( subq0.slice2MaxValue IS NULL )  )  )  ) 
 )  subq

    WHERE
    
subq.DurableKey > 0

    GROUP BY slice0, slice1, slice2
 )  AS tempResultSet
OPTION ( RECOMPILE, USE HINT ( 'FORCE_DEFAULT_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL' ) , NO_PERFORMANCE_SPOOL ) 
