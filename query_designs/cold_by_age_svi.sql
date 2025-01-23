-- select * into #YearlyDateBuckets from projects.ProjectD2D4B4.dbo.YearlyDateBuckets

DECLARE @start_date AS nvarchar(MAX) = '20000101'
DECLARE @end_date   AS nvarchar(MAX) = '20241209'
DECLARE @vax_duration_name AS nvarchar(MAX) = 'W200_W001'

DECLARE @start_age  AS integer = 65
DECLARE @end_age_plus1  AS integer = 151
DECLARE @age_name   AS nvarchar(MAX) =  '65_150'
DECLARE @birth_base_date  AS nvarchar(MAX) =  '2024-12-19'

/**
*  30 106
*  35  964
*  40  3323
*  44  4409
*/

SELECT
* INTO #cold_deaths
FROM
 ( 
    SELECT
     slice0 , slice1 , COUNT_BIG ( DurableKey )  AS count2 , COUNT_BIG ( DurableKey )  AS count3
    FROM
      ( 
        SELECT
         subq0.DurableKey, COALESCE ( subq0.slice0, subq1.slice0 )  AS slice0 , COALESCE ( subq0.slice1, subq1.slice1 )  AS slice1 , NULL AS count2, NULL AS count3
        FROM
          ( 
            SELECT
             DurableKey, slice0 , slice1 , slice1MaxValue , NULL AS count2, NULL AS count3
            FROM
              ( 
                SELECT
                 subq0.DurableKey, subq0.slice0 AS slice0 , subq0.slice1 AS slice1 , subq0.slice1MaxValue AS slice1MaxValue , NULL AS count2, NULL AS count3
                FROM
                  (   ( 
                        SELECT
                         DISTINCT NULL AS count2 , NULL AS count3 , RootTable0.DurableKey AS DurableKey, DateTable4.DateBucket AS slice0 , NULL AS slice1 , NULL AS slice1MaxValue
                        FROM
                         PatientDim AS RootTable0
                        INNER JOIN DiagnosisEventFact AS FilterTable1 ON  ( RootTable0.DurableKey = FilterTable1.PatientDurableKey ) 
                        AND  (  ( FilterTable1.[Type] IN  ( N'Encounter Diagnosis',N'Admitting Diagnosis' )  )  ) 
                        INNER JOIN  (SELECT * from #YearlyDateBuckets where startDate >= @start_date AND startDate < @end_date) AS DateTable4 ON  (  ( FilterTable1.StartDateKey < DateTable4.endDate  ) 
                            AND  ( FilterTable1.EndDateKey >= DateTable4.startDate OR FilterTable1.EndDateKey < 0 )  ) 
                        INNER JOIN dbo.DateDim AS table5 ON FilterTable1.StartDateKey = table5.DateKey
                        INNER JOIN dbo.DateDim AS table6 ON  ( FilterTable1.EndDateKey = table6.DateKey ) 
                        AND  (  (  (  (  ( FilterTable1.StartDateKey < @end_date ) 
                                        AND NOT  ( FilterTable1.StartDateKey < 0 )  ) 
                                    AND  (  ( FilterTable1.EndDateKey >=@start_date )  OR  ( FilterTable1.EndDateKey < 0 )  )  ) 
                                AND  (  ( table5.DateValue < DATEADD ( YEAR, @end_age_plus1,RootTable0.BirthDate )  ) 
                                    AND  (  ( table6.DateValue IS NULL )  OR  ( table6.DateValue >= DATEADD ( YEAR,@start_age,RootTable0.BirthDate )  )  ) 
                                    AND  ( RootTable0.DeathDate IS NULL OR RootTable0.DeathDate >= DATEADD ( YEAR,@start_age,RootTable0.BirthDate )  ) 
                                    AND  ( RootTable0.BirthDate <= DATEADD ( YEAR,-@start_age,  @birth_base_date )  )  )  )  ) 
                        INNER JOIN  ( 
                            SELECT
                             RootTable0.DurableKey AS DurableKey, RootTable0.DeathDate AS startDate
                            FROM
                             PatientDim AS RootTable0
                            WHERE
                              (  (  ( RootTable0.IsValid = 1 ) 
                                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                                AND  ( RootTable0.IsCurrent = 1 ) 
                                AND  (   (  ( CASE
                                            WHEN RootTable0.DeathDate IS NOT NULL THEN 1
                                            ELSE 0 END = '1' )  )   )   )  )  sequential7 ON  ( FilterTable1.PatientDurableKey = sequential7.DurableKey ) 
                        AND  (  ( sequential7.DurableKey > 0 )  ) 
                        AND  (  (  ( sequential7.startDate >=  (  CASE
                                        WHEN  ( CASE
                                            WHEN FilterTable1.StartDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.StartDateKey )  ) 
                                            ELSE CONVERT ( DATE, '90001231' )  END )  < '99991231' THEN DATEADD ( DAY, 0,  ( CASE
                                                WHEN FilterTable1.StartDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.StartDateKey )  ) 
                                                ELSE CONVERT ( DATE, '90001231' )  END )  ) 
                                        ELSE '99991231' END  )  ) 
                                AND  ( sequential7.startDate <=  (  CASE
                                        WHEN  ( CASE
                                            WHEN FilterTable1.StartDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.StartDateKey )  ) 
                                            ELSE CONVERT ( DATE, '90001231' )  END )  < '99991221' THEN DATEADD ( DAY, 10,  ( CASE
                                                WHEN FilterTable1.StartDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.StartDateKey )  ) 
                                                ELSE CONVERT ( DATE, '90001231' )  END )  ) 
                                        ELSE '17530101' END  )  )  )  ) 
                        WHERE
                          (  (  ( RootTable0.IsValid = 1 ) 
                                AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                            AND  ( RootTable0.IsCurrent = 1 ) 
                            AND EXISTS  ( 
                                SELECT
                                 1
                                FROM
                                 #ColdRelatedICD AS GrouperSubquery3
                                WHERE
                                  (  (  (  FilterTable1.DiagnosisKey = GrouperSubquery3.basekey  )  )  )  )   )   )  UNION  ( 
                        SELECT
                         DISTINCT NULL AS count2 , NULL AS count3 , RootTable0.DurableKey AS DurableKey, DateTable4.DateBucket AS slice0 , NULL AS slice1 , NULL AS slice1MaxValue
                        FROM
                         PatientDim AS RootTable0
                        INNER JOIN DiagnosisEventFact AS FilterTable1 ON  ( RootTable0.DurableKey = FilterTable1.PatientDurableKey ) 
                        AND  (  ( FilterTable1.[Type] IN  ( N'Encounter Diagnosis',N'Admitting Diagnosis' )  )  ) 
                        INNER JOIN  ( SELECT * from  #YearlyDateBuckets where startDate >=@start_date AND startDate < @end_date) AS DateTable4 ON  ( ( FilterTable1.StartDateKey < DateTable4.endDate  ) 
                            AND  ( FilterTable1.EndDateKey >= DateTable4.startDate OR FilterTable1.EndDateKey < 0 )  ) 
                        INNER JOIN dbo.DateDim AS table5 ON FilterTable1.StartDateKey = table5.DateKey
                        INNER JOIN dbo.DateDim AS table6 ON  ( FilterTable1.EndDateKey = table6.DateKey ) 
                        AND  (  (  (  (  ( FilterTable1.StartDateKey < @end_date ) 
                                        AND NOT  ( FilterTable1.StartDateKey < 0 )  ) 
                                    AND  (  ( FilterTable1.EndDateKey >=@start_date )  OR  ( FilterTable1.EndDateKey < 0 )  )  ) 
                                AND  (  ( table5.DateValue < DATEADD ( YEAR,@end_age_plus1,RootTable0.BirthDate )  ) 
                                    AND  (  ( table6.DateValue IS NULL )  OR  ( table6.DateValue >= DATEADD ( YEAR,@start_age,RootTable0.BirthDate )  )  ) 
                                    AND  ( RootTable0.DeathDate IS NULL OR RootTable0.DeathDate >= DATEADD ( YEAR,@start_age,RootTable0.BirthDate )  ) 
                                    AND  ( RootTable0.BirthDate <= DATEADD ( YEAR,-@start_age,  @birth_base_date )  )  )  )  ) 
                        INNER JOIN  ( 
                            SELECT
                             RootTable0.DurableKey AS DurableKey,  (select DateBucket FROM #YearlyDateBuckets where
			      			      (  ( RootTable0.DeathDate < endDate ) 
                                        	      	 AND  ( RootTable0.DeathDate >= startDate OR RootTable0.DeathDate IS NULL )  )
			      )  AS slice0 , RootTable0.DeathDate AS startDate , RootTable0.DeathDate AS endDate
                            FROM
                             PatientDim AS RootTable0
                            WHERE
                              (  (  ( RootTable0.IsValid = 1 ) 
                                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                                AND  ( RootTable0.IsCurrent = 1 ) 
                                AND  (   (   (  ( CASE
                                                WHEN RootTable0.DeathDate IS NOT NULL THEN 1
                                                ELSE 0 END = '1' )  )   ) 
                                    AND  (  (  ( RootTable0.DeathDate < @end_date ) 
                                            AND NOT  ( RootTable0.DeathDate IS NULL )  ) 
                                        AND  (  ( RootTable0.DeathDate >= @start_date )  OR  ( RootTable0.DeathDate IS NULL )  )  )  )   )  )  overlapping7 ON  ( FilterTable1.PatientDurableKey = overlapping7.DurableKey ) 
                        AND  (  ( overlapping7.DurableKey > 0 )  ) 
                        AND  (  (  overlapping7.slice0 = DateTable4.DateBucket  )  ) 
                        AND  (  (  (  (  ( overlapping7.startDate <=  ( CASE
                                                WHEN FilterTable1.EndDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.EndDateKey )  ) 
                                                ELSE CONVERT ( DATE, '90001231' )  END )  OR FilterTable1.EndDateKey < 0 )  ) 
                                    AND  (  ( overlapping7.endDate >=  ( CASE
                                                WHEN FilterTable1.StartDateKey > 0 THEN TRY_CONVERT ( DATE, TRY_CONVERT ( VARCHAR ( 8 ) , FilterTable1.StartDateKey )  ) 
                                                ELSE CONVERT ( DATE, '90001231' )  END )  OR overlapping7.endDate IS NULL )  )  )  )  ) 
                        WHERE
                          (  (  ( RootTable0.IsValid = 1 ) 
                                AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                            AND  ( RootTable0.IsCurrent = 1 ) 
                            AND EXISTS  ( 
                                SELECT
                                 1
                                FROM
                                 #ColdRelatedICD AS GrouperSubquery3
                                WHERE
                                  (  (  (  FilterTable1.DiagnosisKey = GrouperSubquery3.basekey  )  )  )  )   )   )   )  subq0  )  subset0  )  subq0
        INNER JOIN  ( 
            SELECT
             RootTable0.DurableKey AS DurableKey, ISNULL ( NotASubquery1.slice1, 4 )  AS slice1 , NULL AS slice1MaxValue , NULL AS slice0 , NULL AS count2 , NULL AS count3
            FROM
             PatientDim AS RootTable0
            LEFT OUTER JOIN  ( 
                SELECT
                 DISTINCT RootTable0.DurableKey AS DurableKey,  ( CASE
                    WHEN  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X *100 <  25  )  THEN 0
                    WHEN  (   (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 <  50  ) 
                        AND  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=  25  )   )  THEN 1
                    WHEN  (   (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 <  75  ) 
                        AND  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=  50  )   )  THEN 2
                    WHEN  (  RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 >=     75 )  THEN 3
                    ELSE NULL END )  AS slice1
                FROM
                 PatientDim AS RootTable0
                WHERE
                  (  (  ( RootTable0.IsValid = 1 ) 
                        AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                    AND  ( RootTable0.IsCurrent = 1 ) 
                    AND  ( RootTable0.SviSocioeconomicPctlRankingByZip2018_X * 100 IS NOT NULL )   )  )  NotASubquery1 ON RootTable0.DurableKey = NotASubquery1.DurableKey
            WHERE
              (  (  ( RootTable0.IsValid = 1 ) 
                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                AND  ( RootTable0.IsCurrent = 1 )  )   )  subq1 ON subq0.DurableKey = subq1.DurableKey
        WHERE
          (  (  (  ( subq0.slice1 <> '4' )  OR  ( subq0.slice1MaxValue IS NULL )  )  ) 
            AND  (  (  ( subq0.slice1 <> '4' )  OR  ( subq0.slice1MaxValue IS NULL )  )  ) 
            AND  (  (  ( subq1.slice1 <> '4' )  OR  ( subq1.slice1MaxValue IS NULL )  )  )  ) 
 )  subq

    WHERE
    
subq.DurableKey > 0

    GROUP BY slice0, slice1
 )  AS tempResultSet
OPTION ( RECOMPILE, USE HINT ( 'FORCE_DEFAULT_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL' ) , NO_PERFORMANCE_SPOOL ) 
