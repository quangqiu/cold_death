SELECT
* INTO #cold_deaths_by_agepr
FROM
 ( 

    SELECT
     slice0 , slice1 , COUNT_BIG ( DurableKey )  AS count2 

    FROM
      ( 

        SELECT
         subq0.DurableKey, subq0.slice0 AS slice0 , subq0.slice1 AS slice1 , 
        FROM
          ( 
            SELECT
             RootTable0.DurableKey AS DurableKey,  (
	             SELECT DateBucket FROM #YearlyDateBuckets where ( RootTable0.DeathDate < endDate  ) 
                        AND  ( RootTable0.DeathDate >= startDate OR RootTable0.DeathDate IS NULL ) 
		)  AS slice0 , NULL AS slice1MaxValue , NULL AS count2 , NULL AS count3 ,
		(
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
             PatientDim AS RootTable0
            WHERE
              (  (  ( RootTable0.IsValid = 1 ) 
                    AND  ( RootTable0.UseInCosmosAnalytics_X = 1 )  ) 
                AND  ( RootTable0.IsCurrent = 1 ) 
                AND  (  ( CASE
                        WHEN RootTable0.DeathDate IS NOT NULL THEN 1
                        ELSE 0 END = '1' )  ) 
                AND  (  (  ( RootTable0.DeathDate < @end_date ) 
                        AND NOT  ( RootTable0.DeathDate IS NULL )  ) 
                    AND  (  ( RootTable0.DeathDate >= @start_date )  OR  ( RootTable0.DeathDate IS NULL )  )  )  )   )  subq0
        WHERE
          (  (  (  ( subq0.slice1 <> '18' )  OR  ( subq0.slice1MaxValue IS NULL )  )  )  ) 
 )  subq

    WHERE
    
subq.DurableKey > 0

    GROUP BY slice0, slice1
 )  AS tempResultSet
OPTION ( RECOMPILE, USE HINT ( 'FORCE_DEFAULT_CARDINALITY_ESTIMATION', 'DISABLE_OPTIMIZER_ROWGOAL' ) , NO_PERFORMANCE_SPOOL ) 
