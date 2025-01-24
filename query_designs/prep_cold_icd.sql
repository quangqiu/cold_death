

select * into #YearlyDateBuckets FROM Projects.ProjectD2D4B4.dbo.YearlyDateBuckets


SELECT
GrouperSubquery3.* INTO #ColdRelatedICD
FROM
 ( 
    SELECT
     DISTINCT table0.DiagnosisKey AS basekey, table0.GroupedNameAndCode AS grouper_id_2, table0.GroupedNameAndCode AS grouper_name_2
    FROM
     dbo.DiagnosisTerminologyDim AS table0
    WHERE
      (  ( table0.[Type]=N'ICD-10-CM' ) 
        AND  ( table0.GroupedNameAndCode IS NOT NULL )  )  )  AS GrouperSubquery3
LEFT OUTER JOIN Epic.CciForBatchMode ON 0 = 1
WHERE
 (   (  GrouperSubquery3.grouper_id_2 = 'Exposure to excessive natural cold( ICD-10-CM: X31.* )'  )  OR  (  GrouperSubquery3.grouper_id_2 = 'Hypothermia( ICD-10-CM: T68.* )'  )  OR  (  GrouperSubquery3.grouper_id_2 = 'Other effects of reduced temperature( ICD-10-CM: T69.* )'  )   ) 


---2022  2023    784   787
---1-7  days    4344  4535
---1-10 days    4344  4535
