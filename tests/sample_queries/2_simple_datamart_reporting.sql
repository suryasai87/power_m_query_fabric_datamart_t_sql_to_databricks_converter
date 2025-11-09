-- T-SQL Query with Running Sum (Embedded in Power M Query / Datamart)
-- Daily Hours Worked with Running Total

WITH DailyHours AS (
    SELECT
        CAST([CreatedDate] AS DATE) AS WorkDate,
        SUM([Hours_Worked__c]) AS TotalHoursForDay
    FROM [dbo].[work_orders_last_12_months]
    GROUP BY CAST([CreatedDate] AS DATE)
)
SELECT
    WorkDate,
    TotalHoursForDay,
    SUM(TotalHoursForDay) OVER (ORDER BY WorkDate) AS RunningTotalHours,
    DATEADD(day, -7, GETDATE()) AS SevenDaysAgo
FROM DailyHours
ORDER BY WorkDate;
