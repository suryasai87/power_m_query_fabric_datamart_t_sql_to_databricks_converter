-- PowerBI Report Query - Last 7 Days
-- Filter daily running sum data for recent analysis

SELECT
    [WorkDate],
    [TotalHoursForDay],
    [RunningTotalHours]
FROM [dbo].[daily_running_sum]
WHERE [WorkDate] >= DATEADD(day, -7, GETDATE())
ORDER BY [WorkDate] DESC;
