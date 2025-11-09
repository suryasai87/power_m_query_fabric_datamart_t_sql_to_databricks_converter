-- Complex T-SQL Example with Multiple Data Types and Patterns

CREATE TABLE [dbo].[Customer] (
    [CustomerId] INT PRIMARY KEY NOT NULL,
    [CustomerGUID] UNIQUEIDENTIFIER DEFAULT NEWID(),
    [FirstName] NVARCHAR(50) NOT NULL,
    [LastName] NVARCHAR(50) NOT NULL,
    [Email] VARCHAR(100),
    [Phone] CHAR(10),
    [Balance] MONEY DEFAULT 0.00,
    [CreditLimit] DECIMAL(10,2),
    [IsActive] BIT DEFAULT 1,
    [CreatedDate] DATETIME2 DEFAULT GETDATE(),
    [ModifiedDate] DATETIME,
    [BirthDate] DATE,
    [ProfileData] NTEXT,
    [Metadata] XML
);

-- Query with various T-SQL patterns
SELECT
    c.[CustomerId],
    c.[FirstName] + ' ' + c.[LastName] AS FullName,
    CAST(c.[CreatedDate] AS DATE) AS SignupDate,
    DATEDIFF(day, c.[CreatedDate], GETDATE()) AS DaysSinceSignup,
    DATEADD(month, 6, c.[CreatedDate]) AS ReviewDate,
    c.[Balance],
    c.[CreditLimit],
    CASE
        WHEN c.[Balance] > c.[CreditLimit] THEN 'Over Limit'
        WHEN c.[Balance] > c.[CreditLimit] * 0.8 THEN 'Warning'
        ELSE 'OK'
    END AS CreditStatus,
    SUM(c.[Balance]) OVER (PARTITION BY DATEPART(year, c.[CreatedDate]) ORDER BY c.[CreatedDate]) AS YearlyRunningBalance
FROM [dbo].[Customer] c
WHERE c.[IsActive] = 1
    AND c.[CreatedDate] >= DATEADD(year, -1, GETDATE())
ORDER BY c.[CustomerId];
