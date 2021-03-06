USE [AdventureWorks]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
IF OBJECT_ID('HumanResources.uspUpdateEmployeeHireInfo_ProfilerProcTest') IS NOT NULL
BEGIN
	DROP PROCEDURE [HumanResources].[uspUpdateEmployeeHireInfo_ProfilerProcTest];
END
GO
CREATE PROCEDURE [HumanResources].[uspUpdateEmployeeHireInfo_ProfilerProcTest]
    @EmployeeID [int], 
    @Title [nvarchar](50), 
    @HireDate [datetime], 
    @RateChangeDate [datetime], 
    @Rate [money], 
    @PayFrequency [tinyint], 
    @CurrentFlag [dbo].[Flag] 
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

	DECLARE @lmsg NVARCHAR(MAX),
		@TraceFailureOccurred INT;

    BEGIN TRY
		EXEC @TraceFailureOccurred = [PerfEye21].[PerformanceEye].[ProfilerTraceBySPID_Start] @TraceCategories=N'All', 
												@IncludePerfWarnings=N'Y',
												@SPID=NULL,			--defaults to current SPID
												@Duration=0,
												@ReturnMessage=@lmsg OUTPUT
												;

        BEGIN TRANSACTION;

        UPDATE [HumanResources].[Employee] 
        SET [Title] = @Title 
            ,[HireDate] = @HireDate 
            ,[CurrentFlag] = @CurrentFlag 
        WHERE [EmployeeID] = @EmployeeID;

        INSERT INTO [HumanResources].[EmployeePayHistory] 
            ([EmployeeID]
            ,[RateChangeDate]
            ,[Rate]
            ,[PayFrequency]) 
        VALUES (@EmployeeID, @RateChangeDate, @Rate, @PayFrequency);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback any active or uncommittable transactions before
        -- inserting information in the ErrorLog
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END

        EXECUTE [dbo].[uspLogError];
    END CATCH;

	IF @TraceFailureOccurred = 0
	BEGIN
		EXEC [PerfEye21].[PerformanceEye].[ProfilerTraceBySPID_Stop] @SPID=NULL,		--will use the current @@SPID to find the sys.traces ID via a CorePE mapping table
												@ReturnMessage=@lmsg OUTPUT
												;
	END
END;
GO

DECLARE @EmployeeID INT, 
		@Title NVARCHAR(50),
		@HireDate DATETIME,
		@RateChangeDate DATETIME,
		@Rate MONEY,
		@PayFrequency TINYINT;

DECLARE @cf dbo.Flag;
SET @cf = 1;

SELECT @EmployeeID = EmployeeID,
	@Title = e.Title + '-PEtest',
	@HireDate = e.HireDate,
	@RateChangeDate = getdate()
FROM HumanResources.Employee e
WHERE EmployeeID = convert(int,100000.*RAND(DATEDIFF(second, convert(datetime,convert(date, getdate())), getdate()))) % 290;

select @Rate = ss.Rate + $0.01, @PayFrequency = ss.PayFrequency
from (
	select Rate, PayFrequency, rn = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	from [HumanResources].[EmployeePayHistory] 
	where EmployeeID = @EmployeeID
) ss
where ss.rn = 1;


EXEC [HumanResources].[uspUpdateEmployeeHireInfo_ProfilerProcTest]
    @EmployeeID = @EmployeeID, 
    @Title = @Title, 
    @HireDate = @HireDate, 
    @RateChangeDate = @Rate, 
    @Rate = @Rate, 
    @PayFrequency = @PayFrequency, 
    @CurrentFlag = @cf 
	;
