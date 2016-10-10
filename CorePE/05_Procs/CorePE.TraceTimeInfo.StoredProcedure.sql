SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CorePE].[TraceTimeInfo] 
/*   
	PROCEDURE:		CorePE.TraceTimeInfo

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Given a point in time (usually executed with the current time), finds the start time and end time
		of the next trace for the @Utility supplied. 

	OUTSTANDING ISSUES: None at this time

    CHANGE LOG:	
				2016-04-24	Aaron Morelli		Final run-through and commenting


	MIT License

	Copyright (c) 2016 Aaron Morelli

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.


To Execute
------------------------
declare @rc INT,
	@pit DATETIME, 
	@en NCHAR(1),
	@st DATETIME, 
	@nd DATETIME

EXEC @rc = CorePE.TraceTimeInfo @Utility=N'AutoWho', @PointInTime = @pit, @UtilityIsEnabled = @en OUTPUT, 
		@UtilityStartTime = @st OUTPUT, @UtilityEndTime = @nd OUTPUT

select @rc as ProcRC, @en as Enabled, @st as StartTime, @nd as EndTime
*/
(
	@Utility NVARCHAR(20),
	@PointInTime DATETIME,
	@UtilityIsEnabled NCHAR(1) OUTPUT,
	@UtilityStartTime DATETIME OUTPUT,
	@UtilityEndTime DATETIME OUTPUT
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @lmsg NVARCHAR(4000),
		@rc INT,
		@timetmp_smaller DATETIME,
		@timetmp_larger DATETIME;

	DECLARE 
		@opt__BeginTime		SMALLINT,		
		@opt__EndTime		SMALLINT
		;

	IF @PointInTime IS NULL
	BEGIN
		SET @PointInTime = GETDATE();
	END

	IF @Utility NOT IN (N'AutoWho', N'ServerEye')
	BEGIN
		RAISERROR('Parameter @Utility must be in the following list: AutoWho, ServerEye', 16, 1);
		RETURN -1;
	END

	IF @Utility = N'AutoWho'
	BEGIN
		SELECT 
			@UtilityIsEnabled		 = [AutoWhoEnabled],
			@opt__BeginTime			 = [BeginTime],
			@opt__EndTime			 = [EndTime]
		FROM AutoWho.Options o;

		--Ok, we have the various option values. Note that if BeginTime is smaller than EndTime, 
		-- we have a trace that does NOT span a day... e.g. 5am to 4pm
		-- However, if EndTime is > BeginTime, then we DO have a trace that spans a day, e.g. 4pm to 5am
		SET @UtilityStartTime = DATEADD(MINUTE, 
										@opt__BeginTime % 100,
										DATEADD(HOUR, 
											@opt__BeginTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		SET @UtilityEndTime = DATEADD(MINUTE, 
										@opt__EndTime % 100,
										DATEADD(HOUR, 
											@opt__EndTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		IF @UtilityEndTime < @UtilityStartTime
		BEGIN
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END

		IF @PointInTime >= @UtilityEndTime
		BEGIN
			SET @UtilityStartTime = DATEADD(DAY, 1, @UtilityStartTime);
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END
	END
	ELSE IF @Utility = N'ServerEye'
	BEGIN
		--Ok validation succeeeded. Get our option values
		SELECT 
			@UtilityIsEnabled		 = [ServerEyeEnabled],
			@opt__BeginTime			 = [BeginTime],
			@opt__EndTime			 = [EndTime]
		FROM ServerEye.Options o
		;

		--Ok, we have the various option values. Note that if BeginTime is smaller than EndTime, 
		-- we have a trace that does NOT span a day... e.g. 5am to 4pm
		-- However, if EndTime is < BeginTime, then we DO have a trace that spans a day, e.g. 4pm to 5am
		SET @UtilityStartTime = DATEADD(MINUTE, 
										@opt__BeginTime % 100,
										DATEADD(HOUR, 
											@opt__BeginTime / 100, 
											CONVERT(DATETIME, 
													CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		SET @UtilityEndTime = DATEADD(MINUTE, 
										@opt__EndTime % 100,
										DATEADD(HOUR, 
											@opt__EndTime / 100, 
											CONVERT(DATETIME, 
												CONVERT(VARCHAR(20), @PointInTime,101)
													)
												)
										);

		IF @UtilityEndTime < @UtilityStartTime
		BEGIN
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END

		IF @PointInTime >= @UtilityEndTime
		BEGIN
			SET @UtilityStartTime = DATEADD(DAY, 1, @UtilityStartTime);
			SET @UtilityEndTime = DATEADD(DAY, 1, @UtilityEndTime);
		END
	END --outside IF/ELSE that controls utility-specific logic	

	RETURN 0;
END 
GO
