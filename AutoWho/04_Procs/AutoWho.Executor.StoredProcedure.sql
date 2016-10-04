SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE   [AutoWho].[Executor]
/*   
	PROCEDURE:		AutoWho.Executor

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Sits in a loop for the duration of an AutoWho trace, calling the collector every X seconds. (By default, 15). 
		This proc is called directly by the SQL Agent job "PerformanceEye - Disabled - AutoWho Trace". 

		See the "Control Flow Summary" comment below for more details.


      CHANGE LOG:	2015-05-22	Aaron Morelli		Dev Begun
					2016-04-24	Aaron Morelli		Final run-through and commenting

To stop the trace before its end-time: 
	exec CorePE.AbortTrace @Utility=N'AutoWho',@TraceID = NULL | <number>, @PreventAllDay = N'N' | N'Y'		--null trace ID means the most recent one


DECLARE @ProcRC INT
DECLARE @lmsg VARCHAR(4000)

EXEC @ProcRC = AutoWho.Executor @ErrorMessage = @lmsg OUTPUT

PRINT 'Return Code: ' + CONVERT(VARCHAR(20),@ProcRC)
PRINT 'Return Message: ' + COALESCE(@lmsg,'<NULL>')

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
*/
(
@ErrorMessage	NVARCHAR(4000) OUTPUT
)
AS
BEGIN

/* Control Flow Summary
	Here's the work done by this proc:

		1. Obtain applock named "AutoWhoBackgroundTrace"

		2. Permissions checks

		3. Check whether the Signal table (a method to communicate with running Executor instances) 
			has any direction in it to prevent new traces from starting today.

		4. Check whether AutoWho tracing is even enabled, and if so, what the start/end time is for 
			the next trace, based on the current point in time.

			a. If StartTime is < current time and EndTime is > current time, the trace should be running.
				The proc aborts if the trace shouldn't be running. In practice, this won't happen because
				the "sqlcrosstools Master" job is responsible for starting the Executor proc (via a SQL Agent job that
				is otherwise disable + no schedule) and it checks the Start/End time as well.

		5. Parse the DB/SPID Inclusion/Exclusion option strings into a filter table variable

		6. Determine whether there are any long-running SPIDs that we don't want to interfere with the Collector's
			threshold-based logic for obtaining expensive stuff like query plans. If there are, populate them into
			the AutoWho.ThresholdFilterSpids

		7. Call the AutoWho.PrePopulateDimensions table to add any dim keys that might not already be there.

		8. Check current time against the End time of the trace and ensure that our trace will run >= 60 seconds

		9. creates a "CorePE" trace, which at this point is just an entry in a table. 
		
		10. Obtains the SQL Server startup time (used in some SPID duration calculations in the collector for spids that have start times like 1900-01-01 and such)

		11. Determines when the last time the Store tables had their "LastTouchedBy" field updated, via the AutoWho.Log table. 
			Then, we call the AutoWho.UpdateStoreLastTouched proc to get the Stores fully up-to-date before we start the collector.

		12. If the Collector should attempt to resolve page latch page IDs, we turn TF 3604 on.

		13. If we want query plans to have extra info in them when pulled, we turn on TF 8666.
		
		14. Enter the loop until the trace is aborted or we reach the end time of the trace.


		The loop's logic is this: 

			a. increment the counter, set the LoopStartTime, and reset the NumSPIDsCaptured variable

			b. Call the Collector procedure with all of our option parameters and the filter table.
				i. If it is time to recompile the AutoWho.Collector procedure, we call the proc WITH RECOMPILE
				ii. Otherwise, call it normally.

			c. If the Collector raised an exception, and this is the 10th straight run w/an exception, signal for a trace abort

			d. Capture the completion time of the collector.

			e. If the collector took > 30000 ms, run the "lightweight collector" just so we have some data on what might be bogging things down.

			f. Evaluate the # of SPIDs capture the last 6 runs to see whether we should recompile the Collector procedure.
				(Some query plans in the collector are more sensitive to large changes in the # of spids collected. 

			g. Every @opt__ThresholdFilterRefresh, empty the AutoWho.ThresholdFilterSpids table and re-calculate.
				We do this because those spids may have stopped & re-started, but under a different spid #.
				Doing this in a high-frequency way would be expensive, but in a low-frequency way would put us at risk
				for not collecting important data for SPIDs that are unrelated to the "running all day threshold-ignore" stuff.

			h. If an abort was signalled (either b/c of 10 exceptions or b/c a human requested the trace stop), we set the
				@lv__EarlyAbort variable so that the loop won't re-execute.

			i. If it has been 10 minutes since we last called AutoWho.UpdateStoreLastTouched, do that now.

			j. If we are not aborting, get current time again, diff it with the LoopStart time, and figure out how
				many seconds to WAITFOR such the we are aligned on @opt__IntervalLength boundaries.

*** after the loop ends:
		15. Delete any one-time abort signals in the signal table and calculate the right text to return in @ErrorMessage

		16. stop the CorePE trace via CorePE.StopTrace

		17. Disable TF 3604 and 8666 if we started them.

		18. Release the "AutoWhoBackgroundTrace" app lock
*/


SET NOCOUNT ON; 
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SET ANSI_PADDING ON;	--Aaron M	2015-05-30	If the calling session has set this setting OFF, the XML method 
						--of parsing the @Inclusion/Exclusion parameters will not work

--Master TRY/CATCH block
BEGIN TRY
	DECLARE @lv__SQLVersion NVARCHAR(10);
	SELECT @lv__SQLVersion = (
	SELECT CASE
			WHEN t.col1 LIKE N'8%' THEN N'2000'
			WHEN t.col1 LIKE N'9%' THEN N'2005'
			WHEN t.col1 LIKE N'10.5%' THEN N'2008R2'
			WHEN t.col1 LIKE N'10%' THEN N'2008'
			WHEN t.col1 LIKE N'11%' THEN N'2012'
			WHEN t.col1 LIKE N'12%' THEN N'2014'
			WHEN t.col1 LIKE N'13%' THEN N'2016'
		END AS val1
	FROM (SELECT CONVERT(SYSNAME, SERVERPROPERTY(N'ProductVersion')) AS col1) AS t);

	IF @lv__SQLVersion IN (N'2000',N'2005')
	BEGIN
		SET @ErrorMessage = N'AutoWho is only compatible with SQL 2008 and above.';
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), -1, N'SQLVersion', @ErrorMessage;
	 
		RETURN -1;
	END

	--General variables 
	DECLARE 
		 @lv__ThisRC					INT, 
		 @lv__ProcRC					INT, 
		 @lv__tmpStr					NVARCHAR(4000),
		 @lv__ScratchInt				INT,
		 @lv__ScratchDateTime			DATETIME,
		 @lv__EarlyAbort				NCHAR(1),
		 @lv__CalcEndTime				DATETIME,
		 @lv__RunTimeMinutes			BIGINT,
		 @lv__LoopStartTime				DATETIME,
		 @lv__AutoWhoCallCompleteTime	DATETIME,
		 @lv__LoopEndTime				DATETIME,
		 @lv__LoopNextStart				DATETIME,
		 @lv__LoopNextStartSecondDifferential INT,
		 @lv__WaitForMinutes			INT,
		 @lv__WaitForSeconds			INT,
		 @lv__WaitForString				VARCHAR(20),
		 @lv__IntervalRemainder			INT,
		 @lv__LoopDurationSeconds		INT,
		 @lv__LoopCounter				INT,
		 @lv__SuccessiveExceptions		INT,
		 @lv__IntervalFrequency			INT,
		 @lv__TraceID					INT,
		 @lv__DBInclusionsExist			BIT,
		 @lv__TempDBCreateTime			DATETIME,
		 @lv__NumSPIDsCaptured			INT,
		 @lv__NumSPIDsAtLastRecompile	INT,
		 @lv__SPIDsCaptured5Ago			INT,
		 @lv__SPIDsCaptured4Ago			INT,
		 @lv__SPIDsCaptured3Ago			INT,
		 @lv__SPIDsCaptured2Ago			INT,
		 @lv__SPIDsCaptured1Ago			INT,
		 @lv__SPIDCaptureHistAvg		INT,
		 @lv__RecompileAutoWho			BIT,
		 @lv__LastThresholdFilterTime	DATETIME,
		 @lv__LastStoreLastTouchedTime	DATETIME
		 ;

	--variables to hold option table contents
	DECLARE 
		@opt__IntervalLength					INT,	
		@opt__IncludeIdleWithTran				NVARCHAR(5),
		@opt__IncludeIdleWithoutTran			NVARCHAR(5),
		@opt__DurationFilter					INT,
		@opt__IncludeDBs						NVARCHAR(500),	
		@opt__ExcludeDBs						NVARCHAR(500),	
		@opt__HighTempDBThreshold				INT,
		@opt__CollectSystemSpids				NCHAR(1),	
		@opt__HideSelf							NCHAR(1),

		@opt__ObtainBatchText					NCHAR(1),	
		@opt__ParallelWaitsThreshold			INT,
		@opt__ObtainLocksForBlockRelevantThreshold	INT,
		@opt__ObtainQueryPlanForStatement		NCHAR(1),	
		@opt__ObtainQueryPlanForBatch			NCHAR(1),
		@opt__InputBufferThreshold				INT,
		@opt__BlockingChainThreshold			INT,
		@opt__BlockingChainDepth				TINYINT,
		@opt__TranDetailsThreshold				INT,
		@opt__ResolvePageLatches				NCHAR(1),
		@opt__Enable8666						NCHAR(1),
		@opt__ThresholdFilterRefresh			INT,
		@opt__QueryPlanThreshold				INT,
		@opt__QueryPlanThresholdBlockRel		INT,

		@opt__DebugSpeed						NCHAR(1),
		@opt__SaveBadDims						NCHAR(1)
		;

	EXEC @lv__ProcRC = sp_getapplock @Resource='AutoWhoBackgroundTrace',
					@LockOwner='Session',
					@LockMode='Exclusive',
					@LockTimeout=5000;

	IF @lv__ProcRC < 0
	BEGIN
		SET @ErrorMessage = N'Unable to obtain exclusive AutoWho Tracing lock.';
		SET @lv__ThisRC = -3;

		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'Obtaining applock', @ErrorMessage;
	 
		RETURN @lv__ThisRC;
	END

	IF has_perms_by_name(null, null, 'VIEW SERVER STATE') <> 1
	BEGIN
		SET @ErrorMessage = N'The VIEW SERVER STATE permission (or permissions/role membership that include VIEW SERVER STATE) is required to execute AutoWho. Exiting...';
		SET @lv__ThisRC = -5;

		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'Perms Validation', @ErrorMessage;

		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END

	/* Moving page latch resolution to the Every 15 minutes Master job
	--If we are going to try to resolve Page IDs (for PAGE%LATCH waits) or enable TF 8666, then we need sysadmin
	IF @opt__ResolvePageLatches = N'Y'
	BEGIN
		IF IS_SRVROLEMEMBER ('sysadmin') <> 1
		BEGIN
			SET @ErrorMessage = N'PageLatch resolution has been requested but the account running AutoWho does not have sysadmin permissions. Exiting...';
			SET @lv__ThisRC = -7;

			INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @lv__ThisRC, N'PageResSecurity', @ErrorMessage;

			EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
			RETURN @lv__ThisRC;
		END
	END
	*/

	--If we have an N'AllDay' AbortTrace flag entry for this day, then exit the procedure
	IF EXISTS (SELECT * FROM AutoWho.SignalTable WITH (ROWLOCK) 
				WHERE LOWER(SignalName) = N'aborttrace' 
				AND LOWER(SignalValue) = N'allday'
				AND DATEDIFF(DAY, InsertTime, GETDATE()) = 0 )
	BEGIN
		SET @ErrorMessage = N'An AbortTrace signal exists for today. This procedure has been told not to run the rest of the day.';
		SET @lv__ThisRC = -9;

		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'Abort flag exists', @ErrorMessage;

		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END

	--Delete any OneTime signals in the table, or signals in the past
	DELETE FROM AutoWho.SignalTable
	WHERE LOWER(SignalName) = N'aborttrace' 
	AND (
		LOWER(SignalValue) = N'onetime'
		OR 
		DATEDIFF(DAY, InsertTime, GETDATE()) > 0
		);

	--Obtain the next start/end times... Note that TraceTimeInfo calls the ValidateOption procedure
	DECLARE @lv__AutoWhoStartTime DATETIME, 
			@lv__AutoWhoEndTime DATETIME, 
			@lv__AutoWhoEnabled NCHAR(1)
			;

	EXEC CorePE.TraceTimeInfo @Utility=N'AutoWho', @PointInTime = NULL, @UtilityIsEnabled = @lv__AutoWhoEnabled OUTPUT,
		@UtilityStartTime = @lv__AutoWhoStartTime OUTPUT, @UtilityEndTime = @lv__AutoWhoEndTime OUTPUT
			;

	IF @lv__AutoWhoEnabled = N'N'
	BEGIN
		SET @ErrorMessage = 'According to the option table, AutoWho is not enabled';
		SET @lv__ThisRC = -11;
	
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'NotEnabled', @ErrorMessage;
	
		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END

	IF NOT (GETDATE() BETWEEN @lv__AutoWhoStartTime AND @lv__AutoWhoEndTime)
	BEGIN
		SET @ErrorMessage = 'The Current time is not within the window specified by BeginTime and EndTime options';
		SET @lv__ThisRC = -13;
	
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'Outside Begin/End', @ErrorMessage;
	
		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END

							
	SELECT 
		@opt__IntervalLength					= [IntervalLength],
		@opt__IncludeIdleWithTran				= [IncludeIdleWithTran],
		@opt__IncludeIdleWithoutTran			= [IncludeIdleWithoutTran],
		@opt__DurationFilter					= [DurationFilter],
		@opt__IncludeDBs						= [IncludeDBs],
		@opt__ExcludeDBs						= [ExcludeDBs],
		@opt__HighTempDBThreshold				= [HighTempDBThreshold],
		@opt__CollectSystemSpids				= [CollectSystemSpids],
		@opt__HideSelf							= [HideSelf],

		@opt__ObtainBatchText					= [ObtainBatchText],
		@opt__ParallelWaitsThreshold			= [ParallelWaitsThreshold],
		@opt__ObtainLocksForBlockRelevantThreshold = [ObtainLocksForBlockRelevantThreshold],
		@opt__ObtainQueryPlanForStatement		= [ObtainQueryPlanForStatement],
		@opt__ObtainQueryPlanForBatch			= [ObtainQueryPlanForBatch],
		@opt__QueryPlanThreshold				= [QueryPlanThreshold], 
		@opt__QueryPlanThresholdBlockRel		= [QueryPlanThresholdBlockRel], 
		@opt__InputBufferThreshold				= [InputBufferThreshold],
		@opt__BlockingChainThreshold			= [BlockingChainThreshold],
		@opt__BlockingChainDepth				= [BlockingChainDepth],
		@opt__TranDetailsThreshold				= [TranDetailsThreshold],
		@opt__ResolvePageLatches				= [ResolvePageLatches],
		@opt__Enable8666						= [Enable8666],
		@opt__ThresholdFilterRefresh			= [ThresholdFilterRefresh],
		@opt__DebugSpeed						= [DebugSpeed],
		@opt__SaveBadDims						= [SaveBadDims]
	FROM AutoWho.Options o

	--Parse the DB include/exclude filter options (comma-delimited) into the user-typed table variable
	DECLARE @FilterTVP AS CorePEFiltersType;
	/*
	CREATE TYPE CorePEFiltersType AS TABLE 
	(
		FilterType TINYINT NOT NULL, 
			--0 DB inclusion
			--1 DB exclusion
			--128 threshold filtering (spids that shouldn't be counted against the various thresholds that trigger auxiliary data collection)
			--down the road, more to come (TODO: maybe filter by logins down the road?)
		FilterID INT NOT NULL, 
		FilterName NVARCHAR(255)
	)
	*/

	IF ISNULL(@opt__IncludeDBs,N'') = N''
	BEGIN
		SET @lv__DBInclusionsExist = 0;		--this flag (only for inclusions) is a perf optimization used by the AutoWho proc
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO @FilterTVP (FilterType, FilterID, FilterName)
				SELECT 0, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @opt__IncludeDBs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @lv__DBInclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @lv__DBInclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @ErrorMessage = N'Error occurred when attempting to convert the "IncludeDBs option (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			SET @lv__ThisRC = -15;
	
			INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @lv__ThisRC, N'DB Inclusions', @ErrorMessage;
	
			EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
			RETURN @lv__ThisRC;
		END CATCH
	END

	IF ISNULL(@opt__ExcludeDBs, N'') <> N''
	BEGIN
		BEGIN TRY 
			INSERT INTO @FilterTVP (FilterType, FilterID, FilterName)
				SELECT 1, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @opt__ExcludeDBs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right
		END TRY
		BEGIN CATCH
			SET @ErrorMessage = N'Error occurred when attempting to convert the "ExcludeDBs option (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			SET @lv__ThisRC = -17;
	
			INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @lv__ThisRC, N'DB Exclusions', @ErrorMessage;
	
			EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
			RETURN @lv__ThisRC;
		END CATCH
	END 


	--The AutoWho.Collector proc contains conditional logic for some of the auxiliary data, that only gets executed
	-- if there are SPIDs that meet various "thresholds". (such as long duration, long transaction, etc)
	-- We want those SPIDs to be collected by the core set of data (e.g. the SessionsAndRequests table) but not 
	-- trigger the auxiliary capture. For example, the Sentinel DMV monitor job runs all day, and thus would 
	-- ALWAYS trigger the auxiliary logic for a "long-running spid" even though we really don't care about the Sentinel
	-- spid very often.

	TRUNCATE TABLE [AutoWho].[ThresholdFilterSpids];
	EXEC [AutoWho].[ObtainSessionsForThresholdIgnore];

	INSERT INTO @FilterTVP (FilterType, FilterID)
	SELECT DISTINCT 128, f.ThresholdFilterSpid
	FROM AutoWho.ThresholdFilterSpids f; 

	SET @lv__LastThresholdFilterTime = GETDATE();

	IF EXISTS (SELECT * FROM @FilterTVP t1 INNER JOIN @FilterTVP t2 ON t1.FilterID = t2.FilterID AND t1.FilterType = 0 AND t2.FilterType = 1)
	BEGIN
		SET @ErrorMessage = N'One or more DB names are present in both the IncludeDBs option and ExcludeDBs option. This is not allowed.';

		SET @lv__ThisRC = -19;
	
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'IncludeExclude', @ErrorMessage;
	
		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END

	--Let's prepopulate the dim tables... may result in fewer Collector runs where dimension values are missing and
	-- have to be inserted
	EXEC AutoWho.PrePopulateDimensions;

	SET @lv__RunTimeMinutes = DATEDIFF(SECOND, GETDATE(), @lv__AutoWhoEndTime);

	IF @lv__RunTimeMinutes < 60
	BEGIN
		SET @ErrorMessage = N'The current time, combined with the BeginTime and EndTime options, have resulted in a trace that will run for < 60 seconds. This is not allowed, and the trace will not be started.';
		SET @lv__ThisRC = -21;
	
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'Less 60sec', @ErrorMessage;
	
		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END


	--Ok, let's get a valid TraceID value and then start the loop!
	BEGIN TRY
		EXEC @lv__TraceID = CorePE.CreateTrace @Utility=N'AutoWho', @Type=N'Background', @IntendedStopTime = @lv__AutoWhoEndTime;

		IF ISNULL(@lv__TraceID,-1) < 0
		BEGIN
			SET @ErrorMessage = N'TraceID value is invalid. The Create Trace procedure failed silently.';
			SET @lv__ThisRC = -23;
	
			INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @lv__ThisRC, N'InvalidTraceID', @ErrorMessage;
	
			EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
			RETURN @lv__ThisRC;
		END
	END TRY
	BEGIN CATCH
		SET @ErrorMessage = N'Exception occurred when creating a new trace: ' + ERROR_MESSAGE();
		SET @lv__ThisRC = -25;
	
		INSERT INTO AutoWho.[Log]
		(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__ThisRC, N'CreateTraceException', @ErrorMessage;

		EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
		RETURN @lv__ThisRC;
	END CATCH

	INSERT INTO AutoWho.[Log]
	(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
	SELECT SYSDATETIME(),  @lv__TraceID, 0, N'Print TraceID', N'Starting AutoWho trace using TraceID ''' + CONVERT(varchar(20),@lv__TraceID) + '''.';

	INSERT INTO AutoWho.[Log]
	(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
	SELECT SYSDATETIME(), @lv__TraceID, 0, N'Runtime calc', N'The AutoWho trace is going to run for ''' + convert(varchar(20),@lv__RunTimeMinutes) + ''' seconds.';

	--We get the startup time for this SQL instance b/c we will start hitting situations where our datetime values could be NULL or even 1900-01-01, and
	-- we need to handle them. Most of the time, we'll use @lv__TempDBCreateTime as our fall-back value
	SET @lv__TempDBCreateTime = (select d.create_date from sys.databases d where d.name = N'tempdb');


	IF @opt__ResolvePageLatches = N'Y' OR @opt__Enable8666 = N'Y'
	BEGIN
		/* Moving the resolution logic to the Every 15 minute Master job
		IF @opt__ResolvePageLatches = N'Y'
		BEGIN
			BEGIN TRY
				DBCC TRACEON(3604) WITH NO_INFOMSGS;
			END TRY
			BEGIN CATCH
				SET @ErrorMessage = N'PageLatch Resolution was requested but cannot enable TF 3604. Message: ' + ERROR_MESSAGE();
				SET @lv__ThisRC = -29;
	
				INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__ThisRC, N'TF3604Enable', @ErrorMessage;

				EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
				RETURN @lv__ThisRC;
			END CATCH
		END
		*/

		IF @opt__Enable8666 = N'Y'
		BEGIN
			BEGIN TRY
				DBCC TRACEON(8666) WITH NO_INFOMSGS;
			END TRY
			BEGIN CATCH
				SET @ErrorMessage = N'Cannot enable TF 8666. Message: ' + ERROR_MESSAGE();
				SET @lv__ThisRC = -31;
	
				INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__ThisRC, N'TF8666Enable', @ErrorMessage;

				EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
				RETURN @lv__ThisRC;
			END CATCH
		END
	END

	SET @lv__LoopCounter = 0;
	SET @lv__SuccessiveExceptions = 0;
	SET @lv__EarlyAbort = N'N';				--O - one time; A - all day
	SET @lv__RecompileAutoWho = 1;		--let's start off by recompiling the proc

	--initialize all to the special "no valid value"
	SET @lv__SPIDsCaptured5Ago = -1;
	SET @lv__SPIDsCaptured4Ago = -1;
	SET @lv__SPIDsCaptured3Ago = -1;
	SET @lv__SPIDsCaptured2Ago = -1;
	SET @lv__SPIDsCaptured1Ago = -1;

	WHILE (GETDATE() < @lv__AutoWhoEndTime AND @lv__EarlyAbort = N'N')
	BEGIN
		--reset certain vars every iteration
		SET @lv__LoopStartTime = GETDATE();
		SET @lv__LoopCounter = @lv__LoopCounter + 1;
		SET @lv__NumSPIDsCaptured = -1;

		BEGIN TRY
			IF ISNULL(@lv__RecompileAutoWho,0) = 1
			BEGIN
				EXEC AutoWho.Collector
					@TempDBCreateTime = @lv__TempDBCreateTime,
					@IncludeIdleWithTran = @opt__IncludeIdleWithTran,
					@IncludeIdleWithoutTran = @opt__IncludeIdleWithoutTran,
					@DurationFilter = @opt__DurationFilter, 
					@FilterTable = @FilterTVP, 
					@DBInclusionsExist = @lv__DBInclusionsExist, 
					@HighTempDBThreshold = @opt__HighTempDBThreshold, 
					@CollectSystemSpids = @opt__CollectSystemSpids, 
					@HideSelf = @opt__HideSelf, 

					@ObtainBatchText = @opt__ObtainBatchText,
					@QueryPlanThreshold = @opt__QueryPlanThreshold,
					@QueryPlanThresholdBlockRel = @opt__QueryPlanThresholdBlockRel,
					@ParallelWaitsThreshold = @opt__ParallelWaitsThreshold, 
					@ObtainLocksForBlockRelevantThreshold = @opt__ObtainLocksForBlockRelevantThreshold,
					@ObtainQueryPlanForStatement = @opt__ObtainQueryPlanForStatement, 
					@ObtainQueryPlanForBatch = @opt__ObtainQueryPlanForBatch,
					@InputBufferThreshold = @opt__InputBufferThreshold, 
					@BlockingChainThreshold = @opt__BlockingChainThreshold,
					@BlockingChainDepth = @opt__BlockingChainDepth, 
					@TranDetailsThreshold = @opt__TranDetailsThreshold,
					--@ResolvePageLatches = @opt__ResolvePageLatches,

					@DebugSpeed = @opt__DebugSpeed,
					@SaveBadDims = @opt__SaveBadDims,
					@NumSPIDs = @lv__NumSPIDsCaptured OUTPUT
				WITH RECOMPILE;

				SET @lv__NumSPIDsAtLastRecompile = @lv__NumSPIDsCaptured;
			END 
			ELSE
			BEGIN
				EXEC AutoWho.Collector
					@TempDBCreateTime = @lv__TempDBCreateTime,
					@IncludeIdleWithTran = @opt__IncludeIdleWithTran,
					@IncludeIdleWithoutTran = @opt__IncludeIdleWithoutTran,
					@DurationFilter = @opt__DurationFilter, 
					@FilterTable = @FilterTVP, 
					@DBInclusionsExist = @lv__DBInclusionsExist, 
					@HighTempDBThreshold = @opt__HighTempDBThreshold, 
					@CollectSystemSpids = @opt__CollectSystemSpids, 
					@HideSelf = @opt__HideSelf, 

					@ObtainBatchText = @opt__ObtainBatchText,
					@QueryPlanThreshold = @opt__QueryPlanThreshold,
					@QueryPlanThresholdBlockRel = @opt__QueryPlanThresholdBlockRel,
					@ParallelWaitsThreshold = @opt__ParallelWaitsThreshold, 
					@ObtainLocksForBlockRelevantThreshold = @opt__ObtainLocksForBlockRelevantThreshold,
					@ObtainQueryPlanForStatement = @opt__ObtainQueryPlanForStatement, 
					@ObtainQueryPlanForBatch = @opt__ObtainQueryPlanForBatch,
					@InputBufferThreshold = @opt__InputBufferThreshold, 
					@BlockingChainThreshold = @opt__BlockingChainThreshold,
					@BlockingChainDepth = @opt__BlockingChainDepth, 
					@TranDetailsThreshold = @opt__TranDetailsThreshold,
					--@ResolvePageLatches = @opt__ResolvePageLatches,

					@DebugSpeed = @opt__DebugSpeed,
					@SaveBadDims = @opt__SaveBadDims,
					@NumSPIDs = @lv__NumSPIDsCaptured OUTPUT
				;
			END
	
			SET @lv__SuccessiveExceptions = 0;
		END TRY
		BEGIN CATCH
			SET @ErrorMessage = 'AutoWho procedure generated an exception: Error Number: ' + 
				CONVERT(VARCHAR(20), ERROR_NUMBER()) + '; Error Message: ' + ERROR_MESSAGE();
				
			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @lv__TraceID, -33, N'AutoWho exception', @ErrorMessage;

			SET @lv__SuccessiveExceptions = @lv__SuccessiveExceptions + 1;

			IF @lv__SuccessiveExceptions >= 10
			BEGIN
				INSERT INTO AutoWho.[Log]
				(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__TraceID, -35, N'Abort b/c exceptions', N'10 consecutive failures; this procedure is terminating.';

				SET @lv__EarlyAbort = N'E';	--signals (to the logic immediately after the WHILE loop's END) how we exited the loop

				--Ok, we've had 10 straight errors. Something is wrong, and we need a human to intervene.
				--To prevent the procedure from just firing up a few minutes later, we insert a record into the signal table
				INSERT INTO AutoWho.SignalTable
				(SignalName, SignalValue, InsertTime)
				SELECT N'AbortTrace', N'AllDay', GETDATE();

				BREAK;		--exit the loop
			END
		END CATCH

		--Note that we put this outside the TRY/CATCH, so that even if we encounter an exception, we can 
		-- still evaluate how long it took to hit that exception, and (if it was a long time), gather info
		-- about the system in a more lightweight way.
		SET @lv__AutoWhoCallCompleteTime = GETDATE();

		IF DATEDIFF(MILLISECOND, @lv__LoopStartTime, @lv__AutoWhoCallCompleteTime) > 30000
		BEGIN
			--the system must be really loaded. Sometimes when things are really bad, AutoWho's results
			-- are a little more suspect (since it can take a long time for the data to be captured and thus the results
			-- do not necessarily represent anything remotely resembling a "point in time"). Let's run a very 
			-- "lightweight AutoWho" capture so that at least we have some useful info (even though there won't 
			-- be a viewer for this) that is closer to representing a specific point in time.
			EXEC AutoWho.LightWeightCollector;

			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(),  @lv__TraceID, 0, 
				N'LightColl', N'Lightweight Collector running due to AutoWho duration of ''' + 
					CONVERT(VARCHAR(20),DATEDIFF(MILLISECOND, @lv__LoopStartTime, @lv__AutoWhoCallCompleteTime)) + ''' ms.';
		END

		IF ISNULL(@lv__RecompileAutoWho,0) = 1
		BEGIN
			--we just recompiled, thus we don't need to evaluate "should I recompile"
			--we DO "reset" the historical tracking variables, and of course set the recompile flag to 0
			SET @lv__RecompileAutoWho = 0;

			SET @lv__SPIDsCaptured5Ago = -1;
			SET @lv__SPIDsCaptured4Ago = -1;
			SET @lv__SPIDsCaptured3Ago = -1;
			SET @lv__SPIDsCaptured2Ago = -1;
			SET @lv__SPIDsCaptured1Ago = @lv__NumSPIDsCaptured;		--next loop iteration's "first historical value"
																	--is this run's #, of course.
		END
		ELSE
		BEGIN
			--we did NOT just recompile. If we have valid historical values (i.e. variables are >= 0), and 
			-- our current run is valid, then we take the average and see if there is a significant 
			-- difference between our recent average # of spids and the # of spids at last recompile

			IF ISNULL(@lv__NumSPIDsCaptured,-1) >= 0
				AND ISNULL(@lv__SPIDsCaptured1Ago,-1) >= 0
				AND ISNULL(@lv__SPIDsCaptured2Ago,-1) >= 0
				AND ISNULL(@lv__SPIDsCaptured3Ago,-1) >= 0
				AND ISNULL(@lv__SPIDsCaptured4Ago,-1) >= 0
				AND ISNULL(@lv__SPIDsCaptured5Ago,-1) >= 0
			BEGIN
				IF ABS(
					@lv__NumSPIDsAtLastRecompile -

					((@lv__SPIDsCaptured1Ago + @lv__SPIDsCaptured2Ago + @lv__SPIDsCaptured3Ago + 
					@lv__SPIDsCaptured4Ago + @lv__SPIDsCaptured5Ago + @lv__NumSPIDsCaptured) / 6)

					) > 100
				BEGIN
					SET @lv__RecompileAutoWho = 1;

					INSERT INTO AutoWho.[Log]
					(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
					SELECT SYSDATETIME(),  @lv__TraceID, 0, N'Recompile', 
						N'AutoWho Collector marked for recompilation (Spids at last recompile: ' + 
							CONVERT(varchar(20),@lv__NumSPIDsAtLastRecompile) + 
							'. Average # of spids captured over last 6 runs: ' + 
						CONVERT(varchar(20),((@lv__SPIDsCaptured1Ago + @lv__SPIDsCaptured2Ago + @lv__SPIDsCaptured3Ago + 
						@lv__SPIDsCaptured4Ago + @lv__SPIDsCaptured5Ago + @lv__NumSPIDsCaptured) / 6))
						;
				END
			END

			--We only "shift the history back" if our current value was a legitimate one
			IF @lv__NumSPIDsCaptured >= 0
			BEGIN
				SET @lv__SPIDsCaptured5Ago = @lv__SPIDsCaptured4Ago;
				SET @lv__SPIDsCaptured4Ago = @lv__SPIDsCaptured3Ago;
				SET @lv__SPIDsCaptured3Ago = @lv__SPIDsCaptured2Ago;
				SET @lv__SPIDsCaptured2Ago = @lv__SPIDsCaptured1Ago;
				SET @lv__SPIDsCaptured1Ago = @lv__NumSPIDsCaptured;
			END
		END		--IF ISNULL(@lv__RecompileAutoWho,0) = 1

		--Every @opt__ThresholdFilterRefresh minutes, we need to recalculate our list of SPIDs to omit from threshold calculations
		IF DATEDIFF(MINUTE, @lv__LastThresholdFilterTime, GETDATE()) > @opt__ThresholdFilterRefresh
		BEGIN
			DELETE FROM @FilterTVP WHERE FilterType = 128;
			TRUNCATE TABLE [AutoWho].[ThresholdFilterSpids];
			EXEC [AutoWho].[ObtainSessionsForThresholdIgnore];

			INSERT INTO @FilterTVP (FilterType, FilterID)
			SELECT DISTINCT 128, f.ThresholdFilterSpid
			FROM AutoWho.ThresholdFilterSpids f;

			SET @lv__LastThresholdFilterTime = GETDATE();
		END

		--now we check to see if someone has asked that we stop the trace (or we've hit our 10-exceptions-in-a-row condition)
		--(this logic implements our manual stop logic)
		SELECT 
			@lv__EarlyAbort = firstchar 
		FROM (
			SELECT TOP 1 
				CASE WHEN LOWER(SignalValue) = N'allday' THEN N'A' 
					WHEN LOWER(SignalValue) = N'onetime' THEN N'O'
					ELSE NULL 
					END as firstchar
			FROM AutoWho.SignalTable WITH (NOLOCK) 
			WHERE SignalName = N'AbortTrace' 
			AND DATEDIFF(DAY, InsertTime, GETDATE()) = 0
			ORDER BY InsertTime DESC		--always used the latest flag if there is more than 1 in a day
		) ss;

		IF @lv__EarlyAbort IS NULL
		BEGIN
			SET @lv__EarlyAbort = N'N';
		END
		ELSE
		BEGIN
			IF @lv__EarlyAbort <> N'N'
			BEGIN
				SET @ErrorMessage = N'An AbortTrace signal value was found (for today), with type: ' + @lv__EarlyAbort;
				INSERT INTO AutoWho.[Log]
				(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__TraceID, 1, N'Abort b/c signal', @ErrorMessage;
			END
		END


		--reached the end of our loop. As long as we are not early-aborting, calculate how long to WAITFOR DELAY
		--Note that our Options check constraint on the IntervalLength column allows intervals ranging from 5 seconds to 300 seconds
		IF @lv__EarlyAbort = N'N'
		BEGIN
			--@lv__LoopStartTime holds the time this iteration of the loop began. i.e. SET @lv__LoopStartTime = GETDATE()
			SET @lv__LoopEndTime = GETDATE();
			SET @lv__LoopNextStart = DATEADD(SECOND, @opt__IntervalLength, @lv__LoopStartTime); 

			--If the Collector proc ran so long that the current time is actually >= @lv__LoopNextStart, we 
			-- increment the target time by the interval until the target is in the future.
			WHILE @lv__LoopNextStart <= @lv__LoopEndTime
			BEGIN
				SET @lv__LoopNextStart = DATEADD(SECOND, @opt__IntervalLength, @lv__LoopNextStart);
			END

			SET @lv__LoopNextStartSecondDifferential = DATEDIFF(SECOND, @lv__LoopEndTime, @lv__LoopNextStart);

			SET @lv__WaitForMinutes = @lv__LoopNextStartSecondDifferential / 60;
			SET @lv__LoopNextStartSecondDifferential = @lv__LoopNextStartSecondDifferential % 60;

			SET @lv__WaitForSeconds = @lv__LoopNextStartSecondDifferential;
		
			SET @lv__WaitForString = '00:' + 
									CASE WHEN @lv__WaitForMinutes BETWEEN 10 AND 59
										THEN CONVERT(varchar(10), @lv__WaitForMinutes)
										ELSE '0' + CONVERT(varchar(10), @lv__WaitForMinutes)
										END + ':' + 
									CASE WHEN @lv__WaitForSeconds BETWEEN 10 AND 59 
										THEN CONVERT(varchar(10), @lv__WaitForSeconds)
										ELSE '0' + CONVERT(varchar(10), @lv__WaitForSeconds)
										END;
		
			WAITFOR DELAY @lv__WaitForString;
		END -- check @lv__EarlyAbort to see if we should construct/execute WAITFOR
	END		--WHILE (GETDATE() < @lv__EndTime OR @lv__EarlyAbort = N'N')

	--clean up any signals that are now irrelevant. (Remember, OneTime signals get deleted immediately after their use
	DELETE FROM AutoWho.SignalTable 
	WHERE SignalName = N'AbortTrace' 
	AND (
		LOWER(SignalValue) = N'onetime'
		OR 
		DATEDIFF(DAY, InsertTime, GETDATE()) > 0
		);

	IF @lv__EarlyAbort = N'E'
	BEGIN
		SET @lv__ThisRC = -37;
		SET @ErrorMessage = 'Exiting wrapper procedure due to exception-based abort';

		INSERT INTO AutoWho.[Log]
		(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__TraceID, @lv__ThisRC, N'Exception exit', @ErrorMessage;
	END
	ELSE IF @lv__EarlyAbort IN (N'O', N'A')
	BEGIN
		SET @lv__ThisRC = -39;
		SET @ErrorMessage = 'Exiting wrapper procedure due to manual abort, type: ' + @lv__EarlyAbort;

		INSERT INTO AutoWho.[Log]
		(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__TraceID, @lv__ThisRC, N'Manual abort exit', @ErrorMessage;
	END
	ELSE 
	BEGIN
		SET @lv__ThisRC = 0;
		SET @ErrorMessage = 'AutoWho trace successfully completed.';

		INSERT INTO AutoWho.[Log]
		(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @lv__TraceID, @lv__ThisRC, N'Successful complete', @ErrorMessage;
	END

	EXEC CorePE.StopTrace @Utility=N'AutoWho', @TraceID = @lv__TraceID, @AbortCode = @lv__EarlyAbort;

	IF @opt__ResolvePageLatches = N'Y' OR @opt__Enable8666 = N'Y'
	BEGIN
		/* Moving the page resolution logic to the Every 15 minute Master
		IF @opt__ResolvePageLatches = N'Y'
		BEGIN
			BEGIN TRY
				DBCC TRACEOFF(3604) WITH NO_INFOMSGS;
			END TRY
			BEGIN CATCH
				SET @ErrorMessage = N'PageLatch Resolution was requested but cannot disable TF 3604. Message: ' + ERROR_MESSAGE();
				SET @lv__ThisRC = -41;
	
				INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__ThisRC, N'TF3604Disable', @ErrorMessage;

				EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
				RETURN @lv__ThisRC;
			END CATCH
		END
		*/

		IF @opt__Enable8666 = N'Y'
		BEGIN
			BEGIN TRY
				DBCC TRACEOFF(8666) WITH NO_INFOMSGS;
			END TRY
			BEGIN CATCH
				SET @ErrorMessage = N'Cannot disable TF 8666. Message: ' + ERROR_MESSAGE();
				SET @lv__ThisRC = -43;
	
				INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @lv__ThisRC, N'TF8666Disable', @ErrorMessage;

				EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';
				RETURN @lv__ThisRC;
			END CATCH
		END
	END

	EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session';

	RETURN @lv__ThisRC;
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @ErrorMessage = N'Unexpected exception occurred: Error #' + ISNULL(CONVERT(nvarchar(20),ERROR_NUMBER()),N'<null>') + 
		N'; State: ' + ISNULL(CONVERT(nvarchar(20),ERROR_STATE()),N'<null>') + 
		N'; Severity' + ISNULL(CONVERT(nvarchar(20),ERROR_SEVERITY()),N'<null>') + 
		N'; Message: ' + ISNULL(ERROR_MESSAGE(), N'<null>');

	INSERT INTO AutoWho.[Log]
	(LogDT, ErrorCode, LocationTag, LogMessage)
	SELECT SYSDATETIME(), -999, N'ExecutorUnexp', @ErrorMessage;

	EXEC sp_releaseapplock @Resource = 'AutoWhoBackgroundTrace', @LockOwner = 'Session'

	RETURN -1;
END CATCH
END


GO
