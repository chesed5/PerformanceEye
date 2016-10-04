SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[PopulateCaptureSummary] 
/*   
	PROCEDURE:		AutoWho.PopulateCaptureSummary

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Pulls data from the AutoWho base tables and aggregates various characteristics of the data into a summary row per SPIDCaptureTime. 
			This procedure assumes that it will only be called by other AutoWho procs (or by sp_SessionViewer), thus error-handling is limited.
			Thus, it catches errors and writes them to the AutoWho.Log table, and simply returns -1 if it does not succeed.

    CHANGE LOG:	
				2016-04-25	Aaron Morelli		Final run-through and commenting


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
EXEC AutoWho.PopulateCaptureSummary @StartTime='2016-04-25 08:00', @EndTime='2016-04-25 09:00'
*/
(
	@StartTime DATETIME,
	@EndTime DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	BEGIN TRY

		--This block of code copied directly from the AutoWho procedure; they are essentially system constants
		DECLARE
			@enum__waitspecial__none			TINYINT,
			@enum__waitspecial__lck				TINYINT,
			@enum__waitspecial__pgblocked		TINYINT,
			@enum__waitspecial__pgio			TINYINT,
			@enum__waitspecial__pg				TINYINT,
			@enum__waitspecial__latchblocked	TINYINT,
			@enum__waitspecial__latch			TINYINT,
			@enum__waitspecial__cxp				TINYINT,
			@enum__waitspecial__other			TINYINT,
			@codeloc							VARCHAR(20),
			@errmsg								VARCHAR(MAX),
			@scratch_int						INT,
			@lv__nullstring						NVARCHAR(8),
			@lv__nullint						INT,
			@lv__nullsmallint					SMALLINT;

		DECLARE @StartTime_MinusOne			DATETIME,
				@StartTime_MinusOne_IsPopulated INT,
				@EndTime_PlusOne			DATETIME,
				@EndTime_PlusOne_IsPopulated INT,
				@StartTime_Effective DATETIME,
				@EndTime_Effective DATETIME;

		SET @enum__waitspecial__none =			CONVERT(TINYINT, 0);
		SET @enum__waitspecial__lck =			CONVERT(TINYINT, 5);
		SET @enum__waitspecial__pgblocked =		CONVERT(TINYINT, 7);
		SET @enum__waitspecial__pgio =			CONVERT(TINYINT, 10);
		SET @enum__waitspecial__pg =			CONVERT(TINYINT, 15);
		SET @enum__waitspecial__latchblocked =	CONVERT(TINYINT, 17);
		SET @enum__waitspecial__latch =			CONVERT(TINYINT, 20);
		SET @enum__waitspecial__cxp =			CONVERT(TINYINT, 30);
		SET @enum__waitspecial__other =			CONVERT(TINYINT, 25);

		SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
		SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
		SET @lv__nullsmallint = -929;			-- overlapping with some special system value

		SET @codeloc = '#CTTP creation';
		CREATE TABLE #CTTP (
			CaptureTimesToProcess DATETIME NOT NULL, 
			RowsActuallyFound CHAR(1) NOT NULL
		);

		--Because we calculate "deltas" for some resource counters, we need to find the SPIDCaptureTime that is immediately before
		-- the first one in our @StartTime/@EndTime range.

		SELECT @StartTime_MinusOne = SPIDCaptureTime,
			@StartTime_MinusOne_IsPopulated = ss.CaptureSummaryPopulated
		FROM (
			SELECT TOP 1 ct.SPIDCaptureTime, ct.CaptureSummaryPopulated
			FROM AutoWho.CaptureTimes ct WITH (nolock)
			WHERE ct.SPIDCaptureTime < @StartTime
			AND ct.RunWasSuccessful = 1
			ORDER BY ct.SPIDCaptureTime DESC
		) ss;

		--If we didn't find a successful run immediately before the current run, then we just revert to our original parameter
		IF @StartTime_MinusOne IS NULL
		BEGIN
			SET @StartTime_Effective = @StartTime
		END
		ELSE
		BEGIN
			--we found a previous runtime. If it has already been populated, then our Effective time is the parameter
			-- that was passed in. But if it HASN'T already been populated, then we want to include it in our below 
			-- population logic
			IF @StartTime_MinusOne_IsPopulated = 1
			BEGIN
				SET @StartTime_Effective = @StartTime
			END
			ELSE
			BEGIN
				SET @StartTime_Effective = @StartTime_MinusOne
			END
		END

		SELECT @EndTime_PlusOne = SPIDCaptureTime, 
			@EndTime_PlusOne_IsPopulated = CaptureSummaryPopulated
		FROM (
			SELECT TOP 1 ct.SPIDCaptureTime, ct.CaptureSummaryPopulated
			FROM AutoWho.CaptureTimes ct WITH (nolock)
			WHERE ct.SPIDCaptureTime > @EndTime
			AND ct.RunWasSuccessful = 1
			ORDER BY ct.SPIDCaptureTime ASC
		) ss;

		IF @EndTime_PlusOne IS NULL
		BEGIN
			SET @EndTime_Effective = @EndTime
		END
		ELSE
		BEGIN
			IF @EndTime_PlusOne_IsPopulated = 1
			BEGIN
				SET @EndTime_Effective = @EndTime
			END
			ELSE
			BEGIN
				SET @EndTime_Effective = @EndTime_PlusOne
			END
		END

		SET @codeloc = '#CTTP population';
		INSERT INTO #CTTP (
			CaptureTimesToProcess,
			RowsActuallyFound
		)
		SELECT ct.SPIDCaptureTime,
			'N'	--start off assuming "No"
		FROM AutoWho.CaptureTimes ct WITH (nolock)
			--Since existing rows in AutoWho.CaptureTimes are only ever updated by this procedure, NOLOCK is safe to use
			-- We do this so that there is no chance of inhibiting/delaying new inserts by the AutoWho collector procedure itself.
		WHERE ct.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
		AND ct.RunWasSuccessful = 1
		AND ct.CaptureSummaryPopulated = 0;

		SET @scratch_int = @@ROWCOUNT;

		IF @scratch_int = 0
		BEGIN
			RETURN 1;		--special code for "no rows to do"
		END

		SET @codeloc = 'BEGIN TRAN';
		BEGIN TRANSACTION

		SET @codeloc = 'CaptureSummary INSERT';
		INSERT INTO AutoWho.CaptureSummary (
			SPIDCaptureTime,		--1
			CapturedSPIDs, 
			Active, 
			ActLongest_ms, 
			ActAvg_ms,				--5

			--We use histograms (instead of just MIN/MAX/AVG) for the various durations to give the user a better sense of the typical length of most SPIDs.
			-- This can be very helpful when trying to determine when/whether an OLTP-style app's user activity has "shifted to the right".
			Act0to1,
			Act1to5,
			Act10to30,
			Act30to60,
			Act60to300,				--10
			Act300plus,
			IdleWithOpenTran, 
			IdlOpTrnLongest_ms,
			IdlOpTrnAvg_ms,
			IdlOpTrn0to1,			--15
			IdlOpTrn1to5,
			IdlOpTrn5to10,
			IdlOpTrn10to30,
			IdlOpTrn30to60,
			IdlOpTrn60to300,		--20
			IdlOpTrn300plus,
			WithOpenTran,
			TranDurLongest_ms,
			TranDurAvg_ms, 
			TranDur0to1,			--25
			TranDur1to5,
			TranDur5to10,
			TranDur10to30,
			TranDur30to60,
			TranDur60to300,			--30
			TranDur300plus,
			Blocked,
			BlockedLongest_ms,
			BlockedAvg_ms,
			Blocked0to1,			--35
			Blocked1to5,
			Blocked5to10,
			Blocked10to30,
			Blocked30to60,
			Blocked60to300,			--40
			Blocked300plus,
			WaitingSPIDs, 
			WaitingTasks,
			WaitingTaskLongest_ms,
			WaitingTaskAvg_ms,		--45
			WaitingTask0to1, 
			WaitingTask1to5,
			WaitingTask5to10,
			WaitingTask10to30,
			WaitingTask30to60,		--50
			WaitingTask60to300,
			WaitingTask300plus,
			AllocatedTasks,
			QueryMemory_MB,
			LargestMemoryGrant_MB,	--55
			TempDB_MB,
			LargestTempDBConsumer_MB, 
			CPUused, 
			LargestCPUConsumer,
			WritesDone,				--60
			LargestWriter,
			LogicalReadsDone, 
			LargestLogicalReader, 
			PhysicalReadsDone,
			LargestPhysicalReader,	--65
			TlogUsed_MB,
			LargestLogWriter_MB, 
			BlockingGraph,
			LockDetails,
			TranDetails				--70
		)
		SELECT 
			ss3.SPIDCaptureTime,		--1
			CapturedSPIDs,
			Active,
			ActLongest_ms,
			ActAvg_ms,				--5
			Act0to1,
			Act1to5,
			Act10to30,
			Act30to60,
			Act60to300,				--10
			Act300plus,
			IdleWithOpenTran,
			IdlOpTrnLongest_ms,
			IdlOpTrnAvg_ms,
			IdlOpTrn0to1,			--15
			IdlOpTrn1to5,
			IdlOpTrn5to10,
			IdlOpTrn10to30,
			IdlOpTrn30to60,
			IdlOpTrn60to300,		--20
			IdlOpTrn300plus,
			WithOpenTran,
			TranDurLongest_ms,
			TranDurAvg_ms, 
			TranDur0to1,			--25
			TranDur1to5,
			TranDur5to10,
			TranDur10to30,
			TranDur30to60,
			TranDur60to300,			--30
			TranDur300plus,
			ISNULL(Blocked,0),
			BlockedLongest_ms,
			BlockedAvg_ms,
			Blocked0to1,			--35
			Blocked1to5,
			Blocked5to10,
			Blocked10to30,
			Blocked30to60,
			Blocked60to300,			--40
			Blocked300plus,
			ISNULL(WaitingSPIDs,0),
			WaitingTasks,
			WaitingTaskLongest_ms,
			WaitingTaskAvg_ms,		--45
			WaitingTask0to1, 
			WaitingTask1to5,
			WaitingTask5to10,
			WaitingTask10to30,
			WaitingTask30to60,		--50
			WaitingTask60to300,
			WaitingTask300plus,
			AllocatedTasks,
			QueryMemory_MB,		
			LargestMemoryGrant_MB,	--55
			TempDB_MB,
			LargestTempDBConsumer_MB,
			CPUused,
			LargestCPUConsumer,
			WritesDone,				--60
			LargestWriter,
			LogicalReadsDone,
			LargestLogicalReader,
			PhysicalReadsDone,
			LargestPhysicalReader,	--65
			--can't use this, as this could be double-counted (same tran across sessions): TLogUsed_MB,
			AggTLog_MB = ISNULL(td2.Tlog_Agg,0)/1024./1024.,
			LargestLogWriter_MB,
			hasBG,
			hasLD,
			hasTD					--70
		FROM (
			SELECT 
				SPIDCaptureTime,
				CapturedSPIDs =			SUM(SPIDCounter),
				Active =				SUM(isActive),
				ActLongest_ms	 =		MAX(CASE WHEN isActive=1 AND sess__is_user_process = 1 THEN calc__duration_ms ELSE NULL END),
				ActAvg_ms		 =		AVG(CASE WHEN isActive=1 AND sess__is_user_process = 1 THEN calc__duration_ms ELSE NULL END),
				Act0to1			 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 0 AND 1000 THEN 1 ELSE 0 END),
				Act1to5			 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 1001 AND 5000 THEN 1 ELSE 0 END),
				Act5to10		 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 5001 AND 10000 THEN 1 ELSE 0 END),
				Act10to30		 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END),
				Act30to60		 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END),
				Act60to300		 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END),
				Act300plus		 =		SUM(CASE WHEN isActive=1 AND sess__is_user_process = 1 AND calc__duration_ms > 300000 THEN 1 ELSE 0 END),

				IdleWithOpenTran =		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 THEN 1 ELSE 0 END),
				IdlOpTrnLongest_ms =	MAX(CASE WHEN isActive = 0 AND hasOpenTran = 1 THEN calc__duration_ms ELSE NULL END),
				IdlOpTrnAvg_ms	=		AVG(CASE WHEN isActive = 0 AND hasOpenTran = 1 THEN calc__duration_ms ELSE NULL END),
				IdlOpTrn0to1	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 0 AND 1000 THEN 1 ELSE 0 END),
				IdlOpTrn1to5	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 1001 AND 5000 THEN 1 ELSE 0 END),
				IdlOpTrn5to10	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 5001 AND 10000 THEN 1 ELSE 0 END),
				IdlOpTrn10to30	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END),
				IdlOpTrn30to60	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END),
				IdlOpTrn60to300	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END),
				IdlOpTrn300plus	=		SUM(CASE WHEN isActive = 0 AND hasOpenTran = 1 AND calc__duration_ms > 300000 THEN 1 ELSE 0 END),

				WithOpenTran =			SUM(hasOpenTran),
				[TranDurLongest_ms] =	MAX(CASE WHEN hasOpenTran = 1 THEN TranLength_ms ELSE NULL END),
				TranDurAvg_ms		= 	AVG(CASE WHEN hasOpenTran = 1 THEN TranLength_ms ELSE NULL END),
				TranDur0to1			=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 0 AND 1000 THEN 1 ELSE 0 END),
				TranDur1to5			=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 1001 AND 5000 THEN 1 ELSE 0 END),
				TranDur5to10		=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 5001 AND 10000 THEN 1 ELSE 0 END),
				TranDur10to30		=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END),
				TranDur30to60		=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END),
				TranDur60to300		=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END),
				TranDur300plus		=   SUM(CASE WHEN hasOpenTran = 1 AND TranLength_ms > 300000 THEN 1 ELSE 0 END),

				Blocked =				SUM(SPIDIsBlocked),
				BlockedLongest_ms =		MAX(LongestBlockedTask),
				BlockedAvg_ms		=	AVG(LongestBlockedTask),
				Blocked0to1			=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 0 AND 1000 THEN 1 ELSE 0 END),
				Blocked1to5			=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 1001 AND 5000 THEN 1 ELSE 0 END),
				Blocked5to10		=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 5001 AND 10000 THEN 1 ELSE 0 END),
				Blocked10to30		=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END),
				Blocked30to60		=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END),
				Blocked60to300		=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END),
				Blocked300plus		=	SUM(CASE WHEN SPIDIsBlocked = 1 AND LongestBlockedTask > 300000 THEN 1 ELSE 0 END),

				WaitingSPIDs =			SUM(SPIDIsWaiting),
				WaitingTasks =			ISNULL(SUM(WaitingUserTasks),0),
				WaitingTaskLongest_ms = MAX(LongestWaitingUserTask),
				WaitingTaskAvg_ms	=	AVG(LongestWaitingUserTask),
				WaitingTask0to1		=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 0 AND 1000 THEN 1 ELSE 0 END),
				WaitingTask1to5		=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 1001 AND 5000 THEN 1 ELSE 0 END),
				WaitingTask5to10	=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 5001 AND 10000 THEN 1 ELSE 0 END),
				WaitingTask10to30	=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END),
				WaitingTask30to60	=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END),
				WaitingTask60to300	=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END),
				WaitingTask300plus	=   SUM(CASE WHEN SPIDIsWaiting = 1 AND LongestWaitingUserTask > 300000 THEN 1 ELSE 0 END),

				AllocatedTasks =		ISNULL(SUM(AllocatedTasks),0),
				QueryMemory_MB =		CONVERT(DECIMAL(38,3),ISNULL(SUM(QueryMemoryRequest)/1024.,0.0)),
				LargestMemoryGrant_MB = CONVERT(DECIMAL(38,3),MAX(QueryMemoryGrant)/1024.),
				TempDB_MB =				CONVERT(DECIMAL(38,3),SUM(ISNULL(TempDB_Use_pages,0)*8./1024.)),
				LargestTempDBConsumer_MB = CONVERT(DECIMAL(38,3),MAX(ISNULL(TempDB_Use_pages,0))*8./1024.),
				CPUused =				ISNULL(SUM(CPUused),0),
				LargestCPUConsumer =	MAX(CPUused),
				WritesDone =			ISNULL(SUM(WritesDone),0),
				LargestWriter =			MAX(WritesDone),
				LogicalReadsDone =		ISNULL(SUM(LogicalReadsDone),0),
				LargestLogicalReader =	MAX(LogicalReadsDone),
				PhysicalReadsDone =		ISNULL(SUM(PhysicalReadsDone),0),
				LargestPhysicalReader = SUM(PhysicalReadsDone),
				TLogUsed_MB =			SUM(TLogUsed)/1024/1024,
				LargestLogWriter_MB =	CONVERT(DECIMAL(38,3),MAX(TlogUsed)/1024./1024.),
				hasBG =					MAX(hasBG),
				hasLD =					MAX(hasLD),
				hasTD =					MAX(hasTD)
			FROM (
				SELECT 
					SPIDCaptureTime,
					SPIDCounter = 1,
					calc__duration_ms,
					TranLength_ms,
					sess__is_user_process,
					isActive = CASE 
							WHEN request_id = @lv__nullsmallint 
							THEN 0 ELSE 1 END,
					AllocatedTasks = tempdb__CalculatedNumberOfTasks,
					hasOpenTran = CASE 
							WHEN sess__open_transaction_count > 0 OR rqst__open_transaction_count > 0
								OR hasTranDetailData = 1
							THEN 1 ELSE 0 END,
					SPIDIsBlocked,
					SPIDIsWaiting,
					WaitingUserTasks = NumWaitingTasks,
					LongestWaitingUserTask = CASE WHEN sess__is_user_process = 1 THEN LongestWaitingTask ELSE NULL END,
					LongestBlockedTask = LongestBlockedTask,
					QueryMemoryRequest = mgrant__requested_memory_kb,
					QueryMemoryGrant = mgrant__granted_memory_kb,
					TempDB_Use_pages = Tdb_Use_pages,
					CPUused = ss.rqst__cpu_time,
					WritesDone = ss.rqst__writes,
					LogicalReadsDone = ss.rqst__logical_reads,
					PhysicalReadsDone = ss.rqst__reads,
					TLogUsed = TranBytes, 
					hasBG, 
					hasLD,
					hasTD
				FROM (
					SELECT 
						sar.SPIDCaptureTime,
						sar.session_id, 
						sar.request_id,
						sar.TimeIdentifier,		--rqst_start_time if active, last_request_end_time if not active
						sar.sess__is_user_process,
						sar.sess__open_transaction_count,
						sar.rqst__open_transaction_count,
						sar.rqst__cpu_time,
						sar.rqst__reads,
						sar.rqst__writes,
						sar.rqst__logical_reads, 

						Tdb_Use_pages = 
							CASE WHEN (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0))
								END + 
							CASE WHEN (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0))
								END + 
							CASE WHEN (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0))
								END + 
							CASE WHEN (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) < 0 THEN 0
								ELSE (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0))
								END,
						sar.tempdb__CalculatedNumberOfTasks,
						sar.mgrant__requested_memory_kb,
						sar.mgrant__granted_memory_kb,
						sar.calc__duration_ms,
						sar.calc__blocking_session_id,
						--sar.calc__is_blocker,
						hasTranDetailData = CASE WHEN td.SPIDCaptureTime IS NOT NULL THEN 1 ELSE 0 END,
						td.TranBytes,
						td.TranLength_ms,
						taw.SPIDIsWaiting,
						taw.SPIDIsBlocked,
						taw.NumBlockedTasks,
						taw.NumWaitingTasks,
						taw.LongestWaitingTask,
						taw.LongestBlockedTask,
						hasLD = CASE WHEN ld.SPIDCaptureTime IS NOT NULL THEN 1 ELSE 0 END,
						hasBG = CASE WHEN bg.SPIDCaptureTime IS NOT NULL THEN 1 ELSE 0 END,
						hasTD = CASE WHEN td.SPIDCaptureTime IS NOT NULL THEN 1 ELSE 0 END
					FROM AutoWho.SessionsAndRequests sar
						INNER JOIN #CTTP ct
							ON ct.CaptureTimesToProcess = sar.SPIDCaptureTime
						LEFT OUTER JOIN (
							--THIS JOIN WAS COPIED OVER, WITH MINOR MODIFICATION, TO AutoWho.ApplyRetentionPolicies.
							-- IF THIS JOIN CHANGES, EVALUATE WHETHER THE CHANGES ARE RELEVANT FOR THAT PROC AS WELL.
							SELECT 
								td.SPIDCaptureTime, 
								td.session_id,
								td.TimeIdentifier, 
									--since a spid could have transactions that span databases 
									-- (from the DMV's point of view, "multiple transactions"), we take the duration
									-- of the longest one.
								[TranLength_ms] = MAX(DATEDIFF(MILLISECOND, td.dtat_transaction_begin_time,td.SPIDCaptureTime)),
								[TranBytes] = SUM(ISNULL(td.dtdt_database_transaction_log_bytes_reserved,0) + 
											ISNULL(td.dtdt_database_transaction_log_bytes_reserved_system,0))
							FROM AutoWho.TransactionDetails td
							WHERE td.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
							AND ISNULL(td.dtdt_database_id,99999) <> 32767

							--This doesn't seem to mean what I think it means:
							--AND td.dtst_is_user_transaction = 1

							/* not sure whether I actually do want the below criteria
							--dtat transaction_type
							--		1 = Read/write transaction
							--		2 = Read-only transaction
							--		3 = System transaction
							--		4 = Distributed transaction
							AND td.dtat_transaction_type NOT IN (2, 3)		--we don't want trans that are read-only or system trans
							--dtdt database_transaction_type
							--		1 = Read/write transaction
							--		2 = Read-only transaction
							--		3 = System transaction
							AND td.dtdt_database_transaction_type NOT IN (2,3) --we don't want DB trans that are read-only or system trans
							*/
							GROUP BY td.SPIDCaptureTime, td.session_id, td.TimeIdentifier
						) td
							ON sar.SPIDCaptureTime = td.SPIDCaptureTime
							AND sar.session_id = td.session_id
							AND sar.TimeIdentifier = td.TimeIdentifier
						LEFT OUTER JOIN (
							SELECT 
								taw.SPIDCaptureTime,
								taw.session_id,
								taw.request_id,
								[SPIDIsWaiting] = MAX(task_is_waiting),
								[SPIDIsBlocked] = MAX(task_is_blocked),
								[NumBlockedTasks] = SUM(task_is_blocked),
								[NumWaitingTasks] = SUM(task_is_waiting),
								[LongestWaitingTask] = MAX(taw.wait_duration_ms),
								[LongestBlockedTask] = MAX(taw.blocked_duration_ms)
							FROM (
								--we treat waits of type cxpacket as "not waiting"... i.e. a query with multiple tasks, and those
								-- tasks either running or cxp waiting, is not considered waiting, since the query is making progress
								SELECT 
									taw.SPIDCaptureTime, 
									taw.session_id,
									taw.request_id,
									--note that in this context, "waiting" and "blocking" are completely non-overlapping concepts. The idea is that in the result set,
									-- the user will at a glance be able to see how much blocking is occurring, and how much "other waiting" is occurring.
									[task_is_waiting] = CASE WHEN taw.wait_special_category IN (@enum__waitspecial__none, @enum__waitspecial__cxp, 
																	@enum__waitspecial__lck, @enum__waitspecial__pgblocked, @enum__waitspecial__latchblocked) 
															THEN 0 ELSE 1 END,
									[task_is_blocked] = CASE WHEN taw.wait_special_category IN (@enum__waitspecial__lck, @enum__waitspecial__pgblocked, @enum__waitspecial__latchblocked) 
													THEN 1 ELSE 0 END,
									[wait_duration_ms] = CASE WHEN taw.wait_special_category IN (@enum__waitspecial__none, @enum__waitspecial__cxp,
																	@enum__waitspecial__lck, @enum__waitspecial__pgblocked, @enum__waitspecial__latchblocked) THEN NULL 
															ELSE ISNULL(taw.wait_duration_ms,0)
															END, 
									[blocked_duration_ms] = CASE WHEN taw.wait_special_category IN (@enum__waitspecial__lck, @enum__waitspecial__pgblocked, @enum__waitspecial__latchblocked)
													THEN ISNULL(taw.wait_duration_ms,0) ELSE NULL END
								FROM AutoWho.TasksAndWaits taw
								WHERE taw.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
							) taw
							GROUP BY taw.SPIDCaptureTime,
								taw.session_id,
								taw.request_id
						) taw
							ON sar.SPIDCaptureTime = taw.SPIDCaptureTime
							AND sar.session_id = taw.session_id
							AND sar.request_id = taw.request_id
						LEFT OUTER JOIN ( 
							SELECT DISTINCT SPIDCaptureTime 
							FROM AutoWho.BlockingGraphs bg
							WHERE bg.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
							) bg
							ON sar.SPIDCaptureTime = bg.SPIDCaptureTime
						LEFT OUTER JOIN (
							SELECT DISTINCT ld.SPIDCaptureTime
							FROM AutoWho.LockDetails ld
							WHERE ld.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
							) ld
							ON ld.SPIDCaptureTime = sar.SPIDCaptureTime
					WHERE sar.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
					AND sar.session_id > 0
					AND ISNULL(sar.calc__threshold_ignore,0) = 0

					--occasionally we see spids that are dormant, and have non-sensical time values
					AND NOT (sar.sess__is_user_process = 1 AND sar.sess__last_request_end_time = '1900-01-01 00:00:00.000'
							AND sar.rqst__start_time IS NULL)
				) ss
			) ss2
			GROUP BY SPIDCaptureTime
		) ss3
			--Since a transaction can be enlisted for several different sessions at once, we have to join
			-- again to TranDetails to get our aggregate number for t-log used
			LEFT OUTER JOIN (
				SELECT SPIDCaptureTime,
					SUM(ISNULL(dtdt_database_transaction_log_bytes_reserved,0) + 
						ISNULL(dtdt_database_transaction_log_bytes_reserved_system,0)) as Tlog_Agg
				FROM (
					SELECT DISTINCT td.SPIDCaptureTime, 
						td.dtat_transaction_id, 
						dtdt_database_id,
						dtdt_database_transaction_log_bytes_reserved, 
						dtdt_database_transaction_log_bytes_reserved_system
					FROM AutoWho.TransactionDetails td
					WHERE td.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
				) tsub
				GROUP BY SPIDCaptureTime
			) td2
				ON ss3.SPIDCaptureTime = td2.SPIDCaptureTime
		;

		--The above logic includes a WHERE ...ISNULL(sar.calc__threshold_ignore,0) = 0 clause.
		-- This makes it not only possible, but probable that certain capture times will not have any rows
		-- placed into the CaptureSummary table. (i.e. when the system is quieter and the only things running
		-- our all-day BizTalk or Sentinel spids). Thus, we insert a dummy/placeholder row into those times.

		UPDATE targ 
		SET RowsActuallyFound = 'Y'
		FROM #CTTP targ 
			INNER JOIN AutoWho.CaptureSummary cs
				ON targ.CaptureTimesToProcess = cs.SPIDCaptureTime
		WHERE cs.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective;

		--dummy row!
		INSERT INTO AutoWho.CaptureSummary (
			SPIDCaptureTime, CapturedSPIDs, 
			Active, --nullable: LongestActive_ms, Act0to1, Act1to5, Act5to10, Act10to30, Act30to60, Act60to300, Act300plus, 
			IdleWithOpenTran, --nullable: IdlOpTrnLongest_ms, IdlOpTrn0to1, IdlOpTrn1to5, IdlOpTrn5to10, IdlOpTrn10to30, IdlOpTrn30to60, IdlOpTrn60to300, IdlOpTrn300plus,
			WithOpenTran, --nullable: TranDurLongest_ms, TranDur0to1, TranDur1to5, TranDur5to10, TranDur10to30, TranDur30to60, TranDur60to300, TranDur300plus,
			Blocked, --nullable: BlockedLongest_ms, Blocked0to1, Blocked1to5, Blocked5to10, Blocked10to30, Blocked30to60, Blocked60to300, Blocked300plus, 
			WaitingSPIDs, WaitingTasks, 
				--nullable: WaitingTaskLongest_ms, WaitingTask0to1, WaitingTask1to5, WaitingTask5to10, WaitingTask10to30, WaitingTask30to60, WaitingTask60to300, WaitingTask300plus, 
			AllocatedTasks, QueryMemory_MB, LargestMemoryGrant_MB, TempDB_MB, LargestTempDBConsumer_MB, 
			CPUused, CPUDelta, LargestCPUConsumer, WritesDone, WritesDelta, LargestWriter, 
			LogicalReadsDone, LogicalReadsDelta, LargestLogicalReader, 
			PhysicalReadsDone, PhysicalReadsDelta, LargestPhysicalReader, 
			TlogUsed_MB, LargestLogWriter_MB, BlockingGraph, LockDetails, TranDetails
		)
		SELECT ss1.CaptureTimesToProcess, 0 as CapturedSPIDs, 
			0 as Active, 
			0 as IdleWithOpenTran,
			0 as WithOpenTran,
			0 as Blocked,
			0 as WaitingSPIDs, 0 as WaitingTasks, 
			0 as AllocatedTasks, 0.0, null, 0.0, null, 
			0 as CPUused, null, null, 0, null, null, 
			0 as LogicalReadsDone, null, null,
			0 as PhysicalReadsDone, null, null, 
			null as TlogUsed_MB, null, 0, 0, 0
		FROM (SELECT t.CaptureTimesToProcess
				FROM #CTTP t
				WHERE t.RowsActuallyFound = 'N') ss1;

		--Now, calc the Deltas for our original (excluding the minus/plus one) range.
		-- Note that we could be RE-calcing some delta values if our current range spans several previously-populated ranges
		UPDATE targ 
		SET 
			CPUDelta = CASE WHEN targ.CPUused IS NULL OR xprev.CPUused IS NULL THEN NULL 
							WHEN xprev.CPUUsed > targ.CPUused THEN NULL 
							ELSE targ.CPUused - xprev.CPUused END,
			WritesDelta = CASE WHEN targ.WritesDone IS NULL OR xprev.WritesDone IS NULL THEN NULL 
							WHEN xprev.WritesDone > targ.WritesDone THEN NULL 
							ELSE targ.WritesDone - xprev.WritesDone END,
			LogicalReadsDelta = CASE WHEN targ.LogicalReadsDone IS NULL OR xprev.LogicalReadsDone IS NULL THEN NULL 
							WHEN xprev.LogicalReadsDone > targ.LogicalReadsDone THEN NULL 
							ELSE targ.LogicalReadsDone - xprev.LogicalReadsDone END,
			PhysicalReadsDelta = CASE WHEN targ.PhysicalReadsDone IS NULL OR xprev.PhysicalReadsDone IS NULL THEN NULL 
							WHEN xprev.PhysicalReadsDone > targ.PhysicalReadsDone THEN NULL 
							ELSE targ.PhysicalReadsDone - xprev.PhysicalReadsDone END
		FROM AutoWho.CaptureSummary targ 
			OUTER APPLY (
				SELECT TOP 1 cs2.CPUused, cs2.WritesDone, cs2.LogicalReadsDone, cs2.PhysicalReadsDone
				FROM AutoWho.CaptureSummary cs2
				WHERE cs2.SPIDCaptureTime < targ.SPIDCaptureTime
				--we don't want the APPLY logic to "match" with a capture time that was much earlier than our min
				-- (e.g. due to the inevitable gaps in our Summary rows)
				AND cs2.SPIDCaptureTime >= ISNULL(@StartTime_MinusOne, @StartTime)
				ORDER BY cs2.SPIDCaptureTime DESC
			) xprev
		WHERE targ.SPIDCaptureTime BETWEEN @StartTime AND @EndTime
		;

		SET @codeloc = 'CaptureTimes UPDATE';
		UPDATE targ 
		SET targ.CaptureSummaryPopulated = 1
		FROM #CTTP t
			INNER loop JOIN AutoWho.CaptureTimes targ WITH (ROWLOCK)
				ON targ.SPIDCaptureTime = t.CaptureTimesToProcess
		WHERE targ.SPIDCaptureTime BETWEEN @StartTime_Effective AND @EndTime_Effective
		AND targ.CaptureSummaryPopulated = 0
		;

		COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;
		SET @errmsg = 'Unexpected exception encountered in ' + OBJECT_NAME(@@PROCID) + ' procedure, at location: ' + @codeloc;
		SET @errmsg = @errmsg + ' Error #: ' + CONVERT(varchar(20),ERROR_NUMBER()) + '; State: ' + CONVERT(varchar(20),ERROR_STATE()) + 
			'; Severity: ' + CONVERT(varchar(20),ERROR_SEVERITY()) + '; msg: ' + ERROR_MESSAGE();

		INSERT INTO AutoWho.[Log] (
			LogDT, TraceID, ErrorCode, LocationTag, LogMessage 
		)
		VALUES (SYSDATETIME(), NULL, ERROR_NUMBER(), N'SummCapturePopulation', @errmsg);

		RETURN -1;
	END CATCH

	RETURN 0;
END

GO
