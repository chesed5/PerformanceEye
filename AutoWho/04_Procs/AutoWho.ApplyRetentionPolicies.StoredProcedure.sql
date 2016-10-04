SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ApplyRetentionPolicies]
/*   
	PROCEDURE:		AutoWho.ApplyRetentionPolicies

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Runs on the schedule defined via parameters to CorePE.PerformanceEyeMaster, 
		and applies various retention policies defined in AutoWho.Options

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-23	Aaron Morelli		Final code run-through and commenting

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
EXEC AutoWho.ApplyRetentionPolicies
*/
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @lv__ErrorMessage NVARCHAR(4000),
			@lv__ErrorState INT,
			@lv__ErrorSeverity INT,
			@lv__ErrorLoc NVARCHAR(40);

	BEGIN TRY
		SET @lv__ErrorLoc = N'Variable declare';
		DECLARE 
			--from AutoWho.Options table
			@opt__HighTempDBThreshold						INT,
			@opt__TranDetailsThreshold						INT,
			@opt__MediumDurationThreshold					INT,
			@opt__HighDurationThreshold						INT,
			@opt__BatchDurationThreshold					INT,
			@opt__LongTransactionThreshold					INT,
			@opt__Retention_IdleSPIDs_NoTran				INT,
			@opt__Retention_IdleSPIDs_WithShortTran			INT,
			@opt__Retention_IdleSPIDs_WithLongTran			INT,
			@opt__Retention_IdleSPIDs_HighTempDB			INT,
			@opt__Retention_ActiveLow						INT,
			@opt__Retention_ActiveMedium					INT,
			@opt__Retention_ActiveHigh						INT,
			@opt__Retention_ActiveBatch						INT,
			@opt__Retention_CaptureTimes					INT,
			@max__RetentionHours							INT,

			--misc general purpose
			@lv__ProcRC										INT,
			@lv__tmpBigInt									BIGINT,
			@lv__tmpStr										NVARCHAR(4000),
			@lv__tmpMinID									BIGINT, 
			@lv__tmpMaxID									BIGINT,
			@lv__nullstring									NVARCHAR(8),
			@lv__nullint									INT,
			@lv__nullsmallint								SMALLINT,

			--derived or intermediate values
			@lv__MaxSPIDCaptureTime							DATETIME,
			@lv__MinPurge_SPIDCaptureTime					DATETIME,
			@lv__MaxPurge_SPIDCaptureTime					DATETIME,
			@lv__TableSize_ReservedPages					BIGINT,
			@lv__PurgeHardDeleteTime						DATETIME
			;

		SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
		SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
		SET @lv__nullsmallint = -929;			-- overlapping with some special system value

		SET @lv__ErrorLoc = N'Temp table creation';
		CREATE TABLE #AutoWhoDistinctStoreKeys (
			[FKSQLStmtStoreID] BIGINT,
			[FKSQLBatchStoreID] BIGINT,
			[FKInputBufferStoreID] BIGINT,
			[FKQueryPlanBatchStoreID] BIGINT,
			[FKQueryPlanStmtStoreID] BIGINT
		);

		CREATE TABLE #StoreTableIDsToPurge (
			ID BIGINT NOT NULL PRIMARY KEY CLUSTERED
		);

		CREATE TABLE #RecordsToPurge (
			SPIDCaptureTime					DATETIME NOT NULL,
			session_id						SMALLINT NOT NULL,
			request_id						INT NOT NULL,
			TimeIdentifier					DATETIME NOT NULL,
			Retain_IdleSPID_HighTempDB		INT NOT NULL,
			Retain_IdleSPID_WithLongTran	INT NOT NULL,
			Retain_IdleSPID_WithShortTran	INT NOT NULL,
			Retain_IdleSPID_WithNoTran		INT NOT NULL,
			Retain_ActiveLow				INT NOT NULL,
			Retain_ActiveMedium				INT NOT NULL,
			Retain_ActiveHigh				INT NOT NULL,
			Retain_ActiveBatch				INT NOT NULL,
			Retain_SpecialRows				INT NOT NULL
		);

		SET @lv__ErrorLoc = N'Option obtain';
		SELECT 
			@opt__HighTempDBThreshold				= [HighTempDBThreshold],
			@opt__MediumDurationThreshold			= [MediumDurationThreshold],
			@opt__HighDurationThreshold				= [HighDurationThreshold],
			@opt__BatchDurationThreshold			= [BatchDurationThreshold],
			@opt__LongTransactionThreshold			= [LongTransactionThreshold],

			@opt__Retention_IdleSPIDs_NoTran		= [Retention_IdleSPIDs_NoTran],
			@opt__Retention_IdleSPIDs_WithShortTran = [Retention_IdleSPIDs_WithShortTran],
			@opt__Retention_IdleSPIDs_WithLongTran  = [Retention_IdleSPIDs_WithLongTran],
			@opt__Retention_IdleSPIDs_HighTempDB	= [Retention_IdleSPIDs_HighTempDB],
			@opt__Retention_ActiveLow				= [Retention_ActiveLow],
			@opt__Retention_ActiveMedium			= [Retention_ActiveMedium],
			@opt__Retention_ActiveHigh				= [Retention_ActiveHigh],
			@opt__Retention_ActiveBatch				= [Retention_ActiveBatch],
			@opt__Retention_CaptureTimes			= [Retention_CaptureTimes]
		FROM AutoWho.Options;


		--We use this "hard-delete" value in several places below. We base it off of our longest retention policy, the
		-- policy for rows in AutoWho.CaptureTimes. 
		SET @lv__PurgeHardDeleteTime = DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());
	
		--Now, we scan the Sessions and Requests table, applying the above retention policies to each record to determine which ones
		-- are safe to purge (i.e. don't meet ANY of the retention policies). Each row in SessionsAndRequests is compared with every 
		-- retention policy we have, and if it meets ANY of those retention policies, the row is kept. Only if the SAR entry = "0"
		-- for every policy do we delete it.

		--To avoid contention with the AutoWho collector proc itself, we first obtain a "max committed SPIDCaptureTime" value that
		-- we will use with our queries to ensure that the records we're looking at are not close to the ones being inserted.
		-- Since none of the retention policies can be < 1 hour, we choose a time that is at least an hour back
		SELECT @lv__MaxSPIDCaptureTime = SPIDCaptureTime
		FROM (
			SELECT TOP 1 SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar WITH (READPAST, ROWLOCK, READCOMMITTED)
			WHERE SPIDCaptureTime < DATEADD(HOUR, -1, GETDATE())
			ORDER BY SPIDCaptureTime DESC
		) ss;

		SET @lv__ErrorLoc = N'#RecordsToPurge pop';
		INSERT INTO #RecordsToPurge (
			SPIDCaptureTime,
			session_id,
			request_id,
			TimeIdentifier,
			Retain_IdleSPID_HighTempDB,
			Retain_IdleSPID_WithLongTran,
			Retain_IdleSPID_WithShortTran,
			Retain_IdleSPID_WithNoTran,
			Retain_ActiveLow,
			Retain_ActiveMedium,
			Retain_ActiveHigh,
			Retain_ActiveBatch,
			Retain_SpecialRows
		)
		SELECT DISTINCT 
			SPIDCaptureTime, 
			session_id, 
			request_id, 
			TimeIdentifier,

			Retain_IdleSPID_HighTempDB,
			Retain_IdleSPID_WithLongTran,
			Retain_IdleSPID_WithShortTran,
			Retain_IdleSPID_WithNoTran,
			Retain_ActiveLow,
			Retain_ActiveMedium,
			Retain_ActiveHigh,
			Retain_ActiveBatch,
			Retain_SpecialRows
		FROM (
			SELECT 
				sar.SPIDCaptureTime, 
				sar.session_id, 
				sar.request_id, 
				sar.TimeIdentifier, 
				[Retain_IdleSPID_HighTempDB] = CASE 
					WHEN sar.session_id > 0 
						AND sar.request_id = @lv__nullsmallint		
						--is idle spid; "High TempDB" retention only applies to idle
						--b/c the goal of the retention (and the scoping inclusion that correlates to the retention policy)
						-- is for spids that were idle w/o a tran, but had a high enough tempdb usage that we want to capture them

						AND (@opt__HighTempDBThreshold <=				--has used >= tempDB than our threshold
								(
								CASE WHEN ISNULL([tempdb__sess_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_user_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__sess_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_user_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__sess_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_internal_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__sess_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__sess_internal_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__task_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_user_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__task_user_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_user_objects_dealloc_page_count],0)
									END + 

								CASE WHEN ISNULL([tempdb__task_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_internal_objects_dealloc_page_count],0) < 0 THEN 0 
									ELSE ISNULL([tempdb__task_internal_objects_alloc_page_count],0) - 
										ISNULL([tempdb__task_internal_objects_dealloc_page_count],0)
									END
								)
							)

						AND (DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_HighTempDB, GETDATE()) < sar.SPIDCaptureTime)
							--the capture time for this record is recent enough that it falls within our retention policy
					THEN 1
					ELSE 0
					END,

				[Retain_IdleSPID_WithLongTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint		--idle spid
						AND ISNULL(sar.sess__open_transaction_count,0) > 0		--has an open transaction
						AND (td.TranLength_sec IS NOT NULL	--"NOT NULL" unnecessary b/c of the AND, but just want to be explicit
															-- here that NULL tran lengths are handled by the "Short Tran" policy.
							AND td.TranLength_sec >= @opt__LongTransactionThreshold)
				
						AND (DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_WithLongTran, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_IdleSPID_WithShortTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint		--idle spid
						AND ISNULL(sar.sess__open_transaction_count,0) > 0		--has an open transaction
						AND (td.TranLength_sec IS NULL 
							OR td.TranLength_sec < @opt__LongTransactionThreshold)

						AND (DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_WithShortTran, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_IdleSPID_WithNoTran] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id = @lv__nullsmallint		--idle spid
						AND ISNULL(sar.sess__open_transaction_count,0) = 0		--no transaction

						--if this idle spid is blocking other spids, we base retention on @opt__Retention_CaptureTimes,
						-- which is our longest retention policy. If the idle spid is NOT blocking other spids, we 
						-- make the retention policy the @opt__Retention_IdleSPIDs_NoTran policy.
						AND (
							(sar.calc__is_blocker = 1
							AND @lv__PurgeHardDeleteTime < sar.SPIDCaptureTime
							)
							OR 
							(sar.calc__is_blocker = 0
							AND (DATEADD(HOUR, 0 - @opt__Retention_IdleSPIDs_NoTran, GETDATE()) < sar.SPIDCaptureTime)
							)
						)
					THEN 1
					ELSE 0
					END,

				[Retain_ActiveLow] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint		--active spid
						--and the duration is lower than our "medium duration threshold"
						AND sar.calc__duration_ms < @opt__MediumDurationThreshold*1000

						AND (DATEADD(HOUR, 0 - @opt__Retention_ActiveLow, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_ActiveMedium] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint		--active spid
						--and the duration is between our medioum and high thresholds
						AND sar.calc__duration_ms >= @opt__MediumDurationThreshold*1000
						AND sar.calc__duration_ms < @opt__HighDurationThreshold*1000

						AND (DATEADD(HOUR, 0 - @opt__Retention_ActiveMedium, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_ActiveHigh] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint		--active spid
						--and the duration is between our High and Batch thresholds
						AND sar.calc__duration_ms >= @opt__HighDurationThreshold*1000
						AND sar.calc__duration_ms < @opt__BatchDurationThreshold*1000

						AND (DATEADD(HOUR, 0 - @opt__Retention_ActiveHigh, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_ActiveBatch] = CASE 
					WHEN sar.session_id > 0 AND sar.request_id <> @lv__nullsmallint		--active spid
						--and the duration is >= our batch threshold
						AND sar.calc__duration_ms >= @opt__BatchDurationThreshold*1000

						--and the capture time of the spid is within our retention policy
						AND (DATEADD(HOUR, 0 - @opt__Retention_ActiveBatch, GETDATE()) < sar.SPIDCaptureTime)
					THEN 1
					ELSE 0
					END,

				[Retain_SpecialRows] = CASE 
					WHEN sar.session_id <= 0 AND @lv__PurgeHardDeleteTime < sar.SPIDCaptureTime
					THEN 1
					ELSE 0
					END
			FROM AutoWho.SessionsAndRequests sar WITH (READUNCOMMITTED)
				--THIS JOIN COPIED (WITH A FEW CHANGES) FROM AutoWho.PopulateCaptureSummary
				-- If that other logic changes, we should change it here as well. 
				LEFT OUTER JOIN (
					SELECT 
						td.SPIDCaptureTime, 
						td.session_id,
						td.TimeIdentifier, 
							--since a spid could have transactions that span databases 
							-- (from the DMV's point of view, "multiple transactions"), we take the duration
							-- of the longest one.
						[TranLength_sec] = MAX(DATEDIFF(SECOND, td.dtat_transaction_begin_time,td.SPIDCaptureTime))
					FROM AutoWho.TransactionDetails td WITH (READUNCOMMITTED)
					WHERE ISNULL(td.dtdt_database_id,99999) <> 32767

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
			WHERE sar.SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		) ss
		--only if a row fails to meet any "Retain" columns do we purge it.
		WHERE Retain_IdleSPID_HighTempDB = 0
		AND Retain_IdleSPID_WithLongTran = 0
		AND Retain_IdleSPID_WithShortTran = 0
		AND Retain_IdleSPID_WithNoTran = 0
		AND Retain_ActiveLow = 0
		AND Retain_ActiveMedium = 0
		AND Retain_ActiveHigh = 0
		AND Retain_ActiveBatch = 0
		AND Retain_SpecialRows = 0
		OPTION(RECOMPILE, MAXDOP 4);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		SET @lv__ErrorLoc = N'#RecordsToPurge index';
		CREATE UNIQUE CLUSTERED INDEX CL1 ON #RecordsToPurge (
			SPIDCaptureTime,
			session_id,
			request_id,
			TimeIdentifier
		);	--No MAXDOP hint to ensure this works on SQL Standard as well!

		SET @lv__ErrorLoc = N'Final prep';
		SELECT 
			@lv__MinPurge_SPIDCaptureTime = ss.minnie,
			@lv__MaxPurge_SPIDCaptureTime = ss.maxie
		FROM (
			SELECT 
				MIN(SPIDCaptureTime)as minnie,
				MAX(SPIDCaptureTime) maxie
			FROM #RecordsToPurge 
		) ss;


		IF @lv__tmpBigInt <= 0
		BEGIN
			DELETE FROM AutoWho.[Log]
			WHERE LogDT <= DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());

			RETURN 0;
		END

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows identified that have no reason to be retained, ranging from ' + 
			ISNULL(CONVERT(NVARCHAR(20), @lv__MinPurge_SPIDCaptureTime),N'<null>') + ' to ' + 
			ISNULL(CONVERT(NVARCHAR(20),@lv__MaxPurge_SPIDCaptureTime),N'<null>') + '.';

		SET @lv__ErrorLoc = N'Lock delete';
		DELETE targ 
		FROM AutoWho.LockDetails targ
			INNER JOIN #RecordsToPurge r
				ON targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.request_session_id = r.session_id
				AND targ.request_request_id = r.request_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.LockDetails.';

		--Just in case rows slip past our normal purge criteria. (Added this after seeing 
		-- rows stick around for a long period of time in this and a few other tables).
		DELETE targ 
		FROM AutoWho.LockDetails targ 
		WHERE targ.SPIDCaptureTime < @lv__PurgeHardDeleteTime; 

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.LockDetails.';

		SET @lv__ErrorLoc = N'tran delete';
		DELETE targ 
		FROM AutoWho.TransactionDetails targ
			INNER JOIN #RecordsToPurge r
				ON targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.TransactionDetails.';

		DELETE targ 
		FROM AutoWho.TransactionDetails targ
		WHERE targ.SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.TransactionDetails.';

		SET @lv__ErrorLoc = N'TAW delete';
		DELETE targ 
		FROM AutoWho.TasksAndWaits targ 
			INNER JOIN #RecordsToPurge r
				ON targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.request_id = r.request_id
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.TasksAndWaits.';

		DELETE targ 
		FROM AutoWho.TasksAndWaits targ 
		WHERE targ.SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.TasksAndWaits.';

		SET @lv__ErrorLoc = N'SAR delete';
		DELETE targ 
		FROM AutoWho.SessionsAndRequests targ 
			INNER JOIN #RecordsToPurge r
				ON targ.SPIDCaptureTime = r.SPIDCaptureTime
				AND targ.session_id = r.session_id
				AND targ.request_id = r.request_id
				AND targ.TimeIdentifier = r.TimeIdentifier
		WHERE targ.SPIDCaptureTime >= @lv__MinPurge_SPIDCaptureTime
		AND targ.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime
		;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.SessionsAndRequests.';

		DELETE targ 
		FROM AutoWho.SessionsAndRequests targ
		WHERE targ.SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows hard-deleted from AutoWho.SessionsAndRequests.';

		--With the BlockingGraphs table, we only delete records for capture times
		-- where there are NO remaining spids in SAR
		SET @lv__ErrorLoc = N'BG delete';
		DELETE targ 
		FROM AutoWho.BlockingGraphs targ
			LEFT OUTER JOIN AutoWho.SessionsAndRequests sar
				ON sar.SPIDCaptureTime = targ.SPIDCaptureTime
				AND sar.SPIDCaptureTime <= @lv__MaxPurge_SPIDCaptureTime		--avoid conflicts with AutoWho
		WHERE sar.SPIDCaptureTime IS NULL
		OPTION(RECOMPILE);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.BlockingGraphs.';

		--For the "Store" tables, which aren't tied to any single SPIDCaptureTime, we delete records
		-- that aren't referenced anymore. However, note that we don't even consider them for
		-- deletion if the table size is still pretty small

		--We'll get rid of stuff in the various "Stores" only if it hasn't been touched since before our retention period.
		-- Which retention period? The longest of them! (not counting the hard-delete one)

		SET @lv__ErrorLoc = N'Store prep';
		SELECT @max__RetentionHours = ss1.col1
		FROM (
			SELECT TOP 1 col1 
			FROM (
				SELECT @opt__Retention_IdleSPIDs_NoTran as col1	UNION
				SELECT @opt__Retention_IdleSPIDs_WithShortTran UNION
				SELECT @opt__Retention_IdleSPIDs_WithLongTran UNION
				SELECT @opt__Retention_IdleSPIDs_HighTempDB UNION
				SELECT @opt__Retention_ActiveLow UNION
				SELECT @opt__Retention_ActiveMedium	UNION
				SELECT @opt__Retention_ActiveHigh UNION
				SELECT @opt__Retention_ActiveBatch
			) ss0
			ORDER BY col1 DESC
		) ss1;
		--if NULL somehow (this shouldn't happen), default to a week.
		SET @max__RetentionHours = ISNULL(@max__RetentionHours,168); 

		--One scan through the SAR table to construct a distinct-keys list is much
		-- more efficient than the previous code, which joined SAR in every DELETE
		INSERT INTO #AutoWhoDistinctStoreKeys (
			[FKSQLStmtStoreID],
			[FKSQLBatchStoreID],
			[FKInputBufferStoreID],
			[FKQueryPlanBatchStoreID],
			[FKQueryPlanStmtStoreID]
		)
		SELECT DISTINCT 
			sar.FKSQLStmtStoreID,
			sar.FKSQLBatchStoreID,
			sar.FKInputBufferStoreID,
			sar.FKQueryPlanBatchStoreID,
			sar.FKQueryPlanStmtStoreID
		FROM AutoWho.SessionsAndRequests sar WITH (NOLOCK)
		;

		SET @lv__ErrorLoc = N'IB delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'InputBufferStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 250*1024/8		--250 MB
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKInputBufferStoreID
			FROM (SELECT DISTINCT sar.FKInputBufferStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKInputBufferStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CorePE.InputBufferStore targ 
					ON targ.PKInputBufferStoreID = sar.FKInputBufferStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKInputBufferStoreID IS NULL 
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CorePE.InputBufferStore targ
					ON targ.PKInputBufferStoreID = t.ID
			WHERE targ.PKInputBufferStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge',
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.InputBufferStore.';
		END

		SET @lv__ErrorLoc = N'QPBS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'QueryPlanBatchStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKQueryPlanBatchStoreID
			FROM (SELECT DISTINCT sar.FKQueryPlanBatchStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKQueryPlanBatchStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CorePE.QueryPlanBatchStore targ
					ON targ.PKQueryPlanBatchStoreID = sar.FKQueryPlanBatchStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKQueryPlanBatchStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CorePE.QueryPlanBatchStore targ
					ON targ.PKQueryPlanBatchStoreID = t.ID
			WHERE targ.PKQueryPlanBatchStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.QueryPlanBatchStore.';
		END

		SET @lv__ErrorLoc = N'QPSS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'QueryPlanStmtStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKQueryPlanStmtStoreID
			FROM (SELECT DISTINCT sar.FKQueryPlanStmtStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKQueryPlanStmtStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CorePE.QueryPlanStmtStore targ
					ON targ.PKQueryPlanStmtStoreID = sar.FKQueryPlanStmtStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKQueryPlanStmtStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CorePE.QueryPlanStmtStore targ
					ON targ.PKQueryPlanStmtStoreID = t.ID
			WHERE targ.PKQueryPlanStmtStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.QueryPlanStmtStore.';
		END

		SET @lv__ErrorLoc = N'SBS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'SQLBatchStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (
				ID 
			)
			SELECT targ.PKSQLBatchStoreID
			FROM (SELECT DISTINCT sar.FKSQLBatchStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKSQLBatchStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CorePE.SQLBatchStore targ
					ON targ.PKSQLBatchStoreID = sar.FKSQLBatchStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKSQLBatchStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CorePE.SQLBatchStore targ
					ON targ.PKSQLBatchStoreID = t.ID
			WHERE targ.PKSQLBatchStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();
			SET @lv__tmpStr = ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.SQLBatchStore.';

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', @lv__tmpStr;
		END

		SET @lv__ErrorLoc = N'SSS delete';
		SELECT @lv__TableSize_ReservedPages = ss.rsvdpgs
		FROM (
			SELECT SUM(ps.reserved_page_count) as rsvdpgs
			FROM sys.dm_db_partition_stats ps
				INNER JOIN sys.objects o
					ON ps.object_id = o.object_id
			WHERE o.name = N'SQLStmtStore'
			AND o.type = 'U'
		) ss;

		IF @lv__TableSize_ReservedPages > 500*1024/8
		BEGIN
			TRUNCATE TABLE #StoreTableIDsToPurge;

			INSERT INTO #StoreTableIDsToPurge (ID)
			SELECT targ.PKSQLStmtStoreID
			FROM (SELECT DISTINCT sar.FKSQLStmtStoreID 
					FROM #AutoWhoDistinctStoreKeys sar WITH (NOLOCK)
					WHERE sar.FKSQLStmtStoreID IS NOT NULL) sar
				RIGHT OUTER hash JOIN CorePE.SQLStmtStore targ
					ON targ.PKSQLStmtStoreID = sar.FKSQLStmtStoreID
			WHERE targ.LastTouchedBy_SPIDCaptureTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
			AND sar.FKSQLStmtStoreID IS NULL
			OPTION(RECOMPILE, FORCE ORDER);

			SELECT @lv__tmpMinID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID ASC) xapp1;

			SELECT @lv__tmpMaxID = xapp1.ID
			FROM (SELECT NULL as col1) ss
				OUTER APPLY (SELECT TOP 1 t.ID
							FROM #StoreTableIDsToPurge t
							ORDER BY t.ID DESC) xapp1;

			DELETE targ 
			FROM #StoreTableIDsToPurge t
				INNER hash JOIN CorePE.SQLStmtStore targ
					ON targ.PKSQLStmtStoreID = t.ID
			WHERE targ.PKSQLStmtStoreID BETWEEN @lv__tmpMinID AND @lv__tmpMaxID
			OPTION(FORCE ORDER, RECOMPILE);

			SET @lv__tmpBigInt = ROWCOUNT_BIG();

			INSERT INTO AutoWho.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT GETDATE(), 0, N'Retention Purge', 
				ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.SQLStmtStore.';
		END

		--LightweightSessions, LightweightTasks, LightweightTrans, SARException, TAWException
		-- since these are heaps, we use tablock to allow the pages to be deallocated
		SET @lv__ErrorLoc = N'Lightweight deletes';
		DELETE FROM AutoWho.LightweightSessions WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		DELETE FROM AutoWho.LightweightTasks WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		DELETE FROM AutoWho.LightweightTrans WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		DELETE FROM AutoWho.SARException WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		DELETE FROM AutoWho.TAWException WITH (TABLOCK)
		WHERE SPIDCaptureTime < @lv__PurgeHardDeleteTime;

		--Get rid of metadata
		SET @lv__ErrorLoc = N'metedata deletes';
		DELETE AutoWho.CaptureTimes
		WHERE SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		AND SPIDCaptureTime < DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());

		DELETE targ 
		FROM AutoWho.CaptureSummary targ 
		WHERE targ.SPIDCaptureTime <= @lv__MaxSPIDCaptureTime
		AND NOT EXISTS (
			SELECT *
			FROM AutoWho.CaptureTimes ct
			WHERE ct.SPIDCaptureTime = targ.SPIDCaptureTime
		);

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureSummary.';

		--Find the list of ordinal caches (each is defined by a start/end time)
		-- that now have at least one "missing member". These are no longer useful
		DELETE targ 
		FROM CorePE.CaptureOrdinalCache targ
			INNER JOIN
				 (
					SELECT DISTINCT ord.StartTime, ord.EndTime
					FROM CorePE.CaptureOrdinalCache ord
						LEFT OUTER JOIN AutoWho.CaptureSummary cs
							ON cs.SPIDCaptureTime = ord.CaptureTime
					WHERE ord.Utility = N'AutoWho' 
					AND (
						ord.StartTime < DATEADD(HOUR, 0-@max__RetentionHours, GETDATE())
						OR cs.SPIDCaptureTime IS NULL 
					)
				) invalidCaches
				ON invalidCaches.StartTime = targ.StartTime
				AND invalidCaches.EndTime = targ.EndTime
		WHERE targ.Utility = N'AutoWho'
		;

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from AutoWho.CaptureOrdinalCache.';

		DELETE FROM CorePE.[Traces]
		WHERE Utility = N'AutoWho'
		AND CreateTime <= DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());

		SET @lv__tmpBigInt = ROWCOUNT_BIG();

		INSERT INTO AutoWho.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT GETDATE(), 0, N'Retention Purge', 
			ISNULL(CONVERT(NVARCHAR(20),@lv__tmpBigInt),N'<null>') + ' rows deleted from CorePE.Traces.';

		DELETE FROM AutoWho.[Log]
		WHERE LogDT <= DATEADD(DAY, 0 - @opt__Retention_CaptureTimes, GETDATE());

		RETURN 0;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;

		SET @lv__ErrorState = ERROR_STATE();
		SET @lv__ErrorSeverity = ERROR_SEVERITY();

		SET @lv__ErrorMessage = N'Exception occurred in procedure: ' + OBJECT_NAME(@@PROCID) + N' at location ("' + ISNULL(@lv__ErrorLoc,N'<null>') + '"). 
		Error #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + '; Message: ' + ISNULL(ERROR_MESSAGE(), N'<null>');

		RAISERROR(@lv__ErrorMessage, @lv__ErrorSeverity, @lv__ErrorState);
		RETURN -999;
	END CATCH
END


GO
