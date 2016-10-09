SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ViewLongBatches] 
/*   
	PROCEDURE:		AutoWho.ViewLongBatches

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: Called by the sp_LongRequests user-facing procedure. 
		The logic below pulls data from the various AutoWho tables, based on parameter values, and combines
		and formats the data as appropriate. 


	FUTURE ENHANCEMENTS: 


    CHANGE LOG:	
				2016-09-26	Aaron Morelli		Final run-through and commenting


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
EXEC AutoWho.ViewLongBatches @start='2016-05-12 02:45', @end='2016-05-12 03:45', @mindur=70, @attr=N'Y', @qplan=N'Y'
*/
(
	@start DATETIME, 
	@end DATETIME,
	@mindur INT,
	@spids NVARCHAR(128)=N'',
	@xspids NVARCHAR(128)=N'',
	@dbs NVARCHAR(512)=N'',
	@xdbs NVARCHAR(512)=N'',
	@attr NCHAR(1),
	@qplan NCHAR(1)
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET XACT_ABORT ON;

	--TODO: a final run-through of the procedure, looking for problems.

	--TODO: This proc needs to be performance-tuned!

	--TODO: create the wrapper sp_ procedure. (Pretty simple, just has param validation, try/catch, and help documentation.

	--TODO: if filtering by spid, display the waits in a vertical manner.




	DECLARE 
		--stmt store
		@PKSQLStmtStoreID			BIGINT, 
		@sql_handle					VARBINARY(64),
		@dbid						INT,
		@objectid					INT,
		@stmt_text					NVARCHAR(MAX),
		@stmt_xml					XML,
		@dbname						NVARCHAR(128),
		@schname					NVARCHAR(128),
		@objectname					NVARCHAR(128),
		@cxpacketwaitid				SMALLINT,
		--input buffer store
		@PKInputBufferStore			BIGINT,
		@ibuf_text					NVARCHAR(4000),
		@ibuf_xml					XML,
		@DBInclusionsExist			INT,
		@DBExclusionsExist			INT,
		@SPIDInclusionsExist		INT,
		@SPIDExclusionsExist		INT,

		--QueryPlan Stmt/Batch store
		@PKQueryPlanStmtStoreID		BIGINT,
		@PKQueryPlanBatchStoreID	BIGINT,
		@plan_handle				VARBINARY(64),
		@query_plan_text			NVARCHAR(MAX),
		@query_plan_xml				XML,


		@enum__waitorder__none				TINYINT,
		@enum__waitorder__lck				TINYINT,
		@enum__waitorder__latchblock		TINYINT,
		@enum__waitorder_pglatch			TINYINT,
		@enum__waitorder__cxp				TINYINT,
		@enum__waitorder__other				TINYINT
		;

	DECLARE 
		--misc
		@lv__scratchint				INT,
		@lv__msg					NVARCHAR(MAX),
		@lv__errsev					INT,
		@lv__errstate				INT,
		@lv__errorloc				NVARCHAR(100),
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@lv__DynSQL					NVARCHAR(MAX);

BEGIN TRY

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	SET @enum__waitorder__none =			CONVERT(TINYINT, 250);		--a. we typically want a "not waiting" task to sort near the end 
	SET @enum__waitorder__lck =				CONVERT(TINYINT, 5);		--b. lock waits should be at the top (so that blocking data is correct)
	SET @enum__waitorder__latchblock =		CONVERT(TINYINT, 10);		--c. sometimes latch waits can have a blocking spid, so those sort next, after lock waits.
																		--	these can be any type of latch (pg, pgio, a memory object, etc); 
	SET @enum__waitorder_pglatch =			CONVERT(TINYINT, 15);		-- Page and PageIO latches are fairly common, and in parallel plans we want them
																		-- to sort higher than other latches, e.g. the fairly common ACCESS_METHODS_DATASET_PARENT
	SET @enum__waitorder__cxp =				CONVERT(TINYINT, 200);		--d. parallel sorts near the end, since a parallel wait doesn't mean the spid is completely halted
	SET @enum__waitorder__other =			CONVERT(TINYINT, 20);		--e. catch-all bucket

	SELECT @cxpacketwaitid = dwt.DimWaitTypeID
	FROM AutoWho.DimWaitType dwt
	WHERE dwt.wait_type = N'CXPACKET';


	SET @lv__errorloc = N'Declare #TT';
	CREATE TABLE #FilterTab
	(
		FilterType TINYINT NOT NULL, 
			--0 DB inclusion
			--1 DB exclusion
			--2 SPID inclusion
			--3 SPID exclusion
		FilterID INT NOT NULL, 
		FilterName NVARCHAR(255)
	);


	CREATE TABLE #LongBatches (
		session_id INT NOT NULL,
		request_id INT NOT NULL,
		rqst__start_time DATETIME NOT NULL,
		BatchIdentifier INT NOT NULL,
		FirstSeen DATETIME NOT NULL,
		LastSeen DATETIME NOT NULL, 
		StartingDBID INT NULL,
		FKInputBufferStore BIGINT,
		[SessAttr] NVARCHAR(MAX),
		FilteredOut INT NOT NULL
	);

	CREATE TABLE #sarcache (
		SPIDCaptureTime DATETIME NOT NULL,
		session_id INT NOT NULL,
		request_id INT NOT NULL,
		rqst__start_time DATETIME NOT NULL,
		BatchIdentifier INT NOT NULL,			--sess/rqst/starttime uniquely identifies a batch, but in our XML grouping code, I don't want to mess
												-- with datetime values, so we create an int that is unique per batch.
		rqst__status_code TINYINT NULL, 
		rqst__cpu_time BIGINT NULL, 
		rqst__reads BIGINT NULL, 
		rqst__writes BIGINT NULL, 
		rqst__logical_reads BIGINT NULL, 
		[rqst__FKDimCommand] SMALLINT NULL, 
		[rqst__FKDimWaitType] SMALLINT NULL, 
		tempdb__CurrentlyAllocatedPages BIGINT NULL, 
		[tempdb__CalculatedNumberOfTasks] SMALLINT NULL,
		[mgrant__granted_memory_kb] BIGINT NULL,
		[mgrant__max_used_memory_kb] BIGINT NULL, 
		[mgrant__dop] SMALLINT NULL, 
		[tran_log_bytes] BIGINT NULL,
		[calc__tmr_wait] TINYINT NULL, 
		[FKSQLStmtStoreID] BIGINT NOT NULL, 
		[FKInputBufferStoreID] BIGINT NULL, 
		[FKQueryPlanStmtStoreID] BIGINT NOT NULL
	);

	CREATE TABLE #tawcache (
		SPIDCaptureTime DATETIME NOT NULL,
		session_id INT NOT NULL,
		request_id INT NOT NULL,
		task_address VARBINARY(8) NOT NULL, 
		BatchIdentifier INT NOT NULL,
		[TaskIdentifier] INT NOT NULL,
		tstate NVARCHAR(20) NOT NULL,
		FKDimWaitType SMALLINT NOT NULL, 
		wait_duration_ms BIGINT NOT NULL, 
		wait_order_category TINYINT NOT NULL, 
		wait_special_tag NVARCHAR(100) NOT NULL,
		wait_special_number INT NOT NULL
	);

	CREATE TABLE #stmtstats (
		BatchIdentifier INT NOT NULL,
		[FKSQLStmtStoreID] BIGINT NOT NULL, 
		[FKQueryPlanStmtStoreID] BIGINT NOT NULL,
		[#Seen] INT NOT NULL,
		[FirstSeen] DATETIME NOT NULL, 
		[LastSeen] DATETIME NOT NULL,
		[StatusCodeAgg] NVARCHAR(100) NULL,
		[Waits] NVARCHAR(4000) NULL,
		[CXWaits] NVARCHAR(4000) NULL,
		MaxTempDB__CurrentlyAllocatedPages BIGINT NULL,
		MaxGrantedMemoryKB BIGINT NULL, 
		MaxUsedMemoryKB BIGINT NULL, 
		MaxNumTasks SMALLINT NULL,
		HiDOP SMALLINT NULL,
		MaxTlog BIGINT,
		MaxCPUTime BIGINT, 
		MaxReads BIGINT, 
		MaxWrites BIGINT, 
		MaxLogicalReads BIGINT
	);

	--Note that this holds data in a partially-aggregated state, b/c of both the "tstate" field and the FKDimWaitType field are present in the grouping,
	-- but our final display will present the full aggregation over the data w/only one of them at a time. (tstate data in one field, waits in another) 
	CREATE TABLE #stmtwaitstats (
		BatchIdentifier INT NOT NULL,
		[FKSQLStmtStoreID] BIGINT NOT NULL, 
		[FKQueryPlanStmtStoreID] BIGINT NOT NULL,
		FKDimWaitType SMALLINT NOT NULL,
		tstate NVARCHAR(20) NOT NULL,
		wait_order_category TINYINT NOT NULL,
		wait_special_tag NVARCHAR(100) NOT NULL,
		NumTasks INT, 
		TotalWaitTime BIGINT
	);

	-- There is also the possibility that conversion to XML will fail, so we don't want to wait until the final join.
	-- This temp table is our workspace for that resolution/conversion work.
	CREATE TABLE #SQLStmtStore (
		PKSQLStmtStoreID			BIGINT NOT NULL,
		[sql_handle]				VARBINARY(64) NOT NULL,
		statement_start_offset		INT NOT NULL,
		statement_end_offset		INT NOT NULL, 
		[dbid]						SMALLINT NOT NULL,
		[objectid]					INT NOT NULL,
		datalen_batch				INT NOT NULL,
		stmt_text					NVARCHAR(MAX) NOT NULL,
		stmt_xml					XML,
		dbname						NVARCHAR(128),
		schname						NVARCHAR(128),
		objname						NVARCHAR(128)
	);

	--Ditto, input buffer conversions to XML can fail.
	CREATE TABLE #InputBufferStore (
		PKInputBufferStoreID		BIGINT NOT NULL,
		inputbuffer					NVARCHAR(4000) NOT NULL,
		inputbuffer_xml				XML
	);

	--Ditto, QP conversions to XML can fail.
	CREATE TABLE #QueryPlanStmtStore (
		PKQueryPlanStmtStoreID		BIGINT NOT NULL,
		[plan_handle]				VARBINARY(64) NOT NULL,
		--statement_start_offset		INT NOT NULL,
		--statement_end_offset		INT NOT NULL,
		--[dbid]						SMALLINT NOT NULL,
		--[objectid]					INT NOT NULL,
		[query_plan_text]			NVARCHAR(MAX) NOT NULL,
		[query_plan_xml]			XML
	);



	IF ISNULL(@dbs,N'') = N''
	BEGIN
		SET @DBInclusionsExist = 0;		--this flag (only for inclusions) is a perf optimization
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 0, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @dbs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @DBInclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @DBInclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to convert the @dbs parameter (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--db inclusion string parsing


	IF ISNULL(@xdbs, N'') = N''
	BEGIN
		SET @DBExclusionsExist = 0;
	END
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 1, d.database_id, d.name
				FROM (SELECT [dbnames] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @xdbs,  N',' , N'</M><M>') + N'</M>' AS XML) AS dblist) xmlparse
					CROSS APPLY dblist.nodes(N'/M') Split(a)
					) SS
					INNER JOIN sys.databases d			--we need the join to sys.databases so that we can get the correct case for the DB name.
						ON LOWER(SS.dbnames) = LOWER(d.name)	--the user may have passed in a db name that doesn't match the case for the DB name in the catalog,
				WHERE SS.dbnames <> N'';						-- and on a server with case-sensitive collation, we need to make sure we get the DB name exactly right

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @DBExclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @DBExclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to convert the @xdbs parameter (comma-separated list of database names) to a table of valid DB names. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--db exclusion string parsing

	IF ISNULL(@spids,N'') = N''
	BEGIN
		SET @SPIDInclusionsExist = 0;		--this flag (only for inclusions) is a perf optimization
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 2, SS.spids, NULL
				FROM (SELECT [spids] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @spids,  N',' , N'</M><M>') + N'</M>' AS XML) AS spidlist) xmlparse
					CROSS APPLY spidlist.nodes(N'/M') Split(a)
					) SS
				WHERE SS.spids <> N'';

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @SPIDInclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @SPIDInclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to convert the @spids parameter (comma-separated list of session IDs) to a table of valid integer values. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--spid inclusion string parsing

	IF ISNULL(@xspids,N'') = N''
	BEGIN
		SET @SPIDExclusionsExist = 0;
	END 
	ELSE
	BEGIN
		BEGIN TRY 
			INSERT INTO #FilterTab (FilterType, FilterID, FilterName)
				SELECT 3, SS.spids, NULL
				FROM (SELECT [spids] = LTRIM(RTRIM(Split.a.value(N'.', 'NVARCHAR(512)')))
					FROM (SELECT CAST(N'<M>' + REPLACE( @xspids,  N',' , N'</M><M>') + N'</M>' AS XML) AS spidlist) xmlparse
					CROSS APPLY spidlist.nodes(N'/M') Split(a)
					) SS
				WHERE SS.spids <> N'';

			SET @lv__ScratchInt = @@ROWCOUNT;

			IF @lv__ScratchInt = 0
			BEGIN
				SET @SPIDExclusionsExist = 0;
			END
			ELSE
			BEGIN
				SET @SPIDExclusionsExist = 1;
			END
		END TRY
		BEGIN CATCH
			SET @lv__msg = N'Error occurred when attempting to convert the @xspids parameter (comma-separated list of session IDs) to a table of valid integer values. Error #: ' +  
				CONVERT(NVARCHAR(20), ERROR_NUMBER()) + N'; State: ' + CONVERT(NVARCHAR(20), ERROR_STATE()) + N'; Severity: ' + CONVERT(NVARCHAR(20), ERROR_SEVERITY()) + '; Message: ' + 
				ERROR_MESSAGE();

			RAISERROR(@lv__msg, 16, 1);
			RETURN -1;
		END CATCH
	END		--spid exclusion string parsing


	IF EXISTS (SELECT * FROM #FilterTab dbs 
					INNER JOIN #FilterTab xdbs
						ON dbs.FilterID = xdbs.FilterID
						AND dbs.FilterType = 0
						AND xdbs.FilterType = 1
			)
	BEGIN
		RAISERROR('A database cannot be specified in both the @dbs and @xdbs filter parameters.', 16, 1);
		RETURN -1;
	END

	IF EXISTS (SELECT * FROM #FilterTab dbs 
					INNER JOIN #FilterTab xdbs
						ON dbs.FilterID = xdbs.FilterID
						AND dbs.FilterType = 2
						AND xdbs.FilterType = 3
			)
	BEGIN
		RAISERROR('A session ID cannot be specified in both the @spids and @xspids filter parameters.', 16, 1);
		RETURN -1;
	END
	
	SET @lv__errorloc = N'Identify long requests';
	INSERT INTO #LongBatches (
		session_id,
		request_id,
		rqst__start_time,
		BatchIdentifier,
		FirstSeen,
		LastSeen,
		FilteredOut
	)
	SELECT 
		session_id, request_id, rqst__start_time, 
		BatchIdentifier = RANK() OVER (ORDER BY rqst__start_time, session_id, request_id),
		FirstSeen,
		LastSeen,
		0
	FROM (
		SELECT 
			sar.session_id, 
			sar.request_id,
			sar.rqst__start_time,
			FirstSeen = MIN(SpidCaptureTime),
			LastSeen = MAX(SpidCaptureTime)
		FROM AutoWho.SessionsAndRequests sar
		WHERE sar.request_id >= 0
		AND sar.rqst__start_time IS NOT NULL 
		AND sar.sess__is_user_process = 1
		AND sar.calc__threshold_ignore = 0
		AND sar.calc__duration_ms > @mindur*1000
		AND sar.SPIDCaptureTime BETWEEN @start AND @end 
		GROUP BY session_id,
			request_id,
			rqst__start_time
	) ss
	OPTION(RECOMPILE)
	;

	IF @SPIDInclusionsExist = 1
	BEGIN
		DELETE FROM #LongBatches
		WHERE EXISTS (
			SELECT *
			FROM #FilterTab f
			WHERE f.FilterType=3
			AND f.FilterID = session_id
		);
	END

	IF @SPIDExclusionsExist = 1
	BEGIN
		DELETE FROM #LongBatches
		WHERE NOT EXISTS (
			SELECT * 
			FROM #FilterTab f
			WHERE f.FilterType = 2
			AND f.FilterID = session_id
		);
	END

	IF @attr = N'N'
	BEGIN
		UPDATE lb 
		SET lb.StartingDBID = sar.sess__database_id
		FROM #LongBatches lb
			INNER JOIN AutoWho.SessionsAndRequests sar 
				ON sar.SPIDCaptureTime = lb.FirstSeen
				AND sar.session_id = lb.session_id
				AND sar.request_id = lb.request_id
				AND sar.rqst__start_time = lb.rqst__start_time
		WHERE sar.SPIDCaptureTime BETWEEN @start and @end;
	END
	ELSE
	BEGIN
		UPDATE lb 
		SET lb.StartingDBID = sar.sess__database_id,
			SessAttr = N'<?spid' + CONVERT(nvarchar(20),sar.session_id) + N' -- ' + NCHAR(10) + NCHAR(13) + 

				N'Connect Time:				' + isnull(convert(nvarchar(30),sar.conn__connect_time,113),N'<null>') + NCHAR(10) + 
				N'Login Time:					' + isnull(convert(nvarchar(30),sar.sess__login_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request Start Time:	' + isnull(convert(nvarchar(30),sar.sess__last_request_start_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request End Time:		' + isnull(convert(nvarchar(30),sar.sess__last_request_end_time,113),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Client PID:					' + isnull(CONVERT(nvarchar(20),sar.sess__host_process_id),N'<null>') + NCHAR(10) +
				N'Client Interface/Version:	' + isnull(dsa.client_interface_name,N'<null>') + N' / ' + isnull(CONVERT(nvarchar(20),dsa.client_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + isnull(dca.net_transport,N'<null>') + NCHAR(10) +
				N'Client Address/Port:		' + isnull(dna.client_net_address,N'<null>') + + N' / ' + isnull(convert(nvarchar(20),nullif(sar.conn__client_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Local Address/Port:			' + isnull(nullif(dna.local_net_address,@lv__nullstring),N'<null>') + N' / ' + isnull(convert(nvarchar(20),nullif(dna.local_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Endpoint (Sess/Conn):		' + isnull(convert(nvarchar(20),dsa.endpoint_id),N'<null>') + N' / ' + isnull(convert(nvarchar(20),dca.endpoint_id),N'<null>') + NCHAR(10) + 
				N'Protocol Type/Version:		' + isnull(dca.protocol_type,N'<null>') + N' / ' + isnull(convert(nvarchar(20),dca.protocol_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + isnull(dca.net_transport,N'<null>') + NCHAR(10) + 
				N'Net Packet Size:			' + isnull(convert(nvarchar(20),dca.net_packet_size),N'<null>') + NCHAR(10) + 
				N'Encrypt Option:				' + isnull(dca.encrypt_option,N'<null>') + NCHAR(10) + 
				N'Auth Scheme:				' + isnull(dca.auth_scheme,N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Node Affinity:				' + isnull(convert(nvarchar(20),dca.node_affinity),N'<null>') + NCHAR(10) +
				N'Group ID (Sess/Rqst):		' + isnull(convert(nvarchar(20),dsa.group_id),N'<null>') + N' / ' + isnull(convert(nvarchar(20),isnull(sar.rqst__group_id,-1)),N'<null>') + NCHAR(10) + 
				N'Scheduler ID:				' + isnull(convert(nvarchar(20),sar.rqst__scheduler_id),N'<null>') + NCHAR(10) + 
				N'Managed Code:				' + isnull(convert(nvarchar(20),sar.rqst__executing_managed_code),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Open Tran Count (Sess/Rqst):		' + isnull(convert(nvarchar(20),sar.sess__open_transaction_count),N'<null>') + N' / ' + isnull(convert(nvarchar(20),sar.rqst__open_transaction_count),N'<null>') + NCHAR(10) + 
				N'Tran Iso Level (Sess/Rqst):			' + isnull(convert(nvarchar(20),dsa.transaction_isolation_level),N'<null>') + N' / ' + isnull(convert(nvarchar(20),sar.rqst__transaction_isolation_level),N'<null>') + NCHAR(10) + 
				N'Lock Timeout (Sess/Rqst):			' + isnull(convert(nvarchar(20),sar.sess__lock_timeout),N'<null>') + N' / ' + isnull(convert(nvarchar(20),sar.rqst__lock_timeout),N'<null>') + NCHAR(10) + 
				N'Deadlock Priority (Sess/Rqst):		' + isnull(convert(nvarchar(20),dsa.deadlock_priority),N'<null>') + N' / ' + isnull(convert(nvarchar(20),sar.rqst__deadlock_priority),N'<null>') + NCHAR(10) + 
					NCHAR(13) + N' -- ?>'
		FROM #LongBatches lb
			INNER JOIN AutoWho.SessionsAndRequests sar 
				ON sar.SPIDCaptureTime = lb.FirstSeen
				AND sar.session_id = lb.session_id
				AND sar.request_id = lb.request_id
				AND sar.rqst__start_time = lb.rqst__start_time
			LEFT OUTER JOIN AutoWho.DimSessionAttribute dsa
				ON sar.sess__FKDimSessionAttribute = dsa.DimSessionAttributeID
			LEFT OUTER JOIN AutoWho.DimNetAddress dna
				ON sar.conn__FKDimNetAddress = dna.DimNetAddressID
			LEFT OUTER JOIN AutoWho.DimConnectionAttribute dca
				ON sar.conn__FKDimConnectionAttribute = dca.DimConnectionAttributeID
		WHERE sar.SPIDCaptureTime BETWEEN @start and @end;
	END

	IF @DBExclusionsExist = 1
	BEGIN
		DELETE FROM #LongBatches 
		WHERE EXISTS (
			SELECT *
			FROM #FilterTab f
			WHERE f.FilterType=1
			AND f.FilterID = StartingDBID
		);
	END


	IF @DBInclusionsExist = 1
	BEGIN
		DELETE FROM #LongBatches
		WHERE NOT EXISTS (
			SELECT *
			FROM #FilterTab f
			WHERE f.FilterType=0
			AND f.FilterID = StartingDBID
		);
	END

	--For efficiency, let's grab the data from SAR and TAW that we will need for these batches, to save repeated
	-- trips to the much larger tables.
	SET @lv__errorloc = N'Populate SAR cache';
	INSERT INTO #sarcache (
		SPIDCaptureTime,
		session_id,
		request_id,
		rqst__start_time,
		BatchIdentifier,
		rqst__status_code,
		rqst__cpu_time,
		rqst__reads,
		rqst__writes,
		rqst__logical_reads,
		[rqst__FKDimCommand],
		[rqst__FKDimWaitType],
		tempdb__CurrentlyAllocatedPages,
		[tempdb__CalculatedNumberOfTasks],
		[mgrant__granted_memory_kb],
		[mgrant__max_used_memory_kb],
		[mgrant__dop],
		[tran_log_bytes],
		[calc__tmr_wait],
		[FKSQLStmtStoreID],
		[FKInputBufferStoreID],
		[FKQueryPlanStmtStoreID]
	)
	SELECT 
		sar.SPIDCaptureTime,				--1
		sar.session_id,
		sar.request_id,
		sar.rqst__start_time,
		lb.BatchIdentifier,
		sar.rqst__status_code,				--5
		sar.rqst__cpu_time,			
		sar.rqst__reads,			
		sar.rqst__writes,			
		sar.rqst__logical_reads,	
		sar.rqst__FKDimCommand,				--10
		sar.rqst__FKDimWaitType,			--11
		[tempdb__usage] = (
				CASE WHEN (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__sess_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_user_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__sess_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__sess_internal_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__task_user_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_user_objects_dealloc_page_count,0)) END + 
				CASE WHEN (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) < 0 THEN 0
					ELSE (ISNULL(sar.tempdb__task_internal_objects_alloc_page_count,0) - ISNULL(sar.tempdb__task_internal_objects_dealloc_page_count,0)) END),
		sar.tempdb__CalculatedNumberOfTasks,	--13	
		sar.mgrant__granted_memory_kb,		--14
		mgrant__max_used_memory_kb = CASE WHEN sar.mgrant__used_memory_kb > sar.mgrant__max_used_memory_kb
										THEN sar.mgrant__used_memory_kb
										ELSE sar.mgrant__max_used_memory_kb
										END,
		sar.mgrant__dop,
		trx.tran_log_bytes,
		sar.calc__tmr_wait,
		
		ISNULL(sar.FKSQLStmtStoreID,-1),				--20
		sar.FKInputBufferStoreID,
		ISNULL(sar.FKQueryPlanStmtStoreID,-1)

	FROM AutoWho.SessionsAndRequests sar
		INNER JOIN #LongBatches lb
			ON lb.session_id = sar.session_id
			AND lb.request_id = sar.request_id
			AND lb.rqst__start_time = sar.rqst__start_time
		LEFT OUTER JOIN (
			SELECT 
				SPIDCaptureTime,
				session_id,
				tran_log_bytes = SUM(tran_log_bytes)
			FROM (
				SELECT td.SPIDCaptureTime,
					td.session_id, 
					[tran_log_bytes] = CASE WHEN (ISNULL(dtdt_database_transaction_log_bytes_used,0) + ISNULL(dtdt_database_transaction_log_bytes_used_system,0)) >= 
													(ISNULL(dtdt_database_transaction_log_bytes_reserved,0) + ISNULL(dtdt_database_transaction_log_bytes_reserved_system,0)) 
											THEN ISNULL(dtdt_database_transaction_log_bytes_used,0) + ISNULL(dtdt_database_transaction_log_bytes_used_system,0)
											ELSE ISNULL(dtdt_database_transaction_log_bytes_reserved,0) + ISNULL(dtdt_database_transaction_log_bytes_reserved_system,0) END
				FROM AutoWho.TransactionDetails td
				WHERE td.SPIDCaptureTime BETWEEN @start AND @end 
			) ss
			GROUP BY SPIDCaptureTime, session_id
		) trx
			ON sar.SPIDCaptureTime = trx.SPIDCaptureTime
			AND sar.session_id = trx.session_id
	WHERE sar.SPIDCaptureTime BETWEEN @start AND @end 
	OPTION(RECOMPILE);



	SET @lv__errorloc = N'Populate TAW cache';
	INSERT INTO #tawcache (
		SPIDCaptureTime,
		session_id,
		request_id,
		task_address,
		BatchIdentifier,
		TaskIdentifier,
		tstate,
		FKDimWaitType,
		wait_duration_ms,
		wait_order_category,
		wait_special_tag,
		wait_special_number
	)
	SELECT 
		SPIDCaptureTime,
		session_id,
		request_id,
		task_address,
		BatchIdentifier,
		TaskIdentifier = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, task_address
											ORDER BY SPIDCaptureTime ASC),
		tstate, 
		FKDimWaitType,
		wait_duration_ms,
		wait_order_category,
		wait_special_tag,
		wait_special_number
	FROM (
		SELECT 
			taw.SPIDCaptureTime,
			taw.session_id, 
			taw.request_id,
			taw.task_address,
			sar.BatchIdentifier,
			tstate, 
			taw.FKDimWaitType,
			taw.wait_duration_ms,
			taw.wait_order_category,
			taw.wait_special_tag,
			taw.wait_special_number
		FROM (
				SELECT 
					SPIDCaptureTime, 
					session_id,
					request_id,
					task_address,
					tstate = CASE WHEN taw.FKDimWaitType = @cxpacketwaitid AND taw.tstate = N'S'
									THEN CONVERT(nvarchar(20),N'Suspended(CX)') 
									WHEN taw.tstate = N'S' THEN N'Suspended'
									WHEN taw.tstate = N'A' THEN N'Runnable'
									WHEN taw.tstate = N'R' THEN N'Running'
									ELSE CONVERT(nvarchar(20),tstate) END,
					FKDimWaitType,
					wait_duration_ms,
					wait_order_category,
					wait_special_tag,
					wait_special_number,
					--A task_address can be waiting on multiple blockers. We just choose the row
					-- that has the largest wait_duration_ms
					rn = ROW_NUMBER() OVER (PARTITION BY SPIDCaptureTime, session_id, request_id, task_address
											ORDER BY wait_duration_ms DESC)
				FROM AutoWho.TasksAndWaits taw
				WHERE taw.SPIDCaptureTime BETWEEN @start AND @end
			) taw
			INNER JOIN #sarcache sar
				ON sar.SPIDCaptureTime = taw.SPIDCaptureTime
				AND sar.session_id = taw.session_id
				AND sar.request_id = taw.request_id
		WHERE taw.rn = 1
	) ss
	OPTION(RECOMPILE);


	SET @lv__errorloc = N'Populate #stmtstats';
	INSERT INTO #stmtstats (
		BatchIdentifier,					--1
		[FKSQLStmtStoreID],
		[FKQueryPlanStmtStoreID],
		[#Seen],
		[FirstSeen],						--5
		[LastSeen],
		MaxTempDB__CurrentlyAllocatedPages,
		MaxGrantedMemoryKB,
		MaxUsedMemoryKB,
		MaxNumTasks,						--10
		HiDOP,
		MaxTlog,
		MaxCPUTime,
		MaxReads,
		MaxWrites,
		MaxLogicalReads						--15
	)
	SELECT 
		s.BatchIdentifier,					--1
		s.FKSQLStmtStoreID,
		s.FKQueryPlanStmtStoreID,
		SUM(1) AS [#Seen], 
		MIN(SPIDCaptureTime) as FirstSeen,		--5
		MAX(SPIDCaptureTime) as LastSeen,
		MAX(tempdb__CurrentlyAllocatedPages) as MaxTempDB__CurrentlyAllocatedPages, 
		MAX(s.mgrant__granted_memory_kb), 
		MAX(s.mgrant__max_used_memory_kb),
		MAX(s.tempdb__CalculatedNumberOfTasks),	--10
		MAX(s.mgrant__dop), 
		MAX(s.tran_log_bytes),
		MAX(s.rqst__cpu_time),
		MAX(s.rqst__reads),
		MAX(s.rqst__writes),
		MAX(s.rqst__logical_reads)
	FROM #sarcache s
	GROUP BY s.BatchIdentifier,
		s.FKSQLStmtStoreID,
		s.FKQueryPlanStmtStoreID
	;

	
	SET @lv__errorloc = N'Populate Stmt Wait Stats';
	INSERT INTO #stmtwaitstats (
		BatchIdentifier,
		[FKSQLStmtStoreID],
		[FKQueryPlanStmtStoreID],
		FKDimWaitType,
		tstate,
		wait_order_category,
		wait_special_tag,
		NumTasks,
		TotalWaitTime
	)
	SELECT
		BatchIdentifier,
		FKSQLStmtStoreID,
		FKQueryPlanStmtStoreID,
		FKDimWaitType,
		tstate, 
		wait_order_category,
		wait_special_tag, 
		NumTasks = SUM(1),
		TotalWaitTime = SUM(wait_duration_ms)
	FROM (
		SELECT 
			sar.BatchIdentifier,
			sar.FKSQLStmtStoreID,
			sar.FKQueryPlanStmtStoreID,
			taw.FKDimWaitType, 
			tstate,
			taw.wait_order_category,				--need this to prevent CXPACKET waits from being the top wait every time

			taw.wait_duration_ms, 
			--For CXPacket waits, we're going to display the wait subtype
			wait_special_tag = CASE WHEN taw.wait_order_category = @enum__waitorder__cxp 
									THEN taw.wait_special_tag + N':' + ISNULL(CONVERT(nvarchar(20),taw.wait_special_number),N'')
									ELSE N'' END
		FROM #sarcache sar
			INNER JOIN (
				--We need to prevent *really* long waits from being double-counted. Join #tawcache to itself
				-- and find where the "current" wait time is actually > the gap between cur & prev SPIDCaptureTimes.
				SELECT 
					cur.BatchIdentifier,
					cur.SPIDCaptureTime,
					cur.session_id,
					cur.request_id,
					cur.tstate,
					cur.FKDimWaitType,
					cur.wait_order_category,
					cur.wait_special_tag,
					cur.wait_special_number,
					wait_duration_ms = CASE WHEN prev.wait_duration_ms IS NULL THEN cur.wait_duration_ms
										ELSE (--we have a match, and we already know the wait type is the same
											CASE WHEN cur.wait_duration_ms > DATEDIFF(millisecond, prev.SPIDCaptureTime, cur.SPIDCaptureTime)
												THEN cur.wait_duration_ms - prev.wait_duration_ms
												ELSE cur.wait_duration_ms
												END
											)
										END
				FROM #tawcache cur
					LEFT OUTER JOIN #tawcache prev
						ON cur.BatchIdentifier = prev.BatchIdentifier
						AND cur.task_address = prev.task_address
						AND cur.FKDimWaitType = prev.FKDimWaitType
						AND cur.TaskIdentifier = prev.TaskIdentifier+1
					) taw
				ON sar.SPIDCaptureTime = taw.SPIDCaptureTime
				AND sar.session_id = taw.session_id
				AND sar.request_id = taw.request_id
		) tbase
	GROUP BY BatchIdentifier,
		FKSQLStmtStoreID,
		FKQueryPlanStmtStoreID,
		FKDimWaitType,
		tstate,
		wait_order_category,
		wait_special_tag
	;



	SET @lv__errorloc = N'Obtain Stmt Store raw';
	INSERT INTO #SQLStmtStore (
		PKSQLStmtStoreID,
		[sql_handle],
		statement_start_offset,
		statement_end_offset,
		[dbid],
		[objectid],
		datalen_batch,
		stmt_text
		--stmt_xml
		--dbname						NVARCHAR(128),
		--objname						NVARCHAR(128)
	)
	SELECT sss.PKSQLStmtStoreID, 
		sss.sql_handle,
		sss.statement_start_offset,
		sss.statement_end_offset,
		sss.dbid,
		sss.objectid,
		sss.datalen_batch,
		sss.stmt_text
	FROM CorePE.SQLStmtStore sss
	WHERE sss.PKSQLStmtStoreID IN (
		SELECT DISTINCT fk.FKSQLStmtStoreID
		FROM #sarcache fk
		WHERE fk.FKSQLStmtStoreID > 0
		)
	;

	SET @lv__errorloc = N'Declare Stmt Store Cursor';
	DECLARE resolveSQLStmtStore CURSOR LOCAL FAST_FORWARD FOR
	SELECT 
		PKSQLStmtStoreID,
		[sql_handle],
		[dbid],
		[objectid],
		stmt_text
	FROM #SQLStmtStore sss
	;

	SET @lv__errorloc = N'Open Stmt Store Cursor';
	OPEN resolveSQLStmtStore;
	FETCH resolveSQLStmtStore INTO @PKSQLStmtStoreID,
		@sql_handle,
		@dbid,
		@objectid,
		@stmt_text
	;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__errorloc = N'In Stmt Store loop';
		--Note that one major assumption of this procedure is that the DBID hasn't changed since the time the spid was 
		-- collected. For performance reasons, we don't resolve DBID in AutoWho.Collector; thus, if a DB is detached/re-attached,
		-- or deleted and the DBID is re-used by a completely different database, confusion can ensue.
		IF @dbid > 0
		BEGIN
			SET @dbname = DB_NAME(@dbid);
		END
		ELSE
		BEGIN
			SET @dbname = N'';
		END

		--Above note about DBID is relevant for this as well. 
		IF @objectid > 0
		BEGIN
			SET @objectname = OBJECT_NAME(@objectid,@dbid);
		END
		ELSE
		BEGIN
			SET @objectname = N'';
		END

		IF @objectid > 0
		BEGIN
			--if we do have a dbid/objectid pair, get the schema for the object
			IF @dbid > 0
			BEGIN
				SET @schname = OBJECT_SCHEMA_NAME(@objectid, @dbid);
			END
			ELSE
			BEGIN
				--if we don't have a valid dbid, we still do a "best effort" attempt to get schema
				SET @schname = OBJECT_SCHEMA_NAME(@objectid);
			END
			
			IF @schname IS NULL
			BEGIN
				SET @schname = N'';
			END
		END
		ELSE
		BEGIN
			SET @schname = N'';
		END

		IF @sql_handle = 0x0
		BEGIN
			SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'sql_handle is 0x0. The current SQL statement cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
			N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			IF @stmt_text IS NULL
			BEGIN
				SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'The statement text is NULL. No T-SQL command to display.' + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
			END
			ELSE
			BEGIN
				BEGIN TRY
					SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + @stmt_text + + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 
					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END TRY
				BEGIN CATCH
					SET @stmt_xml = CONVERT(XML, N'<?Stmt --' + NCHAR(10)+NCHAR(13) + N'Error converting text to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
					N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@PKSQLStmtStoreID,-1)) + 

					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END CATCH
			END
		END

		UPDATE #SQLStmtStore
		SET dbname = @dbname,
			objname = @objectname,
			schname = @schname,
			stmt_xml = @stmt_xml
		WHERE PKSQLStmtStoreID = @PKSQLStmtStoreID;

		FETCH resolveSQLStmtStore INTO @PKSQLStmtStoreID,
			@sql_handle,
			@dbid,
			@objectid,
			@stmt_text
		;
	END	--WHILE loop for SQL Stmt Store cursor
		
	CLOSE resolveSQLStmtStore;
	DEALLOCATE resolveSQLStmtStore;


	SET @lv__errorloc = N'Obtain IB raw';
	INSERT INTO #InputBufferStore (
		PKInputBufferStoreID,
		inputbuffer
		--inputbuffer_xml
	)
	SELECT ibs.PKInputBufferStoreID,
		ibs.InputBuffer
	FROM CorePE.InputBufferStore ibs
	WHERE ibs.PKInputBufferStoreID IN (
		SELECT DISTINCT fk.FKInputBufferStoreID 
		FROM #sarcache fk
		WHERE fk.FKInputBufferStoreID IS NOT NULL 
	)
	;

	SET @lv__errorloc = N'Declare IB cursor';
	DECLARE resolveInputBufferStore  CURSOR LOCAL FAST_FORWARD FOR 
	SELECT 
		PKInputBufferStoreID,
		inputbuffer
	FROM #InputBufferStore
	;

	SET @lv__errorloc = N'Open IB cursor';
	OPEN resolveInputBufferStore;
	FETCH resolveInputBufferStore INTO @PKInputBufferStore,
		@ibuf_text;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @lv__errorloc = N'In IB loop';
		IF @ibuf_text IS NULL
		BEGIN
			SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + N'The Input Buffer is NULL.' + NCHAR(10) + NCHAR(13) + 
			N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			BEGIN TRY
				SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + @ibuf_text + + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END TRY
			BEGIN CATCH
				SET @ibuf_xml = CONVERT(XML, N'<?InputBuffer --' + NCHAR(10)+NCHAR(13) + N'Error converting Input Buffer to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@PKInputBufferStore,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END CATCH
		END

		UPDATE #InputBufferStore
		SET inputbuffer_xml = @ibuf_xml
		WHERE PKInputBufferStoreID = @PKInputBufferStore;

		FETCH resolveInputBufferStore INTO @PKInputBufferStore,
			@ibuf_text;
	END

	CLOSE resolveInputBufferStore;
	DEALLOCATE resolveInputBufferStore;


	SET @lv__errorloc = N'Assign IB to batch';
	UPDATE targ 
	SET targ.FKInputBufferStore = ss2.FKInputBufferStoreID
	FROM #LongBatches targ
		INNER JOIN (
				SELECT 
					session_id, 
					request_id, 
					rqst__start_time, 
					FKInputBufferStoreID,
					rn = ROW_NUMBER() OVER (PARTITION BY session_id, request_id, rqst__start_time, FKInputBufferStoreID 
												ORDER BY NumOccurrences DESC)
				FROM (
					SELECT 
						sar.session_id,
						sar.request_id,
						sar.rqst__start_time,
						sar.FKInputBufferStoreID,
						NumOccurrences = COUNT(*)
					FROM #sarcache sar
					GROUP BY sar.session_id,
						sar.request_id,
						sar.rqst__start_time,
						sar.FKInputBufferStoreID
				) ss
			) ss2
				ON targ.session_id = ss2.session_id
				AND targ.request_id = ss2.request_id
				AND targ.rqst__start_time = ss2.rqst__start_time
	WHERE ss2.rn = 1
	;

	IF @qplan = N'Y'
	BEGIN
		SET @lv__errorloc = N'Obtain query plan store raw';
		INSERT INTO #QueryPlanStmtStore (
			PKQueryPlanStmtStoreID,
			[plan_handle],
			--statement_start_offset,
			--statement_end_offset,
			--[dbid],
			--[objectid],
			[query_plan_text]
			--[query_plan_xml]
		)
		SELECT 
			qpss.PKQueryPlanStmtStoreID,
			qpss.plan_handle,
			qpss.query_plan
		FROM CorePE.QueryPlanStmtStore qpss
		WHERE qpss.PKQueryPlanStmtStoreID IN (
			SELECT DISTINCT fk.FKQueryPlanStmtStoreID
			FROM #sarcache fk
			WHERE fk.FKQueryPlanStmtStoreID IS NOT NULL 
		)
		;

		SET @lv__errorloc = N'Declare query plan cursor';
		DECLARE resolveQueryPlanStmtStore CURSOR LOCAL FAST_FORWARD FOR 
		SELECT qpss.PKQueryPlanStmtStoreID,
			qpss.plan_handle,
			qpss.query_plan_text
		FROM #QueryPlanStmtStore qpss;

		SET @lv__errorloc = N'Open query plan cursor';
		OPEN resolveQueryPlanStmtStore;
		FETCH resolveQueryPlanStmtStore INTO @PKQueryPlanStmtStoreID,
			@plan_handle,
			@query_plan_text;

		WHILE @@FETCH_STATUS = 0
		BEGIN
			SET @lv__errorloc = N'In query plan loop';
			IF @plan_handle = 0x0
			BEGIN
				SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'plan_handle is 0x0. The Statement Query Plan cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
				N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END
			ELSE
			BEGIN
				IF @query_plan_text IS NULL
				BEGIN
					SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'The Statement Query Plan is NULL.' + NCHAR(10) + NCHAR(13) + 
					N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +
					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END
				ELSE
				BEGIN
					BEGIN TRY
						SET @query_plan_xml = CONVERT(XML, @query_plan_text);
					END TRY
					BEGIN CATCH
						--Most common reason for this is the 128-node limit
						SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'Error converting Statement Query Plan to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
						N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@PKQueryPlanStmtStoreID,-1)) +

						CASE WHEN ERROR_NUMBER() = 6335 AND @PKSQLStmtStoreID IS NOT NULL THEN 
							N'-- You can extract this query plan to a file with the below script
							--DROP TABLE dbo.largeQPbcpout
							SELECT query_plan
							INTO dbo.largeQPbcpout
							FROM CorePE.QueryPlanStmtStore q
							WHERE q.PKQueryPlanStmtStoreID = ' + CONVERT(NVARCHAR(20),@PKQueryPlanStmtStoreID) + N'
							--then from a command line:
							bcp dbo.largeQPbcpout out c:\largeqpxmlout.sqlplan -c -S. -T
							'
						ELSE N'' END + 

						NCHAR(10) + NCHAR(13) + N'-- ?>');
					END CATCH
				END
			END

			UPDATE #QueryPlanStmtStore
			SET query_plan_xml = @query_plan_xml
			WHERE PKQueryPlanStmtStoreID = @PKQueryPlanStmtStoreID;

			FETCH resolveQueryPlanStmtStore INTO @PKQueryPlanStmtStoreID,
				@plan_handle,
				@query_plan_text;
		END

		CLOSE resolveQueryPlanStmtStore;
		DEALLOCATE resolveQueryPlanStmtStore;
	END


	SET @lv__errorloc = N'Assign Status Code Agg';
	UPDATE targ 
	SET StatusCodeAgg = t0.status_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			status_nodes.status_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			status_nodes.status_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			status_nodes.status_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			status_nodes.status_node.value('(rqststatformatted/text())[1]', 'NVARCHAR(4000)') AS status_info
		FROM (
			SELECT 
				CONVERT(XML,
					REPLACE
					(
						CONVERT(NVARCHAR(MAX), status_raw.status_xml_raw) COLLATE Latin1_General_Bin2,
						N'</rqststatformatted></status><status><rqststatformatted>',
						N', '
						+ 
					--LEFT(CRYPT_GEN_RANDOM(1), 0)
					LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

					--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
					-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
					)
				) AS status_xml
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.OccurrenceOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					rqststatformatted = StatusCode + N',' + CONVERT(nvarchar(20),Pct) + N'% (' + CONVERT(nvarchar(20),NumOccurrences) + N')' 
				FROM (
					SELECT BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID, StatusCode, NumOccurrences, 
						TotalPerStmt,
						Pct = CONVERT(DECIMAL(4,1),100.*(1.*NumOccurrences) / (1.*TotalPerStmt)), 
						OccurrenceOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
															ORDER BY NumOccurrences DESC)
					FROM (
						SELECT 
							BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID, StatusCode, NumOccurrences, 
							[TotalPerStmt] = SUM(NumOccurrences) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								StatusCode = tstate,
								NumOccurrences = SUM(1)
							FROM #stmtwaitstats sar
							GROUP BY BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								tstate
						) grp
					) grpwithtotal
				) ordered 
				ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID 
				FOR XML PATH(N'status')
			) AS status_raw (status_xml_raw)
		) as status_final
		CROSS APPLY status_final.status_xml.nodes(N'/status') AS status_nodes (status_node)
		WHERE status_nodes.status_node.exist(N'batchidentifier') = 1
		--order by 1, 2, 3
	) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
	;
	

	--Now do non-CX waits
	SET @lv__errorloc = N'Construct nonCX waits';
	UPDATE targ 
	SET Waits = t0.waity_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			waity_nodes.waity_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			waity_nodes.waity_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			waity_nodes.waity_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			waity_nodes.waity_node.value('(waitformatted/text())[1]', 'NVARCHAR(4000)') AS waity_info
		FROM (
			SELECT
					CONVERT(XML,
						REPLACE
						(
							CONVERT(NVARCHAR(MAX), waity_raw.waity_xml_raw) COLLATE Latin1_General_Bin2,
							N'</waitformatted></waity><waity><waitformatted>',
							N', '
							+ 
						--LEFT(CRYPT_GEN_RANDOM(1), 0)
						LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

						--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
						-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
						)
					) AS waity_xml 
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					waitformatted = waittype + N'{' + CONVERT(NVARCHAR(20),NumTasks) + N'x' + 
									CONVERT(NVARCHAR(20),AvgWaitTime) + N'ms=' + 
										CASE WHEN TotalWaitTime = N'''' THEN N'''' 
											ELSE SUBSTRING(TotalWaitTime, 1, CHARINDEX('.',TotalWaitTime)-1) END + 
									N' (' + CONVERT(NVARCHAR(20),WaitPct) + N'%)' + N' }' 
				FROM (
						SELECT 
							BatchIdentifier,
							FKSQLStmtStoreID,
							FKQueryPlanStmtStoreID,
							waittype,
							NumTasks,
							TotalWaitTime=ISNULL(CONVERT(nvarchar(20),CONVERT(money,TotalWaitTime),1),N''),
							AvgWaitTime,
							WaitPct = CASE WHEN AllWaitTime <= 0 THEN -1 ELSE
									CONVERT(DECIMAL(4,1),100*(1.*TotalWaitTime) / (1.*AllWaitTime)) END,
							PriorityOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
																ORDER BY TotalWaitTime DESC)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								waittype = CASE WHEN dwt.latch_subtype <> N'' THEN dwt.wait_type + N'(' + dwt.latch_subtype + N')'
													ELSE dwt.wait_type END,
								NumTasks, 
								TotalWaitTime, 
								AvgWaitTime = CONVERT(DECIMAL(21,1), (1.*TotalWaitTime) / (1.*NumTasks)),
								AllWaitTime = SUM(TotalWaitTime) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
							FROM (
								SELECT 
									BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									FKDimWaitType, 
									--wait_order_category,		--note that we do NOT include this here, because unlike SessionViewer, we DO want to show
																-- the waits by how many/long they are, regardless of category.

									--We need to re-sum (final aggregation) b/c the data in the table has tstate as an additional grouping field,
									-- but we are not including tstate in this data.
									NumTasks = SUM(NumTasks), 
									TotalWaitTime = SUM(TotalWaitTime)
								FROM #stmtwaitstats w
								WHERE w.wait_order_category <> @enum__waitorder__cxp
								AND w.FKDimWaitType <> 1		--we ignore running tasks for this field
								GROUP BY BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									FKDimWaitType
							) grp
							INNER JOIN AutoWho.DimWaitType dwt
								ON grp.FKDimWaitType = dwt.DimWaitTypeID
						) grpwithtotal
							--debug
							--order by BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
					) ordered
					ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID
					FOR XML PATH(N'waity')
				) AS waity_raw (waity_xml_raw)
			) as waity_final
			CROSS APPLY waity_final.waity_xml.nodes(N'/waity') AS waity_nodes (waity_node)
			WHERE waity_nodes.waity_node.exist(N'batchidentifier') = 1
	) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
		;


	--And now CXP waits
	SET @lv__errorloc = N'Construct CX waits';
	UPDATE targ 
	SET CXWaits = t0.mcwaiter_info
	FROM #stmtstats targ
		INNER JOIN (
		SELECT 
			mcwaiter_nodes.mcwaiter_node.value('(batchidentifier/text())[1]', 'INT') AS BatchIdentifier,
			mcwaiter_nodes.mcwaiter_node.value('(fksqlstmtstoreid/text())[1]', 'BIGINT') AS FKSQLStmtStoreID,
			mcwaiter_nodes.mcwaiter_node.value('(fkqueryplanstmtstoreid/text())[1]', 'BIGINT') AS FKQueryPlanStmtStoreID,
			mcwaiter_nodes.mcwaiter_node.value('(waitformatted/text())[1]', 'NVARCHAR(4000)') AS mcwaiter_info
		FROM (
			SELECT
					CONVERT(XML,
						REPLACE
						(
							CONVERT(NVARCHAR(MAX), mcwaiter_raw.mcwaiter_xml_raw) COLLATE Latin1_General_Bin2,
							N'</waitformatted></mcwaiter><mcwaiter><waitformatted>',
							N', '
							+ 
						--LEFT(CRYPT_GEN_RANDOM(1), 0)
						LEFT(CONVERT(NVARCHAR(40),NEWID()),0)

						--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
						-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
						)
					) AS mcwaiter_xml 
			FROM (
				SELECT 
					batchidentifier = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.BatchIdentifier ELSE NULL END, 
					fksqlstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKSQLStmtStoreID ELSE NULL END, 
					fkqueryplanstmtstoreid = CASE WHEN ordered.PriorityOrder = 1 THEN ordered.FKQueryPlanStmtStoreID ELSE NULL END,
					waitformatted = waittype + 
									N'{' + CONVERT(NVARCHAR(20),NumTasks) + N'x' + 
									CONVERT(NVARCHAR(20),AvgWaitTime) + N'ms=' + 
										CASE WHEN TotalWaitTime = N'''' THEN N'''' 
											ELSE SUBSTRING(TotalWaitTime, 1, CHARINDEX('.',TotalWaitTime)-1) END + 
									N' (' + CONVERT(NVARCHAR(20),WaitPct) + N'%)' + N' }' 
				FROM (
						SELECT 
							BatchIdentifier,
							FKSQLStmtStoreID,
							FKQueryPlanStmtStoreID,
							waittype,
							NumTasks,
							TotalWaitTime=ISNULL(CONVERT(nvarchar(20),CONVERT(money,TotalWaitTime),1),N''),
							AvgWaitTime,
							WaitPct = CASE WHEN AllWaitTime <= 0 THEN -1 ELSE 
										CONVERT(DECIMAL(4,1),100*(1.*TotalWaitTime) / (1.*AllWaitTime)) END,
							PriorityOrder = ROW_NUMBER() OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
																ORDER BY TotalWaitTime DESC)
						FROM (
							SELECT 
								BatchIdentifier,
								FKSQLStmtStoreID,
								FKQueryPlanStmtStoreID,
								waittype = wait_special_tag,

								NumTasks, 
								TotalWaitTime, 
								AvgWaitTime = CONVERT(DECIMAL(21,1), (1.*TotalWaitTime) / (1.*NumTasks)),
								AllWaitTime = SUM(TotalWaitTime) OVER (PARTITION BY BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID)
							FROM (
								SELECT 
									BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									wait_special_tag,

									--We need to re-sum (final aggregation) b/c the data in the table has tstate as an additional grouping field,
									-- but we are not including tstate in this data.
									NumTasks = SUM(NumTasks), 
									TotalWaitTime = SUM(TotalWaitTime)
								FROM #stmtwaitstats w
								WHERE w.wait_order_category = @enum__waitorder__cxp
								AND w.FKDimWaitType <> 1		--we ignore running tasks for this field
								AND w.wait_special_tag <> N'?:-929'		-- no node ID, unknown wait sub-type... the resource_description field is prob fragmented
								GROUP BY BatchIdentifier, 
									FKSQLStmtStoreID, 
									FKQueryPlanStmtStoreID, 
									wait_special_tag
							) grp
							--For CX waits, we already know the wait type (CXPACKET!) so we can avoid
							-- the join to DimWaitType completely
							--INNER JOIN AutoWho.DimWaitType dwt
							--	ON grp.FKDimWaitType = dwt.DimWaitTypeID
						) grpwithtotal
							--debug
							--order by BatchIdentifier, FKSQLStmtStoreID, FKQueryPlanStmtStoreID
					) ordered
					ORDER BY ordered.BatchIdentifier, ordered.FKSQLStmtStoreID, ordered.FKQueryPlanStmtStoreID
					FOR XML PATH(N'mcwaiter')
				) AS mcwaiter_raw (mcwaiter_xml_raw)
			) as mcwaiter_final
			CROSS APPLY mcwaiter_final.mcwaiter_xml.nodes(N'/mcwaiter') AS mcwaiter_nodes (mcwaiter_node)
			WHERE mcwaiter_nodes.mcwaiter_node.exist(N'batchidentifier') = 1
		) t0
		ON targ.BatchIdentifier = t0.BatchIdentifier
		AND targ.FKSQLStmtStoreID = t0.FKSQLStmtStoreID
		AND targ.FKQueryPlanStmtStoreID = t0.FKQueryPlanStmtStoreID
		;

	SET @lv__errorloc = N'Construct final dynSQL';
	DECLARE @lv__OuterSelect NVARCHAR(MAX),
			@lv__Union NVARCHAR(MAX);



	SET @lv__OuterSelect = N'
	SELECT 
		SPID, 
		FirstSeen, 
		LastSeen, 
		[Extent(sec)],
		[#Seen], 
		[DB&Object] = DBObject, 
		Cmd,
		Statuses = StatusCodes,
		NonCXwaits,
		CXWaits,
		[Max#Tasks], 
		[HiDOP],
		MaxTdb, 
		MaxQMem,
		MaxUsedMem,
		Tlog,
		CPU = CASE WHEN CPU = N'''' THEN N'''' ELSE 
			SUBSTRING(CPU, 1, CHARINDEX(N''.'',CPU)-1) END, 
		MemReads,
		PhysReads,
		Writes
	' + CASE WHEN @attr = N'Y' OR @qplan = N'Y' THEN N',[Plan&Info] = PNI'
			ELSE N'' END

	SET @lv__Union = N'

	FROM (
	SELECT 
		--ordering fields
		BatchOrderBy = BatchIdentifier, 
		StmtStartOrderBy = rqst__start_time, 
		1 as TieBreaker,

		--visible fields
		SPID = CASE WHEN request_id = 0 THEN CONVERT(nvarchar(20), session_id) 
					ELSE CONVERT(nvarchar(20), session_id) + N'':'' + CONVERT(nvarchar(20), request_id) END,
		CPU = N'''',
		PhysReads = N'''',
		Writes = N'''',
		MemReads = N'''',
		Tlog = N'''',
		[#Seen] = N'''',
		FirstSeen = CONVERT(nvarchar(20),lb.rqst__start_time),
		LastSeen = CONVERT(nvarchar(20),lb.LastSeen),
		[Extent(sec)] = CONVERT(nvarchar(20),DATEDIFF(second, rqst__start_time, LastSeen)),

		DBObject = ISNULL(DB_NAME(StartingDBID),N''''),
		Cmd = xapp1.inputbuffer_xml,
		StatusCodes = N'''',
		NonCXWaits = N'''',
		CXWaits = N'''',
		MaxTdb = N'''', 
		MaxQMem = N'''',
		MaxUsedMem = N'''',
		[Max#Tasks] = N'''',
		[HiDOP] = N'''' 
		' + CASE WHEN @attr=N'N' AND @qplan = N'N' THEN N'' 
			ELSE (CASE WHEN @attr=N'N' THEN N',PNI = N'''' '
					ELSE N',PNI = CONVERT(XML,lb.SessAttr)' 
					END)
			END + N'
	FROM #LongBatches lb
		OUTER APPLY (
			SELECT TOP 1 ib.inputbuffer_xml
			FROM #InputBufferStore ib
			WHERE ib.PKInputBufferStoreID = lb.FKInputBufferStore 
		) xapp1

	UNION ALL

	SELECT 
		--ordering fields
		BatchOrderBy = s.BatchIdentifier, 
		StmtStartOrderBy = FirstSeen, 
		2 as TieBreaker,

		--Visible fields
		SPID = N'''', 
		CPU = CONVERT(nvarchar(20),CONVERT(money,s.MaxCPUTime),1),
		PhysReads = CONVERT(nvarchar(20),CONVERT(money,s.MaxReads*8./1024.),1),
		Writes = CONVERT(nvarchar(20),CONVERT(money,s.MaxWrites*8./1024.),1),
		MemReads = CONVERT(nvarchar(20),CONVERT(money,s.MaxLogicalReads*8./1024.),1),
		Tlog = ISNULL(CONVERT(nvarchar(20), CONVERT(money, s.MaxTlog/1024./1024.),1),N''''),
		[#Seen] = CONVERT(nvarchar(20), [#Seen]),
		FirstSeen = CONVERT(nvarchar(20),CONVERT(TIME(0),FirstSeen)), 
		LastSeen = CONVERT(nvarchar(20),CONVERT(TIME(0),LastSeen)),
		[Extent(sec)] = CONVERT(nvarchar(20),DATEDIFF(second, FirstSeen, LastSeen)),
		DBObject = CASE WHEN sss.dbid = 32767 OR sss.dbid = -929 THEN N''''
					ELSE ISNULL(sss.dbname,N''<null>'') + N''.'' END + 
				ISNULL(sss.schname,N''<null>'') + N''.'' +
				CASE WHEN sss.objectid = -929 THEN N''''
					ELSE ISNULL(sss.objname,N''<null>'') END,
		Cmd = sss.stmt_xml,
		StatusCodes = ISNULL(s.StatusCodeAgg,N''<null>''), 
		NonCXWaits = ISNULL(s.Waits,N''''),
		CXWaits = ISNULL(s.CXWaits,N''''),
		MaxTdb = CONVERT(nvarchar(20),CONVERT(money,s.MaxTempDB__CurrentlyAllocatedPages*8./1024.),1), 
		MaxQMem = ISNULL(CONVERT(nvarchar(20),CONVERT(money,s.MaxGrantedMemoryKB/1024.),1),N''''), 
		MaxUsedMem = ISNULL(CONVERT(nvarchar(20),CONVERT(money,s.MaxUsedMemoryKB/1024.),1),N''''), 
		[Max#Tasks] = CONVERT(nvarchar(20), s.MaxNumTasks), 
		[HiDOP] = ISNULL(CONVERT(nvarchar(20), s.HiDOP),N'''')
		' + CASE WHEN @attr=N'N' AND @qplan = N'N' THEN N'' 
			ELSE (CASE WHEN @qplan=N'N' THEN N',PNI = N'''' '
					ELSE N',PNI = qp.query_plan_xml' 
					END)
			END + N'
	FROM #stmtstats s
		INNER JOIN #SQLStmtStore sss
			ON s.FKSQLStmtStoreID = sss.PKSQLStmtStoreID
		' + CASE WHEN @qplan=N'N' THEN N''
			ELSE N'LEFT OUTER JOIN #QueryPlanStmtStore qp 
				ON qp.PKQueryPlanStmtStoreID = s.FKQueryPlanStmtStoreID' 
			END + N'
	) ss
	ORDER BY BatchOrderBy, 
		-- orders the statements
		StmtStartOrderBy, TieBreaker
	;
	';

	SET @lv__DynSQL = @lv__OuterSelect + @lv__Union;
	SET @lv__errorloc = N'Exec final dyn sql';
	print @lv__DynSQL;
	EXEC (@lv__DynSQL);
	/*
	EXEC sp_executesql @stmt=@lv__DynSQL, 
		@params=N'@lv__nullsmallint SMALLINT, @lv__nullint INT, @lv__nullstring NVARCHAR(8), @start DATETIME, @end DATETIME', 
		@lv__nullsmallint=@lv__nullsmallint, @lv__nullint = @lv__nullint, @lv__nullstring = @lv__nullstring, @start=@start, @end=@end;	
	*/

	RETURN 0;
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;
	SET @lv__errsev = ERROR_SEVERITY();
	SET @lv__errstate = ERROR_STATE();

	IF @lv__errorloc IN (N'Exec first dyn sql')
	BEGIN
		PRINT @lv__DynSQL;
	END

	SET @lv__msg = N'Exception occurred at location ("' + @lv__errorloc + N'"). Error #: ' + CONVERT(NVARCHAR(20),ERROR_NUMBER()) + 
		N'; Severity: ' + CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; State: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + 
		N'; Msg: ' + ERROR_MESSAGE();

	RAISERROR(@lv__msg, @lv__errsev, @lv__errstate);
	RETURN -1
END CATCH

END
GO
