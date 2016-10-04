SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ViewHistoricalQueryProgress] 
/*   
	PROCEDURE:		AutoWho.ViewHistoricalQueryProgress

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Called by the sp_QueryProgress user-facing procedure when historical/AutoWho data is requested. 
		The logic below pulls data from the various AutoWho tables, based on parameter values, and combines
		and formats the data as appropriate. 

	FUTURE ENHANCEMENTS: 

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
Shouldn't be called by programs directly. Only call when debugging.


*/
(
	@hct				DATETIME,				-- cannot be NULL
	--filters:
	@spid				INT,					-- spid to display; cannot be NULL
	@rqst				INT,					-- request
	@nodeassociation	NCHAR(1)=N'N',			--takes some license (i.e. makes assumptions that aren't guaranteed to be correct) to help associate
												--more tasks to nodes.
	@units				NVARCHAR(10)=N'KB',		--"native" the format the DMV presents it
												--"KB" kilobytes; "MB" megabytes
	@savespace			NCHAR(1)=N'N',			-- adjusts the formatting of various columns to reduce horizontal length, thus making the display more compressed so that
												-- more information fits in one screen.
	@effectiveordinal	INT,					-- presents the @offset (parameter to sp_SessionViewer) value to the user to let them know which offset they are at the SPIDCaptureTime range they chose.
	@dir				NVARCHAR(512)			-- misc directives
)
AS
BEGIN
	SET NOCOUNT ON;
	SET ANSI_PADDING ON;

	DECLARE 
		--misc
		@lv__scratchint				INT,
		@lv__msg					NVARCHAR(MAX),
		@lv__nullstring				NVARCHAR(8),
		@lv__nullint				INT,
		@lv__nullsmallint			SMALLINT,
		@lv__NumTaskAndWaitRows		INT,
		@lv__TotalCXS				BIGINT,

		--auxiliary options 
		@lv__IncludeSessConnAttr	BIT,
		@lv__BChainAvailable		BIT,
		@lv__LockDetailsAvailable	BIT,
		@lv__TranDetailsAvailable	BIT,
		@lv__IncludeBChain			BIT,
		@lv__IncludeLockDetails		BIT,
		@lv__IncludeTranDetails		BIT,
		@lv__BChainString			NVARCHAR(MAX),
		@lv__LockString				NVARCHAR(MAX),
		@lv__SessionConnString		NVARCHAR(MAX),

		--wait-type enum values
		@enum__waitspecial__none			TINYINT,
		@enum__waitspecial__lck				TINYINT,
		@enum__waitspecial__pgblocked		TINYINT,
		@enum__waitspecial__pgio			TINYINT,
		@enum__waitspecial__pg				TINYINT,
		@enum__waitspecial__latchblocked	TINYINT,
		@enum__waitspecial__latch			TINYINT,
		@enum__waitspecial__cxp				TINYINT,
		@enum__waitspecial__other			TINYINT,

		--Dynamic SQL variables
		@lv__DummyRow				NVARCHAR(MAX),
		@lv__BaseSELECT1			NVARCHAR(MAX),
		@lv__BaseSELECT2			NVARCHAR(MAX),
		@lv__BaseFROM				NVARCHAR(MAX),
		@lv__Formatted				NVARCHAR(MAX),
		@lv__ResultDynSQL			NVARCHAR(MAX)
		;

	--Cursor variables
	DECLARE 
		--stmt store
		@sql_handle					VARBINARY(64),
		@dbid						INT,
		@objectid					INT,
		@stmt_text					NVARCHAR(MAX),
		@stmt_xml					XML,
		@dbname						NVARCHAR(128),
		@schname					NVARCHAR(128),
		@objectname					NVARCHAR(128),

		--batch store
		@PKSQLBatchStoreID			BIGINT,
		@batch_text					NVARCHAR(MAX),
		@batch_xml					XML,

		--input buffer store
		@ibuf_text					NVARCHAR(4000),
		@ibuf_xml					XML,

		--QueryPlan Stmt/Batch store
		@plan_handle				VARBINARY(64),
		@query_plan_text			NVARCHAR(MAX),
		@query_plan_xml				XML
		;


	--initial values/enum population:

	SET @lv__nullstring = N'<nul5>';		--used the # 5 just to make it that much more unlikely that our "special value" would collide with a DMV value
	SET @lv__nullint = -929;				--ditto, used a strange/random number rather than -999, so there is even less of a chance of 
	SET @lv__nullsmallint = -929;			-- overlapping with some special system value

	--For the "waitspecial" enumeration, the numeric values don't necessarily have any comparison/ordering meaning among each other.
	-- Thus, the fact that @enum__waitspecial__pgblocked = 7 and this is larger than 5 (@enum__waitspecial__lck) isn't significant.
	SET @enum__waitspecial__none =			CONVERT(TINYINT, 0);
	SET @enum__waitspecial__lck =			CONVERT(TINYINT, 5);
	SET @enum__waitspecial__pgblocked =		CONVERT(TINYINT, 7);
	SET @enum__waitspecial__pgio =			CONVERT(TINYINT, 10);
	SET @enum__waitspecial__pg =			CONVERT(TINYINT, 15);
	SET @enum__waitspecial__latchblocked =	CONVERT(TINYINT, 17);
	SET @enum__waitspecial__latch =			CONVERT(TINYINT, 20);
	SET @enum__waitspecial__cxp =			CONVERT(TINYINT, 30);
	SET @enum__waitspecial__other =			CONVERT(TINYINT, 25);

	--Our final result set's top row indicates whether the BGraph, LockDetails, and TranDetails data was collected at this @hct, so that
	-- the user knows whether inspecting that data is even an option. Simple 1/0 flags are stored in AutoWho.CaptureSummary by the 
	-- AutoWho.PopulateCaptureSummary table (which looks at the base data in the AutoWho tables to determine these bit flag values). 
	-- Thus, pull those values
	SELECT 
		@lv__BChainAvailable = ISNULL(BlockingGraph,0),
		@lv__LockDetailsAvailable = ISNULL(LockDetails,0),
		@lv__TranDetailsAvailable = ISNULL(TranDetails,0)
	FROM (SELECT 1 as col1) t
		OUTER APPLY (
			SELECT 
				BlockingGraph,
				LockDetails,
				TranDetails
			FROM AutoWho.CaptureSummary cs
			WHERE cs.SPIDCaptureTime = @hct
		) xapp1;

	/*
	IF @waits = 3 AND @lv__LockDetailsAvailable = 1
	BEGIN
		SET @lv__IncludeLockDetails = 1;
	END
	ELSE
	BEGIN
		SET @lv__IncludeLockDetails = 0;
	END

	IF @tran = N'Y' AND @lv__TranDetailsAvailable = 1
	BEGIN
		SET @lv__IncludeTranDetails = 1;
	END
	ELSE
	BEGIN
		SET @lv__IncludeTranDetails = 0;
	END
	*/


	--The below columns are of interest to our final display.
	-- However, since just 1 row will qualify (equality on SPIDCaptureTime/session_id/request_id)
	-- not sure whether I will actually pull a row into a temp table or just grab the data
	-- from the perm table upon final display. 

	DECLARE
		@SPIDCaptureTime datetime ,
		@session_id smallint ,
		@request_id smallint ,
		@TimeIdentifier datetime ,
		@sess__login_time datetime,
		@sess__host_process_id int,
		@sess__status_code tinyint ,
		@sess__cpu_time int ,
		@sess__memory_usage int ,
		@sess__total_scheduled_time int ,
		@sess__total_elapsed_time int ,
		@sess__last_request_start_time datetime,
		@sess__last_request_end_time datetime,
		@sess__reads bigint ,
		@sess__writes bigint ,
		@sess__is_user_process bit ,
		@sess__logical_reads bigint ,
		@sess__lock_timeout int,
		@sess__row_count bigint ,
		@sess__open_transaction_count int ,
		@sess__database_id smallint ,
		@sess__FKDimSessionAttribute smallint ,
		@conn__connect_time datetime,
		@conn__client_tcp_port int,
		@conn__FKDimNetAddress smallint,
		@conn__FKDimConnectionAttribute smallint,
		@rqst__start_time datetime ,
		@rqst__status_code tinyint ,
		@rqst__blocking_session_id smallint ,
		@rqst__wait_time int ,
		@rqst__wait_resource nvarchar(256) ,
		@rqst__open_transaction_count int ,
		@rqst__open_resultset_count int ,
		@rqst__percent_complete real ,
		@rqst__cpu_time bigint ,
		@rqst__total_elapsed_time int ,
		@rqst__scheduler_id int ,
		@rqst__reads bigint ,
		@rqst__writes bigint ,
		@rqst__logical_reads bigint ,
		@rqst__transaction_isolation_level tinyint ,
		@rqst__lock_timeout int ,
		@rqst__deadlock_priority smallint ,
		@rqst__row_count bigint ,
		@rqst__granted_query_memory int ,
		@rqst__executing_managed_code bit ,
		@rqst__group_id int ,
		@rqst__FKDimCommand smallint ,
		@rqst__FKDimWaitType smallint ,
		@tempdb__sess_user_objects_alloc_page_count bigint ,
		@tempdb__sess_user_objects_dealloc_page_count bigint ,
		@tempdb__sess_internal_objects_alloc_page_count bigint ,
		@tempdb__sess_internal_objects_dealloc_page_count bigint ,
		@tempdb__task_user_objects_alloc_page_count bigint ,
		@tempdb__task_user_objects_dealloc_page_count bigint ,
		@tempdb__task_internal_objects_alloc_page_count bigint ,
		@tempdb__task_internal_objects_dealloc_page_count bigint ,
		@tempdb__CalculatedNumberOfTasks smallint ,
		@mgrant__request_time datetime ,
		@mgrant__grant_time datetime ,
		@mgrant__requested_memory_kb bigint ,
		@mgrant__required_memory_kb bigint ,
		@mgrant__granted_memory_kb bigint ,
		@mgrant__used_memory_kb bigint ,
		@mgrant__max_used_memory_kb bigint ,
		@mgrant__dop smallint ,
		@calc__is_compiling bit ,
		@calc__duration_ms bigint ,
		@calc__duration_seconds bigint ,
		@calc__blocking_session_id smallint ,
		@calc__is_blocker bit ,
		@calc__tmr_wait tinyint ,
		@calc__node_info nvarchar(40) ,
		@calc__status_info nvarchar(20) ,
		@FKSQLStmtStoreID bigint ,
		@FKSQLBatchStoreID bigint ,
		@FKInputBufferStoreID bigint ,
		@FKQueryPlanBatchStoreID bigint ,
		@FKQueryPlanStmtStoreID bigint
		;


	--TODO: do I want to display the blocking graph, if this SPID was present in one?

	--TODO: do I want to display the AutoWho.LockDetails data, if this SPID is present in that table for this SPIDCaptureTime?

	--TODO: definitely want to display AutoWho.TransactionDetails data somehow.

	CREATE TABLE #lhs_rhs (
		lhs_spid int,
		lhs_rqst int,
		lhs_ecid int,
		lhs_wspid int,
		lhs_taddr varbinary(8),
		lhs_paddr varbinary(8),
		lhs_cxs bigint,
		lhs_wait_type varchar(100),
		lhs_waitisincluded bit,
		lhs_cxwaitdirection tinyint,
		lhs_ismaintwait bit,
		lhs_isnoncxp bit,
		lhs_wait_node int,
		lhs_wait_ms bigint,
		lhs_baddr varbinary(8),
		lhs_bspid int,
		lhs_becid int,
		lhs_rdesc varchar(500),

		focus_node smallint,

		rhs_spid int,
		rhs_rqst int,
		rhs_ecid int,
		rhs_wspid int,
		rhs_taddr varbinary(8),
		rhs_paddr varbinary(8),
		rhs_cxs bigint,
		rhs_wait_type varchar(100),
		rhs_waitisincluded bit,
		rhs_cxwaitdirection tinyint,
		rhs_ismaintwait bit,
		rhs_isnoncxp bit,
		rhs_wait_node int,
		rhs_wait_ms bigint,
		rhs_baddr varbinary(8),
		rhs_bspid int,
		rhs_becid int,
		rhs_rdesc varchar(500)
	);

	CREATE TABLE #lhs_pre_output (
		spid smallint,
		rqst smallint, 
		nd smallint, 
		numtasks smallint,
		avgcxs bigint,
		mincxs bigint,
		maxcxs bigint,
		sumcxs bigint,
		cxcounter int,
		sumcxwaitdirection int,
		sumnoncxp int,
		hasmaintwait int,
		hasnoncxp int,
		wait_info varchar(400)
	);

	CREATE TABLE #rhs_pre_output (
		spid smallint,
		rqst smallint, 
		nd smallint, 
		numtasks smallint,
		avgcxs bigint,
		mincxs bigint,
		maxcxs bigint,
		sumcxs bigint,
		cxcounter int,
		sumcxwaitdirection int,
		sumnoncxp int, 
		hasmaintwait int,
		hasnoncxp int,
		wait_info varchar(400)
	);

	CREATE TABLE #TaskAndWaits (
		task_address			VARBINARY(8) NOT NULL, 
		parent_task_address		VARBINARY(8) NULL, 
		
		session_id				INT NOT NULL, 
		request_id				INT NOT NULL,
		exec_context_id			INT NOT NULL,
		task_state				NCHAR(1) NOT NULL,
		scheduler_id			INT NULL,
		context_switches_count	BIGINT NOT NULL,
		wait_type				NVARCHAR(60) NOT NULL,
		wait_duration_ms		BIGINT NOT NULL,

		wait_special_category	TINYINT NOT NULL, 
		wait_order_category		TINYINT NOT NULL, 
		wait_special_number		INT NOT NULL,				--for CXP, holds the node
		wait_special_tag		NVARCHAR(100) NOT NULL,		--for CXP, holds the subtype
		task_priority			INT NOT NULL,

		blocking_task_address	VARBINARY(8) NULL,
		blocking_session_id		INT NOT NULL,				-- = -929 if is same as session_id
		blocking_exec_context_id SMALLINT NOT NULL,
		resource_description	NVARCHAR(3072) NULL,
		resource_dbid			INT,
		resource_associatedobjid	BIGINT NOT NULL,
		cxp_wait_direction		TINYINT NOT NULL,			-- 1 = consumer, 2 = producer
		resolution_successful	BIT NOT NULL,
		resolved_name			NVARCHAR(256) NULL,
		ismaintwait				BIT NOT NULL,
		isnoncxp				BIT NOT NULL,
		wait_node				INT,
		rowisbad				TINYINT				/* Sometimes the rows we get back from sys.dm_os_waiting_tasks can be bad. Here's what I've observed so far,
									along with the numerical code I've assigned:
									1 -> CXPACKET wait type with NULL blocking_task_address and NULL resource_description
									2 -> CXPACKET wait type with NULL blocking_task_address and NON-null resource_description

										In both of the above cases, I mark the row as "bad", and change the wait-type to NULL to indicate that it is a
										running thread. (I do this b/c I suspect that the row returns "corrupt" because SQL is in the middle of changing
										the row in sys.dm_os_waiting_tasks from one wait-type to another, and my query reads the row in the middle of it 
										being changed. Thus, rather than filtering out the row completely, I just pretend that the row was "running" 
										when I saw it.

										NOTE 1: It is common, when querying dm_os_waiting_tasks returns a "corrupt" row, for the query to also return
											a "good" row for the same task_address. Thus, the logic below checks to see if there are any good rows for
											a task_address before using a bad row to construct the output.

										NOTE 2: When the resource_description is not NULL, then we actually can glean the node ID that this thread was
										associated with. TODO: Haven't decided yet if or how I'd like to use that information.
								*/
	);

	SELECT 
		@SPIDCaptureTime = [SPIDCaptureTime],
		@session_id = [session_id],
		@request_id = [request_id],
		@TimeIdentifier = [TimeIdentifier],
		@sess__status_code = [sess__status_code],
		@sess__cpu_time = [sess__cpu_time],
		@sess__memory_usage = [sess__memory_usage],
		@sess__total_scheduled_time = [sess__total_scheduled_time],
		@sess__total_elapsed_time = [sess__total_elapsed_time],
		@sess__reads = [sess__reads],
		@sess__writes = [sess__writes],
		@sess__is_user_process = [sess__is_user_process],
		@sess__logical_reads = [sess__logical_reads],
		@sess__row_count = [sess__row_count],
		@sess__open_transaction_count = [sess__open_transaction_count],
		@sess__database_id = [sess__database_id],
		@sess__FKDimSessionAttribute = [sess__FKDimSessionAttribute],
		@conn__FKDimNetAddress = conn__FKDimNetAddress,
		@conn__FKDimConnectionAttribute = conn__FKDimConnectionAttribute,
		@rqst__start_time = [rqst__start_time],
		@rqst__status_code = [rqst__status_code],
		@rqst__blocking_session_id = [rqst__blocking_session_id],
		@rqst__wait_time = [rqst__wait_time],
		@rqst__wait_resource = [rqst__wait_resource],
		@rqst__open_transaction_count = [rqst__open_transaction_count],
		@rqst__open_resultset_count = [rqst__open_resultset_count],
		@rqst__percent_complete = [rqst__percent_complete],
		@rqst__cpu_time = [rqst__cpu_time],
		@rqst__total_elapsed_time = [rqst__total_elapsed_time],
		@rqst__scheduler_id = [rqst__scheduler_id],
		@rqst__reads = [rqst__reads],
		@rqst__writes = [rqst__writes],
		@rqst__logical_reads = [rqst__logical_reads],
		@rqst__transaction_isolation_level = [rqst__transaction_isolation_level],
		@rqst__lock_timeout = [rqst__lock_timeout],
		@rqst__deadlock_priority = [rqst__deadlock_priority],
		@rqst__row_count = [rqst__row_count],
		@rqst__granted_query_memory = [rqst__granted_query_memory],
		@rqst__executing_managed_code = [rqst__executing_managed_code],
		@rqst__group_id = [rqst__group_id],
		@rqst__FKDimCommand = [rqst__FKDimCommand],
		@rqst__FKDimWaitType = [rqst__FKDimWaitType],
		@tempdb__sess_user_objects_alloc_page_count = [tempdb__sess_user_objects_alloc_page_count],
		@tempdb__sess_user_objects_dealloc_page_count = [tempdb__sess_user_objects_dealloc_page_count],
		@tempdb__sess_internal_objects_alloc_page_count = [tempdb__sess_internal_objects_alloc_page_count],
		@tempdb__sess_internal_objects_dealloc_page_count = [tempdb__sess_internal_objects_dealloc_page_count],
		@tempdb__task_user_objects_alloc_page_count = [tempdb__task_user_objects_alloc_page_count],
		@tempdb__task_user_objects_dealloc_page_count = [tempdb__task_user_objects_dealloc_page_count],
		@tempdb__task_internal_objects_alloc_page_count = [tempdb__task_internal_objects_alloc_page_count],
		@tempdb__task_internal_objects_dealloc_page_count = [tempdb__task_internal_objects_dealloc_page_count],
		@tempdb__CalculatedNumberOfTasks = [tempdb__CalculatedNumberOfTasks],
		@mgrant__request_time = [mgrant__request_time],
		@mgrant__grant_time = [mgrant__grant_time],
		@mgrant__requested_memory_kb = [mgrant__requested_memory_kb],
		@mgrant__required_memory_kb = [mgrant__required_memory_kb],
		@mgrant__granted_memory_kb = [mgrant__granted_memory_kb],
		@mgrant__used_memory_kb = [mgrant__used_memory_kb],
		@mgrant__max_used_memory_kb = [mgrant__max_used_memory_kb],
		@mgrant__dop = [mgrant__dop],
		@calc__is_compiling = [calc__is_compiling],
		@calc__duration_ms = [calc__duration_ms],
		@calc__duration_seconds = (
				CASE 
					WHEN session_id <= 0 THEN NULL
					WHEN (rqst__start_time = '1900-01-01' OR sess__last_request_end_time = '1900-01-01') THEN NULL
					ELSE DATEDIFF(SECOND, TimeIdentifier, sar.SPIDCaptureTime)
				END
			),
		@calc__blocking_session_id = [calc__blocking_session_id],
		@calc__is_blocker = [calc__is_blocker],
		@calc__tmr_wait = [calc__tmr_wait],
		@calc__node_info = [calc__node_info],
		@calc__status_info = [calc__status_info],
		@FKSQLStmtStoreID = [FKSQLStmtStoreID],
		@FKSQLBatchStoreID = [FKSQLBatchStoreID],
		@FKInputBufferStoreID = [FKInputBufferStoreID],
		@FKQueryPlanBatchStoreID = [FKQueryPlanBatchStoreID],
		@FKQueryPlanStmtStoreID = [FKQueryPlanStmtStoreID]
	FROM AutoWho.SessionsAndRequests sar
	WHERE sar.SPIDCaptureTime = @hct
	AND sar.session_id = @spid
	AND sar.request_id = @rqst
	;

	IF @SPIDCaptureTime IS NULL
	BEGIN
		SET @lv__msg = N'
*** No data exists for session_id ' + ISNULL(CONVERT(NVARCHAR(20),@spid),N'<null>') + N', request_id ' + 
ISNULL(CONVERT(NVARCHAR(20),@rqst),N'<null>') + N' at time ' + 
REPLACE(CONVERT(NVARCHAR(20), @hct, 102),'.','-')+' '+CONVERT(NVARCHAR(20), @hct, 108)+'.'+RIGHT(CONVERT(NVARCHAR(20),N'000')+CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @hct)),3) + N'
		';

		PRINT @lv__msg;
		RETURN 0;
	END


	INSERT INTO #TaskAndWaits (
		task_address,					--1
		parent_task_address,
		
		session_id,
		request_id,
		exec_context_id,				--5
		task_state,
		scheduler_id,
		context_switches_count,
		wait_type,
		wait_duration_ms,				--10

		wait_special_category,
		wait_order_category,
		wait_special_number,
		wait_special_tag,
		task_priority,					--15

		blocking_task_address,
		blocking_session_id,
		blocking_exec_context_id,
		resource_description,
		resource_dbid,					--20
		resource_associatedobjid,
		cxp_wait_direction,
		resolution_successful,
		resolved_name,
		ismaintwait,					--25
		isnoncxp,
		wait_node,
		rowisbad						--28
	)
	SELECT 
		taw.task_address,				--1
		taw.parent_task_address, 

		taw.session_id, 
		taw.request_id,
		taw.exec_context_id,			--5
		taw.tstate,
		taw.scheduler_id,
		taw.context_switches_count,
		wait_type = CASE WHEN taw.FKDimWaitType = 1		--special code for running tasks 
							THEN N'*Run*'
						WHEN dwt.wait_type = N'CXPACKET' THEN taw.wait_special_tag 
						WHEN dwt.wait_type LIKE N'LATCH%' THEN dwt.latch_subtype + REPLACE(wait_type, 'LATCH_', '(') + ')' 
						ELSE (
							CASE WHEN @savespace = N'N' THEN dwt.wait_type
								ELSE dwt.wait_type_short
							END
							)
						END,
		taw.wait_duration_ms,			--10

		wait_special_category,
		wait_order_category,
		wait_special_number,
		wait_special_tag,
		task_priority,					--15

		--CMEMTHREAD waits are very rare, but can contain a blocking_task_address value. This can cause the logic below to not work correctly, 
		-- because the DELETE that removes non-CXPACKET rows that are a blocker for CXPACKET will skip anything that has a non-null blocking_task_address value
		blocking_task_address = CASE WHEN dwt.wait_type = N'CMEMTHREAD' THEN NULL 
									ELSE taw.blocking_task_address
									END,
		taw.blocking_session_id, 
		blocking_exec_context_id = CASE --sometimes becid is null for producer waits when the blocking task is Thread 0
									WHEN taw.blocking_session_id = taw.session_id
											AND taw.blocking_task_address = taw.parent_task_address THEN 0
									ELSE blocking_exec_context_id
									END,
		taw.resource_description,
		taw.resource_dbid,				--20
		resource_associatedobjid,
		cxp_wait_direction,
		resolution_successful,
		resolved_name,
		ismaintwait = CASE WHEN dwt.wait_type LIKE N'%BACKUP%' OR wait_type LIKE N'%DBCC%' THEN 1 ELSE 0 END,		--25
		isnoncxp = CASE WHEN dwt.wait_type <> N'CXPACKET' THEN 1 ELSE 0 END,
		wnode = CASE
			WHEN dwt.wait_type <> N'CXPACKET' THEN 
				(CASE WHEN  parent_task_address IS NOT NULL THEN 9999
					ELSE -1
					END )
			ELSE ( CASE WHEN resource_description IS NULL THEN -555
					ELSE wait_special_number
					END 
				 )
			END,
		--handle fragmented TAW rows
		rowisbad = CASE WHEN dwt.wait_type LIKE N'CXP%' AND (taw.blocking_task_address IS NULL OR taw.resource_description IS NULL)
			THEN (CASE WHEN taw.blocking_task_address IS NULL AND taw.resource_description IS NULL THEN 1
						WHEN taw.blocking_task_address IS NULL AND taw.resource_description IS NOT NULL THEN 2
					ELSE 0
				END
				)
			ELSE 0
			END				--28
	FROM AutoWho.TasksAndWaits taw
		INNER JOIN AutoWho.DimWaitType dwt
			ON taw.FKDimWaitType = dwt.DimWaitTypeID
	WHERE taw.SPIDCaptureTime = @hct
	AND taw.session_id = @spid
	AND taw.request_id = @rqst
	;

	SET @lv__NumTaskAndWaitRows = @@ROWCOUNT;


	SELECT 
		@sql_handle = sss.sql_handle,
		@dbid = sss.dbid,
		@objectid = sss.objectid,
		@stmt_text = sss.stmt_text
	FROM CorePE.SQLStmtStore sss
	WHERE sss.PKSQLStmtStoreID = @FKSQLStmtStoreID;

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
		SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'sql_handle is 0x0. The current SQL statement cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
		N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@FKSQLStmtStoreID,-1)) + 
		NCHAR(10) + NCHAR(13) + N'-- ?>');
	END
	ELSE
	BEGIN
		IF @stmt_text IS NULL
		BEGIN
			SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'The statement text is NULL. No T-SQL command to display.' + NCHAR(10) + NCHAR(13) + 
				N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@FKSQLStmtStoreID,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			BEGIN TRY
				SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + @stmt_text + + NCHAR(10) + NCHAR(13) + 
				N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@FKSQLStmtStoreID,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END TRY
			BEGIN CATCH
				SET @stmt_xml = CONVERT(XML, N'<?cmd --' + NCHAR(10)+NCHAR(13) + N'Error converting text to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
				N'PKSQLStmtStoreID: ' + CONVERT(NVARCHAR(20),ISNULL(@FKSQLStmtStoreID,-1)) + 

				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END CATCH
		END
	END



	IF @FKInputBufferStoreID IS NOT NULL
	BEGIN
		SELECT 
			@ibuf_text = ib.InputBuffer
		FROM CorePE.InputBufferStore ib
		WHERE ib.PKInputBufferStoreID = @FKInputBufferStoreID;

		IF @ibuf_text IS NULL
		BEGIN
			SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + N'The Input Buffer is NULL.' + NCHAR(10) + NCHAR(13) + 
			N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@FKInputBufferStoreID,-1)) + 
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			BEGIN TRY
				SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + @ibuf_text + + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@FKInputBufferStoreID,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END TRY
			BEGIN CATCH
				SET @ibuf_xml = CONVERT(XML, N'<?IBuf --' + NCHAR(10)+NCHAR(13) + N'Error converting Input Buffer to XML: ' + ERROR_MESSAGE() + NCHAR(10) + NCHAR(13) + 
				N'PKInputBufferStore: ' + CONVERT(NVARCHAR(20),ISNULL(@FKInputBufferStoreID,-1)) + 
				NCHAR(10) + NCHAR(13) + N'-- ?>');
			END CATCH
		END
	END	--IF @FKInputBufferStoreID IS NOT NULL

	IF @FKQueryPlanStmtStoreID IS NOT NULL
	BEGIN
		SELECT 
			@plan_handle = qpss.plan_handle,
			@query_plan_text = qpss.query_plan
		FROM CorePE.QueryPlanStmtStore qpss
		WHERE qpss.PKQueryPlanStmtStoreID = @FKQueryPlanStmtStoreID
		;

		IF @plan_handle = 0x0
		BEGIN
			SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'plan_handle is 0x0. The Statement Query Plan cannot be displayed.' + NCHAR(10) + NCHAR(13) + 
			N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@FKQueryPlanStmtStoreID,-1)) +
			NCHAR(10) + NCHAR(13) + N'-- ?>');
		END
		ELSE
		BEGIN
			IF @query_plan_text IS NULL
			BEGIN
				SET @query_plan_xml = CONVERT(XML, N'<?StmtPlan --' + NCHAR(10)+NCHAR(13) + N'The Statement Query Plan is NULL.' + NCHAR(10) + NCHAR(13) + 
				N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@FKQueryPlanStmtStoreID,-1)) +
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
					N'PKQueryPlanStmtStoreID: ' + CONVERT(NVARCHAR(20), ISNULL(@FKQueryPlanStmtStoreID,-1)) +

					CASE WHEN ERROR_NUMBER() = 6335 AND @FKSQLStmtStoreID IS NOT NULL THEN 
						N'-- You can extract this query plan to a file with the below script
						--DROP TABLE dbo.largeQPbcpout
						SELECT query_plan
						INTO dbo.largeQPbcpout
						FROM CorePE.QueryPlanStmtStore q
						WHERE q.PKQueryPlanStmtStoreID = ' + CONVERT(NVARCHAR(20),@FKQueryPlanStmtStoreID) + N'
						--then from a command line:
						bcp dbo.largeQPbcpout out c:\largeqpxmlout.sqlplan -c -S. -T
						'
					ELSE N'' END + 

					NCHAR(10) + NCHAR(13) + N'-- ?>');
				END CATCH
			END
		END
	END		--IF @FKQueryPlanStmtStoreID IS NOT NULL


	SELECT @lv__SessionConnString = N'<?spid' + CONVERT(nvarchar(20),@session_id) + N' -- ' + NCHAR(10) + NCHAR(13) + 

				N'Connect Time:				' + isnull(convert(nvarchar(30),@conn__connect_time,113),N'<null>') + NCHAR(10) + 
				N'Login Time:					' + isnull(convert(nvarchar(30),@sess__login_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request Start Time:	' + isnull(convert(nvarchar(30),@sess__last_request_start_time,113),N'<null>') + NCHAR(10) + 
				N'Last Request End Time:		' + isnull(convert(nvarchar(30),@sess__last_request_end_time,113),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Client PID:					' + isnull(CONVERT(nvarchar(20),@sess__host_process_id),N'<null>') + NCHAR(10) +
				N'Client Interface/Version:	' + isnull(dsa.client_interface_name,N'<null>') + N' / ' + isnull(CONVERT(nvarchar(20),dsa.client_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + isnull(dca.net_transport,N'<null>') + NCHAR(10) +
				N'Client Address/Port:		' + isnull(dna.client_net_address,N'<null>') + + N' / ' + isnull(convert(nvarchar(20),nullif(@conn__client_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Local Address/Port:			' + isnull(nullif(dna.local_net_address,@lv__nullstring),N'<null>') + N' / ' + isnull(convert(nvarchar(20),nullif(dna.local_tcp_port,@lv__nullint)),N'<null>') + NCHAR(10) + 
				N'Endpoint (Sess/Conn):		' + isnull(convert(nvarchar(20),dsa.endpoint_id),N'<null>') + N' / ' + isnull(convert(nvarchar(20),dca.endpoint_id),N'<null>') + NCHAR(10) + 
				N'Protocol Type/Version:		' + isnull(dca.protocol_type,N'<null>') + N' / ' + isnull(convert(nvarchar(20),dca.protocol_version),N'<null>') + NCHAR(10) +
				N'Net Transport:				' + isnull(dca.net_transport,N'<null>') + NCHAR(10) + 
				N'Net Packet Size:			' + isnull(convert(nvarchar(20),dca.net_packet_size),N'<null>') + NCHAR(10) + 
				N'Encrypt Option:				' + isnull(dca.encrypt_option,N'<null>') + NCHAR(10) + 
				N'Auth Scheme:				' + isnull(dca.auth_scheme,N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Node Affinity:				' + isnull(convert(nvarchar(20),dca.node_affinity),N'<null>') + NCHAR(10) +
				N'Group ID (Sess/Rqst):		' + isnull(convert(nvarchar(20),dsa.group_id),N'<null>') + N' / ' + isnull(convert(nvarchar(20),isnull(@rqst__group_id,-1)),N'<null>') + NCHAR(10) + 
				N'Scheduler ID:				' + isnull(convert(nvarchar(20),@rqst__scheduler_id),N'<null>') + NCHAR(10) + 
				N'Managed Code:				' + isnull(convert(nvarchar(20),@rqst__executing_managed_code),N'<null>') + NCHAR(10) + NCHAR(13) + 

				N'Open Tran Count (Sess/Rqst):		' + isnull(convert(nvarchar(20),@sess__open_transaction_count),N'<null>') + N' / ' + isnull(convert(nvarchar(20),@rqst__open_transaction_count),N'<null>') + NCHAR(10) + 
				N'Tran Iso Level (Sess/Rqst):			' + isnull(convert(nvarchar(20),dsa.transaction_isolation_level),N'<null>') + N' / ' + isnull(convert(nvarchar(20),@rqst__transaction_isolation_level),N'<null>') + NCHAR(10) + 
				N'Lock Timeout (Sess/Rqst):			' + isnull(convert(nvarchar(20),@sess__lock_timeout),N'<null>') + N' / ' + isnull(convert(nvarchar(20),@rqst__lock_timeout),N'<null>') + NCHAR(10) + 
				N'Deadlock Priority (Sess/Rqst):		' + isnull(convert(nvarchar(20),dsa.deadlock_priority),N'<null>') + N' / ' + isnull(convert(nvarchar(20),@rqst__deadlock_priority),N'<null>') + NCHAR(10) + 
				 NCHAR(13) + N' -- ?>'
	FROM (select 1 col1) ss0
		OUTER APPLY (
			SELECT dsa.*
			FROM AutoWho.DimSessionAttribute dsa
			WHERE dsa.DimSessionAttributeID = @sess__FKDimSessionAttribute
		) dsa
		OUTER APPLY (
			SELECT dca.*
			FROM AutoWho.DimConnectionAttribute dca
			WHERE dca.DimConnectionAttributeID = @conn__FKDimConnectionAttribute
		) dca
		OUTER APPLY (
			SELECT *
			FROM AutoWho.DimNetAddress dna
			WHERE dna.DimNetAddressID = @conn__FKDimNetAddress
		) dna
	;


	--print 'taw rows: ' + convert(nvarchar(20),@lv__NumTaskAndWaitRows);

	IF @lv__NumTaskAndWaitRows > 1
	BEGIN
		--We'll need the total CXS further down below
		SELECT 
			@lv__TotalCXS = SUM(CXS)
		FROM (
			SELECT 
				task_address, 
				CXS = MAX(context_switches_count)
			FROM #TaskAndWaits taw
			GROUP BY task_address
		) dedup
		;

		INSERT INTO #lhs_rhs (
			lhs_spid,
			lhs_rqst,
			lhs_ecid,
			lhs_wspid,
			lhs_taddr,
			lhs_paddr,
			lhs_cxs,
			lhs_wait_type,
			lhs_waitisincluded,
			lhs_cxwaitdirection,
			lhs_ismaintwait,
			lhs_isnoncxp,
			lhs_wait_node,
			lhs_wait_ms,
			lhs_baddr,
			lhs_bspid,
			lhs_becid,
			lhs_rdesc,

			focus_node,

			rhs_spid,
			rhs_rqst,
			rhs_ecid,
			rhs_wspid,
			rhs_taddr,
			rhs_paddr,
			rhs_cxs,
			rhs_wait_type,
			rhs_waitisincluded,
			rhs_cxwaitdirection,
			rhs_ismaintwait,
			rhs_isnoncxp,
			rhs_wait_node,
			rhs_wait_ms,
			rhs_baddr,
			rhs_bspid,
			rhs_becid,
			rhs_rdesc
		)
		SELECT lhs.spid,
			lhs.rqst,

			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.ecid ELSE rhs.ecid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.wspid ELSE rhs.wspid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.taddr ELSE rhs.taddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.paddr ELSE rhs.paddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.cxs ELSE rhs.cxs END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.wait_type ELSE rhs.wait_type END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.waitisincluded ELSE rhs.waitisincluded END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.cxwaitdirection ELSE rhs.cxwaitdirection END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.ismaintwait ELSE rhs.ismaintwait END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.isnoncxp ELSE rhs.isnoncxp END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.wait_node ELSE rhs.wait_node END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.wait_ms ELSE rhs.wait_ms END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.baddr ELSE rhs.baddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.bspid ELSE rhs.bspid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.becid ELSE rhs.becid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.rdesc ELSE rhs.rdesc END,

			--Focus node:
			--CASE WHEN lhs.wait_type <> 'NewRow' THEN lhs.wait_node ELSE rhs.wait_node END,
			lhs.wait_node, 

			rhs.spid,
			rhs.rqst,

			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.ecid ELSE lhs.ecid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.wspid ELSE lhs.wspid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.taddr ELSE lhs.taddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.paddr ELSE lhs.paddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.cxs ELSE lhs.cxs END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.wait_type ELSE lhs.wait_type END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.waitisincluded ELSE lhs.waitisincluded END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.cxwaitdirection ELSE lhs.cxwaitdirection END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.ismaintwait ELSE lhs.ismaintwait END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.isnoncxp ELSE lhs.isnoncxp END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.wait_node ELSE lhs.wait_node END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.wait_ms ELSE lhs.wait_ms END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.baddr ELSE lhs.baddr END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.bspid ELSE lhs.bspid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.becid ELSE lhs.becid END,
			CASE WHEN lhs.wait_type <> 'NewRow' THEN rhs.rdesc ELSE lhs.rdesc END
		FROM (SELECT 
				spid = session_id, 
				rqst = request_id, 
				ecid = exec_context_id, 
				wspid = session_id, 
				taddr = task_address, 
				paddr = parent_task_address, 
				cxs = context_switches_count, 
				wait_type, 
				cxwaitdirection = cxp_wait_direction, 
				ismaintwait, 
				isnoncxp,
				wait_node, 
				wait_ms = wait_duration_ms, 
				baddr = blocking_task_address, 
				bspid = blocking_session_id, 
				becid = blocking_exec_context_id, 
				rdesc = resource_description,
				waitisincluded = 1		--not implementing wait exclusions at this time
			FROM #TaskAndWaits lhs
			WHERE --structuring it this way should cause the extra table access to be only done when rowisbad > 0, which isn't common.
				1 = (CASE WHEN rowisbad = 0 THEN 1
						WHEN rowisbad > 0
								AND NOT EXISTS (SELECT * FROM #TaskAndWaits lhs2 
								WHERE lhs.task_address = lhs2.task_address
								AND lhs2.rowisbad = 0)
						THEN 1
					ELSE 0
					END
				)
			) lhs
			LEFT OUTER JOIN 
				(SELECT 
					spid = session_id, 
					rqst = request_id, 
					ecid = exec_context_id, 
					wspid = session_id, 
					taddr = task_address, 
					paddr = parent_task_address, 
					cxs = context_switches_count, 
					wait_type, 
					cxwaitdirection = cxp_wait_direction, 
					ismaintwait, 
					isnoncxp,
					wait_node, 
					wait_ms = wait_duration_ms, 
					baddr = blocking_task_address, 
					bspid = blocking_session_id, 
					becid = blocking_exec_context_id, 
					rdesc = resource_description,
					waitisincluded = 1		--not implementing wait exclusions at this time
				FROM #TaskAndWaits rhs
				WHERE --structuring it this way should cause the extra table access to be only done when rowisbad > 0, which isn't common.
				1 = (CASE WHEN rowisbad = 0 THEN 1
						WHEN rowisbad > 0
								AND NOT EXISTS (SELECT * FROM #TaskAndWaits rhs2 
								WHERE rhs.task_address = rhs2.task_address
								AND rhs2.rowisbad = 0)
						THEN 1
					ELSE 0
					END
				)
				) rhs
				ON lhs.spid = rhs.spid 
				AND lhs.rqst = rhs.rqst
				AND lhs.baddr = rhs.taddr
		;

		--TODO: need to re-grok this and write a comment for why we're doing this.
		DELETE targ 
		FROM #lhs_rhs targ
		WHERE targ.rhs_taddr IS NULL
		AND EXISTS (
			SELECT * FROM #lhs_rhs t2
			WHERE targ.lhs_taddr = t2.lhs_baddr 
			OR targ.lhs_taddr = t2.rhs_baddr
			);


	/* Node Association logic:

			Sometimes (actually, quite frequently) the dm_os_tasks/dm_os_waiting_tasks output does not identify every consumer or 
			producer for each node. Consider the following example, quite common in real dm_os_tasks/dm_os_waiting_tasks data:
				(ex. data 1)
				<at DOP 4>

				T0			GetRow --> Node 3     Ecid 1
				Ecid 1		GetRow --> Node 7		Ecid <whatever>
				Ecid 2		GetRow --> Node 7		Ecid <whatever>
				Ecid 3		GetRow --> Node 7		Ecid <whatever>
				Ecid 4		GetRow --> Node 7		Ecid <whatever>

			Thread 0 is (obviously) the only consumer for Node 3. However, we know that ECIDs 2 through 4 are producers for Node 3
			even though only ECID 1 is directly tied to Node 3 in the data. We know this because ECIDs 2 through 4 must be in the
			same parallel zone as ECID 1 if they are GetRow-waiting on the same node as ECID 1.

			Because it is "implied" that ECIDs 2 through 4 are producers for node 3, we can make that implication explicit by
			adding in extra rows to our #lhs_rhs table where ECIDs 2 through 4 are on the RHS and Thread 0 is on the LHS.
				(A question arises as to what the relationship should be between Thread 0 and ECIDs 2-4. Should we make T0 GetRow
				blocked by 2-4? Some other wait-type, maybe even an artificial one like "implied"? At this point, I'm 
				undecided, and for now have just implemented the code such that Thread 0 will have the same wait type for ECIDs 2-4
				as it does for ECID 1 (i.e. "GetRow").)

			This is a safe operation ("safe" meaning that we won't accidentally assign a task to a node that it is not a consumer or a
			producer for), but it must be chosen explicitly by the user through the @NodeAssociation parameter
			for 2 reasons: 
				- #lhs_rhs must be scanned twice, joined, and the resulting qualifying rows must be inserted into #lhs_rhs.
				All of this can add overhead, especially for queries with many parallel zones at a higher DOP. (Or when we 
				are not restricting by spid).

				- Even though it is correct to say that ECIDs 2-4 are producers for Node 3, it is not 100% correct to say that
				Thread 0 is actually BLOCKED by them. It is possible that the Exchange row/packet distribution algorithm may be
				such that Thread 0 really is waiting specifically for rows from ECID 1, and not from the other ECIDs. Or, it 
				could be that this wait information is a "best effort" approach by MSFT (perhaps preseting always-complete data
				would be computationally too expensive) and we are not guaranteed to see everything that is relevant. Either way,
				the user may at times want to see implied information, and other times may not want to.
					(I'm punching above my weight here... I don't know enough about Exchange internals to point out some
					useful examples of when you might not want Node Association here. Perhaps in the future I'll add 
					more notes here).

			Node Association has limitations. Let's consider a twist on (ex. data 1), by changing the Node # for one of the child rows:
				(ex. data 2)
				<at DOP 4>

				T0			GetRow --> Node 3     Ecid 1
				Ecid 1		GetRow --> Node 7		Ecid <whatever>
				Ecid 2		GetRow --> Node 7		Ecid <whatever>
				Ecid 3		GetRow --> Node 7		Ecid <whatever>
				Ecid 4		GetRow --> Node 11		Ecid <whatever>

			ECID 4 is waiting on Node 11, instead of Node 7. Now, we might make an educated guess that ECID 4 is in
			the same parallel zone as ECIDs 1 thru 3 (since at DOP 4 it is common to see ECIDs 1-4 in the same zone);
			this sort of data pattern is very feasible for plans that have a parallel zone with a Parallel Merge Join. 
		 
			We might imagine a plan shape like this:

				SELECT<--------Gather Streams<-----(Parallel)Merge Join<-------Repartition Streams (Node 3)
														|
														|
														|
														|<-----------------Repartition Streams (Node 7)

			Thread 0 runs the section to the left of the Gather Streams, of course, and ECIDs 1-4 could all be executing
			the middle parallel zone (that contains the Merge Join), with 3 of the ECIDs waiting for rows on the 
			upper-right Repartition Streams, and 1 ECID waiting for rows on the lower-right Repartition Streams.

			However, this proc does not take the plan shape into account, and thus has no way of knowing that ECID 4 is in the
			same parallel zone as 1-3. The above data could just as easily represent a case where ECIDs 1-3 were in one zone
			and ECID 4 was in a different zone. Therefore, in this case our Node Association logic would only associate 
			ECIDs 2 and 3 as producers of Node 3.


			Consider this next pattern, also seen frequently:

				Let's say that Thread 0 is GetRow waiting on 4 ECIDs, but 3 of those ECIDs are running and thus don't show
				which node(s) they consume from:
				(ex. data 2)

					T0		GetRow	--> Node 3		ECID 1
					T0		GetRow  --> Node 3		ECID 2
					T0		GetRow  --> Node 3		ECID 3
					T0		GetRow  --> Node 3		ECID 4
					ECID 1	<running>
					ECID 2  <running>
					ECID 3	GetRow  --> Node 7		ECID <whatever>
					ECID 4  <running>

				We know that ECIDs 1-4 are all producers at Node 3. Thus, they are all in the same parallel zone. This means that 
				they are all consumers of Node 7 (they may consume from other nodes as well, of course).


			Here's another real-world data pattern:
			(ex. data 3)

				T0		GetRow -->4		ECID 1
				T0		GetRow -->4		ECID 2
				T0		GetRow -->4		ECID 3
				T0		GetRow -->4		ECID 4

				ECID 4		12<-- Newrow	ECID 6

				Again, we know that since ECIDs 1, 2, and 3 are in the same zone as ECID 4, we can tie them as consumers
				to Node 12. We have to be careful, however, if ECIDs 1-3 are in some sort of consumer CX wait already (e.g. GetRow).
				If they ARE in such a consumer wait, and aren't already tied to Node 12, then they are tied to a different node.
				This is where differentiating between the "Focus Node" and "Blocking Node" comes in handy

		*/


		IF @NodeAssociation = N'Y'
		BEGIN
			INSERT INTO #lhs_rhs (
				lhs_spid,
				lhs_rqst,
				--lhs_ecid,
				--lhs_wspid,
				--lhs_taddr,
				--lhs_paddr,
				--lhs_cxs,
				--lhs_wait_type,
				--lhs_waitisincluded,
				--lhs_cxwaitdirection,
				--lhs_ismaintwait,
				--lhs_isnoncxp,
				--lhs_wait_node,
				--lhs_wait_ms,
				--lhs_baddr,
				--lhs_bspid,
				--lhs_becid,
				--lhs_rdesc,

				focus_node,

				rhs_spid,
				rhs_rqst,
				rhs_ecid,
				rhs_wspid,
				rhs_taddr,
				rhs_paddr,
				rhs_cxs,
				rhs_wait_type,
				rhs_waitisincluded,
				rhs_cxwaitdirection,
				rhs_ismaintwait,
				rhs_isnoncxp,
				rhs_wait_node,
				rhs_wait_ms,
				rhs_baddr,
				rhs_bspid,
				rhs_becid,
				rhs_rdesc
			)
			SELECT DISTINCT
				isProducer.lhs_spid, 
				isProducer.lhs_rqst, 

				isProducer.focus_node,

				findOtherConsumers_makeProducers.lhs_spid,
				findOtherConsumers_makeProducers.lhs_rqst,
				findOtherConsumers_makeProducers.lhs_ecid,
				findOtherConsumers_makeProducers.lhs_wspid,
				findOtherConsumers_makeProducers.lhs_taddr,
				findOtherConsumers_makeProducers.lhs_paddr,
				findOtherConsumers_makeProducers.lhs_cxs,
				findOtherConsumers_makeProducers.lhs_wait_type,
				findOtherConsumers_makeProducers.lhs_waitisincluded,
				findOtherConsumers_makeProducers.lhs_cxwaitdirection,
				findOtherConsumers_makeProducers.lhs_ismaintwait,
				findOtherConsumers_makeProducers.lhs_isnoncxp,
				findOtherConsumers_makeProducers.lhs_wait_node,
				findOtherConsumers_makeProducers.lhs_wait_ms,
				findOtherConsumers_makeProducers.lhs_baddr,
				findOtherConsumers_makeProducers.lhs_bspid,
				findOtherConsumers_makeProducers.lhs_becid,
				findOtherConsumers_makeProducers.lhs_rdesc
			FROM #lhs_rhs isProducer
				INNER JOIN #lhs_rhs isConsumer
					ON isProducer.lhs_spid = isConsumer.lhs_spid 
					AND isProducer.lhs_rqst = isConsumer.lhs_rqst 

					AND isProducer.rhs_taddr = isConsumer.lhs_taddr	--find tasks that are producers on one side and consumers on another
					AND isConsumer.focus_node <> 9999		--should be redundant since we shouldn't have 9999 rows for a taddr that has a rhs_taddr match in another row
					AND isProducer.focus_node <> isConsumer.focus_node		--this clause should be redundant/unnecessary unless I have bugs in my code


				INNER JOIN #lhs_rhs findOtherConsumers_makeProducers						--find consumer tasks that can be a producer on a different node
					ON isConsumer.lhs_spid = findOtherConsumers_makeProducers.lhs_spid	--could use either isConsumer or isProducer here for these 2 join cols
					AND isConsumer.lhs_rqst = findOtherConsumers_makeProducers.lhs_rqst 

					AND isConsumer.focus_node = findOtherConsumers_makeProducers.focus_node
					AND isConsumer.lhs_taddr <> findOtherConsumers_makeProducers.lhs_taddr
			;

			INSERT INTO #lhs_rhs (
				lhs_spid,
				lhs_rqst,
				lhs_ecid,
				lhs_wspid,
				lhs_taddr,
				lhs_paddr,
				lhs_cxs,
				lhs_wait_type,
				lhs_waitisincluded,
				lhs_cxwaitdirection,
				lhs_ismaintwait,
				lhs_isnoncxp,
				lhs_wait_node,
				lhs_wait_ms,
				lhs_baddr,
				lhs_bspid,
				lhs_becid,
				lhs_rdesc,

				focus_node

				--rhs_spid,
				--rhs_rqst,
				--rhs_ecid,
				--rhs_wspid,
				--rhs_taddr,
				--rhs_paddr,
				--rhs_cxs,
				--rhs_wait_type,
				--rhs_waitisincluded,
				--rhs_cxwaitdirection,
				--rhs_ismaintwait,
				--rhs_isnoncxp,
				--rhs_wait_node,
				--rhs_wait_ms,
				--rhs_baddr,
				--rhs_bspid,
				--rhs_becid,
				--rhs_rdesc
			)
			SELECT 
				DISTINCT
				isConsumer.lhs_spid,
				isConsumer.lhs_rqst,
				findOtherProducers_makeConsumers.rhs_ecid,
				findOtherProducers_makeConsumers.rhs_wspid,
				findOtherProducers_makeConsumers.rhs_taddr,
				findOtherProducers_makeConsumers.rhs_paddr,
				findOtherProducers_makeConsumers.rhs_cxs,
				findOtherProducers_makeConsumers.rhs_wait_type,
				findOtherProducers_makeConsumers.rhs_waitisincluded,
				findOtherProducers_makeConsumers.rhs_cxwaitdirection,
				findOtherProducers_makeConsumers.rhs_ismaintwait,
				findOtherProducers_makeConsumers.rhs_isnoncxp,
				findOtherProducers_makeConsumers.rhs_wait_node,
				findOtherProducers_makeConsumers.rhs_wait_ms,
				findOtherProducers_makeConsumers.rhs_baddr,
				findOtherProducers_makeConsumers.rhs_bspid,
				findOtherProducers_makeConsumers.rhs_becid,
				findOtherProducers_makeConsumers.rhs_rdesc,
			
				isConsumer.focus_node
			FROM #lhs_rhs isProducer
				INNER JOIN #lhs_rhs isConsumer
					ON isProducer.lhs_spid = isConsumer.lhs_spid 
					AND isProducer.lhs_rqst = isConsumer.lhs_rqst 

					AND isProducer.rhs_taddr = isConsumer.lhs_taddr	--find tasks that are producers on one side and consumers on another
					AND isConsumer.focus_node <> 9999		--should be redundant since we shouldn't have 9999 rows for a taddr that has a rhs_taddr match in another row
					AND isProducer.focus_node <> isConsumer.focus_node		--this clause should be redundant/unnecessary unless I have bugs in my code

				INNER JOIN #lhs_rhs findOtherProducers_makeConsumers
					ON isProducer.lhs_spid = findOtherProducers_makeConsumers.lhs_spid
					AND isProducer.lhs_rqst = findOtherProducers_makeConsumers.lhs_rqst 

					AND isProducer.focus_node = findOtherProducers_makeConsumers.focus_node
					AND isProducer.rhs_taddr <> findOtherProducers_makeConsumers.rhs_taddr
			;
		END		--IF @NodeAssociation = N'Y'

		/* Desired result formatting for the parallel waits data

			ConsWaits		ConsPWaits		Node	ProdPWaits		ProdWaits		ConsCXS						ProdCXS
			-----------------------------------------------------------------------------------------------------------------------------
			{2}  2x*Run*	{1}  4xGetRow	->3->	{4} 12xGetRow					98,114	(37%)  [155%]		47,545	(55%)	[19%]

			At this point we already have:
				- Sum of CXS for all tasks		(calculated up above from the #TAW data)

			We need to calculate:
				- Sum of CXS per node  (well, for each side)
					and Max, and Min, to calculate the diff
				- Number of tasks in non-CXP waits per node		(for each side)
				- Number of tasks in CXP waits per node

				- Number of tasks per wait type per node
				-		average duration
				-		(let's NOT order by duration, let's order by alphabet)

				
		*/

		CREATE TABLE #NodeStats (
			lhs_mincxs BIGINT,				--task with minimum # of CXS for the node, on the LHS
			lhs_maxcxs BIGINT,				--task with max # of CXS for node
			lhs_sumcxs BIGINT,				--Sum of CXS for tasks on the LHS of this node
			lhs_numwithcxp INT,				-- number of tasks that have a CXP wait on this node (LHS)
			lhs_numwithcons INT,			-- number of tasks that have a consumer CXP wait on the lhs of this node
			lhs_numwithprod INT,			-- number of tasks that have a producer CXP wait on the lhs of this node
			lhs_numwithnoncxp INT,			-- num of tasks that have a non-CXP wait on this node (LHS)
			lhs_cxpwaits NVARCHAR(1000),	--comma-separated list of formatted waits that are CXP waits
			lhs_noncxpwaits NVARCHAR(1000),	--comma-separated list of formatted waits that are non-CXP waits
			focus_node INT,
			rhs_mincxs BIGINT,
			rhs_maxcxs BIGINT,
			rhs_sumcxs BIGINT,
			rhs_numwithcxp INT,
			rhs_numwithcons INT,			-- number of tasks that have a consumer CXP wait on the rhs of this node
			rhs_numwithprod INT,			-- number of tasks that have a producer CXP wait on the rhs of this node
			rhs_numwithnoncxp INT,
			rhs_cxpwaits NVARCHAR(1000),
			rhs_noncxpwaits NVARCHAR(1000)
		);


		;WITH lhs_CXSPerNode AS (
			SELECT 
				focus_node,
				mincxs = MIN(lhs_cxs),
				maxcxs = MAX(lhs_cxs), 
				sumcxs = SUM(lhs_cxs)
			FROM (
				SELECT 
					lhs_taddr,
					focus_node,
					lhs_cxs = MAX(lhs_cxs)
				FROM #lhs_rhs t
				GROUP BY lhs_taddr, focus_node
			) dedup
			GROUP BY focus_node 
		),
		rhs_CXSPerNode AS (
			SELECT 
				focus_node,
				mincxs = MIN(rhs_cxs),
				maxcxs = MAX(rhs_cxs), 
				sumcxs = SUM(rhs_cxs)
			FROM (
				SELECT 
					rhs_taddr,
					focus_node,
					rhs_cxs = MAX(rhs_cxs)
				FROM #lhs_rhs t
				GROUP BY rhs_taddr, focus_node
			) dedup
			GROUP BY focus_node 
		), 
		lhs_TasksInWaitCatPerNode AS (
			SELECT 
				focus_node, 
				numwithcxp = SUM(iscxp),
				numwithnoncxp = SUM(isnoncxp),
				numwithcons = SUM(isconswait),
				numwithprod = SUM(isprodwait)
			FROM (
				SELECT DISTINCT 
					focus_node, 
					lhs_taddr, 
					iscxp = CASE WHEN lhs_cxwaitdirection > 0 THEN 1 ELSE 0 END,
					isnoncxp = CASE WHEN lhs_cxwaitdirection = 0 THEN 1 ELSE 0 END,
					isconswait = CASE WHEN lhs_cxwaitdirection = 1 THEN 1 ELSE 0 END,
					isprodwait = CASE WHEN lhs_cxwaitdirection = 2 THEN 1 ELSE 0 END
				FROM #lhs_rhs t
			) dedup
			GROUP BY focus_node 
		),
		rhs_TasksInWaitCatPerNode AS (
			SELECT 
				focus_node, 
				numwithcxp = SUM(iscxp),
				numwithnoncxp = SUM(isnoncxp),
				numwithcons = SUM(isconswait),
				numwithprod = SUM(isprodwait)
			FROM (
				SELECT DISTINCT 
					focus_node, 
					rhs_taddr, 
					iscxp = CASE WHEN rhs_cxwaitdirection > 0 THEN 1 ELSE 0 END,
					isnoncxp = CASE WHEN rhs_cxwaitdirection = 0 THEN 1 ELSE 0 END,
					isconswait = CASE WHEN rhs_cxwaitdirection = 1 THEN 1 ELSE 0 END,
					isprodwait = CASE WHEN rhs_cxwaitdirection = 2 THEN 1 ELSE 0 END
				FROM #lhs_rhs t
			) dedup
			GROUP BY focus_node 
		)
		INSERT INTO #NodeStats (
			lhs_mincxs,
			lhs_maxcxs,
			lhs_sumcxs,
			lhs_numwithcxp,
			lhs_numwithcons,
			lhs_numwithprod,
			lhs_numwithnoncxp,

			focus_node,

			rhs_mincxs,
			rhs_maxcxs,
			rhs_sumcxs,
			rhs_numwithcxp,
			rhs_numwithcons,
			rhs_numwithprod,
			rhs_numwithnoncxp
		)
		SELECT 
			lcxs.mincxs,
			lcxs.maxcxs,
			lcxs.sumcxs,
			lt.numwithcxp,
			lt.numwithcons,
			lt.numwithprod,
			lt.numwithnoncxp,

			lcxs.focus_node,

			rcxs.mincxs,
			rcxs.maxcxs,
			rcxs.sumcxs,
			rt.numwithcxp,
			rt.numwithcons,
			rt.numwithprod,
			rt.numwithnoncxp
		FROM lhs_CXSPerNode lcxs
			INNER JOIN rhs_CXSPerNode rcxs
				ON lcxs.focus_node = rcxs.focus_node
			INNER JOIN lhs_TasksInWaitCatPerNode lt
				ON lcxs.focus_node = lt.focus_node
			INNER JOIN rhs_TasksInWaitCatPerNode rt
				ON lcxs.focus_node = rt.focus_node
		;


		--Now update the formatted fields
		UPDATE targ 
		SET lhs_cxpwaits = ISNULL(lhs_cxp.wait_info,N''),
			lhs_noncxpwaits = ISNULL(lhs_noncxp.wait_info,N''),
			rhs_cxpwaits = ISNULL(rhs_cxp.wait_info,N''),
			rhs_noncxpwaits = ISNULL(rhs_noncxp.wait_info,N'')
		FROM #NodeStats targ
			LEFT OUTER JOIN  (
				SELECT
					[nd] = task_nodes.task_node.value('(nd/text())[1]', 'SMALLINT'),
					task_nodes.task_node.value('(wait_formatted/text())[1]', 'NVARCHAR(4000)') AS wait_info
				FROM (
						SELECT
							CONVERT(XML,
								REPLACE
								(
									CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
									N'</wait_formatted></tasks><tasks><wait_formatted>',
									N', '
									+ 
								--LEFT(CRYPT_GEN_RANDOM(1), 0)
								LEFT(CONVERT(VARCHAR(40),NEWID()),0)

								--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
								-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
								)
							) AS task_xml 
						FROM (
							SELECT
								nd = CASE WHEN rn = 1 THEN nd ELSE NULL END, 
								wait_formatted = 
										CASE WHEN numrowsforwait > 1 THEN CONVERT(VARCHAR(20), numrowsforwait) + 'x' 
											ELSE '' END + 
										CASE WHEN wait_type = '*Run*' THEN '' 
											ELSE CONVERT(VARCHAR(20), avgwait) + 'ms:'
											END + 
										wait_type
								--TODO: consider whether to bring min/max/sum wait further through the code (still deciding what the final output should
								-- be and what options the user should have to customize
							FROM (
								SELECT 
									nd,
									wait_type,
									numrowsforwait,
									avgwait,
									minwait,
									maxwait,
									sumwait,
									rn = ROW_NUMBER() OVER (PARTITION BY nd 
																ORDER BY sumwait DESC)
								FROM (
									SELECT 
										nd,
										wait_type, 
										numrowsforwait = COUNT(*),
										avgwait = AVG(wait_ms),
										minwait = MIN(wait_ms),
										maxwait = MAX(wait_ms),
										sumwait = SUM(wait_ms)
									FROM (
										SELECT DISTINCT	--again, need dup-handling
											nd = focus_node, 
											taddr = lhs_taddr,	--just for DISTINCT calc, not needed beyond this
											baddr = lhs_baddr,	--ditto
											wait_type = lhs_wait_type,
											wait_ms = lhs_wait_ms
										FROM #lhs_rhs l 
										WHERE l.lhs_cxwaitdirection > 0
										) lhs_wait_attrib_base
									GROUP BY nd, wait_type 
								) lhs_wait_attrib_withagg
							) xmlprep
							ORDER BY xmlprep.nd 
							FOR XML PATH(N'tasks')
						) AS tasks_raw (task_xml_raw)
					) as tasks_final
					CROSS APPLY tasks_final.task_xml.nodes(N'/tasks') AS task_nodes (task_node)
					WHERE task_nodes.task_node.exist(N'nd') = 1
			) lhs_cxp
				ON lhs_cxp.nd = targ.focus_node
			LEFT OUTER JOIN (
				SELECT
					[nd] = task_nodes.task_node.value('(nd/text())[1]', 'SMALLINT'),
					task_nodes.task_node.value('(wait_formatted/text())[1]', 'NVARCHAR(4000)') AS wait_info
				FROM (
						SELECT
							CONVERT(XML,
								REPLACE
								(
									CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
									N'</wait_formatted></tasks><tasks><wait_formatted>',
									N', '
									+ 
								--LEFT(CRYPT_GEN_RANDOM(1), 0)
								LEFT(CONVERT(VARCHAR(40),NEWID()),0)

								--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
								-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
								)
							) AS task_xml 
						FROM (
							SELECT
								nd = CASE WHEN rn = 1 THEN nd ELSE NULL END, 
								wait_formatted = 
										CASE WHEN numrowsforwait > 1 THEN CONVERT(VARCHAR(20), numrowsforwait) + 'x' 
											ELSE '' END + 
										CASE WHEN wait_type = '*Run*' THEN '' 
											ELSE CONVERT(VARCHAR(20), avgwait) + 'ms:'
											END + 
										wait_type
								--TODO: consider whether to bring min/max/sum wait further through the code (still deciding what the final output should
								-- be and what options the user should have to customize
							FROM (
								SELECT 
									nd,
									wait_type,
									numrowsforwait,
									avgwait,
									minwait,
									maxwait,
									sumwait,
									rn = ROW_NUMBER() OVER (PARTITION BY nd 
																ORDER BY sumwait DESC)
								FROM (
									SELECT 
										nd,
										wait_type, 
										numrowsforwait = COUNT(*),
										avgwait = AVG(wait_ms),
										minwait = MIN(wait_ms),
										maxwait = MAX(wait_ms),
										sumwait = SUM(wait_ms)
									FROM (
										SELECT DISTINCT	--again, need dup-handling
											nd = focus_node, 
											taddr = lhs_taddr,	--just for DISTINCT calc, not needed beyond this
											baddr = lhs_baddr,	--ditto
											wait_type = lhs_wait_type,
											wait_ms = lhs_wait_ms
										FROM #lhs_rhs l 
										WHERE l.lhs_cxwaitdirection = 0
										) lhs_wait_attrib_base
									GROUP BY nd, wait_type 
								) lhs_wait_attrib_withagg
							) xmlprep
							ORDER BY xmlprep.nd 
							FOR XML PATH(N'tasks')
						) AS tasks_raw (task_xml_raw)
					) as tasks_final
					CROSS APPLY tasks_final.task_xml.nodes(N'/tasks') AS task_nodes (task_node)
					WHERE task_nodes.task_node.exist(N'nd') = 1
			) lhs_noncxp
				ON lhs_noncxp.nd = targ.focus_node
			LEFT OUTER JOIN  (
				SELECT
					[nd] = task_nodes.task_node.value('(nd/text())[1]', 'SMALLINT'),
					task_nodes.task_node.value('(wait_formatted/text())[1]', 'NVARCHAR(4000)') AS wait_info
				FROM (
						SELECT
							CONVERT(XML,
								REPLACE
								(
									CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
									N'</wait_formatted></tasks><tasks><wait_formatted>',
									N', '
									+ 
								--LEFT(CRYPT_GEN_RANDOM(1), 0)
								LEFT(CONVERT(VARCHAR(40),NEWID()),0)

								--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
								-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
								)
							) AS task_xml 
						FROM (
							SELECT
								nd = CASE WHEN rn = 1 THEN nd ELSE NULL END, 
								wait_formatted = 
										CASE WHEN numrowsforwait > 1 THEN CONVERT(VARCHAR(20), numrowsforwait) + 'x'
											ELSE '' END +
										CASE WHEN wait_type = '*Run*' THEN '' 
											ELSE CONVERT(VARCHAR(20), avgwait) + 'ms:'
											END + 
										wait_type
								--wait_type  
							FROM (
								SELECT 
									nd,
									wait_type,
									numrowsforwait,
									avgwait,
									minwait,
									maxwait,
									sumwait,
									rn = ROW_NUMBER() OVER (PARTITION BY nd 
																ORDER BY sumwait DESC)
								FROM (
									SELECT 
										nd,
										wait_type, 
										numrowsforwait = COUNT(*),
										avgwait = AVG(wait_ms),
										minwait = MIN(wait_ms),
										maxwait = MAX(wait_ms),
										sumwait = SUM(wait_ms)
									FROM (
										SELECT DISTINCT  --remember, for rhs we use lhs ID columns
											nd = focus_node, 
											taddr = rhs_taddr,	--just for DISTINCT calc, not needed beyond this
											baddr = rhs_baddr,	--ditto
											wait_type = rhs_wait_type,
											wait_ms = rhs_wait_ms
										FROM #lhs_rhs rhs
										WHERE rhs.rhs_cxwaitdirection > 0
										) rhs_wait_attrib_base
									GROUP BY nd, wait_type 
								) rhs_wait_attrib_withagg
							) xmlprep
							ORDER BY xmlprep.nd 
							FOR XML PATH(N'tasks')
						) AS tasks_raw (task_xml_raw)
					) as tasks_final
					CROSS APPLY tasks_final.task_xml.nodes(N'/tasks') AS task_nodes (task_node)
					WHERE task_nodes.task_node.exist(N'nd') = 1
			) rhs_cxp
				ON rhs_cxp.nd = targ.focus_node
			LEFT OUTER JOIN  (
				SELECT
					[nd] = task_nodes.task_node.value('(nd/text())[1]', 'SMALLINT'),
					task_nodes.task_node.value('(wait_formatted/text())[1]', 'NVARCHAR(4000)') AS wait_info
				FROM (
						SELECT
							CONVERT(XML,
								REPLACE
								(
									CONVERT(NVARCHAR(MAX), tasks_raw.task_xml_raw) COLLATE Latin1_General_Bin2,
									N'</wait_formatted></tasks><tasks><wait_formatted>',
									N', '
									+ 
								--LEFT(CRYPT_GEN_RANDOM(1), 0)
								LEFT(CONVERT(VARCHAR(40),NEWID()),0)

								--This statement sometimes runs very slow, so we are using a "side effecting function" to avoid the default Expression Service caching behavior,
								-- per the Paul White blog article here: http://sqlblog.com/blogs/paul_white/archive/2012/09/05/compute-scalars-expressions-and-execution-plan-performance.aspx 
								)
							) AS task_xml 
						FROM (
							SELECT
								nd = CASE WHEN rn = 1 THEN nd ELSE NULL END, 
								wait_formatted = 
										CASE WHEN numrowsforwait > 1 THEN CONVERT(VARCHAR(20), numrowsforwait) + 'x'
											ELSE '' END +
										CASE WHEN wait_type = '*Run*' THEN '' 
											ELSE CONVERT(VARCHAR(20), avgwait) + 'ms:'
											END + 
										wait_type
								--wait_type  
							FROM (
								SELECT 
									nd,
									wait_type,
									numrowsforwait,
									avgwait,
									minwait,
									maxwait,
									sumwait,
									rn = ROW_NUMBER() OVER (PARTITION BY nd 
																ORDER BY sumwait DESC)
								FROM (
									SELECT 
										nd,
										wait_type, 
										numrowsforwait = COUNT(*),
										avgwait = AVG(wait_ms),
										minwait = MIN(wait_ms),
										maxwait = MAX(wait_ms),
										sumwait = SUM(wait_ms)
									FROM (
										SELECT DISTINCT  --remember, for rhs we use lhs ID columns
											nd = focus_node, 
											taddr = rhs_taddr,	--just for DISTINCT calc, not needed beyond this
											baddr = rhs_baddr,	--ditto
											wait_type = rhs_wait_type,
											wait_ms = rhs_wait_ms
										FROM #lhs_rhs rhs
										WHERE rhs.rhs_cxwaitdirection = 0
										) rhs_wait_attrib_base
									GROUP BY nd, wait_type 
								) rhs_wait_attrib_withagg
							) xmlprep
							ORDER BY xmlprep.nd 
							FOR XML PATH(N'tasks')
						) AS tasks_raw (task_xml_raw)
					) as tasks_final
					CROSS APPLY tasks_final.task_xml.nodes(N'/tasks') AS task_nodes (task_node)
					WHERE task_nodes.task_node.exist(N'nd') = 1
			) rhs_noncxp
				ON rhs_noncxp.nd = targ.focus_node
		;

		/*
SELECT 
									nd,
									wait_type,
									numrowsforwait,
									avgwait,
									minwait,
									maxwait,
									sumwait,
									rn = ROW_NUMBER() OVER (PARTITION BY nd 
																ORDER BY sumwait DESC)
								FROM (
									SELECT 
										nd,
										wait_type, 
										numrowsforwait = COUNT(*),
										avgwait = AVG(wait_ms),
										minwait = MIN(wait_ms),
										maxwait = MAX(wait_ms),
										sumwait = SUM(wait_ms)
									FROM (
										SELECT DISTINCT  --remember, for rhs we use lhs ID columns
											nd = focus_node, 
											taddr = rhs_taddr,	--just for DISTINCT calc, not needed beyond this
											baddr = rhs_baddr,	--ditto
											wait_type = rhs_wait_type,
											wait_ms = rhs_wait_ms
										FROM #lhs_rhs rhs
										WHERE rhs.rhs_cxwaitdirection = 0
										) rhs_wait_attrib_base
									GROUP BY nd, wait_type 
								) rhs_wait_attrib_withagg

		SELECT * 
		FROM #NodeStats;
		*/

		/*
		SELECT
			[spid] = CONVERT(varchar(20),spid) + CASE WHEN rqst = 0 THEN '' ELSE ':' + CONVERT(varchar(20), rqst) END, 
			--Cons_#tasks = l_numtasks, 
			ConsCxSwitch = 
					CASE WHEN l_numtasks = 1 THEN CONVERT(varchar(20),l_sumcxs)
						WHEN l_numtasks = 2 
							THEN CONVERT(varchar(20),l_sumcxs) + ' = ' + 
								CONVERT(varchar(20), l_mincxs) + ' / ' + 
								CONVERT(varchar(20), l_maxcxs) + 
								(CASE WHEN l_mincxs <> 0 AND nd <> 9999
									THEN '  (' + CONVERT(varchar(20),CONVERT(DECIMAL(7,0),((1.0*(l_maxcxs-l_mincxs))/(1.0*l_mincxs)*100.))) + '%)'
									ELSE ''
									END 
								)
						ELSE CONVERT(varchar(20),l_sumcxs) + ' = ' + 
							CONVERT(varchar(20), l_mincxs) + ' / ' + 
							CONVERT(varchar(20), l_avgcxs) + ' / ' +
							CONVERT(varchar(20), l_maxcxs) + 
								(CASE WHEN l_mincxs <> 0 AND nd <> 9999
									THEN '  (' + CONVERT(varchar(20),CONVERT(DECIMAL(7,0),((1.0*(l_maxcxs-l_mincxs))/(1.0*l_mincxs)*100.))) + '%)'
									ELSE ''
									END 
								)
						END,
			ConsWaits = '{' + CONVERT(varchar(20), l_numtasks) + '}    ' + l_wait_info,

			[NodeId] = CASE 
							WHEN (l_hasmaintwait > 0 OR r_hasmaintwait > 0) THEN 'N/A' 
							WHEN nd = -1 THEN ' Thread 0 '
						ELSE ( CASE WHEN l_cxsdiscrepancy = 1 AND nd <> 9999 THEN '! ' ELSE '  ' END +
					
								CASE WHEN l_hasnoncxp > 0 THEN '*' ELSE ' ' END +
					
								CASE WHEN l_cxindicator = 0.0 THEN '    '
									WHEN l_cxindicator = 1.0 THEN ' -->' 
									WHEN l_cxindicator = 2.0 THEN ' <--' 
									ELSE ' <->' 
								END + 

								' ' + ISNULL(CONVERT(varchar(20),NULLIF(nd,9999)),'?') + ' ' + 

								CASE WHEN ISNULL(r_cxindicator,0.0) = 0.0 THEN '    '
								WHEN r_cxindicator = 1.0 THEN '--> ' 
								WHEN r_cxindicator = 2.0 THEN '<-- ' 
								ELSE '<-> '
								END + 

							CASE WHEN r_hasnoncxp > 0 THEN '*' ELSE ' ' END + 

							CASE WHEN r_cxsdiscrepancy = 1 THEN ' !' ELSE '  ' END
							)
						END, 

			ProdWaits = ISNULL('{' + CONVERT(varchar(20), ISNULL(r_numtasks,0)) + '}    ' + r_wait_info,''),
			ProdCxSwitch = ISNULL(
					CASE WHEN r_numtasks = 1 THEN CONVERT(varchar(20),r_sumcxs)
						WHEN r_numtasks = 2 
							THEN CONVERT(varchar(20),r_sumcxs) + ' = ' + 
								CONVERT(varchar(20), r_mincxs) + ' / ' + 
								CONVERT(varchar(20), r_maxcxs) +
								(CASE WHEN r_mincxs <> 0 AND nd <> 9999
									THEN '  (' + CONVERT(varchar(20),CONVERT(DECIMAL(7,0),((1.0*(r_maxcxs-r_mincxs))/(1.0*r_mincxs)*100.))) + '%)'
									ELSE ''
									END 
								)
						ELSE CONVERT(varchar(20),r_sumcxs) + ' = ' + 
							CONVERT(varchar(20), r_mincxs) + ' / ' + 
							CONVERT(varchar(20), r_avgcxs) + ' / ' +
							CONVERT(varchar(20), r_maxcxs) +
							(CASE WHEN r_mincxs <> 0 AND nd <> 9999
									THEN '  (' + CONVERT(varchar(20),CONVERT(DECIMAL(7,0),((1.0*(r_maxcxs-r_mincxs))/(1.0*r_mincxs)*100.))) + '%)'
									ELSE ''
									END 
								)
						END,'')
		FROM (
			SELECT 
				l.spid, 
				l.rqst,
				l_numtasks = l.numtasks,
				l_avgcxs = l.avgcxs,
				l_mincxs = l.mincxs,
				l_maxcxs = l.maxcxs,
				l_sumcxs = l.sumcxs,
				l_cxsdiscrepancy = (
					CASE WHEN (l.maxcxs - l.mincxs) >= l.mincxs AND (l.maxcxs - l.mincxs) >= 500 THEN 1 
						ELSE 0
					END
					),
				l_wait_info = l.wait_info,
				l_cxindicator = CASE WHEN l.cxcounter = 0 THEN CONVERT(DECIMAL(7,1),0)
								ELSE CONVERT(DECIMAL(7,1),l.sumcxwaitdirection) / CONVERT(DECIMAL(7,1),l.cxcounter)
								END,
				l_hasmaintwait = l.hasmaintwait,
				l_hasnoncxp = l.hasnoncxp,
				l.nd,
				--r.nd,
				r_hasnoncxp = r.hasnoncxp,
				r_hasmaintwait = r.hasmaintwait,
				r_cxindicator = CASE WHEN r.cxcounter = 0 THEN 0
								ELSE CONVERT(DECIMAL(7,1),r.sumcxwaitdirection) / CONVERT(DECIMAL(7,1),r.cxcounter)
								END,
				r_wait_info = r.wait_info,
				r_avgcxs = r.avgcxs,
				r_mincxs = r.mincxs,
				r_maxcxs = r.maxcxs,
				r_sumcxs = r.sumcxs,
				r_cxsdiscrepancy = (
					CASE WHEN (r.maxcxs - r.mincxs) >= r.mincxs AND (r.maxcxs - r.mincxs) >= 500 THEN 1 
						ELSE 0
					END
					),
				r_numtasks = r.numtasks
			FROM #lhs_pre_output l
				LEFT OUTER JOIN #rhs_pre_output r
					ON l.spid = r.spid
					AND l.rqst = r.rqst
					AND l.nd = r.nd 
		) ss
		ORDER BY ss.spid, ss.rqst, ss.nd 
		;
		*/
	END		--massive block: IF @lv__NumTaskAndWaitRows > 1
	ELSE
	BEGIN
		--only 1 task and wait row. Not a parallel query or a maintenance operation with multiple tasks
		--TODO: do the logic for "single-task"
		SET @lv__NumTaskAndWaitRows = @lv__NumTaskAndWaitRows;
	END


	/********************************************************************************************************************

											Final Results

	*********************************************************************************************************************/

	CREATE TABLE #ResultsCore (
		OrderingValue			INT NOT NULL, 
		[CapTime/Label]			NVARCHAR(100) NOT NULL,
		SPID					NVARCHAR(100) NOT NULL,
		CntxtDB					NVARCHAR(100) NOT NULL,
		Duration				NVARCHAR(100) NOT NULL,
		[#Tasks]				NVARCHAR(100) NOT NULL
	);

	
	INSERT INTO #ResultsCore (
		OrderingValue,
		[CapTime/Label],
		SPID,
		CntxtDB,
		Duration,
		[#Tasks]
	)
	SELECT 
		OrderingValue, 
		[CapTime/Label] = SPIDCaptureTime, 
		SPID, 
		CntxtDB, 
		Duration,
		[#Tasks]
	FROM (
		--core/header
		SELECT 
			--TODO: need to add this in somehow: CONVERT(nvarchar(20),@effectiveordinal)
			OrderingValue = 1,
			SPIDCaptureTime = 
								REPLACE(CONVERT(NVARCHAR(20), @hct, 102),'.','-')+' '+
								CONVERT(NVARCHAR(20), @hct, 108)+'.'+
								RIGHT(CONVERT(NVARCHAR(20),N'000')+
								CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @hct)),3),
			SPID = CASE WHEN @calc__is_blocker = 1 AND ISNULL(@calc__blocking_session_id,0) = 0 THEN N'(!)  ' 
								WHEN @calc__is_blocker = 1 AND ISNULL(@calc__blocking_session_id,0) > 0 THEN N'*  '
								ELSE N'' END + 
								CASE WHEN @sess__is_user_process = 0 THEN N's' ELSE N'' END + CONVERT(nvarchar(20),@session_id) + 
								CASE WHEN @request_id > 0 THEN N':' + CONVERT(nvarchar(20),@request_id) ELSE N'' END
						,
			CntxtDB = DB_NAME(@sess__database_id), 
			Duration = (CASE WHEN @calc__duration_seconds IS NULL THEN N'???'
						ELSE (
							CASE WHEN @calc__duration_seconds > 863999 
								THEN CONVERT(nvarchar(20), @calc__duration_seconds / 86400) + N'~' +			--day
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(@calc__duration_seconds % 86400)/3600)
												),1,2)) + N':' +			--hour
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)/60)
												),1,2)) + N':' +			--minute
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)%60)
												),1,2)) 					--second

								WHEN @calc__duration_seconds > 86399
								THEN REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),@calc__duration_seconds / 86400)
												),1,2)) + N'~' +			--day
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(@calc__duration_seconds % 86400)/3600)
												),1,2)) + N':' +			--hour
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)/60)
												),1,2)) + N':' +			--minute
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)%60)
												),1,2)) 			--second

								WHEN @calc__duration_seconds > 59
									THEN REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(@calc__duration_seconds % 86400)/3600)
											),1,2)) + N':' +			--hour
										REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)/60)
											),1,2)) + N':' +			--minute
										REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((@calc__duration_seconds % 86400)%3600)%60)
											),1,2)) 			--second

								ELSE
									CONVERT(nvarchar(20),
										CONVERT(DECIMAL(15,1),@calc__duration_seconds) + 

										CONVERT(DECIMAL(15,1),
											DATEDIFF(millisecond,
														DATEADD(second, 
																@calc__duration_seconds,
																@TimeIdentifier
																), 
														@SPIDCaptureTime
													)/1000.
												)
										)
								END)
					END),
			[#Tasks] = CONVERT(nvarchar(20),@tempdb__CalculatedNumberOfTasks) + 
							CASE WHEN @mgrant__dop IS NULL THEN N'' 
								ELSE N'  (DOP:' + CONVERT(nvarchar(20),@mgrant__dop) + N')' END
		
		
		UNION ALL 

		--status
		SELECT 
			[OrderingValue] = 2,
			[#ResultSets] = CASE WHEN @rqst__open_resultset_count IS NULL OR @rqst__open_resultset_count = 0 THEN N''
								ELSE N'#RsltSets:' + CONVERT(nvarchar(20),@rqst__open_resultset_count) END,
			Blocker = CASE WHEN @calc__blocking_session_id IS NULL OR @calc__blocking_session_id = 0 THEN N''
						ELSE N'Blkr:' + CONVERT(nvarchar(20),@calc__blocking_session_id) END,
			[#Rows] = CASE WHEN @rqst__row_count IS NULL OR @rqst__row_count = 0 THEN N''
						ELSE N'#Rows:' + CONVERT(nvarchar(20), @rqst__row_count) END,
			OpenTran = CASE WHEN @rqst__open_transaction_count IS NULL OR @rqst__open_transaction_count = 0 THEN N''
						ELSE N'#OpenTran:' + CONVERT(nvarchar(20), @rqst__open_transaction_count) END,
			NumaNodes = N'Numa: ' + CASE WHEN @calc__node_info <> N'<placeholder>' THEN @calc__node_info ELSE N'' END

		UNION ALL 

		SELECT 
			[OrderingValue] = 3,
			Col1 = N'',
			Col2 = N'',
			Col3 = N'', 
			Col5 = N'', 
			Label = sar2.ProgressPct + 
					--N'Se' + sar2.ProgressSessStat + 
					N'Rq' + sar2.ProgressRqstStat
		FROM (
			SELECT
			ProgressPct = CASE 
						WHEN @request_id = @lv__nullsmallint OR @session_id <= 0 THEN N''
						WHEN ISNULL(@rqst__percent_complete,0) < 0.001 THEN N'' 
						ELSE N'(' + CONVERT(nvarchar(20), convert(decimal(5,2),@rqst__percent_complete)) + N'%), ' 
					END,
			/*
			ProgressSessStat = CASE @sess__status_code
									WHEN 0 THEN N'[run]'
									WHEN 1 THEN N'[sleep]'
									WHEN 2 THEN N'[dormant]'
									WHEN 3 THEN N'[preconn]'
									ELSE N'[sess status: ?]'
								END,
			*/
			ProgressRqstStat = CASE WHEN @tempdb__CalculatedNumberOfTasks > 1 AND @calc__status_info <> N'<placeholder>'
								THEN N'{' + @calc__status_info + N'} '
							WHEN @rqst__status_code IN (1,4) THEN N''
							ELSE (CASE @rqst__status_code
									WHEN 0 THEN N'{bkgd}'
									WHEN 1 THEN N'{run}'
									WHEN 2 THEN N'{rable}'
									WHEN 3 THEN N'{sleep}'
									WHEN 4 THEN N'{susp}'
									ELSE N'{rqst status: ?}'
								END)
						END
		) sar2

		UNION ALL 

		SELECT 
			[OrderingValue] = 4,
			Label,
			RqstCPU = N'CPU: ' + SUBSTRING(RqstCPU, 1, CHARINDEX(N'.',RqstCPU)-1),
			TotCXS = N'Cxs: ' + SUBSTRING(TotCXS, 1, CHARINDEX(N'.',TotCXS)-1),
			RqstElaps = N'Elap:' + SUBSTRING(RqstElaps, 1, CHARINDEX(N'.',RqstElaps)-1),
			Col2 = N''
		FROM (
			SELECT 
				Label = N'CPU (Sched:' + ISNULL(CONVERT(nvarchar(20),sar.rqst__scheduler_id),N'?') + N')   Rqst',
				RqstCPU = CONVERT(nvarchar(20),CONVERT(money,sar.rqst__cpu_time),1),
				TotCXS = CONVERT(nvarchar(20),CONVERT(money,taw2.total_context_switches),1),
				RqstElaps = CONVERT(nvarchar(20),CONVERT(money,sar.rqst__total_elapsed_time),1)
			FROM (
					SELECT [session_id] = @session_id,
						rqst__scheduler_id = @rqst__scheduler_id,
						rqst__cpu_time = @rqst__cpu_time,
						rqst__total_elapsed_time = @rqst__total_elapsed_time
				) sar
				LEFT OUTER JOIN (
					SELECT session_id,
						total_context_switches = SUM(max_context_switches_count)
					FROM (
						SELECT taw0.session_id,
							taw0.task_address,
							max_context_switches_count = MAX(taw0.context_switches_count)
						FROM #TaskAndWaits taw0
						GROUP BY taw0.session_id, taw0.task_address
					) taw1
					GROUP BY session_id
				) taw2
					ON sar.session_id = taw2.session_id
		) sar3

		UNION ALL 

		SELECT 
			[OrderingValue] = 5,
			Label, 
			SessCPU = N'CPU: ' + SUBSTRING(SessCPU, 1, CHARINDEX(N'.',SessCPU)-1),
			SchedTime = N'SchTm: ' + SUBSTRING(SchedTime, 1, CHARINDEX(N'.',SchedTime)-1),
			SessElaps = N'Elap:' + SUBSTRING(SessElaps, 1, CHARINDEX(N'.',SessElaps)-1),
			Col5
		FROM (
			SELECT 
				Label = N'                             Sess', 
				SessCPU = CONVERT(nvarchar(20),CONVERT(money,@sess__cpu_time),1), 
				SchedTime = CONVERT(nvarchar(20),CONVERT(money,@sess__total_scheduled_time),1),
				SessElaps = CONVERT(nvarchar(20),CONVERT(money,@sess__total_elapsed_time),1),
				Col5 = N''
		) sar4 

		UNION ALL 

		SELECT 
			[OrderingValue] = 6,
			Col1 = N'',
			Col2 = N'',
			Col3 = N'', 
			Col4 = N'', 
			Col5 = N''

		UNION ALL 

		SELECT 
			[OrderingValue] = 7,
			Label, 
			DataReads = N'P: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataReads, 1, CHARINDEX(N'.',DataReads)-1)
										ELSE DataReads END,
			DataLogicalReads = N'L: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataLogicalReads, 1, CHARINDEX(N'.',DataLogicalReads)-1)
										ELSE DataLogicalReads END, 
			DataWrites = N'W: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataWrites, 1, CHARINDEX(N'.',DataWrites)-1)
									ELSE DataWrites END, 
			Col1
		FROM (
			SELECT
				Label = N'Data IO (' + CASE WHEN @units=N'NATIVE' THEN N'pages'
										WHEN @units=N'KB' THEN N'KB'
										WHEN @units=N'MB' THEN N'MB'
										ELSE N'?' 
										END + N')        Rqst',
				DataReads =	CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@rqst__reads),1)
								WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__reads)*8),1)
								WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__reads)*8./1024.),1)
							ELSE N'?'
							END,
				DataLogicalReads = CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@rqst__logical_reads),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__logical_reads)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__logical_reads)*8./1024.),1)
									ELSE N'?'
									END,
				DataWrites = CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@rqst__writes),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__writes)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@rqst__writes)*8./1024.),1)
									ELSE N'?'
									END,
				Col1 = N''
		) sar4

		UNION ALL 

		SELECT 
			[OrderingValue] = 8,
			Label, 
			DataReads = N'P: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataReads, 1, CHARINDEX(N'.',DataReads)-1)
										ELSE DataReads END,
			DataLogicalReads = N'L: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataLogicalReads, 1, CHARINDEX(N'.',DataLogicalReads)-1)
										ELSE DataLogicalReads END, 
			DataWrites = N'W: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(DataWrites, 1, CHARINDEX(N'.',DataWrites)-1)
									ELSE DataWrites END, 
			Col1
		FROM (
			SELECT
				Label = N'                             Sess', 
				DataReads =	CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@sess__reads),1)
								WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__reads)*8),1)
								WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__reads)*8./1024.),1)
							ELSE N'?'
							END,
				DataLogicalReads = CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@sess__logical_reads),1)
								WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__logical_reads)*8),1)
								WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__logical_reads)*8./1024.),1)
							ELSE N'?'
							END,
				DataWrites = CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@sess__writes),1)
								WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__writes)*8),1)
								WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,convert(bigint,@sess__writes)*8./1024.),1)
							ELSE N'?'
							END,
				Col1 = N''
		) sar5

		UNION ALL 

		SELECT 
			[OrderingValue] = 9,
			Col1 = N'',
			Col2 = N'',
			Col3 = N'', 
			Col4 = N'', 
			Col5 = N''

		UNION ALL 

		SELECT 
			[OrderingValue] = 10,
			Label, 
			Rqst = N'Rq: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(Rqst, 1, CHARINDEX(N'.', Rqst)-1)
								ELSE Rqst END,
			[Grant] = N'Gr: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING([Grant], 1, CHARINDEX(N'.', [Grant])-1)
								ELSE [Grant] END,
			RqGrant = N'Orig: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(RqGrant, 1, CHARINDEX(N'.', RqGrant)-1)
								ELSE RqGrant END + CASE WHEN @units = N'NATIVE' THEN N' (pages)' ELSE N'' END,
			WaitSec = (CASE WHEN WaitSec IS NULL THEN N''
						ELSE (
							CASE WHEN WaitSec > 863999 
								THEN CONVERT(nvarchar(20), WaitSec / 86400) + N'~' +			--day
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(WaitSec % 86400)/3600)
												),1,2)) + N':' +			--hour
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)/60)
												),1,2)) + N':' +			--minute
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)%60)
												),1,2)) 					--second

								WHEN WaitSec > 86399
								THEN REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),WaitSec / 86400)
												),1,2)) + N'~' +			--day
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(WaitSec % 86400)/3600)
												),1,2)) + N':' +			--hour
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)/60)
												),1,2)) + N':' +			--minute
											REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)%60)
												),1,2)) 			--second

								WHEN WaitSec > 59
									THEN REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),(WaitSec % 86400)/3600)
											),1,2)) + N':' +			--hour
										REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)/60)
											),1,2)) + N':' +			--minute
										REVERSE(SUBSTRING(REVERSE(N'0' + CONVERT(nvarchar(20),((WaitSec % 86400)%3600)%60)
											),1,2)) 			--second

								ELSE CONVERT(nvarchar(20),WaitSec)
								END)
					END)
		FROM (
			SELECT 
				Label = N'Memory (' + CASE WHEN @units=N'NATIVE' OR @units=N'KB' THEN N'KB'
										WHEN @units=N'MB' THEN N'MB'
										ELSE N'?' 
										END + N')',
				Rqst = CASE WHEN @mgrant__requested_memory_kb IS NULL OR @mgrant__requested_memory_kb = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'KB' OR @units = N'NATIVE' 
								THEN CONVERT(nvarchar(20),CONVERT(money,@mgrant__requested_memory_kb),1)
								ELSE CONVERT(nvarchar(20),CONVERT(money,@mgrant__requested_memory_kb/1024.),1) END
							)
						END,
				[Grant] = CASE WHEN @mgrant__granted_memory_kb IS NULL OR @mgrant__granted_memory_kb = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'KB' OR @units = N'NATIVE' 
								THEN CONVERT(nvarchar(20),CONVERT(money,@mgrant__granted_memory_kb),1)
								ELSE CONVERT(nvarchar(20),CONVERT(money,@mgrant__granted_memory_kb/1024.),1) END
							)
						END,
				[WaitSec] = CASE WHEN @mgrant__grant_time IS NOT NULL THEN NULL
								ELSE DATEDIFF(second, @mgrant__request_time, @hct) END,

				RqGrant = CASE WHEN @rqst__granted_query_memory IS NULL OR @rqst__granted_query_memory = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@rqst__granted_query_memory),1)
								WHEN @units = N'KB'
									THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@rqst__granted_query_memory)*8),1)
								ELSE CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@rqst__granted_query_memory)*8./1024.),1) 
							END
						)
						END 
		) sar6

		UNION ALL 

		SELECT 
			[OrderingValue] = 11,
			Label, 
			UsedMem = N'Us: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(UsedMem, 1, CHARINDEX(N'.', UsedMem)-1)
									ELSE UsedMem END,
			[MaxMem] = N'Mx: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(MaxMem, 1, CHARINDEX(N'.', MaxMem)-1)
									ELSE MaxMem END,
			SeMem = N'SeMem: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(SeMem, 1, CHARINDEX(N'.', SeMem)-1)
									ELSE SeMem END,
			Col2 = N''
		FROM (
			SELECT 
				Label = N'',
				UsedMem = CASE WHEN @mgrant__used_memory_kb IS NULL OR @mgrant__used_memory_kb = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'NATIVE' OR @units = N'KB' 
									THEN CONVERT(nvarchar(20),CONVERT(money,@mgrant__used_memory_kb),1)
								ELSE CONVERT(nvarchar(20),CONVERT(money,@mgrant__used_memory_kb/1024.),1)
								END 
							)
						 END,
				[MaxMem] = CASE WHEN @mgrant__max_used_memory_kb IS NULL OR @mgrant__max_used_memory_kb = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'NATIVE' OR @units = N'KB' 
								THEN CONVERT(nvarchar(20),CONVERT(money,@mgrant__max_used_memory_kb),1)
								ELSE CONVERT(nvarchar(20),CONVERT(money,@mgrant__max_used_memory_kb/1024.),1) 
								END
							)
						END,
				SeMem = CASE WHEN @sess__memory_usage IS NULL OR @sess__memory_usage = 0 THEN N'' 
						ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@sess__memory_usage),1)
								WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@sess__memory_usage)*8),1)
								WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@sess__memory_usage)*8./1024.),1)
								ELSE N'?'
								END 
							)
						END 
		) sar6

		UNION ALL 

		SELECT 
			[OrderingValue] = 12,
			Col1 = N'',
			Col2 = N'',
			Col3 = N'', 
			Col4 = N'', 
			Col5 = N''

		UNION ALL 

		SELECT 
			[OrderingValue] = 13,
			Label1,
			TaUsA = N'UsA: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(TaUsA, 1, CHARINDEX(N'.', TaUsA)-1)
									ELSE TaUsA END,
			TaUsD = N'UsD: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(TaUsD, 1, CHARINDEX(N'.', TaUsD)-1)
									ELSE TaUsD END,
			TaInA = N'InA: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(TaInA, 1, CHARINDEX(N'.', TaInA)-1)
									ELSE TaInA END,
			TaInD = N'InD: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(TaInD, 1, CHARINDEX(N'.', TaInD)-1)
									ELSE TaInD END
		FROM (
			SELECT 
				Label1 = N'TempDB (' + CASE WHEN @units=N'NATIVE' THEN N'pages'
										WHEN @units=N'KB' THEN N'KB'
										WHEN @units=N'MB' THEN N'MB'
										ELSE N'?' 
										END + N')     Task',
				TaUsA = CASE WHEN @tempdb__task_user_objects_alloc_page_count IS NULL OR @tempdb__task_user_objects_alloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__task_user_objects_alloc_page_count),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_user_objects_alloc_page_count)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_user_objects_alloc_page_count)*8./1024.),1)
										ELSE N'?'
									END 
								)
							END, 
				TaUsD = CASE WHEN @tempdb__task_user_objects_dealloc_page_count IS NULL OR @tempdb__task_user_objects_dealloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__task_user_objects_dealloc_page_count),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_user_objects_dealloc_page_count)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_user_objects_dealloc_page_count)*8./1024.),1)
										ELSE N'?'
									END 
								)
							END,
				TaInA = CASE WHEN @tempdb__task_internal_objects_alloc_page_count IS NULL OR @tempdb__task_internal_objects_alloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__task_internal_objects_alloc_page_count),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_internal_objects_alloc_page_count)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_internal_objects_alloc_page_count)*8./1024.),1)
										ELSE N'?'
									END
								)
							END,
				TaInD = CASE WHEN @tempdb__task_internal_objects_dealloc_page_count IS NULL OR @tempdb__task_internal_objects_dealloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__task_internal_objects_dealloc_page_count),1)
										WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_internal_objects_dealloc_page_count)*8),1)
										WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__task_internal_objects_dealloc_page_count)*8./1024.),1)
									ELSE N'?'
									END
								)
							END
		) ss

		UNION ALL 

		SELECT 
			[OrderingValue] = 14,
			Label, 
			SeUsA = N'UsA: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(SeUsA, 1, CHARINDEX(N'.', SeUsA)-1)
									ELSE SeUsA END,
			SeUsD = N'UsD: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(SeUsD, 1, CHARINDEX(N'.', SeUsD)-1)
									ELSE SeUsD END,
			SeInA = N'InA: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(SeInA, 1, CHARINDEX(N'.', SeInA)-1)
									ELSE SeInA END,
			SeInD = N'InD: ' + CASE WHEN @units = N'NATIVE' OR @units = N'KB' THEN SUBSTRING(SeInD, 1, CHARINDEX(N'.', SeInD)-1)
									ELSE SeInD END
		FROM (
			SELECT 
				Label = N'                           Sess',
				SeUsA = CASE WHEN @tempdb__sess_user_objects_alloc_page_count IS NULL OR @tempdb__sess_user_objects_alloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__sess_user_objects_alloc_page_count),1)
									WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_user_objects_alloc_page_count)*8),1)
									WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_user_objects_alloc_page_count)*8./1024.),1)
									ELSE N'?'
									END)
							END,
				SeUsD = CASE WHEN @tempdb__sess_user_objects_dealloc_page_count IS NULL OR @tempdb__sess_user_objects_dealloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__sess_user_objects_dealloc_page_count),1)
									WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_user_objects_dealloc_page_count)*8),1)
									WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_user_objects_dealloc_page_count)*8./1024.),1)
									ELSE N'?'
									END
								)
							END, 
				SeInA = CASE WHEN @tempdb__sess_internal_objects_alloc_page_count IS NULL OR @tempdb__sess_internal_objects_alloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__sess_internal_objects_alloc_page_count),1)
									WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_internal_objects_alloc_page_count)*8),1)
									WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_internal_objects_alloc_page_count)*8./1024.),1)
									ELSE N'?'
									END
							)
							END,
				SeInD = CASE WHEN @tempdb__sess_internal_objects_dealloc_page_count IS NULL OR @tempdb__sess_internal_objects_dealloc_page_count = 0 THEN N''
							ELSE (CASE WHEN @units = N'NATIVE' THEN CONVERT(nvarchar(20),CONVERT(money,@tempdb__sess_internal_objects_dealloc_page_count),1)
									WHEN @units = N'KB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_internal_objects_dealloc_page_count)*8),1)
									WHEN @units = N'MB' THEN CONVERT(nvarchar(20),CONVERT(money,CONVERT(bigint,@tempdb__sess_internal_objects_dealloc_page_count)*8./1024.),1)
									ELSE N'?'
									END
							)
							END
		) ss

		/*
		TODO: think through the below columns and whether they should be included in the logic
		[calc__tmr_wait] [tinyint] NULL,

		--use if system spid, I guess...maybe?
		[rqst__blocking_session_id] [smallint] NULL,
		[rqst__wait_time] [int] NULL,
		[rqst__wait_resource] [nvarchar](256) NULL,
		[rqst__FKDimWaitType] [smallint] NULL,
		*/
	) unionqueries
	;


	SELECT 
		--OrderingValue = ISNULL(rc.OrderingValue,ns.OrderingValue),
		rc.[CapTime/Label],
		rc.SPID,
		rc.CntxtDB,
		rc.Duration,
		rc.[#Tasks],

		[ConsWaits] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN ISNULL(ns.lhs_numwithnoncxp,0) = 0 THEN N'' 
						ELSE N'{'+ CONVERT(nvarchar(20), ns.lhs_numwithnoncxp) + N'}   ' + ISNULL(ns.lhs_noncxpwaits, N'<err>')
						END,
		[ConsPWaits] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN ISNULL(ns.lhs_numwithcxp,0) = 0 THEN N'' 
						ELSE N'{'+ CONVERT(nvarchar(20), ns.lhs_numwithcxp) + N'}   ' + ISNULL(ns.lhs_cxpwaits, N'<err>')
						END,


		[Node] = CASE WHEN ns.OrderingValue IS NULL THEN N'' 
					ELSE (
						CASE WHEN lhs_numwithcons = 0 AND lhs_numwithprod = 0 THEN N'     '
							WHEN lhs_numwithcons > 0 AND lhs_numwithprod = 0 
								THEN CONVERT(nvarchar(10),lhs_numwithcons) + N'-->'

							WHEN lhs_numwithcons = 0 AND lhs_numwithprod > 0
								THEN CONVERT(nvarchar(10), lhs_numwithprod) + N'<--'

							WHEN lhs_numwithcons > 0 AND lhs_numwithprod > 0
								THEN CONVERT(nvarchar(10),lhs_numwithcons) + N'->, ' + CONVERT(nvarchar(10), lhs_numwithprod) + N'<-'
							ELSE N'???'
						END + N' {' + 
						CONVERT(nvarchar(10), ns.focus_node) + N'} ' + 
						CASE WHEN rhs_numwithcons = 0 AND rhs_numwithprod = 0 THEN N'     '
							WHEN rhs_numwithcons > 0 AND rhs_numwithprod = 0 
								THEN CONVERT(nvarchar(10),rhs_numwithcons) + N'-->'

							WHEN rhs_numwithcons = 0 AND rhs_numwithprod > 0
								THEN CONVERT(nvarchar(10), rhs_numwithprod) + N'<--'

							WHEN rhs_numwithcons > 0 AND rhs_numwithprod > 0
								THEN CONVERT(nvarchar(10),rhs_numwithcons) + N'->, ' + CONVERT(nvarchar(10), rhs_numwithprod) + N'<-'
							ELSE N'???'
						END
						)
					END, 


		[ProdPWaits] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN ISNULL(ns.rhs_numwithcxp,0) = 0 THEN N'' 
						ELSE N'{'+ CONVERT(nvarchar(20), ns.rhs_numwithcxp) + N'}   ' + ISNULL(ns.rhs_cxpwaits, N'<err>')
						END,
		[ProdWaits] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN ISNULL(ns.rhs_numwithnoncxp,0) = 0 THEN N'' 
						ELSE N'{'+ CONVERT(nvarchar(20), ns.rhs_numwithnoncxp) + N'}   ' + ISNULL(ns.rhs_noncxpwaits, N'<err>')
						END,

		[ConsCXS] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN lhs_CXS_formattedcount IS NULL THEN N'<Err CXS Count>'
						WHEN lhs_CXS_PercentOfTotal IS NULL THEN N'<Err % Tot>'
						WHEN lhs_CXS_PercentDiff IS NULL THEN N'<Err % Diff>'
						WHEN lhs_CXS_formattedcount = N'<none>' THEN CONVERT(nvarchar(20),0)
						ELSE (   --for troubleshooting text alignment
								--replace(
								CONVERT(char(11),SUBSTRING(lhs_CXS_formattedcount, 1, CHARINDEX(N'.',lhs_CXS_formattedcount)-1))
								--,N' ', N'_')
								
								 + 

								--replace(
								CONVERT(char(11), N'(' + CONVERT(NVARCHAR(20),CONVERT(DECIMAL(7,1),lhs_CXS_PercentOfTotal)) + N'%)') 
								--,N' ', N'_')
								+ 

								--replace(
								CASE WHEN lhs_CXS_PercentDiff <= 0.0 THEN N''
									ELSE N'[' + CONVERT(NVARCHAR(20),CONVERT(DECIMAL(7,1),lhs_CXS_PercentDiff)) + N'%]'
									END
								--,N' ', N'_')
						)
						END,

		[ProdCXS] = CASE WHEN ns.OrderingValue IS NULL THEN N''
						WHEN rhs_CXS_formattedcount IS NULL THEN N'<Err CXS Count>'
						WHEN rhs_CXS_PercentOfTotal IS NULL THEN N'<Err % Tot>'
						WHEN rhs_CXS_PercentDiff IS NULL THEN N'<Err % Diff>'
						WHEN rhs_CXS_formattedcount = N'<none>' THEN N''
						ELSE ( CONVERT(char(11),SUBSTRING(rhs_CXS_formattedcount, 1, CHARINDEX(N'.',rhs_CXS_formattedcount)-1)) + 

								CASE WHEN rhs_CXS_PercentOfTotal = -1 THEN N''
								ELSE CONVERT(char(11), N'(' + CONVERT(NVARCHAR(20),CONVERT(DECIMAL(7,1),rhs_CXS_PercentOfTotal)) + N'%)')
								END + 

								CASE WHEN rhs_CXS_PercentDiff <= 0.0 THEN N''
									ELSE N'[' + CONVERT(NVARCHAR(20),CONVERT(DECIMAL(7,1),rhs_CXS_PercentDiff)) + N'%]'
								END
						)
						END,

		/*
		[lhs_CXS_formattedcount],
		[rhs_CXS_formattedcount],
		[lhs_CXS_PercentOfTotal],
		[rhs_CXS_PercentOfTotal],
		[lhs_CXS_PercentDiff],
		[rhs_CXS_PercentDiff],
		*/

		--ns.lhs_mincxs,
		--ns.lhs_maxcxs,
		--ns.lhs_sumcxs,
		--ns.lhs_numwithcxp,
		--ns.lhs_numwithnoncxp,
		--ns.lhs_cxpwaits,
		--ns.lhs_noncxpwaits,
		--ns.focus_node,
		--ns.rhs_mincxs,
		--ns.rhs_maxcxs,
		--ns.rhs_sumcxs,
		--ns.rhs_numwithcxp,
		--ns.rhs_numwithnoncxp,
		--ns.rhs_cxpwaits,
		--ns.rhs_noncxpwaits,

		[Stmt/Plan/Ibuf/Settings] = CASE WHEN rc.OrderingValue = 1 AND @stmt_xml IS NOT NULL THEN @stmt_xml 
										WHEN rc.OrderingValue = 2 AND @query_plan_xml IS NOT NULL THEN @query_plan_xml
										WHEN rc.OrderingValue = 3 AND @ibuf_xml IS NOT NULL THEN @ibuf_xml 
										WHEN rc.OrderingValue = 4 AND @lv__SessionConnString IS NOT NULL THEN @lv__SessionConnString
										ELSE CONVERT(XML,N'')
									END 
	FROM #ResultsCore rc
		FULL OUTER JOIN (
			SELECT 
				OrderingValue = ROW_NUMBER() OVER (ORDER BY focus_node ASC),
				lhs_mincxs,
				lhs_maxcxs,
				lhs_sumcxs,
				lhs_numwithcxp,
				lhs_numwithcons,
				lhs_numwithprod,
				lhs_numwithnoncxp,
				lhs_cxpwaits,
				lhs_noncxpwaits,
				focus_node,
				rhs_mincxs,
				rhs_maxcxs,
				rhs_sumcxs,
				rhs_numwithcxp,
				rhs_numwithcons,
				rhs_numwithprod,
				rhs_numwithnoncxp,
				rhs_cxpwaits,
				rhs_noncxpwaits, 

				[lhs_CXS_formattedcount] = CASE WHEN lhs_sumcxs IS NULL THEN NULL 
												WHEN lhs_sumcxs = 0 THEN N'<none>'
												ELSE CONVERT(nvarchar(20),CONVERT(money, lhs_sumcxs),1)
												END,
				[rhs_CXS_formattedcount] = CASE WHEN ISNULL(rhs_numwithcxp,0) + ISNULL(rhs_numwithnoncxp,0) = 0 THEN N'<none>'
												WHEN ISNULL(rhs_numwithcxp,0) + ISNULL(rhs_numwithnoncxp,0) > 0
													AND rhs_sumcxs IS NULL THEN NULL 
												ELSE CONVERT(nvarchar(20),CONVERT(money, rhs_sumcxs),1)
											END, 
				[lhs_CXS_PercentOfTotal] = CASE WHEN ISNULL(@lv__TotalCXS,0) = 0 THEN NULL 
												WHEN lhs_sumcxs IS NULL THEN NULL 
											ELSE CONVERT(DECIMAL(21,2), 100) * CONVERT(DECIMAL(21,2), lhs_sumcxs) / CONVERT(DECIMAL(21,2),@lv__TotalCXS)
											END,
				[rhs_CXS_PercentOfTotal] = CASE WHEN ISNULL(@lv__TotalCXS,0) = 0 THEN NULL 
												WHEN ISNULL(rhs_numwithcxp,0) + ISNULL(rhs_numwithnoncxp,0) = 0 THEN -1		--no rhs tasks at all.
												WHEN ISNULL(rhs_numwithcxp,0) + ISNULL(rhs_numwithnoncxp,0) > 0 
														AND rhs_sumcxs IS NULL THEN NULL 
											ELSE CONVERT(DECIMAL(21,2), 100) * CONVERT(DECIMAL(21,2), rhs_sumcxs) / CONVERT(DECIMAL(21,2),@lv__TotalCXS)
											END, 
				[lhs_CXS_PercentDiff] = CASE WHEN lhs_mincxs IS NULL OR lhs_maxcxs IS NULL THEN NULL 
											 WHEN lhs_mincxs = 0 THEN -1
											ELSE CONVERT(DECIMAL(21,2), 100) * CONVERT(DECIMAL(21,2),(lhs_maxcxs - lhs_mincxs)) / CONVERT(DECIMAL(21,2),lhs_mincxs)
											END,
				[rhs_CXS_PercentDiff] = CASE WHEN ISNULL(rhs_numwithcxp,0) + ISNULL(rhs_numwithnoncxp,0) = 0 THEN -1
											 WHEN rhs_mincxs IS NULL OR rhs_maxcxs IS NULL THEN NULL 
											 WHEN rhs_mincxs = 0 THEN -1
											ELSE CONVERT(DECIMAL(21,2), 100) * CONVERT(DECIMAL(21,2),(rhs_maxcxs - rhs_mincxs)) / CONVERT(DECIMAL(21,2),rhs_mincxs)
											END
			FROM #NodeStats ns
		) ns
			ON rc.OrderingValue = ns.OrderingValue
	ORDER BY ISNULL(rc.OrderingValue,ns.OrderingValue) ASC
	;

	RETURN 0;
END


GO
