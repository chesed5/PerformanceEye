SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[WaitStatsBySession_Stop]
/*
Here are directions for 2 different ways to capture Wait Stats by Spid. Note that in either case, you cannot place the code
inside a user transaction.

1) Use this stored procedure. The benefit to this is that it is fairly easy, and you do not have to modify any values

	a) Start the XEvent trace like this (put these 2 lines before the code that you want to trace):

		declare @TName NVARCHAR(256)
		exec dbo.waitstatsbysession_start @TraceSessionName=@TName OUTPUT

	b) End the trace like this (these 3 lines immediately after the code that you want to trace, though not in a user transaction)

		declare @g uniqueidentifier
		set @g = newid()
		exec dbo.waitstatsbysession_stop @TraceSessionName=@TName,@LoadTable=1, @guidcol = @g, @note='no MAXDOP setting'

2) Use the below code, modifying the values inside of <> marks. The pro here is that you avoid any T-SQL statements that are unrelated to either
the creation/stopping of the trace, or of the code you are measuring. (For example, the proc calls above do a bit of validation after creating the
trace and before stopping it, and thus you actually could have a few waits in your results that are actually from the tracing code itself.

	a) Create via this:

		DECLARE @waitstatsspid int
		DECLARE @WaitStatsDynSQL varchar(8000)
		SET @waitstatsspid = @@SPID
		SET @WaitStatsDynSQL = '
		CREATE EVENT SESSION [WaitStatsBySpid_Manual] ON SERVER 
			add event sqlos.wait_info
				(where opcode=1 and sqlserver.session_id=' + CONVERT(varchar(20),@waitstatsspid) + ' and duration>0), 
			add event sqlos.wait_info_external
				(where sqlserver.session_id=' + CONVERT(varchar(20),@waitstatsspid) + ' and duration>0)
			add target package0.asynchronous_file_target 
				(SET filename=N''<TraceFilePath>\WaitStatsBySpid_Manual.xel'', 
					metadatafile=N''<TraceFilePath>\WaitStatsBySpid_Manual.xem'') 
			WITH (max_dispatch_latency = 1 seconds, EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS);
			'

		EXEC (@WaitStatsDynSQL) 

	b) Start via this:

		alter event session [WaitStatsBySpid_Manual] on server state = start;

	c) Stop via this:

		alter event session [WaitStatsBySpid_Manual] on server state = stop;

	d) Pull Data like this (into the table, or comment out the INSERT portion):

		INSERT INTO dbo.WaitStatsBySpid (SnapshotDT, 
			--???guidcol, 
			wait_type, num_waits, total_wait_duration_ms, 
			total_resource_wait_duration_ms, total_signal_duration_ms, notes
			)

		select GETDATE(), 
			--???? NEWID(),
			wait_decoded.wait_type, 
			COUNT(*) as num_waits, 
			SUM(wait_decoded.wait_duration) as total_wait_duration_ms, 
			SUM(wait_decoded.wait_duration - wait_decoded.signal_duration) as total_resource_wait_duration_ms, 
			SUM(wait_decoded.signal_duration) as total_signal_duration_ms,
			'' as notes
		from 
		(select eventdata.value('(/event/data[@name=''wait_type'']/text)[1]', 'varchar(100)') as wait_type
			,eventdata.value('(/event/data[@name=''duration'']/value)[1]', 'bigint') as wait_duration
			,eventdata.value('(/event/data[@name=''signal_duration'']/value)[1]', 'bigint') as Signal_duration
		from 
		(select object_name as eventName, convert(xml, event_data) as eventdata 
		from sys.fn_xe_file_target_read_file (
			N'<TraceFilePath>\WaitStatsBySpid_Manual*.xel',
			N'<TraceFilePath>\WaitStatsBySpid_Manual.xem',
			null, null)) ss
		) as wait_decoded
		group by wait_decoded.wait_type 

	e) drop event session [WaitStatsBySpid_Manual] on server;
*/
(
@TraceSessionName NVARCHAR(256),
@LoadTable INT = 0, 
@guidcol uniqueidentifier, 
@note varchar(100)
)
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @TraceFilePath NVARCHAR(1024)
	DECLARE @TraceFileName NVARCHAR(256)
	DECLARE @DynSQL VARCHAR(8000)

	IF @TraceSessionName IS NULL
	BEGIN
		--print 'null @TraceSessionName'
		RETURN 0;
	END

	IF NOT EXISTS (SELECT * FROM sys.server_event_sessions s WHERE s.name = @TraceSessionName)
	BEGIN
		--print 'no event session found'
		RETURN 0;	--exit quietly for now
	END

	IF EXISTS (SELECT * FROM sys.dm_xe_sessions x WHERE x.name = @TraceSessionName)
	BEGIN
		SET @TraceFilePath = (
			SELECT soc.column_value as tracepath
			FROM sys.dm_xe_sessions s
			JOIN sys.dm_xe_session_object_columns soc
				ON s.address = soc.event_session_address
			WHERE s.name = @TraceSessionName
			  AND soc.object_name IN ('asynchronous_file_target', 'event_file')
			  AND soc.column_name = 'filename'
		)

		--print isnull(@TraceFilePath,'<null @TraceFilePath>')

		--need to stop the trace
		SET @DynSQL = 'alter event session ' + @TraceSessionName + ' on server state = stop;'
		WAITFOR DELAY '00:00:01'
		
		BEGIN TRY
			EXEC (@DynSQL)
		END TRY
		BEGIN CATCH
			--print 'could not stop event session'
			--print ERROR_NUMBER()
			--print ERROR_MESSAGE()
		END CATCH
	END

	IF @TraceFilePath IS NULL
	BEGIN
		--print isnull(@TraceFilePath,'<null @TraceFilePath>')
		RETURN 0;
	END

	IF @LoadTable = 0
	BEGIN
		--print 'attempting to pull data'

select wait_decoded.wait_type, 
	COUNT(*) as num_waits, 
	SUM(wait_decoded.wait_duration) as total_wait_duration_ms, 
	SUM(wait_decoded.wait_duration - wait_decoded.signal_duration) as total_resource_wait_duration_ms, 
	SUM(wait_decoded.signal_duration) as total_signal_duration_ms
from 
(select eventdata.value('(/event/data[@name=''wait_type'']/text)[1]', 'varchar(100)') as wait_type
	--,eventdata.value('(/event/data[@name=''opcode'']/text)[1]', 'varchar(20)') as Opcode
	,eventdata.value('(/event/data[@name=''duration'']/value)[1]', 'bigint') as wait_duration
	,eventdata.value('(/event/data[@name=''signal_duration'']/value)[1]', 'bigint') as Signal_duration
from 
(select object_name as eventName, convert(xml, event_data) as eventdata 
from sys.fn_xe_file_target_read_file (
	REPLACE(@TraceFilePath, '.xel', '*.xel'),
	REPLACE(@TraceFilePath, '.xel', '.xem'),
	--N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\session_waits*.xel', 
	--N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\session_waits.xem', 
	null, null)) ss
) as wait_decoded
group by wait_decoded.wait_type 

	END
	ELSE
	BEGIN
		INSERT INTO dbo.WaitStatsBySpid (SnapshotDT, guidcol, 
			wait_type, num_waits, total_wait_duration_ms, 
			total_resource_wait_duration_ms, total_signal_duration_ms, notes
			)

		select GETDATE(), 
			@guidcol as guidcol,
			wait_decoded.wait_type, 
			COUNT(*) as num_waits, 
			SUM(wait_decoded.wait_duration) as total_wait_duration_ms, 
			SUM(wait_decoded.wait_duration - wait_decoded.signal_duration) as total_resource_wait_duration_ms, 
			SUM(wait_decoded.signal_duration) as total_signal_duration_ms, 
			@note as notes
		from 
		(select eventdata.value('(/event/data[@name=''wait_type'']/text)[1]', 'varchar(100)') as wait_type
			--,eventdata.value('(/event/data[@name=''opcode'']/text)[1]', 'varchar(20)') as Opcode
			,eventdata.value('(/event/data[@name=''duration'']/value)[1]', 'bigint') as wait_duration
			,eventdata.value('(/event/data[@name=''signal_duration'']/value)[1]', 'bigint') as Signal_duration
		from 
		(select object_name as eventName, convert(xml, event_data) as eventdata 
		from sys.fn_xe_file_target_read_file (
			REPLACE(@TraceFilePath, '.xel', '*.xel'),
			REPLACE(@TraceFilePath, '.xel', '*.xem'),
			--N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\session_waits*.xel', 
			--N'C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\session_waits.xem', 
			null, null)) ss
		) as wait_decoded
		group by wait_decoded.wait_type 



	END

	--Now we start that trace
	IF EXISTS (SELECT * FROM sys.server_event_sessions s WHERE s.name = @TraceSessionName)
	BEGIN
		SET @DynSQL = 'drop event session [' + @TraceSessionName + '] on server;'

		BEGIN TRY
			--print 'trying to drop event session'
			EXEC (@DynSQL)
		END TRY
		BEGIN CATCH
			--print 'could not stop event session'
			--print ERROR_NUMBER()
			--print ERROR_MESSAGE()
			RETURN 0;	--exit quietly for now
		END CATCH
	END 

	RETURN 0;
END; 
