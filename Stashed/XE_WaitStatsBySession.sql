SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[WaitStatsBySession_Start]
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
@Session_id INT = NULL,
@TraceSessionName NVARCHAR(256) OUTPUT
)
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @TraceFilePath NVARCHAR(1024)
	DECLARE @TraceFileName NVARCHAR(256)

	IF @Session_id IS NULL
	BEGIN
		SET @Session_id = @@SPID 
	END

	IF NOT EXISTS (SELECT * FROM sys.dm_exec_sessions s WHERE s.session_id = @Session_id)
	BEGIN
		Raiserror('This SPID does not exist', 16,1)
		RETURN 0;
	END

	--Where are we placing this file?
	SET @TraceFilePath = (
		SELECT tracepath 
		FROM (
		SELECT TOP 1 tracepath 
		FROM 
		(SELECT [path] as tracepath, 1 as ordcol
		FROM sys.traces t
		WHERE t.path is not null 
		AND substring(REVERSE(LTRIM(RTRIM(t.path))),1, charindex('\',REVERSE(LTRIM(RTRIM(t.path))) )
			) LIKE '%gol%'
	)

	--C:\Program Files\Microsoft SQL Server\MSSQL11.MSSQLSERVER\MSSQL\Log\log_349.trc
	IF @TraceFilePath IS NULL
	BEGIN	--exit quietly for now
		RAISERROR('Null value for @TraceFilePath',16,1)
		RETURN 0;
	END

	--strip off the file name
	SET @TraceFilePath = LTRIM(RTRIM(SUBSTRING(REVERSE(LTRIM(RTRIM(@TraceFilePath))), charindex('\',REVERSE(LTRIM(RTRIM(@TraceFilePath)))), len(@TraceFilePath) )))

	IF SUBSTRING(@TraceFilePath,1,1) <> '\'
	BEGIN
		SET @TraceFilePath = @TraceFilePath + '\'
	END

	SET @TraceFilePath = REVERSE(@TraceFilePath)

	set @TraceFileName = 'WaitStatsBySession_' + CONVERT(varchar(20),@Session_id) + '__' + REPLACE(CONVERT(varchar(30), getdate(), 102),'.','_') + '__' + REPLACE(CONVERT(varchar(30), getdate(), 108), ':', '_')
	SET @TraceSessionName = @TraceFileName
	--print @Tracefilename

	DECLARE @DynSQL VARCHAR(8000)

	--I cannot get the ring buffer target to accept more than 1000 events, unfortunately... it is much faster
	-- add target package0.ring_buffer(SET max_memory=8192, occurrence_number=15000)
	SET @DynSQL = 'CREATE EVENT SESSION [' + @TraceFileName + '] ON SERVER 
	add event sqlos.wait_info
		(where opcode=1 and sqlserver.session_id=' + CONVERT(varchar(20),@Session_id) + ' and duration>0), 
	add event sqlos.wait_info_external
		(where sqlserver.session_id=' + CONVERT(varchar(20),@Session_id) + ' and duration>0)
	add target package0.asynchronous_file_target 
		(SET filename=N''' + @TraceFilePath + @TraceFileName + '.xel'', 
			metadatafile=N''' + @TraceFilePath + @TraceFileName + '.xem'') 
	WITH (max_dispatch_latency = 1 seconds, EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS);
	'

	BEGIN TRY
		EXEC (@DynSQL)
	END TRY
	BEGIN CATCH
		--print 'could not create event session'
		--print ERROR_NUMBER()
		--print ERROR_MESSAGE()
		RETURN 0;	--exit quietly for now
	END CATCH

	--Now we start that trace
	IF EXISTS (SELECT * FROM sys.server_event_sessions s where s.name = @TraceFileName)
	BEGIN
		SET @DynSQL = 'alter event session [' + @TraceFileName + '] on server state = start;'

		BEGIN TRY
			EXEC (@DynSQL)
		END TRY
		BEGIN CATCH
			--print 'could not start event session'
			--print ERROR_NUMBER()
			--print ERROR_MESSAGE()
			RETURN 0;	--exit quietly for now
		END CATCH
	END

	RETURN 0;
END; 
