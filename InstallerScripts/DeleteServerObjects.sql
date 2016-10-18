DECLARE @DBN_input NVARCHAR(256),
		@AutoWhoTraceJobName NVARCHAR(256),
		@PEMasterJobName NVARCHAR(256),
		@ExceptionMessage NVARCHAR(4000),
		@jid uniqueidentifier,
		@DynSQL NVARCHAR(4000);
SET @DBN_input = N'$(DBName)';
SET @AutoWhoTraceJobName = @DBN_input +  N' - AlwaysDisabled - AutoWho Trace';
SET @PEMasterJobName = @DBN_input +  N' - Every 15 Min - Daily - PerfEye Master';

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Parameter "Database" cannot be null.', 16,1);
END
ELSE
BEGIN
	SET @jid = (SELECT j.job_id FROM msdb.dbo.sysjobs j WHERE j.name = @AutoWhoTraceJobName);
	IF @jid IS NOT NULL
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_id=@jid, @delete_unused_schedule=1
	END

	SET @jid = (SELECT j.job_id FROM msdb.dbo.sysjobs j WHERE j.name = @PEMasterJobName);
	IF @jid IS NOT NULL
	BEGIN
		EXEC msdb.dbo.sp_delete_job @job_id=@jid, @delete_unused_schedule=1
	END
END
GO

IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_JobMatrix')
BEGIN
	DROP PROCEDURE dbo.sp_PE_JobMatrix;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_LongRequests')
BEGIN
	DROP PROCEDURE dbo.sp_PE_LongRequests;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_FileUsage')
BEGIN
	DROP PROCEDURE dbo.sp_PE_FileUsage;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_QueryCamera')
BEGIN
	DROP PROCEDURE dbo.sp_PE_QueryCamera;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_QueryProgress')
BEGIN
	DROP PROCEDURE dbo.sp_PE_QueryProgress;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_SessionSummary')
BEGIN
	DROP PROCEDURE dbo.sp_PE_SessionSummary;
END
IF EXISTS (SELECT * FROM sys.procedures p 
			WHERE p.schema_id = schema_id('dbo') 
			AND p.name = N'sp_PE_SessionViewer')
BEGIN
	DROP PROCEDURE dbo.sp_PE_SessionViewer;
END