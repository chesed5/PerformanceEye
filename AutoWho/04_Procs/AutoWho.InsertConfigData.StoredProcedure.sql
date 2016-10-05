SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[InsertConfigData] 
/*   
	PROCEDURE:		CorePE.InsertConfigData

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Runs at install time and inserts configuration data.

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-10-04	Aaron Morelli		Initial release


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
EXEC AutoWho.InsertConfigData @HoursToKeep=336	--14 days

--use to reset the data:
truncate table CorePE.ProcessingTimes
truncate table autowho.Options
truncate table autowho.Options_History
truncate table AutoWho.CollectorOptFakeout
truncate table AutoWho.DimCommand
truncate table AutoWho.DimConnectionAttribute
truncate table AutoWho.DimLoginName
truncate table AutoWho.DimNetAddress
truncate table AutoWho.DimSessionAttribute
truncate table AutoWho.DimWaitType
*/
(
	@HoursToKeep INT
)
AS
BEGIN
	SET NOCOUNT ON;

	IF ISNULL(@HoursToKeep,-1) <= 0 OR ISNULL(@HoursToKeep,9999) > 4320
	BEGIN
		RAISERROR('The @HoursToKeep parameter cannot be <= 0 or > 4320 (180 days).',16,1);
		RETURN -1;
	END

	--To prevent this proc from damaging the installation after it has already been run, check for existing data.
	IF EXISTS (SELECT * FROM AutoWho.Options)
		OR EXISTS (SELECT * FROM AutoWho.CollectorOptFakeout)
		OR EXISTS (SELECT * FROM AutoWho.DimCommand)
		OR EXISTS (SELECT * FROM AutoWho.DimConnectionAttribute)
		OR EXISTS (SELECT * FROM AutoWho.DimLoginName)
		OR EXISTS (SELECT * FROM AutoWho.DimNetAddress)
		OR EXISTS (SELECT * FROM AutoWho.DimSessionAttribute)
		OR EXISTS (SELECT * FROM AutoWho.DimWaitType)
		OR EXISTS (SELECT * FROM CorePE.ProcessingTimes WHERE Label IN (
										N'AutoWhoLastLatchResolve',
										N'AutoWhoLastLockResolve',
										N'AutoWhoLastNodeStatusResolve',
										N'AutoWhoStoreLastTouched')
					)
	BEGIN
		RAISERROR('The configuration tables are not empty. You must clear these tables first before this procedure will insert config data', 16,1);
		RETURN -2;
	END


	--Holds 2 rows. Used in the AutoWho.Collector procedure to achieve as close to a "snapshot time" as possible.
	INSERT INTO AutoWho.CollectorOptFakeout (ZeroOrOne)
	SELECT 0 UNION SELECT 1

	--*** DimCommand
	SET IDENTITY_INSERT [AutoWho].[DimCommand] ON;

	--We want the special null value to be ID = 1 so we can fill it in for null values via code even though the join will fail to produce a match
	INSERT INTO AutoWho.DimCommand (DimCommandID, command, TimeAdded) SELECT 1, '<nul5>',GETDATE();
	--similarly, we want a special code for the GHOST CLEANUP spid, because we want to avoid page latch resolution if GHOST CLEANUP is running (since we've
	-- seen very long DBCC PAGE runtimes when GHOST CLEANUP was running). 
	INSERT INTO AutoWho.DimCommand (DimCommandID, command, TimeAdded) SELECT 2, 'GHOST CLEANUP',GETDATE();
	--Pre-defining the TM REQUEST command lets us handle certain patterns in the dm_exec_requests view more easily
	INSERT INTO AutoWho.DimCommand (DimCommandID, command, TimeAdded) SELECT 3, 'TM REQUEST',GETDATE();

	SET IDENTITY_INSERT [AutoWho].[DimCommand] OFF;

	--*** DimConnectionAttribute
	SET IDENTITY_INSERT [AutoWho].[DimConnectionAttribute] ON;

	--System spids don't have a connection attribute, so assign them to ID=1 in the code
	INSERT INTO AutoWho.DimConnectionAttribute 
	(DimConnectionAttributeID, net_transport, protocol_type, protocol_version, endpoint_id, node_affinity, net_packet_size, encrypt_option, auth_scheme, TimeAdded)
	SELECT 1, '<nul5>', '<nul5>', -929, -929, -929, -929, '<nul5>', '<nul5>', GETDATE();

	SET IDENTITY_INSERT [AutoWho].[DimConnectionAttribute] OFF;

	--***
	SET IDENTITY_INSERT [AutoWho].[DimLoginName] ON;

	--spids with NULL values in both fields get code 1. I'm not sure if this is even possible?
	INSERT INTO AutoWho.DimLoginName (DimLoginNameID, login_name, original_login_name, TimeAdded)
	SELECT 1, '<nul5>', '<nul5>', GETDATE();
	--system spids (which I believe always have 'sa' for both) will get code 2. But what happens if 'sa' login has been disabled?
	INSERT INTO AutoWho.DimLoginName (DimLoginNameID, login_name, original_login_name, TimeAdded)
	SELECT 2, 'sa', 'sa', GETDATE();
	SET IDENTITY_INSERT [AutoWho].[DimLoginName] OFF;

	SET IDENTITY_INSERT [AutoWho].[DimNetAddress] ON;
	--Local connections that come through Shared Memory will have several null fields, so prepopulate this dim with
	-- a pre-defined ID value, so that we can assign via hard-coded logic rather than the join
	INSERT INTO AutoWho.DimNetAddress (DimNetAddressID, client_net_address, local_net_address, local_tcp_port, TimeAdded) 
	SELECT 1, N'<nul5>', '<nul5>', -929, getdate();

	INSERT INTO AutoWho.DimNetAddress (DimNetAddressID, client_net_address, local_net_address, local_tcp_port, TimeAdded) 
	SELECT 2, N'<local machine>', '<nul5>', -929, getdate();

	SET IDENTITY_INSERT [AutoWho].[DimNetAddress] OFF;


	SET IDENTITY_INSERT [AutoWho].[DimSessionAttribute] ON;

	--This is a "null row"; however, system spids have values for several of the attributes, based on what I've seen. 
	-- Thus, most system spids will map to that row when it is inserted.
	INSERT INTO [AutoWho].[DimSessionAttribute] 
	(DimSessionAttributeID, host_name, program_name, client_version, client_interface_name, 
		endpoint_id, transaction_isolation_level, deadlock_priority, group_id, TimeAdded)
	SELECT 1, '<nul5>', '<nul5>', -929, '<nul5>', 
		-929, -929, -929, -929, GETDATE();

	SET IDENTITY_INSERT [AutoWho].[DimSessionAttribute] OFF;

	SET IDENTITY_INSERT [AutoWho].[DimWaitType] ON;

	-- No value... we interpret this to mean "not waiting"
	INSERT INTO [AutoWho].[DimWaitType] 
	(DimWaitTypeID, wait_type, wait_type_short, latch_subtype)
	SELECT 1, '<nul5>', '<nul5>', N'';

	INSERT INTO [AutoWho].[DimWaitType] 
	(DimWaitTypeID, wait_type, wait_type_short, latch_subtype)
	SELECT 2, 'WAITFOR', 'WAITFOR', N'';

	SET IDENTITY_INSERT [AutoWho].[DimWaitType] OFF;



	--Options
	EXEC AutoWho.ResetOptions; 

	--Retention variables are based on the DaysToKeep input parameter
	UPDATE AutoWho.Options 
	SET 
		Retention_IdleSPIDs_NoTran = @HoursToKeep,
		Retention_IdleSPIDs_WithShortTran = @HoursToKeep,
		Retention_IdleSPIDs_WithLongTran = @HoursToKeep,
		Retention_IdleSPIDs_HighTempDB = @HoursToKeep,
		Retention_ActiveLow = @HoursToKeep,
		Retention_ActiveMedium = @HoursToKeep,
		Retention_ActiveHigh = @HoursToKeep,
		Retention_ActiveBatch = @HoursToKeep,
		Retention_CaptureTimes = (@HoursToKeep/24) + 2
	;


	INSERT INTO CorePE.ProcessingTimes (Label, LastProcessedTime)
	SELECT N'AutoWhoStoreLastTouched', NULL;

	INSERT INTO CorePE.ProcessingTimes (Label, LastProcessedTime)
	SELECT N'AutoWhoLastNodeStatusResolve', NULL;

	INSERT INTO CorePE.ProcessingTimes (Label, LastProcessedTime)
	SELECT N'AutoWhoLastLatchResolve', NULL;

	INSERT INTO CorePE.ProcessingTimes (Label, LastProcessedTime)
	SELECT N'AutoWhoLastLockResolve', NULL;

	INSERT INTO CorePE.Version ([Version], EffectiveDate)
		SELECT '0.5', GETDATE()
	;

	RETURN 0;
END

GO
