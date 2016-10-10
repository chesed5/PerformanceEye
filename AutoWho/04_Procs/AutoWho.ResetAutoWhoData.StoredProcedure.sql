SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ResetAutoWhoData]
/*   
	PROCEDURE:		AutoWho.ResetAutoWhoData

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: clear out/reset all "collected" data in the AutoWho tables so that we can start testing
			over again. This proc is primarily aimed at development/testing

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-22	Aaron Morelli		Final run-through and commenting

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
exec AutoWho.ResetAutoWhoData @DeleteConfig=N'N'
*/
(
	@DeleteConfig NCHAR(1)=N'N'
)
AS
BEGIN
	SET NOCOUNT ON;

	IF @DeleteConfig IS NULL OR UPPER(@DeleteConfig) NOT IN (N'N', N'Y')
	BEGIN
		SET @DeleteConfig = N'N';
	END

	TRUNCATE TABLE [AutoWho].[LightweightSessions];
	TRUNCATE TABLE [AutoWho].[LightweightTasks];
	TRUNCATE TABLE [AutoWho].[LightweightTrans];

	TRUNCATE TABLE [AutoWho].[LockDetails];
	TRUNCATE TABLE [AutoWho].[TransactionDetails];
	TRUNCATE TABLE [AutoWho].[TasksAndWaits];
	TRUNCATE TABLE [AutoWho].[SessionsAndRequests];
	TRUNCATE TABLE [AutoWho].[BlockingGraphs];
	TRUNCATE TABLE [AutoWho].[ThresholdFilterSpids];
	TRUNCATE TABLE [AutoWho].[SARException];
	TRUNCATE TABLE [AutoWho].[TAWException];
	TRUNCATE TABLE [AutoWho].[SignalTable];

	--We have pre-reserved certain ID values for certain dimension members, so we need to keep those.
	DELETE FROM [AutoWho].[DimCommand] WHERE DimCommandID > 3;
	DELETE FROM [AutoWho].[DimConnectionAttribute] WHERE DimConnectionAttributeID > 1;
	DELETE FROM [AutoWho].[DimLoginName] WHERE DimLoginNameID > 2;
	DELETE FROM [AutoWho].[DimNetAddress] WHERE DimNetAddressID > 2;
	DELETE FROM [AutoWho].[DimSessionAttribute] WHERE DimSessionAttributeID > 1;
	DELETE [AutoWho].[DimWaitType] WHERE DimWaitTypeID > 2;

	DELETE FROM [CorePE].[OrdinalCachePosition] WHERE Utility IN (N'AutoWho',N'SessionViewer',N'QueryProgress');
	DELETE FROM [CorePE].[CaptureOrdinalCache] WHERE Utility IN (N'AutoWho', N'SessionViewer', N'QueryProgress');
	DELETE FROM [CorePE].[Traces] WHERE Utility = N'AutoWho';

	TRUNCATE TABLE [AutoWho].[CaptureSummary];
	TRUNCATE TABLE [AutoWho].[CaptureTimes];
	TRUNCATE TABLE [AutoWho].[Log];

	IF @DeleteConfig = N'Y'
	BEGIN
		TRUNCATE TABLE [AutoWho].[CollectorOptFakeout];
		TRUNCATE TABLE [AutoWho].[Options];
		TRUNCATE TABLE [AutoWho].[Options_History];
		TRUNCATE TABLE [CorePE].[Version];
		TRUNCATE TABLE [CorePE].[Version_History];

		TRUNCATE TABLE [AutoWho].[DimCommand];
		TRUNCATE TABLE [AutoWho].[DimConnectionAttribute];
		TRUNCATE TABLE [AutoWho].[DimLoginName];
		TRUNCATE TABLE [AutoWho].[DimNetAddress];
		TRUNCATE TABLE [AutoWho].[DimSessionAttribute];
		TRUNCATE TABLE [AutoWho].[DimWaitType];
	END

	RETURN 0;
END
GO
