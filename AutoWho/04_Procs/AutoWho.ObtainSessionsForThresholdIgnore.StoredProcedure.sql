SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[ObtainSessionsForThresholdIgnore]
/*   
	PROCEDURE:		AutoWho.ObtainSessionsForThresholdIgnore

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: The AutoWho collector uses "threshold" parameters to determine whether certain, more-expensive
		activities (collecting query plans, input buffers, tran or lock info, etc) are required. If no SPIDs
		cross those thresholds on a given run, that extra info will not be collected. This strategy helps keep
		the collector efficient. 

		The main driver for these thresholds is spid (active or idle) duration. However, there are sessions that
		we know will be active all day (e.g. the AutoWho and Server Executor/Collector SPIDs), so we need to 
		exclude those sessions from the threshold calculation. This proc is called from AutoWho.Executor every
		5 minutes and re-calcs the Session IDs. 

		NOTE: this proc is intended to be customized by users on an as-needed basis. 

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-24	Aaron Morelli		Final run-through and commenting

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
EXEC AutoWho.ObtainSessionsForThresholdIgnore 

*/
AS
BEGIN
	SET NOCOUNT ON;
	/* 
		Sessions we want to identify:
			Self (because even if the AutoWho.Options table has us including SELF, we still don't
				want it to trigger the thresholds)
			ServerEye's Executor/Collector
	*/
	DECLARE @SPIDsToIB TABLE (SessionID INT, DatabaseID SMALLINT);
	DECLARE @SPIDsToFilter TABLE (SessionID INT); 

	DECLARE @IBResults TABLE (
		EventType VARCHAR(100), 
		[Parameters] INT, 
		InputBuffer NVARCHAR(4000)
	);
	
	INSERT INTO @SPIDsToFilter (SessionID)
	SELECT @@SPID;

	--Obtain the DMViewerCore "run all day" SPIDs
	DECLARE @CurrentDBID INT, 
		@BizTalkMsgBoxDBID INT;

	SELECT @CurrentDBID = DB_ID();

	SELECT @BizTalkMsgBoxDBID = d.database_id
	FROM sys.databases d
	WHERE d.name = 'BizTalkMsgBoxDb';

	INSERT INTO @SPIDsToIB (SessionID, DatabaseID)
	SELECT DISTINCT se.session_id, mds.dbid
	FROM sys.dm_exec_sessions se
		INNER JOIN master.dbo.sysprocesses mds
			ON mds.spid = se.session_id
	WHERE mds.ecid = 0
	AND mds.dbid IN (@CurrentDBID, 
					ISNULL(@BizTalkMsgBoxDBID,-1)		--need ISNULL in case Msgbox doesn't exist on this instance
					)
	AND EXISTS (SELECT * FROM sys.dm_exec_requests r		--make sure the session has an open request
					WHERE r.session_id = se.session_id)
	AND se.[program_name] like '%SQLAgent - TSQL JobStep%';

	DECLARE @tmpSPID INT, 
			@tmpDBID SMALLINT,
			@DynSQL VARCHAR(MAX);

	DECLARE iterateSPIDs CURSOR LOCAL FAST_FORWARD FOR 
	SELECT SessionID, DatabaseID 
	FROM @SPIDsToIB;

	OPEN iterateSPIDs
	FETCH iterateSPIDs INTO @tmpSPID, @tmpDBID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		--print @tmp
		DELETE FROM @IBResults;
		SET @DynSQL = 'DBCC INPUTBUFFER(' + CONVERT(VARCHAR(20),@tmpSPID) + ') WITH NO_INFOMSGS;';

		BEGIN TRY
			INSERT INTO @IBResults
				EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			--no-op
		END CATCH

		--debug:
		--SELECT * FROM @IBResults;
		IF @tmpDBID = @CurrentDBID
		BEGIN
			INSERT INTO @SPIDsToFilter (SessionID)
			SELECT DISTINCT @tmpSPID
			FROM @IBResults t
			WHERE (
				t.InputBuffer LIKE '%AeosDMVMonitoring%'
				OR 
				t.InputBuffer LIKE '%Collector%'	--could be ServerEye or AutoWho
				)
			AND NOT EXISTS (SELECT * FROM @SPIDsToFilter t2
							WHERE t2.SessionID = @tmpSPID);
		END 
		ELSE IF @tmpDBID = @BizTalkMsgBoxDBID
		BEGIN
			INSERT INTO @SPIDsToFilter (SessionID)
			SELECT DISTINCT @tmpSPID
			FROM @IBResults t
			WHERE t.InputBuffer LIKE '%bts_ManageMessageRefCountLog%'
			AND NOT EXISTS (SELECT * FROM @SPIDsToFilter t2
							WHERE t2.SessionID = @tmpSPID);
		END

		FETCH iterateSPIDs INTO @tmpSPID, @tmpDBID;
	END

	CLOSE iterateSPIDs
	DEALLOCATE iterateSPIDs;

	INSERT INTO AutoWho.ThresholdFilterSpids ([ThresholdFilterSpid])
	SELECT DISTINCT t.SessionID
	FROM @SPIDsToFilter t;

	RETURN 0
END


GO
