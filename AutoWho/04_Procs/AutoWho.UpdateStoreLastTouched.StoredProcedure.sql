SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[UpdateStoreLastTouched] 
/*   
Copyright, Aaron Morelli, 2015. All Rights Reserved

	PROCEDURE:		AutoWho.UpdateStoreLastTouched

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Each "store" table (e.g. CorePE.SQLStmtStore, CorePE.QueryPlanStmtStore) has a
		LastTouchedBy_SPIDCaptureTime that holds a datetime of when that entry was last referenced.
		By updating reference times, we prevent the purge routine from deleting query plans or SQL statements
		that are frequently referenced, and thus avoid the cost of re-inserting them again the next time 
		they are seen. 

		At this time, AutoWho is fairly stable while ServerEye is very early in development. So things
		may change, but here's how the LastTouchedBy logic works: 

			- In ServerEye:
				nothing implemented yet. Thus, this field is not yet relevant for that module

			- In AutoWho: 

				- InputBuffer store and Query plan stores (Batch & Statement) 
					Every time an IB or a QP is identified as being needed for a SPID (i.e. the SPID's duration
					is >= the IB or QP thresholds), it is pulled and compared to the store. If missing, it is
					inserted into the store but if already present, the store's "LastTouchedBy" field is updated
					with the @SPIDCaptureTime of that collection run.
					This logic is primarily due to the fact that we need to hash the IB and QP to compare to the store.

				- SQL Stmt and Batch stores
					Because we don't use a hash value that we calculate for the key for these stores, we can compare
					to the store using sql_handle and the offset fields. This means we don't need to pull the statement
					from the cache and hash it to see if it is already in the store. Thus, instead we have a very lightweight
					statement that joins the SQL stmt/batch stores and compares to the #SAR table and updates the FK columns
					with the store entries (if already present). To keep things lightweight, we don't update the store entries
					with that @SPIDCaptureTime. However, to make sure that the LastTouchedBy field is updated and things
					aren't wastefully purged, this procedure is called every X minutes by the AutoWho Executor and 
					updates LastTouchedBy appropriately. 

		Because the Executor is doing this (in between collector runs), we don't need to worry about these
		statements conflicting with the Collector (like we have to worry about w/Purge).

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
EXEC AutoWho.UpdateStoreLastTouched
*/
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @lv__errormsg NVARCHAR(4000),
			@lv__errorsev INT,
			@lv__errorstate INT,
			@lv__erroroccurred INT,
			@lv__AutoWhoStoreLastTouched DATETIME2(7),
			@lv__CurrentExecTime DATETIME2(7),
			@lv__RC BIGINT,
			@lv__DurationStart DATETIME,
			@lv__DurationEnd DATETIME,
			@lv__MinFKSQLStmtStoreID BIGINT,
			@lv__MaxFKSQLStmtStoreID BIGINT,
			@lv__MinFKSQLBatchStoreID BIGINT,
			@lv__MaxFKSQLBatchStoreID BIGINT;

	SET @lv__CurrentExecTime = DATEADD(SECOND, -10, SYSDATETIME());	--a fudge factor to avoid race conditions
	SET @lv__DurationStart = SYSDATETIME();
	SET @lv__erroroccurred = 0;

	SELECT @lv__AutoWhoStoreLastTouched = p.LastProcessedTime
	FROM CorePE.ProcessingTimes p WITH (FORCESEEK)
	WHERE p.Label = N'AutoWhoStoreLastTouched'
	;

	IF @lv__AutoWhoStoreLastTouched IS NULL
	BEGIN
		SELECT @lv__AutoWhoStoreLastTouched = SPIDCaptureTime
		FROM (
			SELECT TOP 1 SPIDCaptureTime
			FROM AutoWho.SessionsAndRequests sar
			ORDER BY sar.SPIDCaptureTime ASC
		) ss;

		--No records, just return
		IF @lv__AutoWhoStoreLastTouched IS NULL
		BEGIN
			RETURN 0;
		END
	END

	--use an intermediate table to calculate the distinct values, so we don't have to scan SAR once for each store.
	CREATE TABLE #DistinctStoreFKsWithMaxCaptureTime (
		FKSQLStmtStoreID BIGINT, 
		FKSQLBatchStoreID BIGINT,
		MaxSPIDCaptureTime DATETIME
	);

	INSERT INTO #DistinctStoreFKsWithMaxCaptureTime (
		FKSQLStmtStoreID,
		FKSQLBatchStoreID,
		MaxSPIDCaptureTime
	)
	SELECT 
		sar.FKSQLStmtStoreID,
		sar.FKSQLBatchStoreID,
		MAX(sar.SPIDCaptureTime)
	FROM AutoWho.SessionsAndRequests sar WITH (NOLOCK)
	WHERE sar.SPIDCaptureTime BETWEEN CONVERT(DATETIME,@lv__AutoWhoStoreLastTouched) AND CONVERT(DATETIME,@lv__CurrentExecTime)
	AND (sar.FKSQLStmtStoreID IS NOT NULL OR sar.FKSQLBatchStoreID IS NOT NULL)
	GROUP BY sar.FKSQLStmtStoreID,
		sar.FKSQLBatchStoreID
	OPTION(RECOMPILE);

	SELECT 
		@lv__MinFKSQLStmtStoreID = MIN(d.FKSQLStmtStoreID),
		@lv__MaxFKSQLStmtStoreID = MAX(d.FKSQLStmtStoreID),
		@lv__MinFKSQLBatchStoreID = MIN(d.FKSQLBatchStoreID),
		@lv__MaxFKSQLBatchStoreID = MAX(d.FKSQLBatchStoreID)
	FROM #DistinctStoreFKsWithMaxCaptureTime d
	;

	IF @lv__MinFKSQLStmtStoreID IS NOT NULL
	BEGIN
		BEGIN TRY
			SET @lv__RC = 0;

			UPDATE targ 
			SET targ.LastTouchedBy_SPIDCaptureTime = ss.LastTouched 
			FROM CorePE.SQLStmtStore targ
				INNER JOIN (
				SELECT t.FKSQLStmtStoreID, MAX(t.MaxSPIDCaptureTime) as LastTouched
				FROM #DistinctStoreFKsWithMaxCaptureTime t
				WHERE t.FKSQLStmtStoreID IS NOT NULL
				GROUP BY t.FKSQLStmtStoreID
				) ss
					ON targ.PKSQLStmtStoreID = ss.FKSQLStmtStoreID
			WHERE targ.PKSQLStmtStoreID BETWEEN @lv__MinFKSQLStmtStoreID AND @lv__MaxFKSQLStmtStoreID
			AND ss.LastTouched > targ.LastTouchedBy_SPIDCaptureTime
			OPTION(RECOMPILE);

			SET @lv__RC = ROWCOUNT_BIG();
			SET @lv__DurationEnd = SYSDATETIME();

			IF @lv__RC > 0
			BEGIN
				INSERT INTO AutoWho.[Log]
				(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), NULL, 0, N'SQLStmtLastTouch', N'Updated LastTouched for ' + CONVERT(NVARCHAR(20),@lv__RC) + 
					' SQL stmt entries in ' + 
					CONVERT(NVARCHAR(20),DATEDIFF(MILLISECOND, @lv__DurationStart, @lv__DurationEnd)) + 
					N' milliseconds.';
			END
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @lv__errorsev = ERROR_SEVERITY();
			SET @lv__errorstate = ERROR_STATE();
			SET @lv__erroroccurred = 1;

			SET @lv__errormsg = N'Update of SQL Stmt Store LastTouched field failed with error # ' + 
				CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; severity: ' + CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + 
				N'; state: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; message: ' + ERROR_MESSAGE()
			;

			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, 0, N'ErrStmtLastTouch', @lv__errormsg;
		END CATCH
	END 

	SET @lv__DurationStart = SYSDATETIME();

	IF @lv__MinFKSQLBatchStoreID IS NOT NULL 
	BEGIN
		BEGIN TRY
			IF EXISTS (SELECT * FROM #DistinctStoreFKsWithMaxCaptureTime d WHERE d.FKSQLBatchStoreID IS NOT NULL)
			BEGIN
				SET @lv__RC = 0;

				UPDATE targ 
				SET targ.LastTouchedBy_SPIDCaptureTime = ss.LastTouched
				FROM CorePE.SQLBatchStore targ
					INNER JOIN (
					SELECT t.FKSQLBatchStoreID, MAX(t.MaxSPIDCaptureTime) as LastTouched
					FROM #DistinctStoreFKsWithMaxCaptureTime t
					WHERE t.FKSQLBatchStoreID IS NOT NULL
					GROUP BY t.FKSQLBatchStoreID
					) ss
						ON targ.PKSQLBatchStoreID = ss.FKSQLBatchStoreID
				WHERE targ.PKSQLBatchStoreID BETWEEN @lv__MinFKSQLBatchStoreID AND @lv__MaxFKSQLBatchStoreID
				AND ss.LastTouched > targ.LastTouchedBy_SPIDCaptureTime;

				SET @lv__RC = ROWCOUNT_BIG();
				SET @lv__DurationEnd = SYSDATETIME(); 

				IF @lv__RC > 0
				BEGIN
					INSERT INTO AutoWho.[Log]
					(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
					SELECT SYSDATETIME(), NULL, 0, N'SQLBatchLastTouch', N'Updated LastTouched for ' + CONVERT(NVARCHAR(20),@lv__RC) + 
						' SQL batch entries in ' + 
						CONVERT(NVARCHAR(20),DATEDIFF(MILLISECOND, @lv__DurationStart, @lv__DurationEnd)) + 
						N' milliseconds.';
				END
			END --if batches exist
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @lv__errorsev = ERROR_SEVERITY();
			SET @lv__errorstate = ERROR_STATE();
			SET @lv__erroroccurred = 1;

			SET @lv__errormsg = N'Update of SQL Batch Store LastTouched field failed with error # ' + 
				CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; severity: ' + CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + 
				N'; state: ' + CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; message: ' + ERROR_MESSAGE()
			;

			INSERT INTO AutoWho.[Log]
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, 0, N'ErrBatchLastTouch', @lv__errormsg;
		END CATCH
	END 

	IF @lv__erroroccurred = 1
	BEGIN
		IF @lv__AutoWhoStoreLastTouched < DATEADD(MINUTE, -45, @lv__CurrentExecTime)
		BEGIN
			SET @lv__AutoWhoStoreLastTouched = DATEADD(MINUTE, -45, @lv__CurrentExecTime)
		END
	END
	ELSE
	BEGIN
		SET @lv__AutoWhoStoreLastTouched = @lv__CurrentExecTime;
	END

	UPDATE targ 
	SET LastProcessedTime = @lv__AutoWhoStoreLastTouched
	FROM CorePE.ProcessingTimes targ WITH (FORCESEEK)
	WHERE targ.Label = N'AutoWhoStoreLastTouched'		


	RETURN 0;
END

GO
