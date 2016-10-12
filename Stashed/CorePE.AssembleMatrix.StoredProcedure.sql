SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [CorePE].[AssembleJobMatrix] 
/*   
	PROCEDURE:		CorePE.AssembleMatrix

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: Called by sp_PE_JobMatrix to assemble a textual matrix that represents job history or predictive job history.

	ASSUMPTIONS: Expects the existence of several temp tables that are created by sp_PE_JobMatrix
				#Jobs
				#TimeWindows_hist
				#TimeWindows_Pred


	OUTSTANDING ISSUES: 

    CHANGE LOG:	
				2016-10-10	Aaron Morelli		Initial Creation


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

*/
(
	@MatrixType						NVARCHAR(10) = 'Hist',		-- 'Hist' or 'Pred'
	@HoursBack						TINYINT=NULL,
	@HoursForward					TINYINT=NULL,
	@FitOnScreen					NCHAR(1),
	@HistOutput						NVARCHAR(10)=NULL,
	@PredictOutput					NVARCHAR(10)=NULL,
	@OverallWindowBeginTime			DATETIME,
	@OverallWindowEndTime			DATETIME,
	@MatrixWidth					SMALLINT,
	@WindowLength_minutes			SMALLINT,
	@SQLServerStartTime				DATETIME=NULL,
	@SQLAgentStartTime				DATETIME=NULL,
	@OutputString					NVARCHAR(MAX) OUTPUT,
	@Debug							INT=0
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;

	DECLARE @lv__EmptyChar						NCHAR(1) = N'_',
			@lv__mtx__MaxJobNameLength			SMALLINT,
			@lv__mtx__HeaderLine				NVARCHAR(4000), 
			@lv__mtx__HeaderHours				NVARCHAR(4000),
			@lv__mtx__Replicate1				SMALLINT, 
			@lv__mtx__Replicate2				SMALLINT,
			@lv__mtx__CountMatrixRows_1			INT, 
			@lv__mtx__CountMatrixRows_3			INT,
			@lv__mtx__CountMatrixRows_5			INT,
			@lv__mtx__Matrix3HasHeader			BIT,
			@lv__mtx__PrintOverallBeginTime		VARCHAR(30), 
			@lv__mtx__PrintOverallEndTime		VARCHAR(30),
	
			@lv__beforedt DATETIME,
			@lv__afterdt DATETIME,
			@lv__slownessthreshold INT=250,
			
			@lv__ErrorText NVARCHAR(MAX),
			@lv__ErrorSeverity INT,
			@lv__ErrorState INT;

	--Populated with a cross join between the #TimeWindows_Hist table and the #Jobs table, which logically gives us
	-- each matrix "line" (a series of cells/time windows for each job)
	CREATE TABLE #JobArrays_Hist (
		JobID				INT NOT NULL, 
		WindowID			INT NOT NULL, 
		WindowBegin			DATETIME NOT NULL, 
		WindowEnd			DATETIME NOT NULL, 
		CellText			NCHAR(1) NOT NULL
	);

	CREATE TABLE #JobArrays_Pred (
		JobID				INT NOT NULL, 
		WindowID			INT NOT NULL, 
		WindowBegin			DATETIME NOT NULL, 
		WindowEnd			DATETIME NOT NULL, 
		CellText			NCHAR(1) NOT NULL
	);

	--We place various substrings here before assembling them into the XML value
	CREATE TABLE #OutputStringLineItems_Hist (
		RowType				TINYINT, 
		JobID				INT, 
		MatrixNumber		INT, 
		DisplayOrder		INT, 
		CellString			NVARCHAR(MAX)
	);

	CREATE TABLE #OutputStringLineItems_Pred (
		RowType				TINYINT, 
		JobID				INT, 
		MatrixNumber		INT, 
		DisplayOrder		INT, 
		CellString			NVARCHAR(MAX)
	);

	SET @lv__beforedt = GETDATE();

	--Now, create our Job Array.
	IF @MatrixType = N'Hist'
	BEGIN
		BEGIN TRY
			INSERT INTO #JobArrays_Hist (JobID, WindowID, WindowBegin, WindowEnd, CellText)
			SELECT ss.JobID, tw.WindowID, tw.WindowBegin, tw.WindowEnd, @lv__EmptyChar
			FROM (
				SELECT j.JobID
				FROM #Jobs j
				) ss
				CROSS JOIN #TimeWindows_Hist tw
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while creating the historical job array. The job history matrix will not be displayed.', 11, 1);
			--responsibility of calling code SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH
	END

	IF @MatrixType = N'Pred'
	BEGIN
		BEGIN TRY
			INSERT INTO #JobArrays_Pred (JobID, WindowID, WindowBegin, WindowEnd, CellText)
			SELECT ss.JobID, tw.WindowID, tw.WindowBegin, tw.WindowEnd, @lv__EmptyChar
			FROM (
				SELECT j.JobID
				FROM #Jobs j
				) ss
				CROSS JOIN #TimeWindows_Pred tw
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while creating the predictive job array. The predictive matrix will not be displayed.', 11, 1);
			--responsibility of calling code SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH
	END

	SET @lv__afterdt = GETDATE();

	IF @Debug = 2
	BEGIN
		IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
		BEGIN
			SET @lv__ErrorText = N'   ***dbg: constructing the Job Arrays took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
			RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
		END
	END

	IF @MatrixType = N'Pred'
	BEGIN
		--TODO: need the name and structure of the hypothetical runs table first
		SET @MatrixType = @MatrixType
	END
	ELSE IF @MatrixType = N'Hist'
	BEGIN
		SET @lv__beforedt = GETDATE();

		--Cell population for failures
		--Ok, first, update the arrays with any job failures. If a failure has occurred in a Time Window, then we mark that 
		-- time window with an 'X'
		BEGIN TRY 
			UPDATE targ
			SET targ.CellText = CASE WHEN xapp1.job_run_status = 0 THEN 'F'
									WHEN xapp1.job_run_status = 2 THEN 'R'
									WHEN xapp1.job_run_status = 3 THEN 'C'
								ELSE 'X'
								END
			FROM #JobArrays_Hist targ
				INNER JOIN #Jobs j
					ON targ.JobID = j.JobID
				CROSS APPLY (		--the use of CROSS rather than OUTER apply is important here. 
						SELECT TOP 1 jc.job_run_status 
						FROM #JobInstances jc
						WHERE j.native_job_id = jc.native_job_id
						AND jc.job_run_status <> 1
						AND jc.JobDisplayEndTime >= targ.WindowBegin
						AND jc.JobDisplayEndTime < targ.WindowEnd	--remember, endpoint of our window is NOT inclusive (because it is the same as the start of the subsequent window)
						ORDER BY jc.job_run_status ASC		--0 (failure) will sort first, 2 (retry) will sort second, and 3 (cancelled) will sort third
							--note that even if there are multiple jobs with the same run_status value, we don't really care, since we just pull the status
					) xapp1
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while populating Matrix with job failures. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH

		SET @lv__afterdt = GETDATE();

		IF @Debug = 2
		BEGIN
			IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
			BEGIN
				SET @lv__ErrorText = N'   ***dbg: populating Matrix with job failures took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
				RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
			END
		END 

		SET @lv__beforedt = GETDATE();

		--Cell population for successes
		--Now, update the array with info on the # of SUCCESSFUL job completions in a given time block. 
		-- Note that we NEVER overwrite a cell that has already been written to.
		BEGIN TRY
			;WITH JobWindowsWithSuccesses AS (
				SELECT ja.JobID, ja.WindowID, 
					COUNT(*) AS NumSuccessfulCompletions
				FROM #JobArrays_Hist ja
					INNER JOIN #Jobs j
						ON ja.JobID = j.JobID
					INNER JOIN #JobInstances jc
						ON j.native_job_id = jc.native_job_id
				WHERE ja.CellText = @lv__EmptyChar
				AND jc.job_run_status = 1
				AND jc.JobDisplayEndTime >= ja.WindowBegin
				AND jc.JobDisplayEndTime < ja.WindowEnd	--remember, endpoint of our window is NOT inclusive (because it is the same as the start of the subsequent window)
				GROUP BY ja.JobID, ja.WindowID, ja.WindowBegin, ja.WindowEnd
			)
			UPDATE targ
			SET targ.CellText = CASE WHEN jw.NumSuccessfulCompletions >= 9 THEN '9'
									WHEN jw.NumSuccessfulCompletions = 1 THEN '/'
								 ELSE CONVERT(CHAR(1), jw.NumSuccessfulCompletions)
								 END
			FROM #JobArrays_Hist targ
				INNER JOIN JobWindowsWithSuccesses jw
					ON targ.JobID = jw.JobID
					AND targ.WindowID = jw.WindowID
			WHERE jw.NumSuccessfulCompletions > 0
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while populating Matrix with job successes. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH

		SET @lv__afterdt = GETDATE();

		IF @Debug = 2
		BEGIN
			IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
			BEGIN
				SET @lv__ErrorText = N'   ***dbg: populating Matrix with job successes took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
				RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
			END
		END 


		SET @lv__beforedt = GETDATE();

		--Cell population for starts
		BEGIN TRY
			;WITH JobStarts AS (
				SELECT ja.JobID, ja.WindowID
				FROM #JobArrays_Hist ja
					INNER JOIN #Jobs j
						ON ja.JobID = j.JobID
				WHERE ja.CellText = @lv__EmptyChar
				AND EXISTS (SELECT * FROM #JobInstances jc
						WHERE j.native_job_id = jc.native_job_id
						AND jc.JobStartTime >= ja.WindowBegin
						AND jc.JobStartTime < ja.WindowEnd	--remember, endpoint of our window is NOT inclusive (because it is the same as the start of the subsequent window)
						)
			)
			UPDATE targ 
			SET targ.CellText = '^'
			FROM #JobArrays_Hist targ
				INNER JOIN JobStarts js
					ON targ.JobID = js.JobID
					AND targ.WindowID = js.WindowID
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while populating the Matrix with job starts. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH

		SET @lv__afterdt = GETDATE();

		IF @Debug = 2
		BEGIN
			IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
			BEGIN
				SET @lv__ErrorText = N'   ***dbg: populating Matrix with job starts took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
				RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
			END
		END


		SET @lv__beforedt = GETDATE();

		--Cell population for "running"
		--Ok, our final update to the array is to mark all cells with a '~' or '!' where a job was running during that window, but its start was before
		-- the time window started and its end is after the time window started.
		BEGIN TRY
			UPDATE ja
			SET CellText = CASE WHEN xapp1.JobExpectedEndTime < ja.WindowBegin THEN '!'
							ELSE '~'
							END
			FROM #JobArrays_Hist ja
				INNER JOIN #Jobs j
					ON ja.JobID = j.JobID
				CROSS APPLY (
						SELECT TOP 1	--there should only be 1 row anyway...
							ji.JobExpectedEndTime
						FROM #JobInstances ji
						WHERE ji.native_job_id = j.native_job_id
						AND ji.JobStartTime < ja.WindowBegin
						AND ji.JobDisplayEndTime >= ja.WindowEnd		--remember, WindowEnd is actually NOT inclusive
						) xapp1
			WHERE ja.CellText = @lv__EmptyChar
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while populating the Matrix with "job running" info. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH

		SET @lv__afterdt = GETDATE();

		IF @Debug = 2
		BEGIN
			IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
			BEGIN
				SET @lv__ErrorText = N'   ***dbg: populating Matrix with "running" tokens took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
				RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
			END

			SELECT ja.JobID, ja.WindowID, ja.WindowBegin, ja.WindowEnd, ja.CellText
			FROM #JobArrays_Hist ja
			ORDER BY ja.JobID, ja.WindowID
		END
	END		--IF @MatrixType = N'Hist'		applying failures/successes/starts/running characters to the cells for the historical matrix

	SET @lv__beforedt = GETDATE();

	--Determine how many characters of the job name we'll be printing (potentially all)
	SELECT @lv__mtx__MaxJobNameLength = MAX(LEN(col1))
	FROM (
		SELECT col1 = (
			CASE WHEN j.IsEnabled = 0 THEN N'*' ELSE N'$' END +
				CONVERT(NVARCHAR(20),j.JobRuns) + N'/' + CONVERT(NVARCHAR(20),j.JobFailures) + N'  ' + 
				CASE WHEN j.CreateDate > @OverallWindowEndTime
							THEN N'(' + j.JobName + N')'
						ELSE j.JobName
					END
			)
		FROM #Jobs j
	) ss0;

	--TODO: consider some way of customizing the max length under different circumstances (@ToConsole values, @FitOnScreen values, a user-specified param, etc)
	SET @lv__mtx__MaxJobNameLength = (
			CASE WHEN @lv__mtx__MaxJobNameLength IS NULL THEN 1		--no SQL Agent jobs exist! we shouldn't reach this point
				WHEN @lv__mtx__MaxJobNameLength <= 55 THEN @lv__mtx__MaxJobNameLength	--50 chars is fine whatever the output
				ELSE 55
			END 
			);

	--Construct the header lines
	SET @lv__mtx__HeaderHours = N'';
	SET @lv__mtx__HeaderLine = N'';

	IF @MatrixType = N'Hist'
	BEGIN
		SELECT @lv__mtx__HeaderHours = @lv__mtx__HeaderHours + tw.TimeHeaderChar
		FROM #TimeWindows_Hist tw
		ORDER BY tw.WindowID DESC;

		SELECT @lv__mtx__HeaderLine = @lv__mtx__HeaderLine + tw.LineHeaderChar
		FROM #TimeWindows_Hist tw
		ORDER BY tw.WindowID DESC;
	END
	ELSE
	BEGIN
		SELECT @lv__mtx__HeaderHours = @lv__mtx__HeaderHours + tw.TimeHeaderChar
		FROM #TimeWindows_Pred tw
		ORDER BY tw.WindowID DESC;

		SELECT @lv__mtx__HeaderLine = @lv__mtx__HeaderLine + tw.LineHeaderChar
		FROM #TimeWindows_Pred tw
		ORDER BY tw.WindowID DESC;
	END


	--Creation of the output strings (before final concatenation, in sub-matrices)

	/* Our matrix is really several sub-matrices. Each matrix holds certain "categories" of jobs, based on those jobs' runs/failures/enabled/disabled status:

		For now, here's how we'll organize them:
		Historical matrix:
				Matrix 1
					Jobs that have had a failure or are currently running  (whether disabled or not)
																						(use MatrixNumber=2 for a spacer line)
				Matrix 3
					Jobs not in Matrix 1 that have had at least 1 run
																						(use MatrixNumber=4 for a spacer line)
				Matrix 5
					All other jobs (jobs that haven't run at all, whether disabled or not)

		Predictive matrix:
				Matrix 1
					Jobs that had at least 1 "run" during the window
																						(use MatrixNumber=2 for a spacer line)
				Matrix 3
					Jobs that didn't "run" during the window
							
	*/
	IF @MatrixType = N'Hist'
	BEGIN
		INSERT INTO #OutputStringLineItems_Hist 
			(RowType, JobID, MatrixNumber, DisplayOrder, CellString)
		SELECT 0,		-1,			-1,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																END + N'$' + @lv__mtx__HeaderHours
		UNION ALL 
		SELECT 1,		-1,			-1,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																END + N'$' + @lv__mtx__HeaderLine
		;
	END 
	ELSE IF @MatrixType = N'Pred'
	BEGIN
		INSERT INTO #OutputStringLineItems_Pred
			(RowType, JobID, MatrixNumber, DisplayOrder, CellString)
		SELECT 0,		-1,			-1,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																END + N'$' + @lv__mtx__HeaderHours
		UNION ALL 
		SELECT 1,		-1,			-1,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																END + N'$' + @lv__mtx__HeaderLine
		;
	END

	SET @lv__afterdt = GETDATE();

	IF @Debug = 2
	BEGIN
		IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
		BEGIN
			SET @lv__ErrorText = N'   ***dbg: obtaining max job name length and header output lines took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
			RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
		END
	END


	SET @lv__beforedt = GETDATE();
	IF @MatrixType = N'Hist'
	BEGIN
		BEGIN TRY 
			INSERT INTO #OutputStringLineItems_Hist
			(RowType, JobID, CellString)
			SELECT 2,
				ja1.JobID, 
				CellString = (
							SELECT [*] = ja2.CellText
							FROM #JobArrays_Hist as ja2
							WHERE ja2.JobID = ja1.JobID
							ORDER BY ja2.WindowID DESC
							FOR XML PATH(N'')
						)
			FROM #JobArrays_Hist AS ja1
			GROUP BY ja1.JobID
			ORDER BY JobID;

			UPDATE targ 
			SET MatrixNumber = j.MatrixNumber, 
				DisplayOrder = j.DisplayOrder
			FROM #OutputStringLineItems_Hist targ
				INNER JOIN #Jobs j
					ON targ.JobID = j.JobID
			;

			UPDATE targ  
			SET targ.CellString = (
					CASE 
						WHEN @FitOnScreen = 'Y' 
							THEN N'|' + targ.CellString + N'|' + ss.JobName
						ELSE SUBSTRING(ss.JobName,1,@lv__mtx__MaxJobNameLength) + N'|' + targ.CellString + N'|'
						END
				)
			FROM #OutputStringLineItems_Hist targ
				INNER JOIN (
						SELECT j.JobID, 
							[JobName] = CASE WHEN j.IsEnabled = 0 THEN N'*' ELSE N' ' END +
							CONVERT(NVARCHAR(20),j.JobRuns) + N'/' + CONVERT(NVARCHAR(20),j.JobFailures) + N'  ' + 
							CASE WHEN j.CreateDate > @OverallWindowEndTime
									THEN N'(' + j.JobName + N')'
								ELSE j.JobName
							END + REPLICATE('$', 50)
						FROM #Jobs j
						) ss
					ON targ.JobID = ss.JobID
			;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while constructing output strings from job arrays. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH
	END
	--ELSE		--TODO: predictive matrix
	--BEGIN
	--END

	SET @lv__afterdt = GETDATE();

	IF @Debug = 2
	BEGIN
		IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
		BEGIN
			SET @lv__ErrorText = N'   ***dbg: constructing output strings from job arrays took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
			RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
		END
	END


	SET @lv__beforedt = GETDATE();

	--Sub-matrix labels
	IF @MatrixType = N'Hist'
	BEGIN
		--Matrices 1,3,5 hold actual rows, while 2,4,6 are labels indicating which matrix it is.
		-- The below labels are all intentionally the same length (39 chars)
		BEGIN TRY
			SET @lv__mtx__Replicate1 = (@MatrixWidth - 39) / 2 + 1
			SET @lv__mtx__Replicate2 = @MatrixWidth - 39 - @lv__mtx__Replicate1

			INSERT INTO #OutputStringLineItems_Hist
				(RowType, JobID, MatrixNumber, DisplayOrder, CellString)
			SELECT 2,		-1,		2,			1, 
				CASE WHEN @FitOnScreen = 'Y' THEN '' 
				ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
				END + N'|' + REPLICATE(N'*', @lv__mtx__Replicate1) + N'Currently-running or at least 1 failure' + REPLICATE(N'*', @lv__mtx__Replicate2) + N'|' + NCHAR(10) + NCHAR(13) + NCHAR(10) + NCHAR(13)+ NCHAR(10) + NCHAR(13)
			UNION ALL
			SELECT 2,		-1,		4,			1, 
				CASE WHEN @FitOnScreen = 'Y' THEN '' 
				ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
				END + N'|' + REPLICATE(N'*', @lv__mtx__Replicate1) + N'Executed >= 1 time and always succeeded' + REPLICATE(N'*', @lv__mtx__Replicate2) + N'|' + NCHAR(10) + NCHAR(13) + NCHAR(10) + NCHAR(13)+ NCHAR(10) + NCHAR(13)
			UNION ALL 
			SELECT 2,		-1,		6,			1,
				CASE WHEN @FitOnScreen = 'Y' THEN '' 
				ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
				END + N'|' + REPLICATE(N'*', @lv__mtx__Replicate1) + N'Did not execute during this time window' + REPLICATE(N'*', @lv__mtx__Replicate2) + N'|' + NCHAR(10) + NCHAR(13) + NCHAR(10) + NCHAR(13)+ NCHAR(10) + NCHAR(13)
			;

			--Decide whether to re-print the header for second and third sub-matrices
			--If there are a lot of job rows in matrix #1 (Currently-running or at least 1 failure), then the user will have to scroll down to see the jobs in 
			-- matrix #3 (Executed >= 1 time, always succeeded). Similarly, if there are a lot of rows in matrix #3, then when the user scrolls down to see them, 
			-- the header rows will not be visible and the user will have to keep scrolling up and down to match times to matrix info. To avoid this, we 
			-- check the # of lines in matrices #1 and #3 combined, and if the result is > a threshold, we add header rows in to matrix 3 as well.
			SELECT 
				@lv__mtx__CountMatrixRows_1 = SUM(CASE WHEN o.MatrixNumber = 1 THEN 1 ELSE 0 END),
				@lv__mtx__CountMatrixRows_3 = SUM(CASE WHEN o.MatrixNumber = 3 THEN 1 ELSE 0 END),
				@lv__mtx__CountMatrixRows_5 = SUM(CASE WHEN o.MatrixNumber = 5 THEN 1 ELSE 0 END)
			FROM #OutputStringLineItems_Hist o;


			SET @lv__mtx__Matrix3HasHeader = 0;
			IF (@lv__mtx__CountMatrixRows_1 + @lv__mtx__CountMatrixRows_3) >= 35
			BEGIN
				INSERT INTO #OutputStringLineItems_Hist 
					(RowType, JobID, MatrixNumber, DisplayOrder, CellString)
				SELECT 0,		-1,			3,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																	ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																	END + N'$' + @lv__mtx__HeaderHours
				UNION ALL 
				SELECT 1,		-1,			3,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																	ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																	END + N'$' + @lv__mtx__HeaderLine
				;

				SET @lv__mtx__Matrix3HasHeader = 1;
			END

			--We need similar logic for Matrix 5
			IF (@lv__mtx__Matrix3HasHeader = 0 AND (@lv__mtx__CountMatrixRows_1 + @lv__mtx__CountMatrixRows_3 + @lv__mtx__CountMatrixRows_5) >= 30)
				OR (@lv__mtx__Matrix3HasHeader = 1 AND (@lv__mtx__CountMatrixRows_3 + @lv__mtx__CountMatrixRows_5) >= 35 )
			BEGIN
				INSERT INTO #OutputStringLineItems_Hist 
					(RowType, JobID, MatrixNumber, DisplayOrder, CellString)
				SELECT 0,		-1,			5,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																	ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																	END + N'$' + @lv__mtx__HeaderHours
				UNION ALL 
				SELECT 1,		-1,			5,			1,			CASE WHEN @FitOnScreen = 'Y' THEN '' 
																	ELSE REPLICATE('$', @lv__mtx__MaxJobNameLength)
																	END + N'$' + @lv__mtx__HeaderLine
			END
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while constructing sub-matrix headers. The job history matrix will not be displayed.', 11, 1);
			--SET @output__DisplayMatrix = 0;
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH
	END
	--ELSE
	--BEGIN


	--END		--IF @MatrixType = N'Pred'

	SET @lv__afterdt = GETDATE();

	IF @Debug = 2
	BEGIN
		IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
		BEGIN
			SET @lv__ErrorText = N'   ***dbg: Sub-matrix headers took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
			RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
		END
	END

	SET @lv__beforedt = GETDATE();

	--Ok, assemble final output
	SET @OutputString = N'';

	IF @MatrixType = N'Hist'
	BEGIN
		BEGIN TRY
			SELECT @OutputString = @OutputString + 
				REPLACE(REPLACE(CellString,N'_', N' '),N'$', N' ') + NCHAR(10) + 
				CASE WHEN RowType < 2 OR MatrixNumber IN (2,4,6)
					THEN N'' 
					ELSE (	N'' --Printing underscores as spacer lines proved to be uglier than just having each line follow consecutively
						/*
							CASE WHEN @Matrix_PrintUnderscores = N'Y' 
								THEN N'|' + REPLICATE(N'_', 156) + N'|' + NCHAR(10)
								ELSE N'' 
							END 
						*/
						)
				END 
			FROM #OutputStringLineItems_Hist
			ORDER BY MatrixNumber, RowType, DisplayOrder
			;

			SET @lv__mtx__PrintOverallBeginTime = CONVERT(VARCHAR(20),@OverallWindowBeginTime,107) + N' ' + CONVERT(VARCHAR(20),@OverallWindowBeginTime,108) + N'.' + CONVERT(VARCHAR(20),DATEPART(MILLISECOND, @OverallWindowBeginTime))
			SET @lv__mtx__PrintOverallEndTime = CONVERT(VARCHAR(20),@OverallWindowEndTime,107) + N' ' + CONVERT(VARCHAR(20),@OverallWindowEndTime,108) + N'.' + CONVERT(VARCHAR(20),DATEPART(MILLISECOND, @OverallWindowEndTime))

			SET @lv__mtx__Replicate1 = @MatrixWidth - LEN(@lv__mtx__PrintOverallBeginTime) - LEN(@lv__mtx__PrintOverallEndTime);

			SET @OutputString = 
					CASE WHEN @HistOutput = N'XML' THEN N'<?JobHistory -- ' + NCHAR(10)
						ELSE N'' END + 
					--HoursBack and cell minute width labels
					N'@HoursBack parameter value: ' + CONVERT(NVARCHAR(20),@HoursBack) + N'   Each matrix cell = ' + CONVERT(NVARCHAR(20),@WindowLength_minutes) + N' minute(s)' + 

					--SQL/Agent Starttime labels
					CASE WHEN @SQLServerStartTime IS NULL THEN N'***** WARNING: could not determine last SQL Server DB engine start time *****'
						 WHEN @SQLServerStartTime = 2 THEN N'***** NOTE: SQL Server has started up at least once since the end time of this matrix *****'
						WHEN @SQLServerStartTime BETWEEN @OverallWindowBeginTime AND @OverallWindowEndTime 
							THEN NCHAR(10) + N'***** SQL Server DB engine started at ' + CONVERT(VARCHAR(20),@SQLServerStartTime,107) + N' ' + CONVERT(VARCHAR(20),@SQLServerStartTime,108) + N'.' + CONVERT(VARCHAR(20),DATEPART(MILLISECOND, @SQLServerStartTime)) + N' *****'
						ELSE N''
					END +

					CASE WHEN @SQLAgentStartTime IS NULL THEN N'***** WARNING: could not determine last SQL Agent start time *****'
						WHEN @SQLAgentStartTime = 2 THEN N'***** NOTE: Could not find the Agent start time immediately preceding this matrix *****'
						WHEN @SQLAgentStartTime BETWEEN @OverallWindowBeginTime AND @OverallWindowEndTime 
							AND ABS(DATEDIFF(MINUTE, @SQLServerStartTime, @SQLAgentStartTime)) > 1
							THEN NCHAR(10) + N'***** SQL Agent started at ' + CONVERT(VARCHAR(20),@SQLAgentStartTime,107) + N' ' + CONVERT(VARCHAR(20),@SQLAgentStartTime,108) + N'.' + CONVERT(VARCHAR(20),DATEPART(MILLISECOND, @SQLAgentStartTime)) + N' *****'
						ELSE N''
					END + NCHAR(10) + NCHAR(13) +

					--Begin/End timestamp labels
					N' ' + CASE WHEN @FitOnScreen = 'N' THEN REPLICATE(' ', @lv__mtx__MaxJobNameLength) ELSE '' END + @lv__mtx__PrintOverallBeginTime + REPLICATE(N' ', @lv__mtx__Replicate1) + @lv__mtx__PrintOverallEndTime + N' ' + NCHAR(10) + 
				@OutputString + 
				CASE WHEN @HistOutput = N'XML' THEN NCHAR(10) + NCHAR(13) + N'-- ?>'
					ELSE N'' END
				;
		END TRY
		BEGIN CATCH
			RAISERROR(N'Error occurred while constructing the final Matrix output string. The job history matrix will not be displayed.', 11, 1);
			SELECT @lv__ErrorText = ERROR_MESSAGE(), 
					@lv__ErrorSeverity	= ERROR_SEVERITY(), 
					@lv__ErrorState = ERROR_STATE();
			SET @lv__ErrorText = @lv__ErrorText + ' (Severity: ' + CONVERT(VARCHAR(20),@lv__ErrorSeverity) + ') (State: ' + CONVERT(VARCHAR(20),@lv__ErrorState) + ')'

			RAISERROR( @lv__ErrorText, 11, 1);
		END CATCH
	END 
	--ELSE
	--BEGIN
		--TODO: pred matrix
	--END

	SET @lv__afterdt = GETDATE();

	IF @Debug = 2
	BEGIN
		IF DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt) > @lv__slownessthreshold
		BEGIN
			SET @lv__ErrorText = N'   ***dbg: constructing final Matrix output took ' + CONVERT(NVARCHAR(40),DATEDIFF(MILLISECOND, @lv__beforedt, @lv__afterdt)) + N' milliseconds';
			RAISERROR(@lv__ErrorText, 10, 1) WITH NOWAIT;
		END
	END


	RETURN 0;
END
GO



