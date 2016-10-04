SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[QueryCamera]
/*   
	PROCEDURE:		AutoWho.QueryCamera

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: The AutoWho.QueryCamera proc snapshots session/request/task/tran state for a given spid (usually at
		a higher frequency than AutoWho's default) with the aim of showing a query's progress. The increased frequency 
		allows the consumer of the data greater ability to determine bottlenecks in the query.


	EXTENDED NOTES: The rules for how the parameters play together and with the loop are complex. 
		Here's the definition:

		If @PKSQLStmtStoreID is specified, "the query" is whatever sql_handle/offsets
			that maps to in the CorePE.SQLStmt
		If @PKSQLStmtStoreID is not specified, "the query" is whatever is seen first on
			this spid/request.


		- If @allcaptures="Y", (and therefore @captures is specified),
			the loop will execute X number of captures (where X = @captures) no matter what

				If @wait = 0 then the @lv__CaptureCounter starts incrementing right away,
					regardless of whether any query is seen or not.
				If @wait > 0 then the @lv__CaptureCounter doesn't start incrementing until 
					a query is seen OR the @wait # of seconds has passed, and the proc
					then continues capturing until the @captures limit has been hit.

		- If @allcaptures="N"  (@captures may or may not be specified)
			the loop will execute until @captures captures have been done or until
			"the query" is not seen anymore, whichever occurs first. At most 1000 captures
			are done.

				If @wait = 0 then the @lv__CaptureCounter starts incrementing right away,
					and if "the query" isn't seen on the first loop, the proc exits immediately.

				If @wait > 0 then the @lv__CaptureCounter doesn't start incrementing 
					until the query is seen or the wait delay has expired. Once the @wait delay passes, if the query
					still has not been seen, the proc exits.


	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-09-26	Aaron Morelli		Dev Begun


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
EXEC AutoWho.PrePopulateDimensions

*/
(
	@spid				INT,
	@request			INT,
	@frequency			INT,
	@captures			INT,
	@wait				INT,
	@allcaptures		NCHAR(1),
	@PKSQLStmtStoreID	BIGINT
)
AS
BEGIN

	SET NOCOUNT ON;

	DECLARE @lv__StartTime				DATETIME,
			@lv__SPIDCaptureTime		DATETIME,
			@lv__QuerySeen				NCHAR(1),
			@lv__KeepLooping			NCHAR(1),
			@lv__LoopCounter			INT,
			@lv__CaptureCounter			INT,
			@lv__SarRows				INT,
			@lv__sqlhandle				VARBINARY(64),
			@lv__statement_start_offset INT,
			@lv__statement_end_offset	INT,
			@lv__PKSQLStmtStoreID		BIGINT,
			@lv__PKQueryPlanStmtStoreID	BIGINT,		--the first time we resolve
			@lv__PKQueryPlanStmtStoreID_tmp	BIGINT,	--every subsequent time
			@lv__GatherRemainingData	NCHAR(1),

			@const__CaptureHardLimit	INT=1000
	;

	--Use a temp table to store our captures that actually obtained data
	CREATE TABLE #SuccessfulCaptures (
		LoopCounter		INT NOT NULL,
		CaptureCounter	INT NOT NULL,
		SPIDCaptureTime	DATETIME NOT NULL,
		PlanChanged		BIT NOT NULL
	);

	--If we were given a @PKSQLStmtStoreID value, obtain the sql handle/offsets for it now
	IF @PKSQLStmtStoreID IS NOT NULL
	BEGIN
		SELECT 
			@lv__sqlhandle = sss.sql_handle,
			@lv__statement_start_offset = sss.statement_start_offset,
			@lv__statement_end_offset = sss.statement_end_offset
		FROM CorePE.SQLStmtStore sss
		WHERE sss.PKSQLStmtStoreID = @PKSQLStmtStoreID
		AND sss.fail_to_obtain = 0;

		IF @lv__sqlhandle IS NULL OR @lv__sqlhandle = 0x0
		BEGIN
			--if the PK handed to us is non-existent or bad in some way, we
			-- essentially "unset" the ID so that the below code captures data
			-- for the first query it encounters on this spid/request
			SET @PKSQLStmtStoreID = NULL;
		END
	END

	SET @lv__StartTime = GETDATE(); 
	SET @lv__QuerySeen = N'N';
	SET @lv__KeepLooping = N'Y';
	SET @lv__GatherRemainingData = N'N';
	SET @lv__LoopCounter = 0;
	SET @lv__CaptureCounter = 0;

	WHILE @lv__KeepLooping = N'Y'
	BEGIN
		SET @lv__LoopCounter = @lv__LoopCounter + 1;
		SET @lv__SarRows = NULL; 

		IF @lv__QuerySeen = N'Y' OR @wait = 0
		BEGIN
			SET @lv__CaptureCounter = @lv__CaptureCounter + 1;
		END

		SET @lv__SPIDCaptureTime = GETDATE(); 

		--BIG INSERT into camera sar. Use a "1 = (CASE " construct to apply the sql_handle filter if specified

		--SET @lv__SarRows = @@ROWCOUNT;

		IF ISNULL(@lv__SarRows,0) = 0
		BEGIN
			--we did NOT see any row/"the query"

			--Do we increment our capture counter?
			IF @wait = 0
			BEGIN
				--Not waiting, so yes, increment immediately.
				SET @lv__CaptureCounter = @lv__CaptureCounter + 1;

				IF @allcaptures = N'N'
				BEGIN	--in a no-wait scenario, unless @allcaptures is Y we exit immediately.
					--this is true whether or not @lv__QuerySeen = Y.
					SET @lv__KeepLooping = N'N';
				END
				ELSE
				BEGIN
					--we're going to run @captures number of capture iterations, darnit! Only exit if we've done that.
					--note that since we're in the @allcaptures="Y' block, we know that @captures has a valid positive value.
					IF @lv__CaptureCounter > @captures
					BEGIN
						SET @lv__KeepLooping = N'N';
					END
				END
			END
			ELSE	--IF @wait = 0
			BEGIN
				--yes, we have/had a delay value. Have we seen the query yet?
				IF @lv__QuerySeen = N'Y'
				BEGIN	--Yes, so keep incrementing our capture counter
					SET @lv__CaptureCounter = @lv__CaptureCounter + 1;

					IF @allcaptures = N'N'
					BEGIN
						--told to wait, and we have see the query, but no longer. Exit
						SET @lv__KeepLooping = N'N';
					END
					ELSE
					BEGIN
						--told to wait, and we have seen the query, but no longer. However,
						-- user wants us to execute @captures loop iterations once we first saw the query
						IF @lv__CaptureCounter > @captures
						BEGIN
							SET @lv__KeepLooping = N'N';
						END
					END		--IF @allcaptures = N'N'
				END
				ELSE
				BEGIN
					--no, no sign of the query yet

					IF DATEADD(second, @wait, @lv__StartTime) <= GETDATE()
					BEGIN
						--we're past our wait/delay period. Increment the capture counter
						SET @lv__CaptureCounter = @lv__CaptureCounter + 1; 

						IF @allcaptures = N'N'
						BEGIN
							-- we've waited long enough and the user has not forced us to execute x number of captures
							SET @lv__KeepLooping = N'N';
						END
						ELSE
						BEGIN
							IF @lv__CaptureCounter > @captures
							BEGIN
								SET @lv__KeepLooping = N'N';
							END
						END
					END		--IF DATEADD(second, @wait, @lv__StartTime) <= GETDATE()
				END		--IF @lv__QuerySeen = N'Y'
			END	--IF @wait = 0
		END		--IF ISNULL(@lv__SarRows,0) = 0
		ELSE
		BEGIN
			--We DID get rows (hopefully just 1!)
			
			IF @lv__QuerySeen = N'N'
			BEGIN
				IF @PKSQLStmtStoreID IS NULL
				BEGIN
					--User didn't give a specific query to watch for. So by definition, THIS
					-- is our query to watch for. 
					
					--TODO: Obtain the sql handle/offset values,
					-- grab the SQL text, check the store, and get/create the PKSQLStmtStoreID value,
					-- storing it in @lv__PKSQLStmtStoreID

					--TODO: obtain the plan handle
					-- Grab the query plan, check the store, and get/create the PKQueryPlanStmtStoreID value,
					-- storing it in @lv__PKQueryPlanStmtStoreID

					SET @lv__QuerySeen = N'Y';
					SET @lv__GatherRemainingData = N'Y';
				END
				ELSE
				BEGIN
					--user gave us a query to look for and we've seen it!
					SET @lv__QuerySeen = N'Y';
					SET @lv__PKSQLStmtStoreID = @PKSQLStmtStoreID;
					SET @lv__GatherRemainingData = N'Y';
				END
			END	--IF @lv__QuerySeen = N'N'

			SET @lv__CaptureCounter = @lv__CaptureCounter + 1;

			IF @lv__CaptureCounter > ISNULL(NULLIF(@captures,0),1000)
			BEGIN
				SET @lv__GatherRemainingData = N'N';
				SET @lv__KeepLooping = N'N';
			END
		END		--IF ISNULL(@lv__SarRows,0) = 0


		--TODO: implement this huge thing
		IF @lv__GatherRemainingData = N'Y'
		BEGIN
			--persist to camera_TAW


			--persist to camera_transactiondetails


			--resolve the query plan info. We need to do this each time. 
			-- Place into @lv__PKQueryPlanStmtStoreID_tmp. If this differs 
			-- from @lv__PKQueryPlanStmtStoreID, we know we have a different
			-- plan (but the same statement!), and thus a different statement. If that happens,
			-- we need to invalidate this capture.
			-- (and what do we do with @allcaptures="Y"?)

			

			--Insert a success row into our #SuccessfulCaptures table
			INSERT INTO #SuccessfulCaptures (
				LoopCounter,
				CaptureCounter,
				SPIDCaptureTime,
				PlanChanged
			)
			SELECT @lv__LoopCounter, 
					@lv__CaptureCounter,
					@lv__SPIDCaptureTime,
					0			--TODO: set to 1 if plan changed
			;
		END		--IF @lv__GatherRemainingData = N'Y'
	END	--master WHILE loop



	RETURN 0;
END
GO
