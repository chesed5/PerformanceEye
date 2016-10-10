SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CorePE].[RetrieveOrdinalCacheEntry] 
/*   
	PROCEDURE:		CorePE.RetrieveOrdinalCacheEntry

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: For a given utility (@ut), and a start/end range (@st/@et), and an ordinal in that range (@ord),
		finds the historical Capture Time (@hct) that corresponds to that ordinal. The first time that a @st/@et
		pair (e.g. "2016-04-24 09:00", "2016-04-24 09:30") is passed into this proc


		This proc has 3 ways of ending:
			1. Finds the @hct successfully and returns 0

			2. Doesn't find the @hct, but this occurs in such a way as to not be worthy of an exception, but rather 
				of just a warning message and a positive return code.

				This gives the calling proc the choice on how to handle the inability to obtain an @hct.

			3. Fails in some way worthy of an exception, and a RETURN -1;

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
EXEC CorePE.RetrieveOrdinalCacheEntry
*/
(
	@ut NVARCHAR(20),
	@st DATETIME,
	@et DATETIME,
	@ord INT,
	@hct DATETIME OUTPUT,
	@msg NVARCHAR(MAX) OUTPUT
)
AS
BEGIN
	
	/*
	The ordinal cache works as follows:
		even though there is only a single table (i.e. denormalized), 
		the ordinal cache is really a series of caches, each of which has a StartTime and EndTime.
		In fact, there can only be one cache for each StartTime/EndTime pair. (Pairs can overlap each other).
		In a pair, both the StartTime & EndTime must be in the past when the cache is first requested.

		When a cache doesn't exist, the AutoWho.CaptureTimes table is first queried to determine whether 
		there is any AutoWho capture data with "CaptureSummaryPopulated=0" for the time range specified by
		the @st and @et parameters to this procedure. If so, this means that while
		the various detail tables have data, the CaptureSummary table hasn't yet been populated for those
		capture times. Thus, this triggers a call to AutoWho.PopulateCaptureSummary to do the population
		for the time range of @st/@et. 

		The actual ordinal cache is then created for @st/@et from the data in the CaptureSummary table. 

		Now, what about invalidation? The user can't specify times in the future, but what if he/she specifies
		a time far enough in the past that data has actually been purged? We handle this by purging the
		older data in the CaptureTimes table (policy controlled by "Retention_CaptureTimes" option in the
		AutoWho.Options table). When a new ordinal cache is requested, if the @et value is older than 
		the very oldest record in the CaptureTimes table, we let the user know that there is no data.

	*/
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	SET @msg = N'';

	DECLARE @codeloc VARCHAR(20),
		@scratchint INT;

	IF ISNULL(@ord,0) = 0
	BEGIN
		SET @msg = N'Parameter @ord must be a non-null, positive or negative number (0 is not allowed)';
		RETURN -1;
	END

	IF @ut IS NULL
	BEGIN
		SET @msg = N'Parameter @ut cannot be null.'
		RETURN -1;
	END

	BEGIN TRY
		--optimistically assume that the entry is already there.
		IF @ord > 0 
		BEGIN
			SET @codeloc = 'HCT1';
			SELECT @hct = c.CaptureTime
			FROM CorePE.CaptureOrdinalCache c
			WHERE c.Utility = @ut
			AND c.StartTime = @st
			AND c.EndTime = @et
			AND c.Ordinal = @Ord;
		END
		ELSE
		BEGIN
			SET @codeloc = 'HCT2';
			SELECT @hct = c.CaptureTime
			FROM CorePE.CaptureOrdinalCache c
			WHERE c.Utility = @ut 
			AND c.StartTime = @st
			AND c.EndTime = @et
			AND c.OrdinalNegative = @Ord;
		END

		IF @hct IS NULL
		BEGIN
			--We weren't able to get a historical capture time from this ordinal cache. 
			-- We can check to see if the cache even exists by doing this:
			IF @ord > 0
			BEGIN
				SET @codeloc = 'Cexists1';
				SELECT @scratchint = ss.Ordinal
				FROM (
					SELECT TOP 1 c.Ordinal		--find the latest row in the cache (if the cache even exists) and get the ordinal
					FROM CorePE.CaptureOrdinalCache c
					WHERE c.Utility = @ut
					AND c.StartTime = @st
					AND c.EndTime = @et
					ORDER BY c.Ordinal DESC
				) ss;
			END
			ELSE
			BEGIN
				SET @codeloc = 'Cexists2';
				SELECT @scratchint = ss.OrdinalNegative
				FROM (
					SELECT TOP 1 c.OrdinalNegative		--find the earliest row in the cache (if the cache even exists) and get the ordinal
					FROM CorePE.CaptureOrdinalCache c
					WHERE c.Utility = @ut 
					AND c.StartTime = @st
					AND c.EndTime = @et
					ORDER BY c.OrdinalNegative ASC
				) ss;
			END

			IF @scratchint IS NOT NULL
			BEGIN
				--the cache exists, the user simply entered too high of an ordinal
				SET @msg = N'The ordinal value specified (' + CONVERT(VARCHAR(20),@ord) + 
					') is outside the range for the @StartTime/@EndTime time range specified. The furthest value in this direction is "' + 
					CONVERT(VARCHAR(20),@scratchint) + '".'
				RETURN 1;
			END
			ELSE
			BEGIN
				--the cache doesn't exist, yet. Our logic here branches based on which utility we're supporting

				IF @ut IN (N'SessionViewer', N'QueryProgress')		--QueryProgress and SessionViewer both are based on the same AutoWho tables.
				BEGIN
					--If the cache doesn't exist yet, then we need to create it, of course.
					-- Technically, we could do this from the AutoWho.CaptureTimes table directly,
					-- which just holds a list of all SPIDCaptureTimes that have occurred for the AutoWho.Collector procedure.
					-- However, we want to ensure that the AutoWho.CaptureSummary table is populated for the 
					-- range we've been given, because as the user iterates through the AutoWho data in
					-- the order specified in their CaptureOrdinalCache, some of the fields in the 
					-- Capture Summary table will be useful to help the Auto Who viewer procedure formulate
					-- its queries. (e.g. one optimization is that even if the user wants to see blocking graph
					-- info, if the Capture Summary indicates that there was no Blocking Graph generated for a given
					-- capture time, then we can skip even looking at the blocking graph table at all).
					SET @codeloc = 'CapSummPopEqZero1';
					IF EXISTS (SELECT * FROM AutoWho.CaptureTimes t 
							WHERE t.SPIDCaptureTime BETWEEN @st and @et 
							AND RunWasSuccessful = 1
							AND CaptureSummaryPopulated = 0)
					BEGIN
						SET @codeloc = 'ExecPopCapSumm';
						EXEC @scratchint = AutoWho.PopulateCaptureSummary @StartTime = @st, @EndTime = @et; 
							--returns 1 if no rows were found in the range
							-- -1 if there was an unexpected exception
							-- 0 if success

						IF @scratchint = 1
						BEGIN
							--no rows for this range. Return special code 2 and let the caller decide what to do
							SET @msg = N'No AutoWho data exists for the time window specified by @StartTime/@EndTime.'
							RETURN 2;
						END

						IF @scratchint < 0
						BEGIN
							SET @msg = N'An error occurred when reviewing AutoWho capture data for the time window specified by @StartTime/@EndTime. ';
							SET @msg = @msg + 'Please consult the AutoWho log, for LocationTag="SummCapturePopulation" or contact your administrator';
							RAISERROR(@msg, 16, 1);
							RETURN -1;
						END
					END

					--Ok, the AutoWho.CaptureSummary table now has entries for all of the capture times that occurred
					-- between @st and @et. Now, build our cache
					SET @codeloc = 'CapOrdCache1';
					INSERT INTO CorePE.CaptureOrdinalCache
					(Utility, StartTime, EndTime, Ordinal, OrdinalNegative, CaptureTime)
					SELECT 
						@ut, @st, @et, 
						Ordinal = ROW_NUMBER() OVER (ORDER BY t.SPIDCaptureTime ASC),
						OrdinalNegative = 0 - ROW_NUMBER() OVER (ORDER BY t.SPIDCaptureTime DESC),
						t.SPIDCaptureTime
					FROM AutoWho.CaptureSummary t
					WHERE t.SPIDCaptureTime BETWEEN @st AND @et 
					;

					SET @scratchint = @@ROWCOUNT;

					IF @scratchint = 0
					BEGIN
						SET @msg = N'Ordinal cache was built for @StartTime "' + CONVERT(nvarchar(20),@st) + 
							'" and @EndTime "' + CONVERT(nvarchar(20),@et) + '" but no AutoWho data was found.';
						RETURN 2;
					END

					--Ok, the cache we just created had rows. Now try to get our capture time for this ordinal all over again:
					SET @hct = NULL;

					IF @ord > 0
					BEGIN
						SET @codeloc = 'HCT3';
						SELECT @hct = c.CaptureTime
						FROM CorePE.CaptureOrdinalCache c
						WHERE c.Utility = @ut 
						AND c.StartTime = @st
						AND c.EndTime = @et
						AND c.Ordinal = @Ord;
					END
					ELSE
					BEGIN
						SET @codeloc = 'HCT4';
						SELECT @hct = c.CaptureTime
						FROM CorePE.CaptureOrdinalCache c
						WHERE c.Utility = @ut 
						AND c.StartTime = @st
						AND c.EndTime = @et
						AND c.OrdinalNegative = @Ord;
					END

					IF @hct IS NULL
					BEGIN
						IF @ord > 0
						BEGIN
							SET @codeloc = 'OrdGet1';
							SELECT @scratchint = ss.Ordinal
							FROM (
								SELECT TOP 1 c.Ordinal
								FROM CorePE.CaptureOrdinalCache c
								WHERE c.Utility = @ut 
								AND c.StartTime = @st
								AND c.EndTime = @et
								ORDER BY c.Ordinal DESC
							) ss;
						END
						ELSE
						BEGIN
							SET @codeloc = 'OrdGet2';
							SELECT @scratchint = ss.OrdinalNegative
							FROM (
								SELECT TOP 1 c.OrdinalNegative
								FROM CorePE.CaptureOrdinalCache c
								WHERE c.Utility = @ut 
								AND c.StartTime = @st
								AND c.EndTime = @et
								ORDER BY c.OrdinalNegative ASC
							) ss;
						END

						IF @scratchint IS NOT NULL
						BEGIN
							--the cache exists, it just doesn't have enough entries to match the ordinal #
							SET @msg = N'The ordinal value specified (' + CONVERT(VARCHAR(20),@ord) + 
								') is outside the range for the @StartTime/@EndTime time range specified. The furthest value in this direction is "' + 
								CONVERT(VARCHAR(20),@scratchint) + '".'
							RETURN 1;
						END
					END
					ELSE
					BEGIN
						SET @msg = N'Success';
						RETURN 0;
					END		--IF @hct IS NULL second try
				END --IF @ut IN (N'SessionViewer', N'QueryProgress')

				--TODO: similar "cache doesn't exist yet" logic for other utilities
			END	--IF @scratchint IS NOT NULL first try
		END	--IF @hct IS NULL first try
		ELSE
		BEGIN
			SET @msg = N'Success';
			RETURN 0;
		END
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK;
		SET @msg = 'Unexpected exception encountered in ' + OBJECT_NAME(@@PROCID) + ' procedure, at location: ' + ISNULL(@codeloc,'<null>');
		SET @msg = @msg + ' Error #: ' + CONVERT(varchar(20),ERROR_NUMBER()) + '; State: ' + CONVERT(varchar(20),ERROR_STATE()) + 
			'; Severity: ' + CONVERT(varchar(20),ERROR_SEVERITY()) + '; msg: ' + ERROR_MESSAGE();

		--log location depends on utility
		IF @ut IN (N'AutoWho', N'SessionViewer', N'QueryProgress')
		BEGIN
			INSERT INTO AutoWho.[Log] (
				LogDT, TraceID, ErrorCode, LocationTag, LogMessage 
			)
			VALUES (SYSDATETIME(), NULL, ERROR_NUMBER(), N'RetrieveOrdinalCache', @msg);
		END
		--TODO: other utility log writes go here

		RETURN -1;
	END CATCH

	RETURN 0;
END

GO
