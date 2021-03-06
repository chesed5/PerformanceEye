USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_PE_QueryProgress]
/*   
	PROCEDURE:		sp_PE_QueryProgress

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: 


	FUTURE ENHANCEMENTS: 
		


    CHANGE LOG:	
				2016-09-26	Aaron Morelli		Final run-through and commenting


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
	@start			DATETIME=NULL,			--if null, query live system (NOT YET IMPLEMENTED)
	@end			DATETIME=NULL,
	@offset			INT=99999,

	--filter variables
	@spid			INT,
	@request		INT=0,					--most of the time, user won't need to enter this
	@nodeassociate	NCHAR(1)=N'N',
	@units			NVARCHAR(10)=N'kb',		--"native" DMV raw presentation; "KB" kilobytes; "MB" megabytes
	@savespace		NCHAR(1)=N'N',

	/* think through these as I go
	--auxiliary info
	@attr			NCHAR(1)=N'N',			--Session & Connection attributes
	@resources		NCHAR(1)=N'N',			--TempDB, memory, reads/writes, CPU info
	@batch			NCHAR(1)=N'N',			-- Include the full SQL batch text. For historical data, only available if AutoWho was set to collect it.
	@plan			NVARCHAR(20)=N'none',	--"none", "statement", "full"		For historical data, ""statement" and "full" are only available if AutoWho was set to collect it.
	@ibuf			NCHAR(1)=N'N',
	@tran			NCHAR(1)=N'N',			-- Show transactions related to the spid.
	@waits			TINYINT=0,			--0 basic info; 1 adds more info about lock and latch waits; 
										-- 2 adds still more info for lock and latch waits; 3 displays aggregated dm_tran_locks data for SPIDs that were blockers or blockees, if available

	--Other options
	@directives		NVARCHAR(512)=N'',		
	*/
	@help			NVARCHAR(10)=N'N'		--params, columns, all
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET ANSI_PADDING ON;

	DECLARE @lv__ViewCurrent						BIT,
			@lv__HistoricalSPIDCaptureTime			DATETIME,
			@lv__effectiveordinal					INT,
			@scratch__int							INT,
			@helpstr								NVARCHAR(MAX),
			@helpexec								NVARCHAR(4000),
			@scratch__nvarchar						NVARCHAR(MAX),
			@err__msg								NVARCHAR(MAX),
			@lv__DynSQL								NVARCHAR(MAX),
			@lv__OptionsHash_str					NVARCHAR(4000),
			@lv__OptionsHash						VARBINARY(64),
			@lv__LastOptionsHash					VARBINARY(64)
			;

	SET @helpexec = N'
EXEC dbo.sp_PE_QueryProgress @start=''<start datetime>'',@end=''<end datetime>'', --@offset=99999,	--99999
						@spid=<int>, @request=0, @nodeassociate=N''N'', @units=N''native'',		--native / kb / mb
						@help=N''N''		--"query(ies)"

	';

	IF ISNULL(@Help,N'z') <> N'N'
	BEGIN
		GOTO helpbasic
	END

	DECLARE @lv__SQLVersion NVARCHAR(10);
	SELECT @lv__SQLVersion = (
	SELECT CASE
			WHEN t.col1 LIKE N'8%' THEN N'2000'
			WHEN t.col1 LIKE N'9%' THEN N'2005'
			WHEN t.col1 LIKE N'10.5%' THEN N'2008R2'
			WHEN t.col1 LIKE N'10%' THEN N'2008'
			WHEN t.col1 LIKE N'11%' THEN N'2012'
			WHEN t.col1 LIKE N'12%' THEN N'2014'
			WHEN t.col1 LIKE N'13%' THEN N'2016'
		END AS val1
	FROM (SELECT CONVERT(SYSNAME, SERVERPROPERTY(N'ProductVersion')) AS col1) AS t);

	DECLARE @dir__shortcols BIT;
	SET @dir__shortcols = CONVERT(BIT,0);

	DECLARE @lv__UtilityName NVARCHAR(30);
	SET @lv__UtilityName = N'QueryProgress';

	IF @start IS NULL
	BEGIN
		IF @end IS NOT NULL
		BEGIN
			SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
							);
			RAISERROR(@helpexec,10,1);
			RAISERROR('Parameter @end cannot have a value when @start is NULL or unspecified',16,1);
			RETURN -1;
		END

		--ok, so both are NULL. This run will look at live SQL DMVs
		SET @lv__ViewCurrent = CONVERT(BIT,1);
		SET @lv__effectiveordinal = NULL;	--n/a to current runs
	END
	ELSE
	BEGIN
		--ok, @start is non-null

		--Put @start's value into our helpexec string
		SET @helpexec = REPLACE(@helpexec,'<start datetime>', REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @start, 108) + '.' + 
														RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
						);

		IF @end IS NULL
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR('Parameter @end must have a value when @start has been specified.',16,1);
			RETURN -1;
		END

		--@end is also NOT NULL. Put it into our helpexec string
		SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
							);

		IF @start >= GETDATE() OR @end >= GETDATE()
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR('Neither of the parameters @start or @end can be in the future.',16,1);
			RETURN -1;
		END

		IF @end <= @start
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR('Parameter @end cannot be <= to parameter @start', 16, 1);
			RETURN -1;
		END

		SET @lv__ViewCurrent = CONVERT(BIT,0);

		--@offset must be specified for historical runs
		IF @offset IS NULL
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR('Parameter @offset must be non-null when @start is non-null.',16,1);
			RETURN -1;
		END
		ELSE
		BEGIN
			SET @lv__effectiveordinal = @offset;
		END
	END		--IF @start IS NULL

	IF ISNULL(@spid,-1) <= 0
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @spid cannot be NULL and must be a positive integer > 0; it should refer to a Session ID that was active between @start and @end.',16,1);
		RETURN -1;
	END

	IF ISNULL(@request,-1) < 0
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @request cannot be NULL and must be 0 or a positive integer; it should refer the request ID (defaults to 0) for the @spid specified.',16,1);
		RETURN -1;
	END

	SET @nodeassociate = UPPER(@nodeassociate);

	IF ISNULL(@nodeassociate,N'Z') NOT IN (N'Y', N'N')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @nodeassociate must be Y or N.',16,1);
		RETURN -1;
	END

	SET @units = UPPER(@units);

	IF ISNULL(@units,N'Z') NOT IN (N'NATIVE', N'KB', N'MB')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @units must be "native", "kb", or "mb".',16,1);
		RETURN -1;
	END

	SET @savespace = UPPER(@savespace);

	IF ISNULL(@savespace,N'Z') NOT IN (N'Y', N'N')
	BEGIN
		RAISERROR(@helpexec,10,1);
		RAISERROR('Parameter @savespace must be "Y" or "N"',16,1);
		RETURN -1;
	END

	CREATE TABLE #HistoricalCaptureTimes (
		hct		DATETIME NOT NULL	PRIMARY KEY CLUSTERED
	);

	/* The below comment is only applicable to sp_SessionViewer in historical mode.
	
	We have two different, but related, caching tables. 
		CorePE.OrdinalCachePosition --> acts a bit like a cursor in the sense that it keeps track of which "position" the user is at currently w/sp_QueryProgress.
			An "ordinal cache" has a key of Utility name/Start Time/End Time/session_id (spid of of the user running sp_QueryProgress). As the user repeatedly
			presses F5, the position increments by 1 each time (or decrements, if @offset = -99999 instead of the default 99999), and this incrementation is stored
			in the OrdinalCachePosition table.

		CorePE.CaptureOrdinalCache --> When a user first enters a Utility/Start Time/End Time (in this case, Utility="QueryProgress" of course), the CaptureOrdinalCache
			is populated with all of the SPID Capture Time values between Start Time and End Time. That is, the CorePE.CaptureOrdinalCache will hold every run of
			the AutoWho.Collector between @start and @end inclusive. All of those capture times are numbered from 1 to X and -1 to -X, in time order ascending and descending.
			Thus, given a number (e.g. 5), the table can be used to find the SPID Capture Time that is the 5th one in the series of captures starting with the first one >= @Start time,
			and ending with the last one <= @End time. Or if the number is -5, the table can be used to obtain the SPID Capture Time that is 5th from @End, going backwards towards @Start.
			
			As mentioned above, each time the user hits F5 to execute the proc, the position in CorePE.OrdinalCachePosition is incremented and returned/store in @lv__effectiveordinal,
			and then this position is used to probe into CorePE.CaptureOrdinalCache to find the SPID Capture Time that corresponds to the @lv__effectiveordinal in the @start/@end
			time series that has been specified. 

		This complicated design behind the scenes is to give the user a relatively simple experience in moving through time when examining AutoWho data.
	*/

	--The below code block sets/updates CorePE.OrdinalCachePosition appropriately.
	-- If @offset = 0, we simply ignore the position logic completely. This has the nice effect of
	-- letting the user start with a position, switch to @offset=0 partway through, then switch back to the position they were on
	-- seamlessly.
	IF @lv__ViewCurrent = CONVERT(BIT,0) AND @offset <> 0
	BEGIN
		--this is a historical run, so let's get our "effective position". A first-time run creates a position marker entry in the cache table,
		-- a follow-up run modifies the position marker.

		--Aaron 2016-05-28: We create a string of all the options used and then hash it.
		-- We exclude @start & @end b/c they are keys, and we exclude @offset in case the only
		-- option changed was @offset (from 99999 to -99999 or vice versa)

		SET @lv__OptionsHash_str = 
			/*
			N'@start=' + 
			CASE WHEN @start IS NULL THEN N'NULL' 
			ELSE REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-')+' '+CONVERT(NVARCHAR(20), @start, 108)+'.'+RIGHT(CONVERT(NVARCHAR(20),N'000')+CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
			END + N',@end=' + 
			CASE WHEN @end IS NULL THEN N'NULL' 
			ELSE REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-')+' '+CONVERT(NVARCHAR(20), @end, 108)+'.'+RIGHT(CONVERT(NVARCHAR(20),N'000')+CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
			END + 
			N',@offset=' +		ISNULL(CONVERT(nvarchar(20), @offset),N'NULL') + 
			*/
			N',@spid=' +	ISNULL(CONVERT(nvarchar(20),@spid),N'NULL') + 
			N',@help=' +		ISNULL(@help,N'NULL')
			;

		IF @lv__SQLVersion IN (N'2016')
		BEGIN
			--SHA1 is deprecated in 2016
			SET @lv__OptionsHash = HASHBYTES('SHA2_256',@lv__OptionsHash_str); 
		END
		ELSE
		BEGIN
			SET @lv__OptionsHash = HASHBYTES('SHA1',@lv__OptionsHash_str); 
		END

		IF NOT EXISTS (
			SELECT * FROM @@PEDATABASENAME@@.CorePE.OrdinalCachePosition t WITH (NOLOCK)
			WHERE t.Utility = @lv__UtilityName
			AND t.StartTime = @start
			AND t.EndTime = @end 
			AND t.session_id = @@SPID 
			)
		BEGIN
			INSERT INTO @@PEDATABASENAME@@.CorePE.OrdinalCachePosition
			(Utility, StartTime, EndTime, session_id, CurrentPosition, LastOptionsHash)
			SELECT @lv__UtilityName, @start, @end, @@SPID, 
				CASE WHEN @offset = 99999 THEN 1
					WHEN @offset = -99999 THEN -1
					ELSE @offset END,
				@lv__OptionsHash;
		END
		ELSE
		BEGIN	--cache already exists, so someone has already run with this start/endtime before on this spid
			--If @offset = 99999, we want to increment by 1		(Aaron 2016-05-28: but only if the Options Hash is the same)
			--If @offset = -99999, we want to decrement by 1		""
			--If offset = a different value, we want to set the position to that value
			SELECT 
				@lv__LastOptionsHash = LastOptionsHash
			FROM @@PEDATABASENAME@@.CorePE.OrdinalCachePosition
			WHERE Utility = @lv__UtilityName
			AND StartTime = @start
			AND EndTime = @end 
			AND session_id = @@SPID
			;

			IF @lv__LastOptionsHash <> @lv__OptionsHash
			BEGIN
				--user changed the options in some way. Retain the some position if @offset wasn't explicit
				UPDATE @@PEDATABASENAME@@.CorePE.OrdinalCachePosition
				SET LastOptionsHash = @lv__OptionsHash, 
					CurrentPosition = CASE WHEN @offset IN (99999,-99999) THEN CurrentPosition
										ELSE @offset
										END
				WHERE Utility = @lv__UtilityName
				AND StartTime = @start
				AND EndTime = @end 
				AND session_id = @@SPID
				;
			END
			ELSE
			BEGIN
				--options stayed the same, in/decrement the position
				UPDATE @@PEDATABASENAME@@.CorePE.OrdinalCachePosition
				SET CurrentPosition = CASE 
					WHEN @offset = 99999 THEN CurrentPosition + 1
					WHEN @offset = -99999 THEN CurrentPosition - 1
					ELSE @offset 
					END
				WHERE Utility = @lv__UtilityName
				AND StartTime = @start
				AND EndTime = @end 
				AND session_id = @@SPID
				;
			END 
		END		--if cache already exists

		SELECT @lv__effectiveordinal = t.CurrentPosition
		FROM @@PEDATABASENAME@@.CorePE.OrdinalCachePosition t
		WHERE t.Utility = @lv__UtilityName 
		AND t.StartTime = @start
		AND t.EndTime = @end 
		AND t.session_id = @@SPID
		;
	END		--if historical run and @offset <> 0


	--is it a historical run?
	IF @lv__ViewCurrent = CONVERT(BIT, 0)
	BEGIN
		--Regardless of whether we are pulling for a specific ordinal or pulling for a range, we need to ensure
		-- that the capture summary table has all appropriate data in the time range. However, the way that CaptureSummary
		-- is populated differs slightly based on the value in @offset/@lv__effectiveordinal. 
		--If @offset=0, the user isn't using the position marker cache, so we don't need to interact at all with CorePE.OrdinalCachePosition.
		-- In fact, we don't need to interact at all with CorePE.CaptureOrdinalCache either, as we just need a list of all the 
		-- SPIDCaptureTime values between @start and @end. So, for @offset=0, we just check that CaptureSummary is up-to-date 
		-- based on AutoWho.CaptureTimes rows, and then we pull a list of datetime values.
		IF @lv__effectiveordinal = 0
		BEGIN
			IF EXISTS (SELECT * FROM @@PEDATABASENAME@@.AutoWho.CaptureTimes ct
						WHERE ct.RunWasSuccessful = 1
						AND ct.CaptureSummaryPopulated = 0
						AND ct.SPIDCaptureTime BETWEEN @start AND @end)
			BEGIN
				EXEC @scratch__int = @@PEDATABASENAME@@.AutoWho.PopulateCaptureSummary @StartTime = @start, @EndTime = @end; 
					--returns 1 if no rows were found in the range
					-- -1 if there was an unexpected exception
					-- 0 if success

				IF @scratch__int = 1
				BEGIN
					--no rows for this range. Return special code 2 and let the caller decide what to do
					RAISERROR(@helpexec,10,1);
					RAISERROR('
			There is no AutoWho data for the time range specified.',10,1);
					RETURN 1;
				END

				IF @scratch__int < 0
				BEGIN
					SET @err__msg = 'Unexpected error occurred while retrieving the AutoWho data. More info is available in the AutoWho log under the tag "SummCapturePopulation" or contact your administrator.'
					RAISERROR(@err__msg, 16, 1);
					RETURN -1;
				END
			END
		END
		ELSE
		BEGIN
			--However, if @offset is <> 0, then @lv__effectiveordinal is either < 0 or > 0. In that case, we DO need to 
			-- interact with the position cache and, since we're after just one SPID Capture Time value referenced by an offset, 
			-- the CaptureOrdinalCache table. But again, there's no guarantee that the CaptureSummary table is up-to-date.
			-- Thus, we rely on the fact that the procedure CorePE.RetrieveOrdinalCacheEntry will populate the CaptureSummary
			-- and the CaptureOrdinalCache if those are not yet populated. 

			SET @lv__HistoricalSPIDCaptureTime = NULL;

			--First, optimistically assume that the cache already exists, and grab the ordinal's HCT
			IF @lv__effectiveordinal < 0 
			BEGIN
				SELECT @lv__HistoricalSPIDCaptureTime = c.CaptureTime
				FROM @@PEDATABASENAME@@.CorePE.CaptureOrdinalCache c
				WHERE c.Utility = @lv__UtilityName
				AND c.StartTime = @start
				AND c.EndTime = @end
				AND c.OrdinalNegative = @lv__effectiveordinal;
			END
			ELSE IF @lv__effectiveordinal > 0
			BEGIN
				SELECT @lv__HistoricalSPIDCaptureTime = c.CaptureTime
				FROM @@PEDATABASENAME@@.CorePE.CaptureOrdinalCache c
				WHERE c.Utility = @lv__UtilityName
				AND c.StartTime = @start
				AND c.EndTime = @end
				AND c.Ordinal = @lv__effectiveordinal;
			END

			--If still NULL, the cache may not exist, or the ordinal is out of range. 
			IF @lv__HistoricalSPIDCaptureTime IS NULL
			BEGIN
				SET @scratch__int = NULL;
				SET @scratch__nvarchar = NULL;
				EXEC @scratch__int = @@PEDATABASENAME@@.CorePE.RetrieveOrdinalCacheEntry @ut = @lv__UtilityName, @st=@start, 
					@et=@end, @ord=@lv__effectiveordinal, @hct=@lv__HistoricalSPIDCaptureTime OUTPUT, @msg=@scratch__nvarchar OUTPUT;

					--returns 0 if successful,
					-- -1 if exception occurred
					-- 1 if the ordinal passed is out-of-range
					-- 2 or 3 if there is no AutoWho data for the time range specified

				IF @scratch__int IS NULL OR @scratch__int < 0
				BEGIN
					SET @err__msg = 'Unexpected error occurred while retrieving the AutoWho data. More info is available in the AutoWho log under the tag "RetrieveOrdinalCache", or contact your administrator.'
					RAISERROR(@err__msg, 16, 1);
					RETURN -1;
				END

				IF @scratch__int = 1
				BEGIN
					IF @scratch__nvarchar IS NULL
					BEGIN
						IF @lv__effectiveordinal < 0
						BEGIN
							SET @scratch__nvarchar = N'The value passed in for parameter @offset is out of range. Try a larger (closer to zero) value.';
						END
						ELSE
						BEGIN
							SET @scratch__nvarchar = N'The value passed in for parameter @offset is out of range. Try a smaller value.';
						END
					END

					RAISERROR(@helpexec,10,1);
					RAISERROR(@scratch__nvarchar,16,1);
					RETURN -1;
				END

				IF @scratch__int IN (2,3)
				BEGIN
					RAISERROR(@helpexec,10,1);
					RAISERROR('There is no AutoWho data for the time range specified.',10,1);
					RETURN 1;
				END
			END		--IF @lv__HistoricalSPIDCaptureTime IS NULL
		END		--IF @lv__effectiveordinal = 0

		--PRINT CONVERT(VARCHAR(20),@lv__HistoricalSPIDCaptureTime,102) + ' ' + CONVERT(VARCHAR(20), @lv__HistoricalSPIDCaptureTime,108);
		--return 0;


		IF @lv__effectiveordinal = 0
		BEGIN
			INSERT INTO #HistoricalCaptureTimes (
				hct
			)
			SELECT DISTINCT cs.SPIDCaptureTime
			FROM @@PEDATABASENAME@@.AutoWho.CaptureSummary cs
			WHERE cs.SPIDCaptureTime BETWEEN @start AND @end;

			DECLARE iterateHCTs CURSOR FOR
			SELECT hct 
			FROM #HistoricalCaptureTimes
			ORDER BY hct ASC;

			OPEN iterateHCTs;
			FETCH iterateHCTs INTO @lv__HistoricalSPIDCaptureTime;

			WHILE @@FETCH_STATUS = 0
			BEGIN
				EXEC @@PEDATABASENAME@@.AutoWho.ViewHistoricalQueryProgress @hct = @lv__HistoricalSPIDCaptureTime, 
					@spid = @spid,
					@rqst = @request,
					@nodeassociation=@nodeassociate,
					@units=@units,
					/*
					@attr = @attr,
					@resource = @resources,
					@batch = @batch,
					@plan = @plan,
					@ibuf = @ibuf,
					@bchain = @bchain,
					@waits = @waits,
					@tran = @tran,
					*/
					@savespace = @savespace,
					@effectiveordinal = @lv__effectiveordinal,
					@dir = N''; --todo: implement? @directives;

				FETCH iterateHCTs INTO @lv__HistoricalSPIDCaptureTime;
			END

			CLOSE iterateHCTs;
			DEALLOCATE iterateHCTs;
		END
		ELSE
		BEGIN
			--Just executing for 1 SPID Capture time
			EXEC @@PEDATABASENAME@@.AutoWho.ViewHistoricalQueryProgress @hct = @lv__HistoricalSPIDCaptureTime, 
				@spid = @spid,
				@rqst = @request,
				@nodeassociation=@nodeassociate,
				@units=@units,
				/*
				@attr = @attr,
				@resource = @resources,
				@batch = @batch,
				@plan = @plan,
				@ibuf = @ibuf,
				@bchain = @bchain,
				@waits = @waits,
				@tran = @tran,
				*/
				@savespace = @savespace,
				@effectiveordinal = @lv__effectiveordinal,
				@dir = N'';		--todo: implement? @directives;
		END	--IF @lv__effectiveordinal = 0

		--we always print out at least the EXEC command
		GOTO helpbasic
	END
	ELSE
	BEGIN
		IF has_perms_by_name(null, null, 'VIEW SERVER STATE') <> 1
		BEGIN
			RAISERROR(@helpexec,10,1);
			RAISERROR(N'The VIEW SERVER STATE permission (or permissions/role membership that include VIEW SERVER STATE) is required to execute sp_QueryProgress. Exiting...', 11,1);
			RETURN -1;
		END
		ELSE
		BEGIN
			--TODO: query the live system

			--we always print out at least the EXEC command
			GOTO helpbasic
		END
		RETURN 0;
	END		--IF @lv__ViewCurrent = CONVERT(BIT,0)


helpbasic:

	IF @Help <> N'N'
	BEGIN
		IF @Help NOT IN (N'params', N'columns', N'all')
		BEGIN
			--user may have typed gibberish... which is ok, give him/her all the help
			SET @Help = N'all'
		END
	END

	--If the user DID enter @start/@end info, then we use those values to replace the <datetime> tags
	-- in the @helpexec string.
	IF @start IS NOT NULL
	BEGIN
		SET @helpexec = REPLACE(@helpexec,'<start datetime>', REPLACE(CONVERT(NVARCHAR(20), @start, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @start, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @start)),3)
							);
	END 

	IF @end IS NOT NULL 
	BEGIN
		SET @helpexec = REPLACE(@helpexec,'<end datetime>', REPLACE(CONVERT(NVARCHAR(20), @end, 102),'.','-') + ' ' + CONVERT(NVARCHAR(20), @end, 108) + '.' + 
															RIGHT(CONVERT(NVARCHAR(20),N'000') + CONVERT(NVARCHAR(20),DATEPART(MILLISECOND, @end)),3)
							);
	END 


	SET @helpstr = @helpexec;
	RAISERROR(@helpstr,10,1) WITH NOWAIT;

	IF @Help=N'N'
	BEGIN
		RETURN 0;
	END

	IF @Help NOT IN (N'params',N'all')
	BEGIN
		GOTO helpcolumns
	END

helpparams:
	SET @helpstr = N'
Parameters
---------------------------------------------------------------------------------------------------------------------------------------------------------------
TODO
';
	RAISERROR(@helpstr,10,1);

	IF @Help = N'params'
	BEGIN
		GOTO exitloc
	END
	ELSE
	BEGIN
		SET @helpstr = N'
		';
		RAISERROR(@helpstr,10,1);
	END

helpcolumns:
	SET @helpstr = N'
Columns
---------------------------------------------------------------------------------------------------------------------------------------------------------------
TODO';

	RAISERROR(@helpstr,10,1);

exitloc:

	RETURN 0;
END

GO
