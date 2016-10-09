SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [PerformanceEye].[ProfilerTraceBySPID_Stop]
/*   
	PROCEDURE:		PerformanceEye.ProfilerTraceBySPID_Stop

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: Called ad-hoc by users of PerformanceEye when wanting to stop a trace started
		by [PerformanceEye].[ProfilerTraceBySPID_Start]

		Note that error handling for this proc is carefully constructed not to raise any
		exceptions, and no transaction management is undertaken. This is to avoid any
		disruption at all to the calling code. The goal is for the calling code to 
		call this proc to start the trace, then call it later to close the trace, and
		otherwise not have to worry about any disruption to its logic.

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-10-06	Aaron Morelli		Incorporated into PerformanceEye

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
minimal param usage:
DECLARE @lmsg NVARCHAR(MAX);
EXEC [PerformanceEye].[ProfilerTraceBySPID_Start] @TraceCategories=N'Performance,Stored Procedures', 
												@IncludePerfWarnings=N'Y',
												@SPID=NULL,			--defaults to current SPID
												@Duration=250000,	--250 ms will ignore lots of unimportant statements
												@ReturnMessage=@lmsg OUTPUT
												;
	--get more categories from SELECT DISTINCT category_name FROM CorePE.ProfilerTraceEvents

Then to stop the trace, call:

*/
(
	@TID				INT				= NULL,		--user can either stop the trace via a TID or via SPID
	@SPID				INT				= NULL,
	@SearchByBoth		NCHAR(1)		= N'N',		-- If both are specified, user must tell us that we can use either to find the trace
													-- We default this to "N" because of the possibility of accidentally stopping the
													-- wrong trace. For example, if the trace handle isn't found, but a trace *is*
													-- found indirectly (via CorePE.Traces & a SPID #), how do we know the sys.traces
													-- trace is the one we really are to stop? So there is an element of uncertainty
													-- here that we need to user to agree upon.
	@ReturnMessage 		NVARCHAR(MAX)	= NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @ConfirmedTraceID INT,
			@FullPathname NVARCHAR(512),
			@TraceCreateTime DATETIME,
			@TraceStatus INT,
			@TraceFound NCHAR(1)=N'N',
			@rc INT,
			@ErrorCode INT;

	IF @TID IS NULL AND @SPID IS NULL
	BEGIN
		SET @ReturnMessage = N'Either the @TID or @SPID parameters must be specified (as a positive integer).';
		RETURN -1;
	END

	IF ISNULL(@TID,-99) < 1
	BEGIN
		SET @ReturnMessage = N'If specified, the @TID parameter must be a positive integer.';
		RETURN -2;
	END

	IF ISNULL(@SPID,-99) < 1
	BEGIN
		SET @ReturnMessage = N'If specified, the @SPID parameter must be a positive integer.';
		RETURN -3;
	END

	IF @TID IS NOT NULL AND @SPID IS NOT NULL
	BEGIN
		IF @SearchByBoth = N'N'
		BEGIN
			--B/c user didn't "authorize" searching by both, we disable @SPID searching and rely
			-- completely on the trace handle searching.
			SET @SPID = NULL;
		END
	END

	IF @TID IS NOT NULL
	BEGIN
		IF EXISTS (SELECT * FROM sys.traces t WHERE t.id = @TID)
		BEGIN
			SET @TraceFound = N'Y';
			SET @ConfirmedTraceID = @TID;
		END
	END

	IF @TraceFound = N'N'
	BEGIN
		IF @SPID IS NULL
		BEGIN
			--We already failed at searching by @TID, so give up.
			SET @ReturnMessage = N'Error: SQLTrace with Trace handle of ' + CONVERT(NVARCHAR(20), @TID) + ' not found. Exiting...';
			SET @ErrorCode = -4;
			INSERT INTO CorePE.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
			RETURN @ErrorCode;
		END
		ELSE
		BEGIN
			--search by SPID in our CorePE traces table
			SET @TID = NULL;
			SET @TraceCreateTime = NULL; 

			SELECT 
				 @TID = t.Payload_bigint,
				 @TraceCreateTime = t.Payload_datetime
			FROM CorePE.Traces t
			WHERE t.Payload_int = @SPID;

			IF @TID IS NULL
			BEGIN
				SET @ReturnMessage = N'Unable to find PerformanceEye trace registration by SPID (' + 
						ISNULL(CONVERT(NVARCHAR(20),@SPID),N'<null>') + '). Exiting...';
				SET @ErrorCode = -5;
				INSERT INTO CorePE.[Log]
					(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
				RETURN @ErrorCode;
			END
			ELSE
			BEGIN
				--compare with the actual sys.traces to see if it is a match
				SET @ConfirmedTraceID = NULL; 
				SELECT 
					@ConfirmedTraceID = t.id,
					@TraceStatus = t.[status]
				FROM sys.traces t
				WHERE t.id = @TID 
				AND ABS(DATEDIFF(MINUTE, @TraceCreateTime, t.start_time)) <= 2
				AND t.is_default <> 1				--um... yeah, don't stop that one.
				;

				IF @ConfirmedTraceID IS NULL
				BEGIN
					SET @ReturnMessage = N'Unable to find PerformanceEye trace registration by TID (' + 
						ISNULL(CONVERT(NVARCHAR(20),@TID),N'<null>') + ') and start time (' + 
						ISNULL(CONVERT(NVARCHAR(20),@TraceCreateTime),N'<null>') + '). Exiting...';
					SET @ErrorCode = -6;
					INSERT INTO CorePE.[Log]
						(LogDT, ErrorCode, LocationTag, LogMessage)
					SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
					RETURN @ErrorCode;
				END
				BEGIN
					SET @TraceFound = N'Y';
				END
			END
		END		--IF @SPID IS NULL
	END		--IF @TraceFound = N'N'

	--If we get here we found the trace.
	IF @TraceStatus = 1
	BEGIN
		BEGIN TRY
			EXEC @rc = sp_trace_setstatus @ConfirmedTraceID, 0;

			IF ISNULL(@rc,99) <> 0
			BEGIN
				SET @ReturnMessage = N'sp_trace_setstatus (when stopping a running trace) returned non-zero return code: ' + ISNULL(CONVERT(NVARCHAR(20),@rc),N'<null>');
				SET @ErrorCode = -7;
				INSERT INTO CorePE.[Log]
					(LogDT, ErrorCode, LocationTag, LogMessage)
				SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
				RETURN @ErrorCode;
			END
		END TRY
		BEGIN CATCH
			SET @ReturnMessage = N'Unexpected exception occurred while stopping a running trace. Error # ' + 
				CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; State ' + 
				CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; Sev ' + 
				CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; Line ' + 
				CONVERT(NVARCHAR(20),ERROR_LINE()) + N'; Msg ' + 
				ERROR_MESSAGE();
			SET @ErrorCode = -8;
			INSERT INTO CorePE.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
			RETURN @ErrorCode;
		END CATCH
	END		--IF @TraceStatus = 1
	
	--trace should now be in state = 0 (stopped). Remove it.
	BEGIN TRY
		EXEC @rc = sp_trace_setstatus @ConfirmedTraceID, 2;

		IF ISNULL(@rc,99) <> 0
		BEGIN
			SET @ReturnMessage = N'sp_trace_setstatus (when removing a stopped trace) returned non-zero return code: ' + ISNULL(CONVERT(NVARCHAR(20),@rc),N'<null>');
			SET @ErrorCode = -9;
			INSERT INTO CorePE.[Log]
				(LogDT, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
			RETURN @ErrorCode;
		END
	END TRY
	BEGIN CATCH
		SET @ReturnMessage = N'Unexpected exception occurred while removing a stopped trace. Error # ' + 
			CONVERT(NVARCHAR(20),ERROR_NUMBER()) + N'; State ' + 
			CONVERT(NVARCHAR(20),ERROR_STATE()) + N'; Sev ' + 
			CONVERT(NVARCHAR(20),ERROR_SEVERITY()) + N'; Line ' + 
			CONVERT(NVARCHAR(20),ERROR_LINE()) + N'; Msg ' + 
			ERROR_MESSAGE();
		SET @ErrorCode = -10;
		INSERT INTO CorePE.[Log]
			(LogDT, ErrorCode, LocationTag, LogMessage)
		SELECT SYSDATETIME(), @ErrorCode, OBJECT_NAME(@@PROCID), @ReturnMessage
		RETURN @ErrorCode;
	END CATCH

	SET @ReturnMessage = N'Trace with ID ' + ISNULL(CONVERT(NVARCHAR(20),@ConfirmedTraceID),N'<null>') + ' removed successfully.';
	RETURN 0;
END 
