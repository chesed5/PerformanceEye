SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CorePE].[StopTrace]
/*   
	PROCEDURE:		CorePE.StopTrace

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: This is the "more graceful" way to stop a CorePE trace (than CorePE.StopTrace). @AbortCode
		can be used to show whether the trace was stopped with any sort of problem. 

		@Utility is either "AutoWho" or "ServerEye" at this time. 

		@TraceID cannot be NULL (unlike AutoWho.AbortTrace), since it is assumed that whatever started the trace
		will keep the handle (ID) to that trace until ready to stop that trace.

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
EXEC CorePE.StopTrace @Utility=N'AutoWho', @TraceID=5, @AbortCode=N'N'
*/
(
	@Utility		NVARCHAR(20),
	@TraceID		INT,
	@AbortCode		NCHAR(1) = N'N'
)
AS
BEGIN
	DECLARE @RowExists INT,
		@StopTime DATETIME;

	IF @Utility IS NULL
	BEGIN
		RAISERROR('Parameter @Utility cannot be null',16,1);
		RETURN -1;
	END

	IF @TraceID IS NULL
	BEGIN
		RAISERROR('Parameter @TraceID cannot be null',16,1);
		RETURN -1;
	END

	SELECT @RowExists = t.TraceID,
		@StopTime = t.StopTime
	FROM CorePE.[Traces] t
	WHERE t.TraceID = @TraceID
	AND t.Utility = @Utility;

	IF @RowExists IS NULL
	BEGIN
		RAISERROR('Parameter value for @TraceID for this value of @Utility not found in the CorePE trace table',16,1);
		RETURN -1;
	END

	IF @StopTime IS NOT NULL
	BEGIN
		RAISERROR('Parameter value for @TraceID refers to a trace that has already been stopped.',16,1);
		RETURN -1;
	END
	
	--If we get this far, there is a not-stopped trace.
	UPDATE CorePE.[Traces]
	SET StopTime = GETDATE(),
		AbortCode = ISNULL(@AbortCode,N'N')
	WHERE TraceID = @TraceID;

	RETURN 0;
END

GO
