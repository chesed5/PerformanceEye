SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[CollectorDuration] 
/*   
	PROCEDURE:		AutoWho.CollectorDuration

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Splits out the comma-separated entries in the "DurationBreakdown" field of AutoWho.CaptureTimes
		into aggregated rows, to allow us to see which statements in the AutoWho Collector are typically the
		most expensive. The Messages tab also holds a query that can be used to view the detailed data for
		ad-hoc analysis.

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-23	Aaron Morelli		Final run-through and commenting

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
EXEC AutoWho.CollectorDuration @StartTime='2016-04-23 04:00', @EndTime = '2016-04-23 06:00'
*/
(
	@StartTime	DATETIME=NULL,
	@EndTime	DATETIME=NULL
)
AS
BEGIN
	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	IF @StartTime IS NULL
	BEGIN
		--All-time past
		SET @StartTime = CONVERT(DATETIME,'2000-01-01');
	END

	IF @EndTime IS NULL
	BEGIN
		--All-time future
		SET @EndTime = CONVERT(DATETIME,'2100-01-01');
	END

	IF @EndTime <= @StartTime
	BEGIN
		RAISERROR('Parameter @EndTime must be greater than parameter @StartTime.', 16, 10);
		RETURN -1;
	END

	SELECT Tag, 
		[NumExecutions] = COUNT(*), 
		[SumDuration_ms] = SUM(TagDuration_ms),
		[AvgDuration_ms] = AVG(TagDuration_ms),
		[MinDuration_ms] = MIN(TagDuration_ms),
		[MaxDuration_ms] = MAX(TagDuration_ms)
	FROM (
		SELECT 
			SPIDCaptureTime, 
			AutoWhoDuration_ms, 
			Tag, 
			[TagDuration_ms] = CONVERT(BIGINT,TagDuration_ms)
		FROM (
			SELECT 
				ss2.SPIDCaptureTime, 
				ss2.AutoWhoDuration_ms, 
				[Tag] = SUBSTRING(TagWithDuration,1, CHARINDEX(':', TagWithDuration)-1), 
				[TagDuration_ms] = SUBSTRING(TagWithDuration, CHARINDEX(':', TagWithDuration)+1, LEN(TagWithDuration)),
				TagWithDuration
			FROM (
				SELECT ss.*, 
					[TagWithDuration] = Split.a.value(N'.', 'NVARCHAR(512)')
				FROM (
					SELECT 
						t.SPIDCaptureTime, 
						t.AutoWhoDuration_ms, 
						[loclist] = CAST(N'<M>' + REPLACE(DurationBreakdown,  N',' , N'</M><M>') + N'</M>' AS XML)
					FROM AutoWho.CaptureTimes t WITH (NOLOCK)
					WHERE t.RunWasSuccessful = 1
					AND t.SPIDCaptureTime BETWEEN @StartTime AND @EndTime
				) ss
					CROSS APPLY loclist.nodes(N'/M') Split(a)
			) ss2
			WHERE LTRIM(RTRIM(TagWithDuration)) <> ''
		) ss3
	) ss4
	GROUP BY Tag
	ORDER BY 3 DESC
	OPTION(RECOMPILE);

	--We print out the un-grouped query to the Messages tab so the user can sift through & filter
	-- the detailed data in an ad-hoc fashion
	DECLARE @printtotab NVARCHAR(4000);

	SET @printtotab = N'
	DECLARE @StartTime datetime = ''' + 
		REPLACE(CONVERT(NVARCHAR(40),@StartTime,102),'.','-') + ' ' + 
		CONVERT(NVARCHAR(40), @StartTime,108) + '.' + 
		CONVERT(NVARCHAR(40),DATEPART(MILLISECOND, @StartTime)) + ''';
	DECLARE @EndTime datetime = ''' + 
		REPLACE(CONVERT(NVARCHAR(40),@EndTime,102),'.','-') + ' ' + 
		CONVERT(NVARCHAR(40), @EndTime,108) + '.' + 
		CONVERT(NVARCHAR(40),DATEPART(MILLISECOND, @EndTime)) + '''; 
	SELECT 
		SPIDCaptureTime, 
		AutoWhoDuration_ms, 
		Tag, 
		TagDuration_ms
	FROM (
		SELECT 
			ss2.SPIDCaptureTime, 
			ss2.AutoWhoDuration_ms, 
			[Tag] = SUBSTRING(TagWithDuration,1, CHARINDEX('':'', TagWithDuration)-1), 
			[TagDuration_ms] = SUBSTRING(TagWithDuration, CHARINDEX('':'', TagWithDuration)+1, LEN(TagWithDuration)),
			[TagWithDuration]
		FROM (
			SELECT ss.*, 
				[TagWithDuration] = Split.a.value(N''.'', ''NVARCHAR(512)'')
			FROM (
				SELECT 
					t.SPIDCaptureTime, 
					t.AutoWhoDuration_ms, 
					[loclist] = CAST(N''<M>'' + REPLACE(DurationBreakdown,  N'','' , N''</M><M>'') + N''</M>'' AS XML)
				FROM AutoWho.CaptureTimes t with(nolock)
				WHERE t.RunWasSuccessful = 1
				AND t.SPIDCaptureTime BETWEEN @StartTime AND @EndTime
			) ss
				CROSS APPLY loclist.nodes(N''/M'') Split(a)
		) ss2
		WHERE LTRIM(RTRIM(TagWithDuration)) <> ''''
	) ss3
	--ORDER BY 
	OPTION(RECOMPILE)
	;';

	PRINT @printtotab;
	RETURN 0;
END 


GO
