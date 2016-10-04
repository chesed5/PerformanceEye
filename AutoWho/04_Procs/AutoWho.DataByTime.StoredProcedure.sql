SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[DataByTime] 
/*   
	PROCEDURE:		AutoWho.DataByTime

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Just dumps out data for each table organized by time. Mainly for quick data collection review during development.

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-22	Aaron Morelli		Final Commenting

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
EXEC AutoWho.DataByTime
*/
AS
BEGIN
	SET NOCOUNT ON;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.SignalTable' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT * 
		FROM [AutoWho].[SignalTable]
		) d
		ON 1=1;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.ThresholdFilterSpids' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT *
		FROM [AutoWho].[ThresholdFilterSpids]
		) d
		ON 1=1;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.Log' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE, LogDT) as LogDT, TraceID, COUNT(*) as NumRows
			FROM [AutoWho].[Log]
			GROUP BY CONVERT(DATE, LogDT), TraceID
		) d
		ON 1=1
	ORDER BY d.LogDT ASC, d.TraceID;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'CorePE.Traces' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT *
		FROM [CorePE].[Traces]
		WHERE Utility = N'AutoWho'
		) d
		ON 1=1
	ORDER BY d.TraceID;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'CorePE.CaptureOrdinalCache' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT DISTINCT t.StartTime, t.EndTime
			FROM [CorePE].[CaptureOrdinalCache] t
			WHERE t.Utility = N'AutoWho'
		) d
		ON 1=1
	ORDER BY d.StartTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.CaptureSummary' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE,cs.SPIDCaptureTime) as CaptureDT
			FROM [AutoWho].[CaptureSummary] cs
		) d
		ON 1=1
	ORDER BY d.CaptureDT;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.CaptureTimes' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT CONVERT(DATE,ct.SPIDCaptureTime) as CaptureDT
			FROM [AutoWho].[CaptureTimes] ct
		) d
		ON 1=1
	ORDER BY d.CaptureDT;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightSessions' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightSessions] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightTasks' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightTasks] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LightweightTrans' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT l.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LightweightTrans] l
			GROUP BY l.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.BlockingGraphs' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[BlockingGraphs] bg
			GROUP BY SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.LockDetails' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[LockDetails]
			GROUP BY SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.TransactionDetails' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT t.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[TransactionDetails] t
			GROUP BY t.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.SessionsAndRequests' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT sar.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[SessionsAndRequests] sar
			GROUP BY sar.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.TasksAndWaits' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT taw.SPIDCaptureTime, COUNT(*) as NumRows
			FROM [AutoWho].[TasksAndWaits] taw
			GROUP BY taw.SPIDCaptureTime
		) d
		ON 1=1
	ORDER BY d.SPIDCaptureTime;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CorePE.InputBufferStore' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, Insertedby_SPIDCaptureTime)) + 
							' ' + CONVERT(varchar(30),DATEPART(HOUR, Insertedby_SPIDCaptureTime)) + ':00'
			FROM [CorePE].[InputBufferStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CorePE.QueryPlanBatchStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, Insertedby_SPIDCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, Insertedby_SPIDCaptureTime)) + ':00'
		FROM [CorePE].[QueryPlanBatchStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CorePE.QueryPlanStmtStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, Insertedby_SPIDCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, Insertedby_SPIDCaptureTime)) + ':00'
		FROM [CorePE].[QueryPlanStmtStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CorePE.SQLBatchStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, Insertedby_SPIDCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, Insertedby_SPIDCaptureTime)) + ':00'
		FROM [CorePE].[SQLBatchStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.InsertHour, COUNT(InsertHour) as NumRows
	FROM
	(SELECT 'CorePE.SQLStmtStore' as TableName) Tb
		LEFT OUTER JOIN (
		SELECT InsertHour = CONVERT(varchar(30),CONVERT(DATE, Insertedby_SPIDCaptureTime)) + 
						' ' + CONVERT(varchar(30),DATEPART(HOUR, Insertedby_SPIDCaptureTime)) + ':00'
		FROM [CorePE].[SQLStmtStore]
		) d
		ON 1=1
	GROUP BY tb.TableName, d.InsertHour
	ORDER BY d.InsertHour;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimCommand' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimCommand]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimConnectionAttribute' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimConnectionAttribute]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimLoginName' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimLoginName]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimNetAddress' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimNetAddress]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimSessionAttribute' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimSessionAttribute]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	SELECT tb.TableName, d.*
	FROM
	(SELECT 'AutoWho.DimWaitType' as TableName) Tb
		LEFT OUTER JOIN (
			SELECT * FROM [AutoWho].[DimWaitType]
		) d
		ON 1=1
	ORDER BY d.TimeAdded;

	RETURN 0;
END
GO
