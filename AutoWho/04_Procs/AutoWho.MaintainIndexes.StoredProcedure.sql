SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [AutoWho].[MaintainIndexes] 
/*   
	PROCEDURE:		AutoWho.MaintainIndexes

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Evaluates CorePE and AutoWho indexes for whether they should be rebuilt or not. 

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-29	Aaron Morelli		Final run-through and commenting

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
EXEC AutoWho.MaintainIndexes
*/
AS
BEGIN
	-- EXEC [AutoWho].[MaintainIndexes] 
	SET NOCOUNT ON;
	SET DEADLOCK_PRIORITY LOW;

	--Cursor variables
	DECLARE @SchemaName sysname,
		@TableName sysname,
		@IndexName sysname,
		@index_id int,
		@alloc_unit_type_desc nvarchar(60),
		@avg_fragment_size_in_pages float,
		@avg_fragmentation_in_percent float,
		@avg_page_space_used_in_percent float,
		@forwarded_record_count bigint,
		@ghost_record_count bigint,
		@page_count bigint,
		@record_count bigint,
		@version_ghost_record_count bigint,
		@min_record_size_in_bytes int,
		@max_record_size_in_bytes int,
		@avg_record_size_in_bytes int;

	--Variables used when calculating the rebuild score per index.
	DECLARE @CurrentRebuildScore INT,
			@NumBadRows BIGINT,
			@RatioBadRows FLOAT;

	--Variables relevant to the rebuild section
	DECLARE @DynSQL VARCHAR(8000),
		@OriginalRebuildStartTime DATETIME,
		@RebuildStartTime DATETIME,
		@RebuildEndTime DATETIME, 
		@FinalRebuildEndTime DATETIME,
		@LastSleepTime DATETIME,
		@LogMessage NVARCHAR(4000),
		@ErrorNumber INT;

	CREATE TABLE #AutoWhoIndexToEval (
		[SchemaName] [sysname] NOT NULL,
		[TableName] [sysname] NOT NULL,
		[IndexName] [sysname] NULL,
		[index_id] [int] NOT NULL,
		[alloc_unit_type_desc] [nvarchar](60) NULL,
		[avg_fragment_size_in_pages] [float] NULL,
		[avg_fragmentation_in_percent] [float] NULL,
		[avg_page_space_used_in_percent] [float] NULL,
		[forwarded_record_count] [bigint] NULL,
		[ghost_record_count] [bigint] NULL,
		[page_count] [bigint] NULL,
		[record_count] [bigint] NULL,
		[version_ghost_record_count] [bigint] NULL,
		[min_record_size_in_bytes] [int] NULL,
		[max_record_size_in_bytes] [int] NULL,
		[avg_record_size_in_bytes] [int] NULL
	);

	CREATE TABLE #AutoWhoIndexRebuildScore (
		[SchemaName] [sysname] NOT NULL,
		[TableName] [sysname] NOT NULL,
		[IndexName] [sysname] NULL,
		[RebuildScore] [int] NOT NULL
	);

	INSERT INTO #AutoWhoIndexToEval (
		[SchemaName],
		[TableName],
		[IndexName],
		[index_id],
		[alloc_unit_type_desc],
		[avg_fragment_size_in_pages],
		[avg_fragmentation_in_percent],
		[avg_page_space_used_in_percent],
		[forwarded_record_count],
		[ghost_record_count],
		[page_count],
		[record_count],
		[version_ghost_record_count],
		[min_record_size_in_bytes],
		[max_record_size_in_bytes],
		[avg_record_size_in_bytes]
	)
	SELECT s.name as SchemaName, 
		o.name as TableName, 
		i.name as IndexName, 
		i.index_id, 
		ps.alloc_unit_type_desc, 
		ps.avg_fragment_size_in_pages,
		ps.avg_fragmentation_in_percent,
		ps.avg_page_space_used_in_percent,
		ps.forwarded_record_count,
		ps.ghost_record_count, 
		ps.page_count,
		ps.record_count,
		ps.version_ghost_record_count,
		ps.min_record_size_in_bytes, 
		ps.max_record_size_in_bytes, 
		ps.avg_record_size_in_bytes
	FROM sys.objects o 
		INNER JOIN sys.schemas s
			ON o.schema_id = s.schema_id
		INNER JOIN sys.indexes i
			ON o.object_id = i.object_id
		CROSS APPLY sys.dm_db_index_physical_stats(db_id(), 
			o.object_id, i.index_id, null, 'DETAILED') ps
	WHERE o.type = 'U'
	AND o.schema_id IN (schema_id('CorePE'), schema_id('AutoWho'))
	AND ps.index_level = 0
	AND i.index_id <> 0
	;

	/* debug
	SELECT * 
	FROM #AutoWhoIndexToEval
	WHERE TableName = 'InputBufferStore'
	ORDER BY SchemaName, TableName, IndexName, alloc_unit_type_desc;
	*/

	INSERT INTO #AutoWhoIndexRebuildScore (
		[SchemaName],
		[TableName],
		[IndexName],
		[RebuildScore]
	)
	SELECT DISTINCT SchemaName, TableName, IndexName, 0
	FROM #AutoWhoIndexToEval;


	DECLARE iterateAutoWhoIndexes CURSOR FOR
	SELECT 
		t.SchemaName,
		t.TableName,
		t.IndexName,
		t.alloc_unit_type_desc, 
		t.page_count,
		t.avg_fragmentation_in_percent, 
		t.avg_page_space_used_in_percent, 
		t.avg_fragment_size_in_pages, 
		t.record_count,
		t.forwarded_record_count,
		t.ghost_record_count, 
		t.version_ghost_record_count,
		t.min_record_size_in_bytes,
		t.max_record_size_in_bytes,
		t.avg_record_size_in_bytes
	FROM #AutoWhoIndexToEval t
	WHERE t.page_count > 0
	ORDER BY t.SchemaName, t.TableName, t.index_id, t.alloc_unit_type_desc;

	OPEN iterateAutoWhoIndexes;
	FETCH iterateAutoWhoIndexes INTO @SchemaName,
		@TableName,
		@IndexName,
		@alloc_unit_type_desc, 
		@page_count,
		@avg_fragmentation_in_percent, 
		@avg_page_space_used_in_percent, 
		@avg_fragment_size_in_pages, 
		@record_count,
		@forwarded_record_count,
		@ghost_record_count, 
		@version_ghost_record_count,
		@min_record_size_in_bytes,
		@max_record_size_in_bytes,
		@avg_record_size_in_bytes;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @CurrentRebuildScore = 0;
		SET @NumBadRows = ISNULL(@forwarded_record_count,0) + 
						ISNULL(@ghost_record_count,0) + 
						ISNULL(@version_ghost_record_count,0);

		IF @record_count <= 0
		BEGIN
			SET @RatioBadRows = @NumBadRows;
		END
		ELSE
		BEGIN
			SET @RatioBadRows = ( @NumBadRows * 1.0 ) / ( @record_count * 1.0 ); 
		END

		IF @page_count = 1
		BEGIN
			--the only reason to rebuild w/1 page is if we have "undesirable" rows on the page or there are 0 rows total. (Does rebuild ever deallocate down to 0 pages?)
			IF @NumBadRows > 0
				OR @record_count = 0
			BEGIN
				SET @CurrentRebuildScore = 5;
			END
			ELSE
			BEGIN
				SET @CurrentRebuildScore = -99
			END
		END
		ELSE IF @page_count <= 4
		BEGIN
			--If index size is 4 pages or less, essentially disqualify from a rebuild UNLESS 
			--the page used space is < 50%
			--OR the ghost+forwarded+version_ghost is > 10% of the rows AND the page used space is < 75%
			IF @avg_page_space_used_in_percent < 50.0
				OR (@avg_page_space_used_in_percent < 75.0 
					AND @RatioBadRows >= 0.1)
			BEGIN 
				--small table is cheap and current status is pretty inefficient. Rebuild it
				SET @CurrentRebuildScore = @CurrentRebuildScore + 5;
			END
			ELSE
			BEGIN
				--essentially disable a rebuild
				SET @CurrentRebuildScore = -99;
			END
		END		--IF @page_count <= 4
		ELSE
		BEGIN
			--@page_count > 4

			--Avoid buffer pool wasted space... rebuild when pages are not very full.
			IF @avg_page_space_used_in_percent < 60.0
			BEGIN
				SET @CurrentRebuildScore += 5;
			END
			ELSE IF @avg_page_space_used_in_percent < 70.0
			BEGIN
				SET @CurrentRebuildScore += 3;
			END
			ELSE IF @avg_page_space_used_in_percent < 80.0
			BEGIN
				SET @CurrentRebuildScore += 1;
			END
			
			IF @RatioBadRows >= 0.25
			BEGIN
				SET @CurrentRebuildScore += 3;
			END
			ELSE IF @RatioBadRows >= 0.15
			BEGIN
				SET @CurrentRebuildScore += 2;
			END
			ELSE IF @RatioBadRows >= 0.10
			BEGIN
				SET @CurrentRebuildScore += 1;
			END

			IF @page_count >= 128	--1 MB
			BEGIN
				--large enough to care (at least a little) about how contiguous the pages are
				IF @avg_fragment_size_in_pages < 2.0
				BEGIN
					SET @CurrentRebuildScore += 3;
				END
				ELSE IF @avg_fragment_size_in_pages  < 4.0
				BEGIN
					SET @CurrentRebuildScore += 2;
				END
				ELSE IF @avg_fragment_size_in_pages  < 6.0
				BEGIN
					SET @CurrentRebuildScore += 1;
				END
			END

			IF @page_count >= 128000	--1 GB
			BEGIN
				--index is large enough that we might actually care about the commonly-used metric
				IF @avg_fragmentation_in_percent > 75.0
				BEGIN
					SET @CurrentRebuildScore += 3;
				END
				ELSE IF @avg_fragmentation_in_percent > 50.0
				BEGIN
					SET @CurrentRebuildScore += 2;
				END
				ELSE IF @avg_fragmentation_in_percent > 40.0
				BEGIN
					SET @CurrentRebuildScore += 2;
				END
			END
		END

		UPDATE #AutoWhoIndexRebuildScore
		SET RebuildScore = RebuildScore + @CurrentRebuildScore		--multiple alloc units per index means we need to
																	-- take multiple loop iterations per index into account.
		WHERE SchemaName = @SchemaName
		AND TableName = @TableName
		AND IndexName = @IndexName;

		FETCH iterateAutoWhoIndexes INTO @SchemaName,
			@TableName,
			@IndexName,
			@alloc_unit_type_desc, 
			@page_count,
			@avg_fragmentation_in_percent, 
			@avg_page_space_used_in_percent, 
			@avg_fragment_size_in_pages, 
			@record_count,
			@forwarded_record_count,
			@ghost_record_count, 
			@version_ghost_record_count,
			@min_record_size_in_bytes,
			@max_record_size_in_bytes,
			@avg_record_size_in_bytes;
	END

	CLOSE iterateAutoWhoIndexes;
	DEALLOCATE iterateAutoWhoIndexes;

	/*
	--for debugging
	SELECT *
	FROM #AutoWhoIndexRebuildScore
	WHERE TableName = 'InputBufferStore'
	order by SchemaName, TableName, IndexName;
	*/



	DECLARE iterateScores CURSOR FOR
	SELECT SchemaName, TableName, IndexName
	FROM #AutoWhoIndexRebuildScore t
	WHERE t.RebuildScore >= 5;

	OPEN iterateScores
	FETCH iterateScores INTO @SchemaName, @TableName, @IndexName;

	SET @OriginalRebuildStartTime = GETDATE();
	SET @LastSleepTime = GETDATE();

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @DynSQL = 'ALTER INDEX ' + QUOTENAME(@IndexName) 
			+ ' ON ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + ' REBUILD;'

		--PRINT @DynSQL

		SET @RebuildStartTime = GETDATE();
		BEGIN TRY
			EXEC (@DynSQL);

			SET @LogMessage = N'Successfully rebuilt index ' + QUOTENAME(@IndexName) + ' on table ' + 
				QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);

			INSERT INTO AutoWho.Log 
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, 0, 'AutoWhoRbld', @LogMessage;
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @ErrorNumber = ERROR_NUMBER();
			SET @LogMessage = 'Error ' + CONVERT(varchar(20), ERROR_NUMBER()) + ' occurred while attempting to rebuild
				index ' + QUOTENAME(@IndexName) + ' on table ' + 
				QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName) + '. State: ' + CONVERT(varchar(20),ERROR_STATE()) +
				'; Severity: ' + CONVERT(varchar(20),ERROR_SEVERITY()) + ' Message: ' + ERROR_MESSAGE();

			INSERT INTO AutoWho.Log 
			(LogDT, TraceID, ErrorCode, LocationTag, LogMessage)
			SELECT SYSDATETIME(), NULL, @ErrorNumber, 'AutoWhoRbld', @LogMessage;
		END CATCH

		SET @RebuildEndTime = GETDATE(); 


		IF DATEDIFF(second, @RebuildStartTime, @RebuildEndTime) >= 5 
			OR DATEDIFF(second, @LastSleepTime, @RebuildEndTime) >= 15
		BEGIN
			--If the last index rebuild actually took some time, then we may be inhibiting AutoWho. 
			--Waiting 3 seconds before we go to the next index gives AutoWho time (if it was blocked) to finish its run and go back to sleep
			WAITFOR DELAY '00:00:03';
			SET @LastSleepTime = GETDATE();
		END

		FETCH iterateScores INTO @SchemaName, @TableName, @IndexName;
	END

	SET @FinalRebuildEndTime = GETDATE();

	CLOSE iterateScores;
	DEALLOCATE iterateScores;


	RETURN 0;
END
GO
