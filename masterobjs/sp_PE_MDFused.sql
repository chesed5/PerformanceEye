USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[sp_PE_MDFused]
/*   
	PROCEDURE:		sp_PE_MDFUsed

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: Displays info about the used space in data files, broken down into small sections so that the user can tell where there are larger and smaller
		chunks of free space.

	FUTURE ENHANCEMENTS: 
		Display the allocated/unallocated space in "chunks" for the DB files in this database

		TODOs:
			- Mainly display logic, then perf test at a small, then medium, then large client


			- Add support for partitions?


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
TODO
*/
(
	@DBName				SYSNAME					=NULL,			--defaults to DB_NAME()
	@JustThisFile		NVARCHAR(128)			=NULL,			--either the logical file name or the file ID value
	@ObjectName			SYSNAME					=NULL,			--Filter results to just this object name (schema is optional)
	@Index				NVARCHAR(128)			=NULL,			--Filter results to just this index (either the name or ID can be specified). If non-null, object id is required.
	@ChunkSize_MB		INT						=NULL,			--defaults to a basic algorithm based on the file size
	@WindowStart_MB		INT						=NULL,			--start of the file to focus on
	@WindowEnd_MB		INT						=NULL,			--end of the file to focus on
	@ObjectBreakout		NCHAR(1)				=N'N',			-- Y/N
	@AllocBreakout		NVARCHAR(20)			=N'NO',			-- no, all, inrow, lob, overflow
	@UnitsInPages		NCHAR(1)				=N'N',			--Y/N
	@ExtraAttributes	NCHAR(1)				=N'N',			--Y/N
	@ForceExec			NCHAR(1)				=N'N',			--Y/N
	@Help				NCHAR(1)				=N'N'			--TODO
)
AS
BEGIN

	SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	DECLARE @lv__ErrMsg VARCHAR(4000),
			@lv__DBName SYSNAME,
			@lv__numDBFiles INT,
			@lv__DBID INT,
			@lv__ObjectParmCleaned NVARCHAR(128),
			@lv__pos INT,
			@lv__tmpInt INT,
			@lv__SchemaPortion NVARCHAR(128),
			@lv__ObjectPortion NVARCHAR(128),
			@lv__ObjectID INT,
			@lv__IndexParmCleaned NVARCHAR(128),
			@lv__IndexID INT,
			@lv__AllocBreakoutLogic NVARCHAR(10),
			@lv__WindowStart_pages INT,
			@lv__WindowEnd_pages INT,
			@lv__ChunkSize_pages INT,
			@lv__DynSQL NVARCHAR(4000);

	IF @DBName IS NULL
	BEGIN
		SET @DBName = DB_NAME();
		SET @lv__DBName = @DBName;
	END
	ELSE
	BEGIN
		--handle case-sensitive collations
		SELECT @lv__DBName = d.name
		FROM sys.databases d
		WHERE UPPER(d.name) = UPPER(@DBName);

		--Parm validation
		IF @lv__DBName IS NULL
		BEGIN
			SET @lv__ErrMsg = 'Database ' + @DBName + ' does not exist on this SQL instance.';
			RAISERROR(@lv__ErrMsg,16,1);
			RETURN -1;
		END
	END

	SET @lv__DBID = DB_ID(@lv__DBName);


	--Note, if @ChunkSize_MB is NULL, we'll handle it further below with an algorithm based on the file size
	IF @ChunkSize_MB < 1 OR @ChunkSize_MB > 20480
	BEGIN
		RAISERROR('If specified, parameter @ChunkSize_MB must be between 1 and 20480.', 16,1);
		RETURN -1;
	END

	IF ISNULL(@WindowStart_MB,0) < 0
	BEGIN
		RAISERROR('If specified, parameter @WindowStart_MB cannot be < 0.', 16,1);
		RETURN -1;
	END

	IF ISNULL(@WindowEnd_MB,999999999) <= 0 OR ISNULL(@WindowEnd_MB,999999999) <= ISNULL(@WindowStart_MB,0)
	BEGIN
		RAISERROR('If specified, parameter @WindowEnd_MB cannot be <= 0 and cannot be <= @WindowStart_MB.', 16,1);
		RETURN -1;
	END

	SET @lv__WindowStart_pages = ISNULL(@WindowStart_MB,0)*128;

	--handle case-sensitive
	SET @ForceExec = ISNULL(UPPER(@ForceExec),N'N');
	SET @AllocBreakout = ISNULL(UPPER(@AllocBreakout),N'NO');
	SET @ExtraAttributes = ISNULL(UPPER(@ExtraAttributes),N'N');
	SET @UnitsInPages = ISNULL(UPPER(@UnitsInPages),N'N');

	IF @ForceExec NOT IN (N'Y',N'N')
	BEGIN
		RAISERROR('Parameter @ForceExec must be either "N" (default) or "Y".',16,1);
		RETURN -1;
	END

	IF @ExtraAttributes NOT IN (N'Y',N'N')
	BEGIN
		RAISERROR('Parameter @ExtraAttributes must be either "N" (default) or "Y".',16,1);
		RETURN -1;
	END
	
	IF @UnitsInPages NOT IN (N'Y',N'N')
	BEGIN
		RAISERROR('Parameter @UnitsInPages must be either "N" (default) or "Y".',16,1);
		RETURN -1;
	END	

	CREATE TABLE #AllocTypes (
		AllocUnitType NVARCHAR(40)
	);

	IF @AllocBreakout LIKE '%ALL%'
	BEGIN
		SET @lv__AllocBreakoutLogic = N'ALL'

		INSERT INTO #AllocTypes (AllocUnitType)
		SELECT N'IN_ROW_DATA' UNION ALL
		SELECT N'LOB_DATA' UNION ALL
		SELECT N'ROW_OVERFLOW_DATA' 
	END
	ELSE
	BEGIN
		IF @AllocBreakout LIKE N'%INROW%'
			OR @AllocBreakout LIKE N'%LOB%'
			OR @AllocBreakout LIKE N'%OVERFLOW%'
		BEGIN
			SET @lv__AllocBreakoutLogic = N'PARTIAL'

			IF @AllocBreakout LIKE N'%INROW%'
			BEGIN
				INSERT INTO #AllocTypes (AllocUnitType)
				SELECT N'IN_ROW_DATA'
			END

			IF @AllocBreakout LIKE N'%LOB%'
			BEGIN
				INSERT INTO #AllocTypes (AllocUnitType)
				SELECT N'LOB_DATA'
			END

			IF @AllocBreakout LIKE N'%OVERFLOW%'
			BEGIN
				INSERT INTO #AllocTypes (AllocUnitType)
				SELECT N'ROW_OVERFLOW_DATA' 
			END
		END
		ELSE
		BEGIN
			SET @lv__AllocBreakoutLogic = N'TOTAL'

			INSERT INTO #AllocTypes (AllocUnitType)
			SELECT N'TOTAL'
		END
	END


	--Validate that @ObjectName refers to a valid object in the database
	CREATE TABLE #InsertExecValueStore (
		strval varchar(20),
		intval int
	);

	IF @ObjectName IS NOT NULL
	BEGIN
		--inj!
		SET @lv__ObjectParmCleaned = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
										@ObjectName,
									N';', N''),
									N'--',N''),
									N'/*',N''),
									N'*/',N''),
									N'''', N''),
									N'xp_',N''),
									N'Xp_',N''),
									N'xP_',N''),
									N'XP_',N''),
									NCHAR(0), N''),
									NCHAR(1), N''),
									NCHAR(2), N''),
									NCHAR(3), N''),
									NCHAR(4), N''),
									NCHAR(5), N''),
									NCHAR(6), N''),
									NCHAR(7), N''),
									NCHAR(8), N''),
									NCHAR(9), N''),
									NCHAR(10), N''),
									NCHAR(11), N''),
									NCHAR(12), N''),
									NCHAR(13), N''),
									NCHAR(14), N''),
									NCHAR(15), N''),
									NCHAR(16), N''),
									NCHAR(17), N''),
									NCHAR(18), N''),
									NCHAR(19), N''),
									NCHAR(20), N''),
									NCHAR(21), N''),
									NCHAR(22), N''),
									NCHAR(23), N''),
									NCHAR(24), N''),
									NCHAR(25), N''),
									NCHAR(26), N''),
									NCHAR(27), N''),
									NCHAR(28), N''),
									NCHAR(29), N''),
									NCHAR(30), N''),
									NCHAR(31), N'')
									;
		--Just to play things safe, if our input had any of the above characters, something ain't right, and just abort.
		IF LEN(@ObjectName) <> LEN(@lv__ObjectParmCleaned) OR 
			DATALENGTH(@ObjectName) <> DATALENGTH(@lv__ObjectParmCleaned)
		BEGIN
			SET @lv__ErrMsg = 'Value for input parameter @ObjectName is not in an acceptable format.'
			RAISERROR(@lv__ErrMsg,16,1);
			RETURN -1;
		END

		--Ok, we consider the Object Name to be "clean". We need to handle input with strange (but legal) characters like space and
		-- quote and such. Thus, we need to QUOTENAME, but that function won't work properly if someone specified a schema name.

		SET @lv__pos = CHARINDEX(N'.',@lv__ObjectParmCleaned,1);

		IF @lv__pos > 0 
		BEGIN
			SET @lv__SchemaPortion = SUBSTRING(@lv__ObjectParmCleaned,1,@lv__pos-1);
			SET @lv__ObjectPortion = SUBSTRING(@lv__ObjectParmCleaned,@lv__pos+1,LEN(@lv__ObjectParmCleaned));

			SET @lv__ObjectParmCleaned = QUOTENAME(@lv__SchemaPortion) + N'.' + QUOTENAME(@lv__ObjectPortion)
		END
		ELSE
		BEGIN
			SET @lv__ObjectParmCleaned = QUOTENAME(@lv__ObjectParmCleaned);
		END

		--select @lv__ObjectParmCleaned;
		
		SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N'; 
		SELECT OBJECT_ID(''' + @lv__ObjectParmCleaned + ''')
		';

		TRUNCATE TABLE #InsertExecValueStore;

		INSERT INTO #InsertExecValueStore (intval)
			EXEC sp_executesql @lv__DynSQL;

		SET @lv__ObjectID = NULL;
		SELECT @lv__ObjectID = intval
		FROM #InsertExecValueStore;

		IF @lv__ObjectID IS NULL
		BEGIN
			SET @lv__ErrMsg = 'Object ' + @ObjectName + ' not found in database ' + @lv__DBName;
			RAISERROR(@lv__ErrMsg, 16,1);
			RETURN -1;
		END
	END

	--Now, check index
	IF @Index IS NOT NULL
	BEGIN
		IF @ObjectName IS NULL
		BEGIN
			RAISERROR('A valid @ObjectName must be specified if @Index is not null',16,1);
			RETURN -1;
		END

		--inj!
		SET @lv__IndexParmCleaned = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
									REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
										@Index,
									N';', N''),
									N'--',N''),
									N'/*',N''),
									N'*/',N''),
									N'''', N''),
									N'xp_',N''),
									N'Xp_',N''),
									N'xP_',N''),
									N'XP_',N''),
									NCHAR(0), N''),
									NCHAR(1), N''),
									NCHAR(2), N''),
									NCHAR(3), N''),
									NCHAR(4), N''),
									NCHAR(5), N''),
									NCHAR(6), N''),
									NCHAR(7), N''),
									NCHAR(8), N''),
									NCHAR(9), N''),
									NCHAR(10), N''),
									NCHAR(11), N''),
									NCHAR(12), N''),
									NCHAR(13), N''),
									NCHAR(14), N''),
									NCHAR(15), N''),
									NCHAR(16), N''),
									NCHAR(17), N''),
									NCHAR(18), N''),
									NCHAR(19), N''),
									NCHAR(20), N''),
									NCHAR(21), N''),
									NCHAR(22), N''),
									NCHAR(23), N''),
									NCHAR(24), N''),
									NCHAR(25), N''),
									NCHAR(26), N''),
									NCHAR(27), N''),
									NCHAR(28), N''),
									NCHAR(29), N''),
									NCHAR(30), N''),
									NCHAR(31), N'')
									;

		--Just to play things safe, if our input had any of the above characters, something ain't right, and just abort.
		IF LEN(@Index) <> LEN(@lv__IndexParmCleaned) OR 
			DATALENGTH(@Index) <> DATALENGTH(@lv__IndexParmCleaned)
		BEGIN
			SET @lv__ErrMsg = 'Value for input parameter @Index is not in an acceptable format.'
			RAISERROR(@lv__ErrMsg,16,1);
			RETURN -1;
		END

		SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N'; 
		SELECT i.index_id
		FROM sys.objects o 
			INNER JOIN sys.indexes i
				ON o.object_id = i.object_id
		WHERE o.object_id = ' + CONVERT(NVARCHAR(20),@lv__ObjectID) + ' 
		AND i.name = ''' + @lv__IndexParmCleaned + ''' 
		AND o.type IN (''U'', ''V'');
		';

		TRUNCATE TABLE #InsertExecValueStore;

		INSERT INTO #InsertExecValueStore (intval)
			EXEC sp_executesql @lv__DynSQL;

		SET @lv__IndexID = NULL;
		SELECT @lv__IndexID = intval
		FROM #InsertExecValueStore;

		IF @lv__IndexID IS NULL
		BEGIN
			--the user may have entered an index ID rather than a name.
			SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N'; 
			SELECT i.index_id
			FROM sys.objects o 
				INNER JOIN sys.indexes i
					ON o.object_id = i.object_id
			WHERE o.object_id = ' + CONVERT(NVARCHAR(20),@lv__ObjectID) + ' 
			AND i.index_id = ' + @lv__IndexParmCleaned + ' 
			AND o.type IN (''U'', ''V'');
			';

			TRUNCATE TABLE #InsertExecValueStore;

			INSERT INTO #InsertExecValueStore (intval)
				EXEC sp_executesql @lv__DynSQL;

			SET @lv__IndexID = NULL;
			SELECT @lv__IndexID = intval
			FROM #InsertExecValueStore;

			IF @lv__IndexID IS NULL
			BEGIN
				SET @lv__ErrMsg = 'Index ' + @Index + ' for @ObjectName ' + @ObjectName + ' not found in database ' + @DBName + ', either by index name or index ID';
				RAISERROR(@lv__ErrMsg, 16,1);
				RETURN -1;
			END
		END --IF @lv__IndexID IS NULL
	END	--IF @Index IS NOT NULL


	IF OBJECT_ID('TempDB..#showfilestats44') IS NOT NULL
	BEGIN
	 DROP TABLE #showfilestats1 
	END

	SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N'; 
		EXEC (''DBCC showfilestats'');
	';

	CREATE TABLE #showfilestats44 (
		FileID INT, 
		[FileGroup] INT, 
		TotalExtents BIGINT, 
		UsedExtents BIGINT, 
		lname SYSNAME, 
		pname SYSNAME
	);

	INSERT INTO #showfilestats44 (FileID, [FileGroup], TotalExtents, UsedExtents, lname, pname)
		EXEC sp_executesql @lv__DynSQL;


	IF OBJECT_ID('tempdb..#HeaderResultSet44') IS NOT NULL
	BEGIN
		DROP TABLE #HeaderResultSet44
	END

	CREATE TABLE #HeaderResultSet44 (
		DBName								SYSNAME, 
		FileGroupID							INT,
		FileGroupName						SYSNAME,
		FileID								INT,
		[FileLogicalName]					SYSNAME,
		[FilePhysicalName]					NVARCHAR(2048),
		[FileSize_pages]					BIGINT,
		[FileUsedSize_pages]				BIGINT,
		[TotalDataSize_pages]				BIGINT,
		[TotalUsedSize_pages]				BIGINT,
		[VolumeMountPoint]					NVARCHAR(512),
		[VolumeID]							NVARCHAR(512),
		[VolumeLogicalName]					NVARCHAR(512),
		[FileSystemType]					NVARCHAR(128),
		[VolumeTotalBytes]					BIGINT,
		[VolumeAvailableBytes]				BIGINT,
		[VolumeIsReadOnly]					INT,
		[VolumeIsCompressed]				INT,
		[VolumeSupportsCompression]			INT,
		[VolumeSupportsAlternateStreams]	INT,
		[VolumeSupportsSparseFiles]			INT,
		[IncludeInDetailedOutput]			NCHAR(1)
	);

	SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N'; 
		SELECT 
			[DBName] = DB_NAME(), 
			[FileGroupID] = t.FileGroup,
			[FileGroupName] = dsp.name, 
			[FileID] = df.file_id,
			[FileLogicalName] = df.name, 
			[FilePhysicalName] = df.physical_name, 
			[FileSize_pages] = df.size, 
			[FileUsedSize_pages] = t.UsedExtents*8, 
			vs.volume_mount_point, 
			vs.volume_id, 
			vs.logical_volume_name, 
			vs.file_system_type, 
			vs.total_bytes, 
			vs.available_bytes, 
			vs.is_read_only, 
			vs.is_compressed, 
			vs.supports_compression, 
			vs.supports_alternate_streams, 
			vs.supports_sparse_files,
			[IncludeInDetailedOutput] = N''Y''
		FROM #showfilestats44 t 
		INNER JOIN sys.database_files df
			ON t.FileID = df.file_id
		INNER JOIN sys.data_spaces dsp
			ON dsp.data_space_id = t.FileGroup
		OUTER APPLY sys.dm_os_volume_stats(' + CONVERT(nvarchar(20), @lv__DBID) + ', df.file_id) vs
	';
	
	INSERT INTO #HeaderResultSet44 (
		DBName
		,FileGroupID
		,FileGroupName
		,FileID
		,[FileLogicalName]
		,[FilePhysicalName]
		,[FileSize_pages]
		,[FileUsedSize_pages]
		--,[TotalDataSize_pages]
		--,[TotalUsedSize_pages]
		,[VolumeMountPoint]
		,[VolumeID]
		,[VolumeLogicalName]
		,[FileSystemType]
		,[VolumeTotalBytes]
		,[VolumeAvailableBytes]
		,[VolumeIsReadOnly]
		,[VolumeIsCompressed]
		,[VolumeSupportsCompression]
		,[VolumeSupportsAlternateStreams]
		,[VolumeSupportsSparseFiles]
		,[IncludeInDetailedOutput] 
	)
		EXEC sp_executesql @lv__DynSQL;

	--If the DB has multiple files, update the TotalDataSize_pages, TotalUsedSize_pages columns
	SET @lv__numDBFiles = (SELECT COUNT(*) FROM #HeaderResultSet44);

	IF @lv__numDBFiles > 1
	BEGIN
		UPDATE targ 
		SET TotalDataSize_pages = ss0.TotalDataSize_pages,
			TotalUsedSize_pages = ss0.TotalUsedSize_pages
		FROM #HeaderResultSet44 targ
			INNER JOIN 
			(
			SELECT 
				FileID, 
				FileSize_pages, 
				FileUsedSize_pages,
				TotalDataSize_pages = SUM(FileSize_pages) OVER (),
				TotalUsedSize_pages = SUM(FileUsedSize_pages) OVER ()
			FROM #HeaderResultSet44 t
			) ss0
				ON targ.FileID = ss0.FileID
	END


	--If we're going to go any further and process the more expensive datasets, check to see how big the DB is and whether
	-- the user is really aware

	IF @ForceExec = N'N'
	BEGIN
		SET @lv__tmpInt = (SELECT SUM(FileSize_pages) FROM #HeaderResultSet44)

		IF @lv__tmpInt*8/1024 > 200
		BEGIN
			RAISERROR('Database MDF files SUM to more than 200 MB. Running detailed logic on a larger database may take a while. If you want to continue, specify @ForceExec="Y"', 16, 1);
			RETURN -1;
		END
	END


	IF @JustThisFile IS NOT NULL
	BEGIN
		IF EXISTS (SELECT * FROM #HeaderResultSet44 t
					WHERE t.FileLogicalName = @JustThisFile)
		BEGIN
			UPDATE #HeaderResultSet44
			SET IncludeInDetailedOutput = N'N'
			WHERE FileLogicalName <> @JustThisFile
		END
		ELSE
		BEGIN
			--didn't find it by file logical name, try by ID
			IF ISNUMERIC(@JustThisFile) = 1
			BEGIN
				BEGIN TRY
					IF EXISTS (SELECT * FROM #HeaderResultSet44 t
								WHERE t.FileID = @JustThisFile)
					BEGIN
						UPDATE #HeaderResultSet44
						SET IncludeInDetailedOutput = N'N'
						WHERE FileID <> @JustThisFile
					END
				END TRY
				BEGIN CATCH END CATCH		--don't try that hard (for now)
			END
		END
	END		--IF @JustThisFile IS NOT NULL

	IF OBJECT_ID('tempdb..#DBPageAllocs_temp') IS NOT NULL
	BEGIN
		DROP TABLE #DBPageAllocs_temp
	END

	--This view is pretty expensive to call and gives the optimizer probs if we join to it directly. 
	-- Cache the results for reuse and better plans
	CREATE TABLE #DBPageAllocs_temp (
		object_id int,
		index_id int,
		allocation_unit_type_desc varchar(128),
		extent_file_id int,
		extent_page_id int,
		allocated_page_page_id int,
		is_allocated int,
		is_iam_page int,
		is_mixed_page_allocation int,
		has_ghost_records int
	);

	INSERT INTO #DBPageAllocs_temp (
		object_id,
		index_id,
		allocation_unit_type_desc,
		extent_file_id,
		extent_page_id,
		allocated_page_page_id,
		is_allocated,
		is_iam_page,
		is_mixed_page_allocation,
		has_ghost_records
	)
	SELECT object_id, 
		index_id, 
		allocation_unit_type_desc, 
		extent_file_id, 
		extent_page_id, 
		allocated_page_page_id, 
		is_allocated, 
		is_iam_page, 
		is_mixed_page_allocation, 
		has_ghost_records
	FROM sys.dm_db_database_page_allocations(db_id(@lv__DBName), @lv__ObjectID, @lv__IndexID, null, 'LIMITED');

	CREATE CLUSTERED INDEX CL1 ON #DBPageAllocs_temp (extent_file_id, allocated_page_page_id);


	IF OBJECT_ID('tempdb..#DBFileBuckets') IS NOT NULL
	BEGIN
		DROP TABLE #DBFileBuckets
	END

	CREATE TABLE #DBFileBuckets (
		file_id int,
		bucket_id int,
		display_position_id int,
		AllocationType NVARCHAR(128),
		DataFound INT,
		SizeMB_RangeStart int,
		SizeMB_RangeEnd int,
		SizePages_RangeStart int,
		SizePages_RangeEnd int, 
		NumAllocatedExtents int,
		NumAllocatedPages int,
		NumAllocatedPages_PageAllocationStatusZero int,
		NumIAMPages int,
		NumMixedPages int,
		NumPagesWithGhostRecords int, 
		RunTotalAlloc_FromStart_Pages_asc int, 
		RunTotalAllocByAllocUnit_FromStart_Pages_asc int,
		RunTotalAlloc_FromEnd_Pages_desc int, 
		RunTotalAllocByAllocUnit_FromEnd_Pages_desc int
	);

	DECLARE iterateFiles1 CURSOR FOR 
	SELECT [FileID], FileSize_pages, FileSize_pages*8/1024 as FileSize_MB
	FROM #HeaderResultSet44
	WHERE IncludeInDetailedOutput = N'Y'
	ORDER BY [FileGroupName], [FileID] ASC;

	DECLARE @curFileID INT, 
		@cursize_pages INT, 
		@curSize_MB INT;

	OPEN iterateFiles1
	FETCH iterateFiles1 INTO @curFileID, @cursize_pages, @curSize_MB

	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @WindowEnd_MB IS NULL
		BEGIN
			SET @lv__WindowEnd_pages = @cursize_pages;
		END
		ELSE
		BEGIN
			IF @WindowEnd_MB >= @curSize_MB
			BEGIN
				SET @lv__WindowEnd_pages = @cursize_pages;
			END
			ELSE
			BEGIN
				SET @lv__WindowEnd_pages = @WindowEnd_MB*128;
			END
		END

		IF @ChunkSize_MB IS NOT NULL	--specified by user
		BEGIN
			SET @lv__ChunkSize_pages = 128*@ChunkSize_MB
		END 
		ELSE IF @curSize_MB < 2560
		BEGIN
			--break DB into 128 MB chunks
			SET @lv__ChunkSize_pages = 128*128			--1 page is 8kb. 128 is 1 MB
		END
		ELSE IF @curSize_MB >= 2560 AND @curSize_MB < 5120
		BEGIN
			SET @lv__ChunkSize_pages = 128*256
		END
		ELSE IF @curSize_MB >= 5120 AND @curSize_MB < 20480
		BEGIN
			SET @lv__ChunkSize_pages = 128*512
		END
		ELSE IF @curSize_MB >= 20480 AND @curSize_MB < 51200
		BEGIN
			SET @lv__ChunkSize_pages = 128*1024
		END
		ELSE IF @curSize_MB >= 51200 AND @curSize_MB < 102400
		BEGIN
			SET @lv__ChunkSize_pages = 128*2048
		END
		ELSE
		BEGIN
			SET @lv__ChunkSize_pages = 128*4192
		END

		--select @lv__ChunkSize_pages, @cursize_pages;

		;WITH NumberBase0 AS (
			SELECT 1 col1 UNION ALL SELECT 0
		),
		NumberBase1 AS (
			SELECT 1 as col2 FROM NumberBase0 t0 
						CROSS JOIN NumberBase0 t1
						CROSS JOIN NumberBase0 t2
						CROSS JOIN NumberBase0 t3
		),
		NumberBase2 AS (
			SELECT 1 as col3 FROM NumberBase1 t0 
						CROSS JOIN NumberBase1 t1
						CROSS JOIN NumberBase1 t2
						CROSS JOIN NumberBase1 t3
		),
		AssignNumbers (rn) AS (
			SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
			FROM NumberBase2
		),
		LimitRows AS (
			SELECT 
				SizeMB_RangeStart = ((rn-1)*@lv__ChunkSize_pages)*8/1024,
				SizeMB_RangeEnd = (rn*@lv__ChunkSize_pages)*8/1024,
				SizePages_RangeStart = (rn-1)*@lv__ChunkSize_pages,
				SizePages_RangeEnd = rn*@lv__ChunkSize_pages
			FROM AssignNumbers 
			WHERE (rn-1)*@lv__ChunkSize_pages <= CASE WHEN @lv__WindowEnd_pages IS NULL THEN @cursize_pages
													WHEN @lv__WindowEnd_pages > @cursize_pages THEN @cursize_pages
													ELSE @lv__WindowEnd_pages END
			AND (rn-1)*@lv__ChunkSize_pages >= @lv__WindowStart_pages
		),
		Bucketize AS (
			SELECT 
				BucketID = ROW_NUMBER() OVER (ORDER BY SizePages_RangeStart), 
				SizeMB_RangeStart,
				SizeMB_RangeEnd,
				SizePages_RangeStart,
				SizePages_RangeEnd
			FROM LimitRows lr
		),
		AllocUnitExplosion AS (
			SELECT b.*, AllocUnitTypes.AllocUnitType
			FROM Bucketize b
				CROSS JOIN #AllocTypes AllocUnitTypes
		)
		INSERT INTO #DBFileBuckets (
			file_id,
			bucket_id,
			display_position_id,
			AllocationType, 
			SizeMB_RangeStart,
			SizeMB_RangeEnd,
			SizePages_RangeStart,
			SizePages_RangeEnd
		)
		SELECT 
			@curFileID,
			BucketID,
			display_position_id = ROW_NUMBER() OVER (ORDER BY BucketID, AllocUnitType),
			AllocUnitType,
			SizeMB_RangeStart,
			SizeMB_RangeEnd,
			SizePages_RangeStart,
			SizePages_RangeEnd
		FROM AllocUnitExplosion a
		;

		FETCH iterateFiles1 INTO @curFileID, @cursize_pages, @curSize_MB
	END

	CLOSE iterateFiles1;
	DEALLOCATE iterateFiles1;


	/* Now that we have the buckets defined for each file, bucketize the results
	*/
		 
	UPDATE targ 
	SET 
		DataFound = 1,
		NumAllocatedExtents = ss2.NumAllocatedExtents, 
		NumAllocatedPages = ss2.NumAllocatedPages,
		NumAllocatedPages_PageAllocationStatusZero = ss2.NumAllocatedPages_StatusZero,
		NumIAMPages = ss2.NumIAMPages,
		NumMixedPages = ss2.NumMixedPages,
		NumPagesWithGhostRecords = ss2.NumPagesWithGhostRecords
	FROM #DBFileBuckets targ
	INNER JOIN (
		SELECT 
			extent_file_id, 
			SizePages_RangeStart,
			SizePages_RangeEnd,
			AllocType, 
			COUNT(*) as NumAllocatedExtents, 
			SUM(NumAllocatedPages) as NumAllocatedPages, 
			SUM(NumAllocatedPages_StatusZero) as NumAllocatedPages_StatusZero,
			SUM(NumIAMPages) as NumIAMPages,
			SUM(NumMixedPages) as NumMixedPages,
			SUM(NumPagesWithGhostRecords) as NumPagesWithGhostRecords
		FROM (
			SELECT extent_file_id, 
				SizePages_RangeStart,
				SizePages_RangeEnd,
				AllocType, 
				extent_page_id, 
				COUNT(*) as NumAllocatedPages,
				--allocated_page_page_id, 
				SUM(NumAllocatedPages_StatusZero) as NumAllocatedPages_StatusZero, 
				SUM(is_iam_page) as NumIAMPages, 
				SUM(is_mixed_page_allocation) as NumMixedPages, 
				SUM(has_ghost_records) as NumPagesWithGhostRecords
			FROM (
				SELECT alloc.extent_file_id, 
					buck.SizePages_RangeStart,
					buck.SizePages_RangeEnd, 
					AllocType = CASE WHEN @lv__AllocBreakoutLogic = N'TOTAL' THEN N'TOTAL' ELSE alloc.allocation_unit_type_desc END,
					alloc.extent_page_id, 
					alloc.allocated_page_page_id, 
					CONVERT(INT,is_iam_page) as is_iam_page, 
					CONVERT(INT,is_mixed_page_allocation) as is_mixed_page_allocation,
					CONVERT(INT,has_ghost_records) as has_ghost_records,
					NumAllocatedPages_StatusZero = CASE WHEN is_allocated = 0 THEN CONVERT(INT,1) ELSE CONVERT(INT,0) END
				FROM #DBFileBuckets buck
					INNER JOIN #DBPageAllocs_temp alloc
						ON alloc.extent_file_id = buck.file_id
						AND alloc.allocated_page_page_id >= buck.SizePages_RangeStart 
						AND	alloc.allocated_page_page_id < buck.SizePages_RangeEnd
				WHERE buck.file_id = @curFileID
			) ss0
			GROUP BY extent_file_id,
				SizePages_RangeStart,
				SizePages_RangeEnd,
				AllocType, 
				extent_page_id
			) ss1
		GROUP BY extent_file_id, 
			AllocType,
			SizePages_RangeStart,
			SizePages_RangeEnd
	) ss2
		ON targ.file_id = ss2.extent_file_id
		AND targ.SizePages_RangeStart = ss2.SizePages_RangeStart
		AND targ.AllocationType = ss2.AllocType
;

	UPDATE targ 
	SET RunTotalAlloc_FromStart_Pages_asc = rtTot.RunTotalFromStart,
		RunTotalAllocByAllocUnit_FromStart_Pages_asc = RunTotalFromStartByAlloc,
		RunTotalAlloc_FromEnd_Pages_desc = rtTot.RunTotalFromEnd,
		RunTotalAllocByAllocUnit_FromEnd_Pages_desc = RunTotalFromEndByAlloc
	FROM #DBFileBuckets targ 
		INNER JOIN (
			SELECT 
				file_id, 
				SizePages_RangeStart,
				SizePages_RangeEnd,
				[RunTotalFromStart] =	SUM(NumAllocatedPages) OVER (PARTITION BY file_id ORDER BY SizePages_RangeStart ASC ROWS UNBOUNDED PRECEDING),
				[RunTotalFromEnd] =		SUM(NumAllocatedPages) OVER (PARTITION BY file_id ORDER BY SizePages_RangeStart DESC ROWS UNBOUNDED PRECEDING)
			FROM (
				SELECT 
					file_id, 
					SizePages_RangeStart,
					SizePages_RangeEnd, 
					SUM(NumAllocatedPages) as NumAllocatedPages
				FROM #DBFileBuckets
				GROUP BY file_id, SizePages_RangeStart, SizePages_RangeEnd
				) ss
		) rtTot
			ON targ.file_id = rtTot.file_id
			AND targ.SizePages_RangeStart = rtTot.SizePages_RangeStart
		INNER JOIN (
			SELECT 
				file_id, 
				AllocationType,
				SizePages_RangeStart,
				SizePages_RangeEnd,
				[RunTotalFromStartByAlloc] =	SUM(NumAllocatedPages) OVER (PARTITION BY file_id, AllocationType	ORDER BY SizePages_RangeStart ASC ROWS UNBOUNDED PRECEDING),
				[RunTotalFromEndByAlloc] =		SUM(NumAllocatedPages) OVER (PARTITION BY file_id, AllocationType	ORDER BY SizePages_RangeStart DESC ROWS UNBOUNDED PRECEDING)
			FROM #DBFileBuckets
		) rtAlloc
			ON targ.file_id = rtAlloc.file_id
			AND targ.AllocationType = rtAlloc.AllocationType
			AND targ.SizePages_RangeStart = rtAlloc.SizePages_RangeStart
	;

	/*************************************** Return Results ****************************/
	/*
	#HeaderResultSet44 (
		DBName								SYSNAME, 
		FileGroupID							INT,
		FileGroupName						SYSNAME,
		FileID								INT,
		[FileLogicalName]					SYSNAME,
		[FilePhysicalName]					NVARCHAR(2048),
		[FileSize_pages]					BIGINT,
		[FileUsedSize_pages]				BIGINT,
		[TotalDataSize_pages]				BIGINT,
		[TotalUsedSize_pages]				BIGINT,
		[VolumeMountPoint]					NVARCHAR(512),
		[VolumeID]							NVARCHAR(512),
		[VolumeLogicalName]					NVARCHAR(512),
		[FileSystemType]					NVARCHAR(128),
		[VolumeTotalBytes]					BIGINT,
		[VolumeAvailableBytes]				BIGINT,
		[VolumeIsReadOnly]					INT,
		[VolumeIsCompressed]				INT,
		[VolumeSupportsCompression]			INT,
		[VolumeSupportsAlternateStreams]	INT,
		[VolumeSupportsSparseFiles]			INT,
		[IncludeInDetailedOutput]			NCHAR(1)
	)
	*/
	SET @lv__DynSQL = N'
		SELECT t.DBName
			,[FGName] = t.FileGroupName
			,[FileLogicName] = t.FileLogicalName
			,[FilePhysName] = t.FilePhysicalName
			 
			' + CASE WHEN @UnitsInPages = N'Y' THEN N',[FileOSSize (pages)] = t.FileSize_pages'
					ELSE N',[FileOSSize (MB)] = CONVERT(DECIMAL(25,3),t.FileSize_pages*8./1024)' END + N'

			' + CASE WHEN @UnitsInPages = N'Y' THEN N',[UsedSize (pages)] = t.FileUsedSize_pages'
					ELSE N',[UsedSize (MB)] = CONVERT(DECIMAL(25,3),t.FileUsedSize_pages*8./1024)' END + N'

			,[FileUse%] = CONVERT(DECIMAL(5,2),((t.FileUsedSize_pages*1.)/(t.FileSize_pages*1.)*100.))

			' + CASE WHEN @lv__numDBFiles > 1 THEN (
						CASE WHEN @UnitsInPages = N'Y' THEN N',[TotalOSSize (pages)] = t.TotalDataSize_pages'
							ELSE N',[TotalOSSize (MB)] = CONVERT(DECIMAL(25,3),t.TotalDataSize_pages*8./1024)'
						END )
					ELSE N'' END + N'
			' + CASE WHEN @lv__numDBFiles > 1 THEN (
						CASE WHEN @UnitsInPages = N'Y' THEN N',[TotalUsedSize (pages)] = t.TotalUsedSize_pages'
							ELSE N',[TotalUsedSize (MB)] = CONVERT(DECIMAL(25,3),t.TotalUsedSize_pages*8./1024)'
						END )
					ELSE N'' END + N'
			' + CASE WHEN @lv__numDBFiles > 1 THEN N',[%ofTotalOSSize] = CONVERT(DECIMAL(15,1),100*((t.FileSize_pages*1.) / (t.TotalDataSize_pages*1.)))'
				ELSE N'' END + N'
			' + CASE WHEN @lv__numDBFiles > 1 THEN N',[%ofTotalUsedSize] = CONVERT(DECIMAL(15,1),100*((t.FileUsedSize_pages*1.) / (t.TotalUsedSize_pages*1.)))'
				ELSE N'' END + N'
		FROM #HeaderResultSet44 t
		ORDER BY t.FileID;

	';

	EXEC(@lv__DynSQL);

	SELECT b.file_id,
		b.bucket_id,
		b.display_position_id,
		b.AllocationType,
		b.NumAllocatedPages,
		b.RunTotalAlloc_FromStart_Pages_asc,
		b.RunTotalAlloc_FromEnd_Pages_desc,
		b.RunTotalAllocByAllocUnit_FromStart_Pages_asc,
		b.RunTotalAllocByAllocUnit_FromEnd_Pages_desc
	FROM #DBFileBuckets b
	ORDER BY b.file_id, b.bucket_id


	/*
	SELECT file_id as FileID, 
		[FileRange (MB)] = CONVERT(varchar(20),SizeMB_RangeStart) + ' - ' + CONVERT(varchar(20),SizeMB_RangeEnd),
		[UsedMB] = CONVERT(decimal(25,3),(NumAllocatedPages*8./1024.)), 
		[Used%] = CONVERT(decimal(25,2),100*(NumAllocatedPages*1.)/(SizePages_RangeEnd - SizePages_RangeStart))
	FROM #DBFileBuckets;
	*/



	RETURN 0;
END

GO
