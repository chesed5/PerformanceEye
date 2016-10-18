USE [master]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_PE_FileUsage] 
/*   
	PROCEDURE:		sp_PE_FileUsage

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: Pulls file size/usage information for all files (currently, all *data* files), and if requested, 
		detailed info from the various system bitmaps showing where allocated/unallocated sections of the file are.
		The intended applications for the detailed data are:
			1) Seeing how much data is near the end of a file before doing a shrink operation
			2) Seeing whether a specific heap has data near the end of a file (which can drastically slow down shrinks)
			3) Seeing where LOB data is
			4) Seeing how many unallocated sections of various bucket sizes there are (e.g. 1 extent, 2 extents, etc) before
				doing an index rebuild of a large index. 


	FUTURE ENHANCEMENTS: 
		Consider adding running totals

    CHANGE LOG:	
				2016-10-14	Aaron Morelli		Initial Creation


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
	@Summary					NCHAR(1)			= N'Y',			--Whether the summary result set is returned or not.
	@BitmapType					NVARCHAR(20)		= NULL,			-- "NONE", "GAM", "BCM", "DCM", or "IAM"
																	--	TODO: take SGAMs into consideration somehow (prob just for UNALLOC). (otherwise you can end up with plenty of 
																	-- single-extent unalloc spaces in the GAM results that aren't really unalloc)
																	--  TODO: enable functionality where BCM or DCM can be selected but object/index filtering is still taken into
																	-- account, by overlaying the IAM results with the BCM/DCM results.

	--file filtering. If filtering by object/index, any filtering here must be limited to file IDs that correspond to the
	-- filegroups/partitions for the heap/index in question
	@FileNames					NVARCHAR(1024)		= N'',			-- comma-separated list of logical file names
	@FileIDs					NVARCHAR(128)		= N'',			-- comma-separatetd list of file IDs

	--object/index filtering (only 1 heap/index/partition can be displayed at a time). Currently only valid when @BitmapType = "IAM"
	@ObjectID					INT					= NULL,			-- The object ID. If this is specified, it takes precedence over @ObjectName
	@ObjectName					NVARCHAR(128)		= NULL,			-- The object name (schema-qualified). Must work with OBJECT_ID()
	@IndexID					INT					= NULL,			-- the index ID. If this is specified, it takes precedence over @IndexName
	@IndexName					NVARCHAR(128)		= NULL,			-- The index name
	@PartitionNumber			INT					= 1,
	@AllocType					NVARCHAR(20)		= N'',			-- "ALL", "INROW", "OVERFLOW", "LOB"		only used when a specific object/index is chosen

	--return formatting
	@BitmapReturn				NVARCHAR(128)		= N'',			-- Empty string, "TOTAL" (total alloc & unalloc, more useful for BCM/DCM), 
																	--  "MAP" (file broken down into chunks), "ALLOC" or "UNALLOC" number of extents by runlength.  
	@SegmentSize				INT					= 0,			-- 0 (auto-logic: ~5% of the data file size, w/max up to 32 GB), positive int (megabytes) between 32 MB and 32 GB
																	-- Only applicable when @BitmapReturn = "MAP"

	@Pivot						NCHAR(1)			= N'Y',			-- If Y, then multiple files and/or multiple alloc units are printed out left-to-right than just a vertical result set
	@Progress					NCHAR(1)			= N'N',			-- If @BitmapType is not an empty string, prints out progress between each bitmap page examined.
	@Help						NVARCHAR(20)		= N'N',
	@Directives					NVARCHAR(512)		= N'',
	@Debug						INT					= 0
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

	/* 
														Part 0: Variables, Validation, Temp Table Definitions
	*/
	DECLARE 
		--error handling, help, other metadata
		@helpstr							NVARCHAR(MAX),
		@helpexec							NVARCHAR(4000),
		@lv__ErrorText						NVARCHAR(MAX),
		@lv__ErrorSeverity					INT,
		@lv__ErrorState						INT,
		@lv__beforedt						DATETIME,
		@lv__afterdt						DATETIME,
		@lv__slownessthreshold				SMALLINT,
		@lv__DynSQL							NVARCHAR(MAX),

		--SQL inj protection
		@lv__StringLength					INT,
		@lv__StringPosition					INT,
		@lv__CurrChar						INT,

		--current DB
		@lv__DBName							NVARCHAR(128),
		@lv__DBID							INT,

		--Filtering and Files
		@lv__FilterObjID					INT,
		@lv__FilterIndexID					INT,
		@lv__numDBFiles						INT,

		--display
		@lv__ChunkSize_MB					INT,
		@lv__LargestFile					BIGINT,
		@lv__NumSegments					INT,

		--loop control
		@lv__IAMDoneLooping					NCHAR(1),		--giggle
		@lv__LoopIteration_zerobased		INT,
		@lv__LoopIteration_onebased			INT,
		@lv__CurrentFileID					INT,
		@lv__CurrentBitmapPageID			BIGINT,
		@lv__CurrentPageType_str			NVARCHAR(256),
		@lv__CurrentPageType_int			INT,
		@lv__CurrentBitmapRangeStartPage	BIGINT,
		@lv__CurrentBitmapRangeEndPage		BIGINT,
		@lv__CurrentFileSize_pages			BIGINT,
		@lv__CurrentAllocUnitType			NVARCHAR(128),
		@lv__CurrentAllocUnitID				BIGINT,
		@lv__NextFileID						INT,
		@lv__NextPageID						INT,

		--IAM validation variables
		@lv__IAMval__ObjID_str				NVARCHAR(256),
		@lv__IAMval__IdxID_str				NVARCHAR(256),
		@lv__IAMval__PrevFilePage_code		NVARCHAR(256),
		@lv__IAMval__PrevFilePage_page		NVARCHAR(256),
		@lv__IAMval__NextFilePage_str		NVARCHAR(256),
		@lv__IAMval__sequenceNumber_str		NVARCHAR(256),
		@lv__IAMval__sequenceNumber			BIGINT
		;

	SET @lv__DBID = DB_ID();
	SET @lv__DBName = DB_NAME();

	--Table variables
	DECLARE @tv__FileNameInclusions TABLE	(name SYSNAME);
	DECLARE @tv__FileIDInclusions TABLE		(file_id INT);

	CREATE TABLE #t__AllDBFiles		(file_id INT, type INT, data_space_id INT, name SYSNAME, physical_name NVARCHAR(2048), state_desc NVARCHAR(128), size BIGINT,
												UserIncluded TINYINT --0 excluded; 1 included by default; 2 included explicitly
									);

	CREATE TABLE #t__ObjectRetrieve (
		object_id INT,
		object_name SYSNAME,
		type NVARCHAR(2)
	);

	CREATE TABLE #t__IndexRetrieve (
		object_id INT,
		index_id INT,
		index_name SYSNAME,
		type INT
	);

	CREATE TABLE #t__AURetrieve (
		object_id INT,
		index_id INT,
		partition_id BIGINT,
		partition_number INT,
		hobt_id BIGINT,
		partition_rows BIGINT,
		data_compression_desc NVARCHAR(128),
		allocation_unit_id BIGINT,
		type_desc NVARCHAR(128),
		container_id BIGINT,
		data_space_id INT,
		au_total_pages BIGINT,
		au_used_pages BIGINT,
		au_data_pages BIGINT,
		au_first_iam_page BINARY(6), 
		FileID_Decoded INT,
		PageID_Decoded INT
	);

	CREATE TABLE #t__ShowFileStats (
		FileID INT, 
		[FileGroup] INT, 
		TotalExtents BIGINT, 
		UsedExtents BIGINT, 
		lname SYSNAME, 
		pname SYSNAME
	);

	CREATE TABLE #t__HeaderResultSet (
		DBName								NVARCHAR(128), 
		FileGroupID							INT,
		FileGroupName						NVARCHAR(128),
		FileID								INT,
		[FileLogicalName]					NVARCHAR(128),
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

	CREATE TABLE #DBCCPageOutput (
		ParentObject		NVARCHAR(256),
		[Object]			NVARCHAR(256),
		[Field]				NVARCHAR(256),
		[Value]				NVARCHAR(256),
		FileID				INT,
		RangeFirstPage		BIGINT,	--always extent boundaries
		RangeLastPage		BIGINT,
		RangeSize			BIGINT,
		AllocState			BIT
	);

	CREATE TABLE #GAMLoopExtentData (
		GAMPage_FileID		INT,
		GAMPage_PageID		BIGINT,
		GAMInterval			INT,
		IntervalStartPage	BIGINT,
		IntervalEndPage		BIGINT,
		RangeFirstPage		BIGINT,
		RangeLastPage		BIGINT,
		RangeSize_pages		BIGINT,
		RangeSize_extents	BIGINT,
		AllocState			BIT
		--TODO: should we add fields here for mapping the actual GAM interval start/end pages to the display start/end boundary points?
	);

	CREATE TABLE #IAMLoopExtentData (
		IAMPage_FileID		INT,
		IAMPage_PageID		BIGINT,
		sequenceNumber		INT,
		RangeFirstPage		BIGINT,
		RangeLastPage		BIGINT,
		RangeSize_pages		BIGINT,
		RangeSize_extents	BIGINT,
		AllocState			BIT
		--TODO: should we add fields here for mapping the actual GAM interval start/end pages to the display start/end boundary points?
	);

	SET @Help = UPPER(ISNULL(@Help,N'Z'));
	IF @Directives LIKE N'%wrapper%'
	BEGIN
		SET @helpexec = N'';
	END
	ELSE
	BEGIN
		SET @helpexec = N'
/* MIT License		Copyright (c) 2016 Aaron Morelli */
EXEC sp_PE_FileUsage	TODO: params here
';
	END

	IF @Help <> N'N'
	BEGIN
		GOTO helpbasic
	END

	--Parameter validation
	SET @Summary = UPPER(ISNULL(@Summary,N'Y'));
	SET @BitmapType = UPPER(ISNULL(@BitmapType,N'NONE'));
	SET @FileIDs = ISNULL(@FileIDs,N'');
	SET @FileIDs = ISNULL(@FileIDs,N'');

	SET @ObjectName = NULLIF(LTRIM(RTRIM(@ObjectName)),N'');
	SET @IndexName = NULLIF(LTRIM(RTRIM(@IndexName)),N'');

	IF @ObjectID <= 0
	BEGIN
		RAISERROR('Object ID cannot be zero or a negative number.', 16, 1);
		RETURN -1;
	END

	-- we need to protect against SQL injection. For now, we only support alphanumeric characters, plus spaces and brackets (to support QUOTENAME-type input)
	-- We already LTRIM/RTRIM'd the string, now loop through and look for characters other than these.
	SET @lv__StringPosition = 1;

	IF @ObjectName IS NOT NULL
	BEGIN
		WHILE @lv__StringPosition <= DATALENGTH(@ObjectName)
		BEGIN  
			SET @lv__CurrChar = UNICODE(SUBSTRING(@ObjectName,@lv__StringPosition,1));
			IF @lv__CurrChar NOT BETWEEN 65 AND 90		--N'A' to N'Z'
				AND @lv__CurrChar NOT BETWEEN 97 AND 122	--N'a' to N'z'
				AND @lv__CurrChar NOT BETWEEN 48 AND 57		--N'0' to N'9'
				AND @lv__CurrChar NOT IN (32, 46, --N' ', N'.',
											91,93,95							-- N'[', N']', N'_'
											)

				--may expand to: AND @lv__CurrChar NOT IN (32, 33, 35, 36, 37, 38, 42, 43,  --N' ', N'!', N'#', N'$', N'%', N'&', N'*', N'+', 
				--						 46, 58, 61, 91,93,95							--N'.', N':', N'=', N'[', N']', N'_'
				--						 )
			BEGIN
				RAISERROR('Only alphanumeric, space, period, underscore, and [ and ] characters are allowed in the @ObjectName parameter.', 16, 1);
				RETURN -1;
			END

			SET @lv__StringPosition = @lv__StringPosition + 1;
		END
	END

	IF @IndexID < 0
	BEGIN
		RAISERROR('Index ID cannot be a negative number.', 16, 1);
		RETURN -1;
	END

	SET @lv__StringPosition = 1;

	IF @IndexName IS NOT NULL
	BEGIN
		WHILE @lv__StringPosition <= DATALENGTH(@IndexName)
		BEGIN  
			SET @lv__CurrChar = UNICODE(SUBSTRING(@IndexName,@lv__StringPosition,1));
			IF @lv__CurrChar NOT BETWEEN 65 AND 90		--N'A' to N'Z'
				AND @lv__CurrChar NOT BETWEEN 97 AND 122	--N'a' to N'z'
				AND @lv__CurrChar NOT BETWEEN 48 AND 57		--N'0' to N'9'
				AND @lv__CurrChar NOT IN (32, 46, --N' ', N'.',
											91,93,95							-- N'[', N']', N'_'
											)

				--may expand to: AND @lv__CurrChar NOT IN (32, 33, 35, 36, 37, 38, 42, 43,  --N' ', N'!', N'#', N'$', N'%', N'&', N'*', N'+', 
				--						 46, 58, 61, 91,93,95							--N'.', N':', N'=', N'[', N']', N'_'
				--						 )
			BEGIN
				RAISERROR('Only alphanumeric, space, period, underscore, and [ and ] characters are allowed in the @ObjectName parameter.', 16, 1);
				RETURN -1;
			END

			SET @lv__StringPosition = @lv__StringPosition + 1;
		END
	END

	IF @PartitionNumber IS NULL
	BEGIN
		SET @PartitionNumber = 1;
	END
	ELSE 
	BEGIN
		IF @PartitionNumber <= 0
		BEGIN
			RAISERROR('Partition number cannot be zero or a negative number.', 16, 1);
			RETURN -1;
		END
	END

	SET @AllocType = UPPER(ISNULL(@AllocType,N'ALL'));

	SET @BitmapReturn = UPPER(ISNULL(@BitmapReturn,N''));
	SET @SegmentSize = ISNULL(@SegmentSize,0);
	SET @Progress = UPPER(ISNULL(@Progress,N''));

	IF @Summary NOT IN (N'N', N'Y')
	BEGIN
		RAISERROR('Parameter @Summary must be either Y or N', 16, 1);
		RETURN -1;
	END

	IF @BitmapType NOT IN (N'NONE', N'GAM', N'BCM', N'DCM', N'IAM')
	BEGIN
		RAISERROR('If specified, Parameter @BitmapType must be either NONE (default), GAM, BCM, DCM, or IAM.', 16, 1);
		RETURN -1;
	END

	IF @BitmapType <> N'IAM' 
	BEGIN
		IF (@ObjectID IS NOT NULL
			OR @ObjectName IS NOT NULL
			OR @IndexID IS NOT NULL
			OR @IndexName IS NOT NULL
			OR NULLIF(@PartitionNumber,1) IS NOT NULL
			)
		BEGIN
			RAISERROR('Object/Index/Partition filtering is currently only allowed when @BitmapType = "IAM".', 16, 1);
			RETURN -1;
		END
	END
	ELSE
	BEGIN
		--bitmap is IAM. We require a filter.
		IF (@ObjectID IS NULL AND @ObjectName IS NULL)
			OR (@IndexID IS NULL AND @IndexName IS NULL)
			OR (@PartitionNumber IS NULL)			--this actually shouldn't happen since we set @PartitionNumber to 1 above if nul
		BEGIN
			RAISERROR('Object/Index/Partition filtering is required when @BitmapType = "IAM".', 16, 1);
			RETURN -1;
		END
	END

	IF @BitmapType IN (N'GAM', N'BCM', N'DCM', N'IAM')
	BEGIN
		IF @BitmapReturn NOT IN (N'TOTAL', N'MAP', N'ALLOC', N'UNALLOC')
		BEGIN
			RAISERROR('If @BitmapType is specified, parameter @BitmapReturn must be either TOTAL, MAP, ALLOC, or UNALLOC.', 16, 1);
			RETURN -1;
		END

		IF @BitmapReturn = N'MAP'
		BEGIN
			IF @SegmentSize < 0
			BEGIN
				RAISERROR('If @BitmapType is specified and @BitmapReturn = "MAP", parameter @SegmentSize must be a positive value.', 16, 1);
				RETURN -1;
			END
		END

		IF @Progress NOT IN (N'N', N'Y')
		BEGIN
			RAISERROR('If @BitmapType is specified, parameter @Progress must be either Y or N.', 16, 1);
			RETURN -1;
		END
	END

	--First, we get all DB files. Note that even though this is an sp_ object, we still get the "master" DB objects when we query
	-- views like sys.database_files. So we use the master_files view (and also use dynamic SQL with a USE <db> in other places in this proc)
	INSERT INTO #t__AllDBFiles (
		file_id, type, data_space_id, name, physical_name, 
		state_desc, size,
		UserIncluded --0 excluded; 1 included by default; 2 included explicitly
	)
	SELECT mf.file_id, mf.type, mf.data_space_id, mf.name, mf.physical_name,
		mf.state_desc, mf.size,
		UserIncluded = CASE WHEN mf.type = 0 THEN 1 ELSE 0 END		--start off assuming all data files are in scope.
	FROM sys.master_files mf
	WHERE mf.database_id = @lv__DBID
	;

	IF @FileNames <> N''
	BEGIN
		BEGIN TRY 
			;WITH StringSplitter AS ( 
				SELECT CAST('<M>' + REPLACE( col1,  ',' , '</M><M>') + '</M>' AS XML) AS Names 
				FROM (SELECT @FileNames as col1) ss1 
			) 
			INSERT INTO @tv__FileNameInclusions (name)
			SELECT SS.FNames
			FROM (
				SELECT LTRIM(RTRIM(Split.a.value('.', 'NVARCHAR(128)'))) AS FNames
				FROM StringSplitter 
				CROSS APPLY Names.nodes('/M') Split(a)
				) SS
			WHERE SS.FNames <> N'';
		END TRY
		BEGIN CATCH
			RAISERROR('Error occurred when attempting to convert the @Filenames parameter (comma-separated list of logical file names) to a table of strings.', 11, 1);
			RETURN -1;
		END CATCH
	END

	IF EXISTS (SELECT * FROM @tv__FileNameInclusions)
	BEGIN
		IF EXISTS (SELECT *
					FROM @tv__FileNameInclusions t
					WHERE NOT EXISTS (SELECT * FROM #t__AllDBFiles a
										WHERE a.name = t.name)
					)
		BEGIN
			RAISERROR('One or more logical file names passed in via the @FileNames parameter does not exist in sys.master_files.', 11, 1);
			RETURN -1;
		END

		IF EXISTS (SELECT *
					FROM @tv__FileNameInclusions t
							INNER JOIN #t__AllDBFiles a
								ON t.name = a.name
					WHERE a.type <> 0
					)
		BEGIN
			RAISERROR('Currently only data files are valid for the @FileNames and @FileIDs filtering parameters.', 11, 1);
			RETURN -1;
		END
	END

	IF @FileIDs <> N''
	BEGIN
		BEGIN TRY 
			;WITH StringSplitter AS ( 
				SELECT CAST('<M>' + REPLACE( col1,  ',' , '</M><M>') + '</M>' AS XML) AS Names 
				FROM (SELECT @FileIDs as col1) ss1 
			)
			INSERT INTO @tv__FileIDInclusions (file_id)
				SELECT SS.FID
				FROM (
					SELECT CONVERT(INT,LTRIM(RTRIM(Split.a.value('.', 'NVARCHAR(100)')))) AS FID
					FROM StringSplitter 
					CROSS APPLY Names.nodes('/M') Split(a)
					) SS
				WHERE SS.FID <> 0;
		END TRY
		BEGIN CATCH
			RAISERROR('Error occurred when attempting to convert the @FileIDs parameter (comma-separated list of File IDs) to a table of integers.', 11, 1);
			RETURN -1;
		END CATCH
	END

	IF EXISTS (SELECT * FROM @tv__FileIDInclusions)
	BEGIN
		IF EXISTS (SELECT *
					FROM @tv__FileIDInclusions t
					WHERE NOT EXISTS (SELECT * FROM #t__AllDBFiles a
										WHERE a.file_id = t.file_id
										)
					)
		BEGIN
			RAISERROR('One or more file IDs passed in via the @FileIDs parameter does not exist in sys.master_files.', 11, 1);
			RETURN -1;
		END

		IF EXISTS (SELECT *
					FROM @tv__FileIDInclusions t
							INNER JOIN #t__AllDBFiles a
								ON t.file_id = a.file_id
					WHERE a.type <> 0
					)
		BEGIN
			RAISERROR('Currently only data files are valid for the @FileNames and @FileIDs filtering parameters.', 11, 1);
			RETURN -1;
		END
	END

	IF EXISTS (SELECT * FROM @tv__FileNameInclusions)
		OR EXISTS (SELECT * FROM @tv__FileIDInclusions)
	BEGIN
		UPDATE targ
		SET targ.UserIncluded = CASE WHEN u.file_id IS NOT NULL THEN 2
									ELSE 0
								END
		FROM #t__AllDBFiles targ
			LEFT OUTER JOIN (
				--If both were supplied and the inclusion lists aren't identical, instead of complaining we'll just take everything
				SELECT a.file_id
				FROM @tv__FileNameInclusions f
					INNER JOIN #t__AllDBFiles a
						ON f.name = a.name
				UNION 
				SELECT f.file_id
				FROM @tv__FileIDInclusions f
			) u
				ON u.file_id = targ.file_id
		;
	END

	--If any of the files that are still in scope are not online, error out
	IF EXISTS (SELECT * FROM #t__AllDBFiles f WHERE f.UserIncluded > 0 AND f.state_desc <> N'ONLINE')
	BEGIN
		SELECT f.file_id, f.name, f.physical_name, f.state_desc
		FROM #t__AllDBFiles f;

		RAISERROR('One or more files that are requested for inclusion (all data files are included by default) is not ONLINE. See resulting data set.', 16, 1);
		RETURN -1;
	END

	--Object/Index/Partition validation. Remember that b/c of a check above, we know that if any of these are non-NULL, 
	-- @BitmapType must be IAM
	IF @ObjectID IS NOT NULL OR @ObjectName IS NOT NULL
	BEGIN
		--Object ID takes precedence. Already tested for 0 or negative IDs above
		IF @ObjectID IS NOT NULL
		BEGIN
			SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
			INSERT INTO #t__ObjectRetrieve 
				(object_id, object_name, type)
			SELECT o.object_id, o.name, o.type
			FROM sys.objects o
			WHERE o.object_id = ' + CONVERT(NVARCHAR(20),@ObjectID) + N'
			';

			EXEC (@lv__DynSQL);

			IF NOT EXISTS (SELECT * FROM #t__ObjectRetrieve o
						WHERE o.type IN (N'U', N'V')
					)
			BEGIN
				RAISERROR('Value supplied for @ObjectID is not a valid user table or view', 11, 1);
				RETURN -1;
			END
		END
		ELSE
		BEGIN
			--we already tested for sql injection above. Pass @ObjectName to OBJECT_ID() function
			SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
			INSERT INTO #t__ObjectRetrieve 
				(object_id, object_name, type)
			SELECT o.object_id, o.name, o.type
			FROM sys.objects o
			WHERE o.object_id = object_id(' + @ObjectName + N')
			';

			EXEC (@lv__DynSQL);

			IF NOT EXISTS (SELECT * FROM #t__ObjectRetrieve o
						WHERE o.type IN (N'U', N'V')
					)
			BEGIN
				RAISERROR('Value supplied for @ObjectName is not a valid user table or view', 11, 1);
				RETURN -1;
			END
		END

		SELECT 
			@lv__FilterObjID = o.object_id
		FROM #t__ObjectRetrieve o;

		--If we get here, we have a row in #t__ObjectRetrieve and it is a table or view (we don't know
		-- about materialized view yet)
		IF @IndexID IS NOT NULL
		BEGIN
			--Already tested for negative IDs above
			SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
			INSERT INTO #t__IndexRetrieve 
				(object_id, index_id, index_name, type)
			SELECT o.object_id, i.index_id, i.name, i.type
			FROM sys.indexes i
				INNER JOIN #t__ObjectRetrieve o
					ON i.object_id = o.object_id
			WHERE i.index_id = ' + CONVERT(NVARCHAR(20),@IndexID) + N'
			';

			EXEC (@lv__DynSQL);

			IF NOT EXISTS (SELECT * FROM #t__IndexRetrieve i
						WHERE i.type IN (0,1,2)
					)
			BEGIN
				RAISERROR('Value supplied for @IndexID is not a valid heap, clustered b-tree index, or nonclustered b-tree index.', 11, 1);
				RETURN -1;
			END
		END
		ELSE
		BEGIN
			--We already tested for sql injection above
			SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
			INSERT INTO #t__IndexRetrieve 
				(object_id, index_id, index_name, type)
			SELECT o.object_id, i.index_id, i.name, i.type
			FROM sys.indexes i
				INNER JOIN #t__ObjectRetrieve o
					ON i.object_id = o.object_id
			WHERE i.name = ' + @IndexName + N'
			';

			EXEC (@lv__DynSQL);
			
			IF NOT EXISTS (SELECT * FROM #t__IndexRetrieve i
						WHERE i.type IN (0,1,2)
					)
			BEGIN
				RAISERROR('Value supplied for @IndexName is not a valid heap, clustered b-tree index, or nonclustered b-tree index.', 11, 1);
				RETURN -1;
			END
		END

		SELECT 
			@lv__FilterIndexID = i.index_id
		FROM #t__IndexRetrieve i;

		--If we get here, we have a valid object row that points to a user table or an indexed view,
		-- and a valid index row that points to a heap or b-tree index on that table/indexed view.

		IF @AllocType = N''
		BEGIN
			SET @AllocType = N'ALL'
		END
		ELSE
		BEGIN
			IF @AllocType NOT IN (N'ALL', N'INROW', N'OVERFLOW', N'LOB')
			BEGIN
				RAISERROR('If @AllocType is specified it must be INROW, OVERFLOW, LOB, or ALL.', 11, 1);
				RETURN -1;
			END
		END

		--Now, get partition and allocation_unit info

		--the data_space_id column is used in a number of places and is a bit confusing at first. Here's what I've learned:
		-- If a table uses the TEXTIMAGE_ON keyword, sys.tables.lob_data_space_id points to the data_space where LOB data
		-- is stored for that table. (Amendment: actually, it seems to record wherever LOB data is stored, even if it is
		-- in the same FG as the base data). You can then join that lob_data_space_id over to sys.database_files.data_space_id to
		-- get the files that store LOB data.
		-- The sys.indexes.data_space_id is the data space that we typically think of when we think of the storage for
		-- the primary data in that heap, cl idx, or ncl idx. THAT data_space_id can either:
		--		point to sys.data_spaces (whether or not the index is partitioned... that view will hold a record for the partition scheme or the FG)
		--		point to sys.destination_data_spaces (but only if the index is partitioned)
		-- However, to definitively get the file(s) where data is stored, you can ignore the sys.tables and sys.indexes data spaces, 
		-- and instead do this: sys.partitions-->sys.allocation_units.data_space_id-->sys.database_files.data_space_id
		-- If an index is on an FG w/multiple files, the AU will still only have 1 row, but there will be multiple sys.database_files
		-- records that join to that AU.
		SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
			INSERT INTO #t__AURetrieve (
				object_id,
				index_id,
				partition_id,
				partition_number,
				hobt_id,
				partition_rows,
				data_compression_desc,
				allocation_unit_id,
				type_desc,
				container_id,
				data_space_id,
				au_total_pages,
				au_used_pages,
				au_data_pages,
				au_first_iam_page
				--FileID_Decoded,
				--PageID_Decoded
			)
			SELECT 
				p.object_id,
				p.index_id,
				p.partition_id,
				p.partition_number,
				p.hobt_id,
				p.rows,
				p.data_compression_desc,
				au.allocation_unit_id,
				au.type_desc,
				au.container_id,
				au.data_space_id,
				au.total_pages,
				au.used_pages,
				au.data_pages,
				siau.first_iam_page
			FROM #t__ObjectRetrieve o
				INNER JOIN #t__IndexRetrieve i
					ON o.object_id = i.object_id
				INNER JOIN sys.partitions p
					ON p.object_id = o.object_id
					AND p.index_id = i.index_id
					AND p.partition_number = ' + CONVERT(NVARCHAR(20),@PartitionNumber) + N'
				INNER JOIN sys.allocation_units au
					ON au.container_id = CASE WHEN au.type IN (1,3) THEN p.hobt_id
												WHEN au.type = 2 THEN p.partition_id
												ELSE -1
												END
				INNER JOIN sys.system_internals_allocation_units siau
					ON au.allocation_unit_id = siau.allocation_unit_id
			WHERE au.container_id <> 0 --deferred drop
			AND au.type <> 0	--deferred drop
			AND ' + CASE WHEN @AllocType = N'ALL' 
							THEN N'au.type_desc IN (N''IN_ROW_DATA'', N''ROW_OVERFLOW_DATA'', N''LOB_DATA'') '
						WHEN @AllocType = N'INROW'
							THEN N'au.type_desc = N''IN_ROW_DATA'' '
						WHEN @AllocType = N'OVERFLOW'
							THEN N'au.type_desc = N''ROW_OVERFLOW_DATA'' '
						WHEN @AllocType = N'LOB'
							THEN N'au.type_desc = N''LOB_DATA'' '
						ELSE N'1=1'
						END + N';'

		EXEC (@lv__DynSQL);

		IF NOT EXISTS (SELECT * FROM #t__AURetrieve au)
		BEGIN
			RAISERROR('No allocation units found for this object/index/partition combo.', 11, 1);
			RETURN -1;
		END

		UPDATE #t__AURetrieve
		SET FileID_Decoded = CONVERT(INT, (
											SUBSTRING(au_first_iam_page,6,1) + 
											SUBSTRING(au_first_iam_page,5,1)
										)
									),
			PageID_Decoded = CONVERT(INT, (
											SUBSTRING(au_first_iam_page,4,1) + 
											SUBSTRING(au_first_iam_page,3,1) + 
											SUBSTRING(au_first_iam_page,2,1) + 
											SUBSTRING(au_first_iam_page,1,1)
										)
									)
		;

		--If the user has requested any files (in @tv__UserRequestedInclusions) that are NOT in the list of File IDs
		-- for the alloc unit(s) that belong to this obj/idx/part (and pass the @AllocType param filter), then
		-- we warn them and display the files that are valid for this obj/idx/part/@AllocType combo.

		IF EXISTS ( SELECT * 
					FROM #t__AllDBFiles t
					WHERE t.UserIncluded = 2
					AND NOT EXISTS (
						SELECT t2.file_id
						FROM #t__AURetrieve t1
							INNER JOIN #t__AllDBFiles t2
								ON t1.data_space_id = t2.data_space_id
						WHERE t.file_id = t2.file_id
						)
			)
		BEGIN
			SET @lv__ErrorText = N'One or more files requested for inclusion are not valid files for this set of Object/Index/Partition/@AllocType parameter values.';
			SET @lv__ErrorText = @lv__ErrorText + N'Please review the returned set of file IDs that are valid for this Object/Index/Partition/@AllocType combo.';

			SELECT t.index_id, t.partition_number, t.partition_rows, t.allocation_unit_id, t.type_desc,
				t.data_space_id, t.au_total_pages, f.file_id, f.name
			FROM #t__AURetrieve t
				LEFT OUTER JOIN #t__AllDBFiles f
					ON t.data_space_id = f.data_space_id
			ORDER BY t.allocation_unit_id ASC;

			RAISERROR(@lv__ErrorText,16,1);
			RETURN -1;
		END
	END	--IF @ObjectID IS NOT NULL OR @ObjectName IS NOT NULL

	--Out of the files that are still in play, what's the biggest size? This affects the display of the data
	SELECT @lv__LargestFile = ss.size
	FROM (
		SELECT TOP 1 df.size
		FROM #t__AllDBFiles df
		WHERE df.type = 0
		AND df.UserIncluded > 0
		ORDER BY df.size desc
		) ss
	;
	
	IF @BitmapReturn = N'MAP' AND @BitmapType <> N'NONE'
	BEGIN
		--We default the segment size to 5% of the largest file.
		IF @SegmentSize = 0
		BEGIN
			SET @SegmentSize = CONVERT(BIGINT,(@lv__LargestFile*8/1024)*0.05);
		END

		--Compare the user-submitted @SegmentSize to a bunch of accepted standard chunk sizes and choose the one with the lowest absolute difference
		;WITH SegSize AS (
			SELECT SegSize = @SegmentSize
		),
		ValidChunkSizes AS (
			SELECT AlignedChunkSize = 32
			UNION ALL SELECT 64
			UNION ALL SELECT 128
			UNION ALL SELECT 256
			UNION ALL SELECT 512
			UNION ALL SELECT 1024
			UNION ALL SELECT 2048
			UNION ALL SELECT 4096
			UNION ALL SELECT 8192
			UNION ALL SELECT 16384
			UNION ALL SELECT 32768
		),
		AbsDiff AS (
			SELECT 
				AbsDiff = ABS(AlignedChunkSize - @SegmentSize),
				AlignedChunkSize
			FROM SegSize CROSS JOIN ValidChunkSizes
		),
		MinDiff AS (
			SELECT TOP 1 AlignedChunkSize
			FROM AbsDiff
			ORDER BY AbsDiff ASC
		)
		SELECT 
			@lv__ChunkSize_MB = AlignedChunkSize
		FROM MinDiff
		;

		SET @lv__NumSegments = ((@lv__LargestFile*8/1024) / @lv__ChunkSize_MB) + 1;
	END

	/* Ok, at this point validation is done and we're ready to start doing real work!
	*/

	INSERT INTO #t__ShowFileStats (FileID, [FileGroup], TotalExtents, UsedExtents, lname, pname)
		EXEC ('DBCC showfilestats');

	INSERT INTO #t__HeaderResultSet (
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
	SELECT 
			[DBName] = @lv__DBName, 
			[FileGroupID] = t.FileGroup,
			[FileGroupName] = N'n/a', 
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
			[IncludeInDetailedOutput] = N'Y'
		FROM #t__AllDBFiles df
			LEFT OUTER JOIN #t__ShowFileStats t 
			ON t.FileID = df.file_id
		OUTER APPLY sys.dm_os_volume_stats(@lv__DBID, df.file_id) vs
	;

	--For some reason referencing local views like sys.database_files and sys.data_spaces refers to master, not the 
	-- DB that this proc is called from. :-(
	SET @lv__DynSQL = N'USE ' + QUOTENAME(@lv__DBName) + N';
	UPDATE targ
	SET [FileGroupName] = dsp.name
	FROM #t__HeaderResultSet targ
		INNER JOIN sys.data_spaces dsp
			ON dsp.data_space_id = targ.FileGroupID
	;
	';

	EXEC (@lv__DynSQL);

	SET @lv__numDBFiles = (SELECT COUNT(*) FROM #t__HeaderResultSet);

	IF @lv__numDBFiles > 1
	BEGIN
		UPDATE targ 
		SET TotalDataSize_pages = ss0.TotalDataSize_pages,
			TotalUsedSize_pages = ss0.TotalUsedSize_pages
		FROM #t__HeaderResultSet targ
			INNER JOIN 
			(
			SELECT 
				FileID, 
				FileSize_pages, 
				FileUsedSize_pages,
				TotalDataSize_pages = SUM(FileSize_pages) OVER (),
				TotalUsedSize_pages = SUM(FileUsedSize_pages) OVER ()
			FROM #t__HeaderResultSet t
			) ss0
				ON targ.FileID = ss0.FileID
		;
	END

	/*
	SELECT * 
	FROM #t__AllDBFiles;

	SELECT * 
	FROM #t__AURetrieve;

	SELECT * 
	FROM #t__HeaderResultSet;
	*/

gamloop:

	IF @BitmapType = N'IAM'
	BEGIN
		goto iamloop
	END

	DECLARE GAMLoopIterateFiles CURSOR LOCAL FAST_FORWARD FOR
	SELECT 
		f.file_id, f.size
	FROM #t__AllDBFiles f
	WHERE f.UserIncluded > 0;

	OPEN GAMLoopIterateFiles;
	FETCH GAMLoopIterateFiles INTO @lv__CurrentFileID, @lv__CurrentFileSize_pages;

	WHILE @@FETCH_STATUS = 0	--For each file
	BEGIN
		SET @lv__LoopIteration_zerobased = 0;
		SET @lv__LoopIteration_onebased = 1;

		SET @lv__CurrentBitmapRangeStartPage = @lv__LoopIteration_zerobased*511232;
		SET @lv__CurrentBitmapRangeEndPage = ((@lv__LoopIteration_zerobased+1)*511232)-1;
		
		WHILE @lv__CurrentBitmapRangeStartPage <= @lv__CurrentFileSize_pages
		BEGIN
			IF @lv__CurrentBitmapRangeEndPage > @lv__CurrentFileSize_pages
			BEGIN
				SET @lv__CurrentBitmapRangeEndPage = @lv__CurrentFileSize_pages
			END

			SELECT @lv__CurrentBitmapPageID = (CASE WHEN @lv__LoopIteration_zerobased = 0
										THEN (CASE WHEN @BitmapType = N'GAM' THEN 2
												WHEN @BitmapType = N'SGAM' THEN 3
												WHEN @BitmapType = N'PFS' THEN 1
												ELSE -1
												END 
											)
										ELSE --not the first GAM interval
											(CASE WHEN @BitmapType = N'GAM' THEN 511232*@lv__LoopIteration_zerobased
												WHEN @BitmapType = N'SGAM' THEN (511232*@lv__LoopIteration_zerobased) + 1
												WHEN @BitmapType = N'PFS' THEN 8088*@lv__LoopIteration_zerobased
												ELSE -1
												END
											)
										END
									);

			SET @lv__DynSQL = N'DBCC PAGE(' + CONVERT(NVARCHAR(20),@lv__DBID) + N', ' + 
							CONVERT(NVARCHAR(20),@lv__CurrentFileID) + N', ' + 
							CONVERT(NVARCHAR(20),@lv__CurrentBitmapPageID) + N', 3) WITH TABLERESULTS, NO_INFOMSGS;';

			TRUNCATE TABLE #DBCCPageOutput;

			INSERT INTO #DBCCPageOutput
				(ParentObject, [Object], [Field], [Value])
			EXEC (@lv__DynSQL);

			SELECT 
				@lv__CurrentPageType_str = t.Value
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'm_type'
			;

			BEGIN TRY
				SET @lv__CurrentPageType_int = CONVERT(INT, @lv__CurrentPageType_str);
			END TRY
			BEGIN CATCH
				CLOSE GAMLoopIterateFiles;
				DEALLOCATE GAMLoopIterateFiles;

				SET @lv__ErrorText = N'Error converting the ' + @BitmapType + N' m_type field to INT for file: ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + N', page: ' + 
					ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>') + N', m_type: ' + 
					ISNULL(@lv__CurrentPageType_str,N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END CATCH

			IF @lv__CurrentPageType_int <> (CASE WHEN @BitmapType = N'GAM' THEN 8
												WHEN @BitmapType = N'DCM' THEN 16
												WHEN @BitmapType = N'BCM' THEN 17
												ELSE -1
												END
											)
			BEGIN
				CLOSE GAMLoopIterateFiles;
				DEALLOCATE GAMLoopIterateFiles;

				SET @lv__ErrorText = N'Unexpected value for page m_type. Found ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentPageType_int),N'<null>') + 
					N', expected ' + (CASE WHEN @BitmapType = N'GAM' THEN 8
												WHEN @BitmapType = N'DCM' THEN 16
												WHEN @BitmapType = N'BCM' THEN 17
												ELSE -1
												END
											) + 
					N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>') + 
					N', m_type raw: ' + ISNULL(@lv__CurrentPageType_str,N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			;WITH ScrubPrep AS (
				SELECT 
					SplitPos = CHARINDEX(N'-', [Field]),
					--RawText = LTRIM(RTRIM(REPLACE(REPLACE([Field], N'(', N''), N')', N'')))
					RawText = [Field],
					Alloc = LTRIM(RTRIM([Value]))
				FROM #DBCCPageOutput
				WHERE 1 = (CASE WHEN @BitmapType = N'GAM' AND [Object] LIKE N'GAM: Extent Alloc Status%' THEN 1
								WHEN @BitmapType = N'SGAM' AND [Object] LIKE N'SGAM: Extent Alloc Status%' THEN 1
								WHEN @BitmapType = N'PFS' AND [Object] LIKE N'PFS: Page Alloc Status%' THEN 1
							ELSE 0
							END)
			),
			Scrubbed AS (
				SELECT
					FirstExtent = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(SUBSTRING(RawText, 1, SplitPos),N'(', N''),N')', N''),N'-', N''))),
					LastExtent = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(SUBSTRING(RawText, SplitPos, 100),N'(', N''),N')', N''),N'-', N''))),
					Alloc1 = CASE WHEN Alloc LIKE N'NOT AL%' THEN 0 ELSE 1 END
				FROM ScrubPrep s
			),
			Final AS (
				SELECT 
					FirstExtent1 = CONVERT(BIGINT,SUBSTRING(FirstExtent, CHARINDEX(N':',FirstExtent)+1, 100)),
					LastExtent1 = CASE WHEN LastExtent = N'' THEN -1
									ELSE CONVERT(BIGINT,SUBSTRING(LastExtent, CHARINDEX(N':',LastExtent)+1, 100))
									END,
					Alloc1
				FROM Scrubbed
			)
			--SELECT * FROM Final 
			INSERT INTO #GAMLoopExtentData (
				GAMPage_FileID,						--1
				GAMPage_PageID,
				GAMInterval,
				IntervalStartPage,
				IntervalEndPage,					--5
				RangeFirstPage,
				RangeLastPage,
				RangeSize_pages,
				RangeSize_extents,
				AllocState							--10
			)
			SELECT 
				@lv__CurrentFileID,					--1
				@lv__CurrentBitmapPageID,
				GAMInterval = @lv__LoopIteration_onebased, 
				IntervalStartPage = @lv__CurrentBitmapRangeStartPage, 
				IntervalEndPage = @lv__CurrentBitmapRangeEndPage,			--5
				RangeFirstPage = FirstExtent1,
				RangeLastPage = CASE WHEN LastExtent1 = -1 THEN FirstExtent1 + 7
									ELSE LastExtent1 + 7
									END,
				RangeSize_pages = CASE WHEN LastExtent1 = -1 THEN 8
									ELSE (LastExtent1 + 7) - FirstExtent1 + 1
									END,
				RangeSize_pages = (CASE WHEN LastExtent1 = -1 THEN 8
									ELSE (LastExtent1 + 7) - FirstExtent1 + 1
									END)/8,
				AllocState = Alloc1 
			FROM Final
			;


			SET @lv__LoopIteration_zerobased = @lv__LoopIteration_zerobased + 1;
			SET @lv__LoopIteration_onebased = @lv__LoopIteration_onebased + 1;
			SET @lv__CurrentBitmapRangeStartPage = @lv__LoopIteration_zerobased*511232;
			SET @lv__CurrentBitmapRangeEndPage = ((@lv__LoopIteration_zerobased+1)*511232)-1;
		END
		

		FETCH GAMLoopIterateFiles INTO @lv__CurrentFileID, @lv__CurrentFileSize_pages;
	END

	CLOSE GAMLoopIterateFiles;
	DEALLOCATE GAMLoopIterateFiles;

	--if we get here, we can skip the iamloop b/c this was a global allocation map loop, so skip to
	-- afterloops
	goto afterloops

iamloop:
	DECLARE followIAMChain CURSOR LOCAL FAST_FORWARD FOR
	SELECT 
		t.allocation_unit_id,
		t.type_desc,
		--t.au_first_iam_page
		t.FileID_Decoded,
		t.PageID_Decoded
	FROM #t__AURetrieve t
	ORDER BY t.type_desc; 

	OPEN followIAMChain;
	FETCH followIAMChain INTO @lv__CurrentAllocUnitID, @lv__CurrentAllocUnitType, @lv__CurrentFileID, @lv__CurrentBitmapPageID;

	WHILE @@FETCH_STATUS = 0		--for each alloc unit
	BEGIN
		SET @lv__LoopIteration_zerobased = 0;
		SET @lv__LoopIteration_onebased = 1;
		SET @lv__IAMDoneLooping = N'N';
		SET @lv__IAMval__PrevFilePage_code = N'(0:0)';

		WHILE @lv__IAMDoneLooping = N'N'
		BEGIN
			TRUNCATE TABLE #DBCCPageOutput;

			SET @lv__DynSQL = N'DBCC PAGE(' + CONVERT(NVARCHAR(20),@lv__DBID) + N', ' + 
							CONVERT(NVARCHAR(20),@lv__CurrentFileID) + N', ' + 
							CONVERT(NVARCHAR(20),@lv__CurrentBitmapPageID) + N', 3) WITH TABLERESULTS, NO_INFOMSGS;';

			INSERT INTO #DBCCPageOutput
				(ParentObject, [Object], [Field], [Value])
			EXEC (@lv__DynSQL);

			--First, validate page type
			SET @lv__CurrentPageType_str = NULL;
			SELECT 
				@lv__CurrentPageType_str = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'm_type'
			;

			BEGIN TRY
				SET @lv__CurrentPageType_int = CONVERT(INT, @lv__CurrentPageType_str);
			END TRY
			BEGIN CATCH
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Error converting the ' + @BitmapType + N' m_type field to INT for file: ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + N', page: ' + 
					ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>') + N', m_type: ' + 
					ISNULL(@lv__CurrentPageType_str,N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END CATCH

			IF @lv__CurrentPageType_int <> 10
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected value for page m_type. Found ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentPageType_int),N'<null>') + 
					N', expected 10. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>') + 
					N', m_type raw: ' + ISNULL(@lv__CurrentPageType_str,N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			--Next, validate that this page belongs to the object/index we think it does
			SET @lv__IAMval__ObjID_str = NULL;
			SELECT 
				@lv__IAMval__ObjID_str = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'Metadata: ObjectId'
			;

			IF ISNULL(@lv__IAMval__ObjID_str,N'<null1>') <> ISNULL(CONVERT(NVARCHAR(20),@lv__FilterObjID),N'<null2>')
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected value for Metadata: ObjectId field. Found ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__IAMval__ObjID_str),N'<null>') + 
					N', expected ' + ISNULL(CONVERT(NVARCHAR(20),@lv__FilterObjID),N'<null>') + 
					N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			SET @lv__IAMval__IdxID_str = NULL;
			SELECT 
				@lv__IAMval__IdxID_str = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'Metadata: IndexId'
			;

			IF ISNULL(@lv__IAMval__IdxID_str,N'<null1>') <> ISNULL(CONVERT(NVARCHAR(20),@lv__FilterIndexID),N'<null2>')
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected value for Metadata: IndexId field. Found ' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__IAMval__IdxID_str),N'<null>') + 
					N', expected ' + ISNULL(CONVERT(NVARCHAR(20),@lv__FilterIndexID),N'<null>') + 
					N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			--validate m_prevpage
			SET @lv__IAMval__PrevFilePage_page = NULL;
			SELECT 
				@lv__IAMval__PrevFilePage_page = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'm_prevPage'
			;

			IF ISNULL(@lv__IAMval__PrevFilePage_page,N'<null1>') <> @lv__IAMval__PrevFilePage_code
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected value for m_prevPage field. Found ' + 
					ISNULL(@lv__IAMval__PrevFilePage_page,N'<null>') + 
					N', expected ' + ISNULL(@lv__IAMval__PrevFilePage_code,N'<null>') + 
					N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			--Validate sequenceNumber
			SET @lv__IAMval__sequenceNumber_str = NULL;
			SELECT 
				@lv__IAMval__sequenceNumber_str = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'sequenceNumber'
			;

			IF ISNULL(@lv__IAMval__sequenceNumber_str,N'<null1>') <> CONVERT(NVARCHAR(20),@lv__LoopIteration_zerobased)
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected value for sequenceNumber field. Found ' + 
					ISNULL(@lv__IAMval__sequenceNumber_str,N'<null>') + 
					N', expected ' + CONVERT(NVARCHAR(20),@lv__LoopIteration_zerobased) + 
					N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			/****** Actual work! *****/
			;WITH ScrubPrep AS (
				SELECT 
					SplitPos = CHARINDEX(N'-', [Field]),
					--RawText = LTRIM(RTRIM(REPLACE(REPLACE([Field], N'(', N''), N')', N'')))
					RawText = [Field],
					Alloc = LTRIM(RTRIM([Value]))
				FROM #DBCCPageOutput
				WHERE [Object] LIKE N'%IAM: Extent Alloc Status%'
			),
			Scrubbed AS (
				SELECT
					FirstExtent = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(SUBSTRING(RawText, 1, SplitPos),N'(', N''),N')', N''),N'-', N''))),
					LastExtent = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(SUBSTRING(RawText, SplitPos, 100),N'(', N''),N')', N''),N'-', N''))),
					Alloc1 = CASE WHEN Alloc LIKE N'NOT AL%' THEN 0 ELSE 1 END
				FROM ScrubPrep s
			),
			Final AS (
				SELECT 
					FirstExtent1 = CONVERT(BIGINT,SUBSTRING(FirstExtent, CHARINDEX(N':',FirstExtent)+1, 100)),
					LastExtent1 = CASE WHEN LastExtent = N'' THEN -1
									ELSE CONVERT(BIGINT,SUBSTRING(LastExtent, CHARINDEX(N':',LastExtent)+1, 100))
									END,
					Alloc1
				FROM Scrubbed
			)
			--SELECT * FROM Final 
			INSERT INTO #IAMLoopExtentData (
				IAMPage_FileID,
				IAMPage_PageID,
				sequenceNumber,
				RangeFirstPage,
				RangeLastPage,
				RangeSize_pages,
				RangeSize_extents,
				AllocState
				--TODO: should we add fields here for mapping the actual GAM interval start/end pages to the display start/end boundary points?
			)
			SELECT 
				@lv__CurrentFileID,					--1
				@lv__CurrentBitmapPageID,
				sequenceNumber = @lv__LoopIteration_zerobased, 
				RangeFirstPage = FirstExtent1,
				RangeLastPage = CASE WHEN LastExtent1 = -1 THEN FirstExtent1 + 7
									ELSE LastExtent1 + 7
									END,
				RangeSize_pages = CASE WHEN LastExtent1 = -1 THEN 8
									ELSE (LastExtent1 + 7) - FirstExtent1 + 1
									END,
				RangeSize_pages = (CASE WHEN LastExtent1 = -1 THEN 8
									ELSE (LastExtent1 + 7) - FirstExtent1 + 1
									END)/8,
				AllocState = Alloc1 
			FROM Final
			;
			/****** Actual work! *****/

			SET @lv__IAMval__PrevFilePage_code = N'(' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + N':' + 
					ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentBitmapPageID),N'<null>') + N')';

			--Parse out next page: m_nextPage
			SET @lv__IAMval__NextFilePage_str = NULL;
			SELECT 
				@lv__IAMval__NextFilePage_str = LTRIM(RTRIM(t.Value))
			FROM #DBCCPageOutput t 
			WHERE t.ParentObject LIKE N'%PAGE HEADER%'
			AND t.Field = N'm_nextPage'
			;
			SET @lv__IAMval__NextFilePage_str = ISNULL(@lv__IAMval__NextFilePage_str,N'');

			IF CHARINDEX(N'(', @lv__IAMval__NextFilePage_str) <= 0
				OR CHARINDEX(N')', @lv__IAMval__NextFilePage_str) <= 0
				OR CHARINDEX(N':', @lv__IAMval__NextFilePage_str) <= 0
			BEGIN
				CLOSE followIAMChain;
				DEALLOCATE followIAMChain;

				SET @lv__ErrorText = N'Unexpected format for m_nextPage field. Found ' + 
					ISNULL(@lv__IAMval__NextFilePage_str,N'<null>') + 
					N', expected (<number>:<number>). File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
					N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

				RAISERROR(@lv__ErrorText,16,1);
				RETURN -1;
			END

			IF @lv__IAMval__NextFilePage_str = N'(0:0)'
			BEGIN
				SET @lv__IAMDoneLooping = N'Y';
			END
			ELSE
			BEGIN
				BEGIN TRY
					SET @lv__NextFileID = CONVERT(INT,
												SUBSTRING(@lv__IAMval__NextFilePage_str, 
														CHARINDEX(N'(', @lv__IAMval__NextFilePage_str) + 1,
														CHARINDEX(N':', @lv__IAMval__NextFilePage_str) - (CHARINDEX(N'(', @lv__IAMval__NextFilePage_str)+1)
														)
												);
					SET @lv__NextPageID = CONVERT(BIGINT,
												SUBSTRING(@lv__IAMval__NextFilePage_str, 
														CHARINDEX(N':', @lv__IAMval__NextFilePage_str) + 1,
														CHARINDEX(N')', @lv__IAMval__NextFilePage_str) - CHARINDEX(N':', @lv__IAMval__NextFilePage_str) - 1
														)
												);
				END TRY
				BEGIN CATCH
					CLOSE followIAMChain;
					DEALLOCATE followIAMChain;

					SET @lv__ErrorText = N'Error occurred when parsing the m_nextPage field. Found ' + 
						ISNULL(@lv__IAMval__NextFilePage_str,N'<null>') + 
						N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
						N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

					RAISERROR(@lv__ErrorText,16,1);
					RETURN -1;
				END CATCH

				IF @lv__NextFileID <= 0 OR @lv__NextPageID <= 0
				BEGIN
					CLOSE followIAMChain;
					DEALLOCATE followIAMChain;

					SET @lv__ErrorText = N'Abnormal end to IAM chain encountered. Found ' + 
						ISNULL(@lv__IAMval__NextFilePage_str,N'<null>') + 
						N'. File ID: ' + ISNULL(CONVERT(NVARCHAR(20),@lv__CurrentFileID),N'<null>') + 
						N', page ID: ' + ISNULL(CONVERT(NVARCHAR(20), @lv__CurrentBitmapPageID), N'<null>');

					RAISERROR(@lv__ErrorText,16,1);
					RETURN -1;
				END
			END

			SET @lv__LoopIteration_zerobased = @lv__LoopIteration_zerobased + 1;
			SET @lv__LoopIteration_onebased = @lv__LoopIteration_onebased + 1
		END	--end of WHILE @lv__IAMDoneLooping = N'N' loop
		

		FETCH followIAMChain INTO @lv__CurrentAllocUnitID, @lv__CurrentAllocUnitType, @lv__CurrentFileID, @lv__CurrentBitmapPageID;
	END

	CLOSE followIAMChain;
	DEALLOCATE followIAMChain;


	/*
	DECLARE @allocation_unit_id BIGINT, 
			@allocation_type_desc NVARCHAR(40),
			@au_data_space_id INT,
			@au_first_iam_page BINARY(6),


	DECLARE iterateAU CURSOR LOCAL FAST_FORWARD FOR 
	SELECT 
		au.allocation_unit_id,
		au.type_desc,
		au.data_space_id,
		au.au_first_iam_page,
		au.FileID_Decoded,
		au.PageID_Decoded
	FROM #t__AURetrieve au 
	;

	OPEN iterateAU
	FETCH iterateAU INTO 
	*/


afterloops:

	/*
	SELECT g.*
	FROM #GAMLoopExtentData g
	ORDER BY g.GAMPage_FileID, g.GAMPage_PageID
	*/

	SELECT * 
	FROM #IAMLoopExtentData
	ORDER BY IAMPage_FileID, IAMPage_PageID;


	/*
														Part X: Print help
	*/
helpbasic:
	SET @helpstr = @helpexec;
	RAISERROR(@helpstr,10,1) WITH NOWAIT;

	IF @Help=N'N'
	BEGIN
		RETURN 0;
	END

	IF @Help <> N'N'
	BEGIN
		IF @Help LIKE N'PA%'
		BEGIN
			SET @Help = N'PARAMS';
		END
		ELSE IF @Help LIKE N'CO%'
		BEGIN
			SET @Help = N'COLUMNS';
		END
		ELSE
		BEGIN
			--user may have typed gibberish... which is ok, give him/her all the help
			SET @Help = N'ALL';
		END
	END

	IF @Help = N'PARAMS'
	BEGIN
		GOTO helpparams
	END
	ELSE IF @Help = N'COLUMNS'
	BEGIN
		GOTO helpcolumns
	END

helpparams:

	SET @helpstr = N'
	';
	RAISERROR(@helpstr,10,1);

	IF @Help = N'PARAMS'
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
	';

	RAISERROR(@helpstr,10,1);

	IF @Help = N'COLUMNS'
	BEGIN
		GOTO exitloc
	END
	ELSE
	BEGIN
		SET @helpstr = N'
		';
		RAISERROR(@helpstr,10,1);
	END

exitloc:
	RETURN 0;
END
GO
