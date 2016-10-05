SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#CleanUpPerformanceEyeObjects') IS NOT NULL
BEGIN
	DROP TABLE #CleanUpPerformanceEyeObjects
END
GO
CREATE TABLE #CleanUpPerformanceEyeObjects (
	ObjectName NVARCHAR(256)
);

IF OBJECT_ID('tempdb..#FailedPerformanceEyeObjects') IS NOT NULL
BEGIN
	DROP TABLE #FailedPerformanceEyeObjects
END
GO
CREATE TABLE #FailedPerformanceEyeObjects (
	ObjectName NVARCHAR(256),
	NumFailures INT,
	LastFailureMessage NVARCHAR(MAX)
);

DECLARE @curObjectName NVARCHAR(256),
		@DynSQL NVARCHAR(512),
		@FailureMessage NVARCHAR(MAX);

--procedures
WHILE EXISTS (SELECT * FROM sys.procedures p
				WHERE p.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						) 
			)
	AND NOT EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpPerformanceEyeObjects;

	INSERT INTO #CleanUpPerformanceEyeObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(p.schema_id)) + N'.' + QUOTENAME(p.name)
	FROM sys.procedures p
	WHERE p.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						) 
	;

	DECLARE IteratePerformanceEyeObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpPerformanceEyeObjects o
	ORDER BY o.ObjectName;

	OPEN IteratePerformanceEyeObjects;
	FETCH IteratePerformanceEyeObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP PROCEDURE ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedPerformanceEyeObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedPerformanceEyeObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IteratePerformanceEyeObjects INTO @curObjectName;
	END

	CLOSE IteratePerformanceEyeObjects;
	DEALLOCATE IteratePerformanceEyeObjects;
END

IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Procedures' as ObjType,* 
	FROM #FailedPerformanceEyeObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping procedures',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedPerformanceEyeObjects;


--functions
WHILE EXISTS (SELECT * FROM sys.objects f
				WHERE f.type in (N'FN', N'IF', N'TF') 
				AND f.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						) 
			)
	AND NOT EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpPerformanceEyeObjects;

	INSERT INTO #CleanUpPerformanceEyeObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(p.schema_id)) + N'.' + QUOTENAME(p.name)
	FROM sys.procedures p
	WHERE p.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						) 
	;

	DECLARE IteratePerformanceEyeObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpPerformanceEyeObjects o
	ORDER BY o.ObjectName;

	OPEN IteratePerformanceEyeObjects;
	FETCH IteratePerformanceEyeObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP FUNCTION ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedPerformanceEyeObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedPerformanceEyeObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IteratePerformanceEyeObjects INTO @curObjectName;
	END

	CLOSE IteratePerformanceEyeObjects;
	DEALLOCATE IteratePerformanceEyeObjects;
END

IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Functions' as ObjType,* 
	FROM #FailedPerformanceEyeObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping functions',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedPerformanceEyeObjects;

--views
WHILE EXISTS (SELECT * FROM sys.views v
				WHERE v.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						) 
				)
	AND NOT EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpPerformanceEyeObjects;

	INSERT INTO #CleanUpPerformanceEyeObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(v.schema_id)) + N'.' + QUOTENAME(v.name)
	FROM sys.views v
	WHERE v.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						)
	;

	DECLARE IteratePerformanceEyeObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpPerformanceEyeObjects o
	ORDER BY o.ObjectName;

	OPEN IteratePerformanceEyeObjects;
	FETCH IteratePerformanceEyeObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP VIEW ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedPerformanceEyeObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedPerformanceEyeObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IteratePerformanceEyeObjects INTO @curObjectName;
	END

	CLOSE IteratePerformanceEyeObjects;
	DEALLOCATE IteratePerformanceEyeObjects;
END

IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Views' as ObjType,* 
	FROM #FailedPerformanceEyeObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping views',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedPerformanceEyeObjects;

--tables
WHILE EXISTS (SELECT * FROM sys.tables t
				WHERE t.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						)
				)
	AND NOT EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	TRUNCATE TABLE #CleanUpPerformanceEyeObjects;

	INSERT INTO #CleanUpPerformanceEyeObjects (
		ObjectName
	)
	SELECT 
		QUOTENAME(SCHEMA_NAME(t.schema_id)) + N'.' + QUOTENAME(t.name)
	FROM sys.tables t
	WHERE t.schema_id IN ( 
						SCHEMA_ID('CorePE'), 
						SCHEMA_ID('AutoWho'), SCHEMA_ID('ServerEye'), 
						SCHEMA_ID('PerformanceEye'), SCHEMA_ID('HEM')
						)
	;

	DECLARE IteratePerformanceEyeObjects CURSOR FOR 
	SELECT o.ObjectName
	FROM #CleanUpPerformanceEyeObjects o
	ORDER BY o.ObjectName;

	OPEN IteratePerformanceEyeObjects;
	FETCH IteratePerformanceEyeObjects INTO @curObjectName;

	WHILE @@FETCH_STATUS = 0
	BEGIN
		BEGIN TRY
			SET @DynSQL = N'DROP TABLE ' + @curObjectName + N';'
			PRINT @DynSQL;
			EXEC (@DynSQL);
		END TRY
		BEGIN CATCH
			IF @@TRANCOUNT > 0 ROLLBACK;

			SET @FailureMessage = N'Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
				N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
				N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
				N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

			IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects o
						WHERE o.ObjectName = @curObjectName)
			BEGIN
				UPDATE #FailedPerformanceEyeObjects
				SET NumFailures = NumFailures + 1,
					LastFailureMessage = @FailureMessage
				WHERE ObjectName = @curObjectName;
			END
			ELSE
			BEGIN
				INSERT INTO #FailedPerformanceEyeObjects (ObjectName, NumFailures, LastFailureMessage)
				SELECT @curObjectName, 0, @FailureMessage;
			END
		END CATCH

		FETCH IteratePerformanceEyeObjects INTO @curObjectName;
	END

	CLOSE IteratePerformanceEyeObjects;
	DEALLOCATE IteratePerformanceEyeObjects;
END

IF EXISTS (SELECT * FROM #FailedPerformanceEyeObjects WHERE NumFailures > 5)
BEGIN
	SELECT 'Tables' as ObjType,* 
	FROM #FailedPerformanceEyeObjects
	ORDER BY ObjectName;

	RAISERROR('> 5 failures (for one object) encountered when dropping tables',16,1);
	GOTO ScriptFailure
END

TRUNCATE TABLE #FailedPerformanceEyeObjects;

BEGIN TRY
	IF EXISTS (SELECT * FROM sys.types t WHERE t.name = N'CorePEFiltersType')
	BEGIN
		PRINT 'DROP TYPE dbo.CorePEFiltersType;'
		DROP TYPE dbo.CorePEFiltersType;
	END
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @FailureMessage = N'Drop Types: Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
		N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
		N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
		N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

	RAISERROR(@FailureMessage, 16, 1);
	GOTO ScriptFailure
END CATCH 

BEGIN TRY
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'AutoWho')
	BEGIN
		PRINT 'DROP SCHEMA [AutoWho];'
		DROP SCHEMA [AutoWho];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'ServerEye')
	BEGIN
		PRINT 'DROP SCHEMA [ServerEye];'
		DROP SCHEMA [ServerEye];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'HEM')
	BEGIN
		PRINT 'DROP SCHEMA [HEM];'
		DROP SCHEMA [HEM];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'PerformanceEye')
	BEGIN
		PRINT 'DROP SCHEMA [PerformanceEye];'
		DROP SCHEMA [PerformanceEye];
	END
	IF EXISTS (SELECT * FROM sys.schemas s WHERE s.name = N'CorePE')
	BEGIN
		PRINT 'DROP SCHEMA [CorePE];'
		DROP SCHEMA [CorePE];
	END
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0 ROLLBACK;

	SET @FailureMessage = N'Drop Schemas: Err #: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_NUMBER()),N'<null>') + 
		N'; State: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_STATE()),N'<null>') +
		N'; Sev: ' + ISNULL(CONVERT(NVARCHAR(20),ERROR_SEVERITY()),N'<null>') + 
		N'; Msg: ' + ISNULL(ERROR_MESSAGE(),N'<null>');

	RAISERROR(@FailureMessage, 16, 1);
	GOTO ScriptFailure
END CATCH

ScriptFailure:

