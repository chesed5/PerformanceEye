DECLARE @DBN_input NVARCHAR(256),
		@ExceptionMessage NVARCHAR(4000),
		@DynSQL NVARCHAR(4000);
SET @DBN_input = '$(DBName)';

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Parameter "Database" cannot be null.', 16,1)
END
ELSE
BEGIN
	IF EXISTS (SELECT * FROM sys.databases d
					WHERE d.name = @DBN_input)
	BEGIN
		SET @ExceptionMessage = N'Database "' + @DBN_input + N'" already exists.'
		RAISERROR(@ExceptionMessage, 16, 1);
	END
	ELSE
	BEGIN
		SET @DynSQL = N'CREATE DATABASE ' + @DBN_input;
		EXEC (@DynSQL);
	END
END
