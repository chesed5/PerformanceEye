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
	IF NOT EXISTS (SELECT * FROM sys.databases d
					WHERE d.name = @DBN_input)
	BEGIN
		SET @ExceptionMessage = N'Database "' + @DBN_input + N'" does not exists.'
		RAISERROR(@ExceptionMessage, 16, 1);
	END
	ELSE
	BEGIN
		--TODO: enable some sort of "loop through spids connected to this DB and kill them" logic (and a parm to control this)
		SET @DynSQL = N'DROP DATABASE ' + @DBN_input;
		EXEC (@DynSQL);
	END
END
