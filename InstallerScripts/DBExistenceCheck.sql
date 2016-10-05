DECLARE @DBN_input NVARCHAR(256),
		@DBExists NVARCHAR(20),
		@ExceptionMessage NVARCHAR(4000),
		@DynSQL NVARCHAR(4000);
SET @DBN_input = N'$(DBName)';
SET @DBExists = N'$(DBExists)'

IF @DBN_input IS NULL
BEGIN
	RAISERROR('Script input variable DBName cannot be null.', 16,1)
END
ELSE
BEGIN
	IF EXISTS (SELECT * FROM sys.databases d
					WHERE d.name = @DBN_input)
	BEGIN
		IF @DBExists = N'N'
		BEGIN
			--We were told that it doesn't.
			SET @ExceptionMessage = N'Database "' + @DBN_input + N'" already exists but -DBExists was set to N (or defaulted to N)'
			RAISERROR(@ExceptionMessage, 16, 1);
		END
		--else, exit quietly
	END
	ELSE
	BEGIN
		IF @DBExists = N'Y'
		BEGIN
			--We were told that it does.
			SET @ExceptionMessage = N'Database "' + @DBN_input + N'" does not exist but -DBExists was set to Y'
			RAISERROR(@ExceptionMessage, 16, 1);
		END
		--else, exit quietly
	END
END
