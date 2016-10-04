SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [CorePE].[UpdateDBMapping] 
/*   
	PROCEDURE:		CorePE.UpdateDBMapping

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/amorelli005/PerformanceEye

	PURPOSE: 	Since our app stored historical data, and DBs are sometimes detached/re-attached, etc,
	 we want to keep a mapping between DBID and DBName. (Much of our storage just keeps DBID rather than DBName).
	 We make the (usually-safe, but not always) assumption that 2 DBs with the same name are really the same database.

	OUTSTANDING ISSUES: None at this time.

    CHANGE LOG:	
				2016-04-29	Aaron Morelli		Dev Begun & Final Commenting


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
EXEC CorePE.UpdateDBMapping
*/
AS
BEGIN
	SET NOCOUNT ON;
	DECLARE @EffectiveTime DATETIME = GETDATE();

	CREATE TABLE #CurrentDBIDNameMapping (
		DBID int not null, 
		DBName nvarchar(256) not null
	);

	INSERT INTO #CurrentDBIDNameMapping (
		DBID, DBName
	)
	SELECT d.database_id, d.name
	FROM sys.databases d
	;

	-- In the below joins, we typically connect the contents of #DBIDchanges to the "current" set of rows in 
	-- CorePE.DBIDNameMapping, i.e. where EffectiveEndTime is null

	-- First, find matches on DBName, where the DBID is different.
	--		a. first, close out the our row (EffectiveEndTime = GETDATE()
	--		b. second, insert the new pair in. Note that this also takes care of completely new DBName values.

	UPDATE targ 
	SET EffectiveEndTime = @EffectiveTime
	FROM CorePE.DBIDNameMapping targ 
		INNER JOIN #CurrentDBIDNameMapping t
			ON t.DBName = targ.DBName
			AND t.DBID <> targ.DBID
	WHERE targ.EffectiveEndTime IS NULL
	;

	INSERT INTO CorePE.DBIDNameMapping
	(DBID, DBName, EffectiveStartTime, EffectiveEndTime)
	SELECT t.DBID, t.DBName, @EffectiveTime, NULL 
	FROM #CurrentDBIDNameMapping t
	WHERE NOT EXISTS (
		SELECT * 
		FROM CorePE.DBIDNameMapping m
		WHERE m.DBName = t.DBName
		AND m.EffectiveEndTime IS NULL 
	);

	UPDATE targ 
	SET EffectiveEndTime = @EffectiveTime
	FROM CorePE.DBIDNameMapping targ 
	WHERE targ.EffectiveEndTime IS NULL
	AND NOT EXISTS (
		SELECT * FROM #CurrentDBIDNameMapping t
		WHERE t.DBName = targ.DBName
	)
	;

	RETURN 0;
END
GO
