SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [CorePE].[SimulateJobRuns]
/*   
	PROCEDURE:		CorePE.SimulateJobRuns

	AUTHOR:			Aaron Morelli
					email@TBD.com
					@sqlcrossjoin
					sqlcrossjoin.wordpress.com
					https://github.com/AaronMorelli/PerformanceEye

	PURPOSE: Called by sp_PE_JobMatrix. Simulates (using historical average durations) the running of jobs to generate a 
		"future" job history table that can be turned into a textual job matrix, simulating what will job execution will 
		be like in the next few hours.


	ASSUMPTIONS: Several temp tables are expected to have been created by sp_PE_JobMatrix
			#Jobs
			#HypotheticalRuns

	OUTSTANDING ISSUES: 


    CHANGE LOG:	
				2016-10-10	Aaron Morelli		Initial creation


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
	@OverallWindowEndTime		DATETIME
)
AS
BEGIN
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	SET DATEFIRST 7;		--Needed for the predictive matrix when handling schedules

	--Remember to take into account currently-running jobs.
	DECLARE @lv__pred__cursor__HypotheticalCurrentTime		DATETIME,
			@lv__pred__cursor__KeepOuterLooping				NCHAR(1),
			@lv__pred__cursor__KeepInnerLooping				NCHAR(1),
			@lv__pred__cursor__KeepScheduleLooping			NCHAR(1),
			@lv__pred__cursor__NativeJobID					UNIQUEIDENTIFIER,
			@lv__pred__cursor__JobName						NVARCHAR(500),
			@lv__pred__cursor__PreviousJobName				NVARCHAR(500),
			@lv__pred__cursor__AvgJobDur_seconds			BIGINT,
			@lv__pred__cursor__AvgSuccessDur_seconds		BIGINT,
			@lv__pred__cursor__LastStartTime				DATETIME,
			@lv__pred__cursor__NextScheduledTime			DATETIME,
			@lv__pred__cursor__HypoStartTime				DATETIME,
			@lv__pred__cursor__HypoEndTime					DATETIME,
			@lv__pred__cursor__HypoLastStartTime			DATETIME,
			@lv__pred__cursor__tmptime						DATETIME,
			@lv__pred__cursor__tmptime2						DATETIME,
			@lv__pred__cursor__tmptime3						DATETIME
			;

	DECLARE @lv__pred__schedule__freq_type					SMALLINT, 
		 @lv__pred__schedule__freq_interval					SMALLINT,
		 @lv__pred__schedule__freq_subday_type				SMALLINT,
		 @lv__pred__schedule__freq_subday_interval			SMALLINT,
		 @lv__pred__schedule__freq_relative_interval		SMALLINT,
		 @lv__pred__schedule__freq_recurrence_factor		SMALLINT,
		 @lv__pred__schedule__active_start_date				INT,
		 @lv__pred__schedule__active_end_date				INT,
		 @lv__pred__schedule__active_start_time				INT, 
		 @lv__pred__schedule__active_end_time				INT,
		 @lv__pred__schedule__ActiveStartTime				DATETIME, 
		 @lv__pred__schedule__ActiveEndtime					DATETIME,
		 @lv__pred__schedule__DailyStartTime				DATETIME, 
		 @lv__pred__schedule__DailyEndtime					DATETIME,
		 @lv__pred__schedule__MinNextTime					DATETIME,
		 @lv__pred__schedule__NextScheduleTimeFromSystem	DATETIME,
		 @lv__pred__schedule__lotime						DATETIME, 
		 @lv__pred__schedule__hitime						DATETIME, 
		 @lv__pred__schedule__intvdiff						INT
		 ;


	CREATE TABLE #ScheduleNextExecTimes (
		ExecStartTime			DATETIME NOT NULL
	);

	--a table to cache query results for job schedules
	CREATE TABLE #SchedulesForThisJob (
		freq_type				SMALLINT NOT NULL, 
		freq_interval			SMALLINT NOT NULL,
		freq_subday_type		SMALLINT NOT NULL,
		freq_subday_interval	SMALLINT NOT NULL,
		freq_relative_interval	SMALLINT NOT NULL,
		freq_recurrence_factor	SMALLINT NOT NULL,
		active_start_date		INT,
		active_end_date			INT,
		active_start_time		INT, 
		active_end_time			INT,
		ActiveStartTime			DATETIME, 
		ActiveEndTime			DATETIME,
		NextScheduleTimeFromSystem DATETIME
	);

	--Important variables that are used in the schedule loop
	DECLARE @lv__DayOf_withActiveStartTime DATETIME,
			@lv__DayOf_withActiveEndTime DATETIME,
			@lv__NextDay_withActiveStartTime DATETIME;

	DECLARE @debugtime DATETIME;
	SET @debugtime = DATEADD(MINUTE, 30, GETDATE());
	
	SET @lv__pred__cursor__PreviousJobName = '<Loop not yet started>';
	SET @lv__pred__cursor__KeepOuterLooping = N'Y';

	SELECT native_job_id,													--to ensure a stable view of msdb.dbo.sysjobactivity
		j.JobName,
		AvgJobDur_seconds, 
		AvgSuccessDur_seconds
	FROM #Jobs j
	WHERE j.IsEnabled = 1
	--debug: 
	--AND j.JobName = 'MultipleSchedules'
	AND j.JobName = 'EveryXWeeks'
	ORDER BY JobName ASC;
	
	DECLARE CreateHypotheticalRuns CURSOR LOCAL STATIC READ_ONLY FOR		--use static instead of fast_forward b/c we want
	SELECT native_job_id,													--to ensure a stable view of msdb.dbo.sysjobactivity
		j.JobName,
		AvgJobDur_seconds, 
		AvgSuccessDur_seconds,
		NULL, 
		NULL
		--xapp1.start_execution_date,			--I don't appear to be using either of
		--xapp1.next_scheduled_run_date		-- these 2 columns!
	FROM #Jobs j
		/*
		OUTER APPLY (
			SELECT TOP 1 
				ja.next_scheduled_run_date, ja.start_execution_date
			FROM msdb.dbo.sysjobactivity ja
			WHERE ja.job_id = j.native_job_id
			ORDER BY ja.session_id DESC, 
					ja.next_scheduled_run_date DESC
			) xapp1
			*/
	WHERE j.IsEnabled = 1
	--debug: 
	--AND j.JobName = 'SimpleTest'
--	AND j.JobName = 'MultipleSchedules'
	AND j.JobName = 'EveryXWeeks'
	ORDER BY JobName ASC;

	OPEN CreateHypotheticalRuns;

	WHILE @lv__pred__cursor__KeepOuterLooping = N'Y'
	BEGIN
		FETCH CreateHypotheticalRuns INTO 
			@lv__pred__cursor__NativeJobID,
			@lv__pred__cursor__JobName,
			@lv__pred__cursor__AvgJobDur_seconds,
			@lv__pred__cursor__AvgSuccessDur_seconds,
			@lv__pred__cursor__LastStartTime,				--I'm not using either of
			@lv__pred__cursor__NextScheduledTime;			-- these 2 variables!

		IF @@FETCH_STATUS <> 0
		BEGIN
			SET @lv__pred__cursor__KeepOuterLooping = N'N';
			CONTINUE;
		END

		SET @lv__pred__cursor__HypotheticalCurrentTime = GETDATE();
		--even if @PointInTime is in the future, we still need to start
		--from the present because when we arrive at @PointInTime we 
		--need to know which jobs are "currently running".

		/*
		SELECT @lv__pred__cursor__NativeJobID as JobID,
			@lv__pred__cursor__JobName as JobName,
			@lv__pred__cursor__AvgJobDur_seconds as AvgDur_seconds,
			@lv__pred__cursor__AvgSuccessDur_seconds as AvgSuccessDur_seconds,
			@lv__pred__cursor__LastStartTime as LastStartTime,
			@lv__pred__cursor__NextScheduledTime as NextScheduledTime
		*/

		--If a job is currently running, insert a hypothetical row for its current run and set the
		-- hypothetical time to the end of that job's execution
		--also make sure to handle the @lv__pred__cursor__LastStartTime variable correctly
		SET @lv__pred__cursor__HypoStartTime = NULL;
		SET @lv__pred__cursor__HypoEndTime = NULL;

		SELECT 
			@lv__pred__cursor__HypoStartTime = ss.JobStartTime,
			@lv__pred__cursor__HypoEndTime = ss.JobEndTime
		FROM (
			SELECT TOP 1 
				c.JobStartTime, 
				JobEndTime = DATEADD(SECOND, @lv__pred__cursor__AvgSuccessDur_seconds, c.JobStartTime)
			FROM #CurrentlyRunningJobs2 c
			WHERE c.native_job_id = @lv__pred__cursor__NativeJobID
			ORDER BY c.JobStartTime DESC
		) ss;

		IF @lv__pred__cursor__HypoStartTime IS NOT NULL AND @lv__pred__cursor__HypoEndTime IS NOT NULL
		BEGIN
			INSERT INTO #HypotheticalRuns 
			(native_job_id, JobStartTime, JobExpectedEndTime)
			SELECT @lv__pred__cursor__NativeJobID, @lv__pred__cursor__HypoStartTime, @lv__pred__cursor__HypoEndTime;

			--Set our starting point to the time when the currently-running job is expected to end.
			SET @lv__pred__cursor__HypotheticalCurrentTime = DATEADD(SECOND, 1, @lv__pred__cursor__HypoEndTime);

			--TODO: do we need to track "last start time" or "last completion time"? if so, do that here
		END
		ELSE
		BEGIN
			--the job is not currently-running. 
			SET @lv__pred__cursor__HypotheticalCurrentTime = GETDATE();
		END

		TRUNCATE TABLE #SchedulesForThisJob;

		INSERT INTO #SchedulesForThisJob (
			freq_type,
			freq_interval,
			freq_subday_type,
			freq_subday_interval,
			freq_relative_interval,
			freq_recurrence_factor,
			active_start_date,
			active_end_date,
			active_start_time,
			active_end_time,
			ActiveStartTime,
			ActiveEndtime,
			NextScheduleTimeFromSystem
		)
		select 
			--ss.schedule_id, 
			--ss.name as ScheduleName, 
			--ss.enabled, 
			ss.freq_type, 
			ss.freq_interval, 
			ss.freq_subday_type,
			ss.freq_subday_interval,
			ss.freq_relative_interval,
			ss.freq_recurrence_factor,
			ss.active_start_date, 
			ss.active_end_date,
			ss.active_start_time,
			ss.active_end_time,
			ActiveStartTime = (
					CASE WHEN (ss.active_start_date IS NULL OR ss.active_start_time IS NULL )
							THEN GETDATE()
						ELSE CAST(STR(ss.active_start_date, 8, 0) AS DATETIME) + 
							CAST(STUFF(STUFF(REPLACE(STR(ss.active_start_time, 6), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME)
						END),
			ActiveEndTime = (
					CASE WHEN (ss.active_end_date IS NULL OR ss.active_end_time IS NULL )
							THEN GETDATE()
						ELSE CAST(STR(ss.active_end_date, 8, 0) AS DATETIME) + 
							CAST(STUFF(STUFF(REPLACE(STR(ss.active_end_time, 6), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME)
						END),
			NextScheduleTimeFromSystem = (
						CASE WHEN ( ISNULL(js.next_run_date,0) = 0 OR ISNULL(js.next_run_time,0) = 0 )
							THEN NULL
						ELSE CAST(STR(js.next_run_date, 8, 0) AS DATETIME) + 
							CAST(STUFF(STUFF(REPLACE(STR(js.next_run_time, 6), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME)
						END)
		from msdb.dbo.sysjobs j
			inner join msdb.dbo.sysjobschedules js
				on js.job_id = j.job_id
			inner join msdb.dbo.sysschedules ss
				on js.schedule_id = ss.schedule_id
		where 1=1
		and j.job_id = @lv__pred__cursor__NativeJobID
		and ss.enabled = 1
	--	debug: omitting this and ss.enabled = 1
		and ss.freq_type NOT IN (64,128)		--64=SQL Agent startup, and 128 is CPU idle; can't predict that so exclude those schedules
		;

		--print @@ROWCOUNT

		SELECT * FROM #SchedulesForThisJob;


		SET @lv__pred__cursor__KeepInnerLooping = N'Y'

		--Until our hypothetical time exceeds the predictive-matrix end time, we need
		-- to continue generating "hypothetical" runs for the job and moving the hypothetical time forward. 
		WHILE @lv__pred__cursor__KeepInnerLooping = N'Y'
			AND @lv__pred__cursor__HypotheticalCurrentTime <= @OverallWindowEndTime
		BEGIN
			--We are basically going to iterate through each schedule for this job and find the next start time,
			-- and add that start time to this 1-field table. We then find the MIN() of those values and
			-- that's the next time this job will run. We then do craft an insert with that MIN start time
			-- into the hypothetical runs table.
			TRUNCATE TABLE #ScheduleNextExecTimes;

			DECLARE PredictIterationSchedules CURSOR FOR
			SELECT freq_type,
				freq_interval,
				freq_subday_type,
				freq_subday_interval,
				freq_relative_interval,
				freq_recurrence_factor,
				active_start_date,
				active_end_date,
				active_start_time,
				active_end_time,
				ActiveStartTime,
				ActiveEndtime,
				NextScheduleTimeFromSystem
			FROM #SchedulesForThisJob;

			SET @lv__pred__cursor__KeepScheduleLooping = N'Y';

			OPEN PredictIterationSchedules;

			WHILE @lv__pred__cursor__KeepScheduleLooping = N'Y'
			BEGIN
				FETCH PredictIterationSchedules INTO 
					@lv__pred__schedule__freq_type,
					 @lv__pred__schedule__freq_interval,
					 @lv__pred__schedule__freq_subday_type,
					 @lv__pred__schedule__freq_subday_interval,
					 @lv__pred__schedule__freq_relative_interval,
					 @lv__pred__schedule__freq_recurrence_factor,
					 @lv__pred__schedule__active_start_date,
					 @lv__pred__schedule__active_end_date,
					 @lv__pred__schedule__active_start_time,
					 @lv__pred__schedule__active_end_time,
					 @lv__pred__schedule__ActiveStartTime,
					 @lv__pred__schedule__ActiveEndtime,
					 @lv__pred__schedule__NextScheduleTimeFromSystem

				IF @@FETCH_STATUS <> 0
				BEGIN
					SET @lv__pred__cursor__KeepScheduleLooping = N'N';
					CONTINUE;
				END

				/*
				
				select @lv__pred__cursor__JobName,@lv__pred__schedule__freq_type,
					 @lv__pred__schedule__freq_interval,
					 @lv__pred__schedule__freq_subday_type,
					 @lv__pred__schedule__freq_subday_interval,
					 @lv__pred__schedule__freq_relative_interval,
					 @lv__pred__schedule__freq_recurrence_factor,
					 @lv__pred__schedule__active_start_date,
					 @lv__pred__schedule__active_end_date,
					 @lv__pred__schedule__active_start_time,
					 @lv__pred__schedule__active_end_time,
					 @lv__pred__schedule__ActiveStartTime,
					 @lv__pred__schedule__ActiveEndtime,
					 @lv__pred__schedule__DailyStartTime,
					 @lv__pred__schedule__DailyEndTime
					 */

				--Our Daily Start and Daily End times always refer to the same day as our Hypothetical time does, but are based on 
				-- the Active Start and Active End times of the schedule, and thus need to be reset each time through the loop, i.e. for each schedule
				-- These variables are useful when referring to a several-times-a-day schedule (i.e. a sub-day time window)
				SET @lv__pred__schedule__DailyStartTime = CONVERT(DATETIME,CONVERT(VARCHAR(20), @lv__pred__cursor__HypotheticalCurrentTime,101)) + 
															CAST(STUFF(STUFF(REPLACE(STR(@lv__pred__schedule__active_start_time, 6), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME);
				SET @lv__pred__schedule__DailyEndTime = CONVERT(DATETIME,CONVERT(VARCHAR(20), @lv__pred__cursor__HypotheticalCurrentTime,101)) + 
															CAST(STUFF(STUFF(REPLACE(STR(@lv__pred__schedule__active_end_time, 6), ' ', '0'), 3, 0, ':'), 6, 0, ':') AS DATETIME);


				-- For each schedule, regardless of its type (excluding those dynamic conditions like 64 [SQL Agent start] and 128 [computer idle]), we
				-- can divide the logic into 2 main parts:
				--		1. Get the next day that the job should run on this schedule
				--				Note that if the job is to run today, we should also get its "next" day on this schedule, in case 
				--				we find (when evaluating point #2) that we've passed the one-time-run or recurring-run-time-window for today.
				--		2. Find the next time that we should run, whether it is a one-time run or a recurring run.

				-- Thus, we break the below logic into main 2 parts. This allows some code-reuse (especially in part #2)

				--Thus, these first 2 vars will always be non-null *if* the curr hypo day is a day when the job should run
				-- on this schedule, and they will hold the start/end of any time range
				-- The 3rd var will *always* be non-null once we've found the next day to run on this schedule beyond curr hypo.
				SET @lv__DayOf_withActiveStartTime = NULL;
				SET @lv__DayOf_withActiveEndTime = NULL;
				SET @lv__NextDay_withActiveStartTime = NULL;

/*************************************************************************************************************************
**************************************************************************************************************************
************************************************** Next Day(s) Logic *****************************************************
**************************************************************************************************************************
*************************************************************************************************************************/

				--One-time jobs are easy... just look at the ActiveStartTime value and if it is in the future, add it
				-- We don't need anything else, even in the Time portion of the logic
				IF @lv__pred__schedule__freq_type = 1
				BEGIN
					IF @lv__pred__schedule__ActiveStartTime >= @lv__pred__cursor__HypotheticalCurrentTime
					BEGIN
						INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
						SELECT @lv__pred__schedule__ActiveStartTime;
					END
				END

				--Every X Days - day portion
				IF @lv__pred__schedule__freq_type = 4
				BEGIN
					--When freq_type = 4 (daily), freq_interval means "every X days". (Stored in @lv__pred__schedule__freq_interval)
					-- It *appears* that SQL Agent uses the "active_start_date" field in msdb as the base starting point for the 
					-- "every X days" formula. (We've converted that ugly format to a more usable datetime variable: @lv__pred__schedule__ActiveStartTime)

					SET @lv__pred__schedule__intvdiff = DATEDIFF(DAY, 
																	@lv__pred__schedule__ActiveStartTime,	--this could be YEARS ago! or days, or today
																	@lv__pred__cursor__HypotheticalCurrentTime
																) 
																/ @lv__pred__schedule__freq_interval;

					--We multiply the day-diff (integer only) with the freq_interval and with the (freq_interval+1) to get
					-- 2 days, one of which is guaranteed to be the same day or earlier than the "hypothetical current time" and 
					-- the other which is guaranteed to be later than the "hypothetical current time" (never the same day)
					SET @lv__pred__schedule__lotime = DATEADD(DAY, 
																	@lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_interval, 
																	@lv__pred__schedule__ActiveStartTime
																);

					SET @lv__pred__schedule__hitime = DATEADD(DAY, (@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_interval, 
																@lv__pred__schedule__ActiveStartTime);

					IF DATEDIFF(DAY, @lv__pred__schedule__lotime, @lv__pred__cursor__HypotheticalCurrentTime) = 0
					BEGIN
						--lotime is the same day as current hypo time. 
						SET @lv__DayOf_withActiveStartTime = @lv__pred__schedule__lotime;

						--set the endtime, too, just in case this is a subday-recurring schedule rather than a one-time-in-a-day sched
						--Remember that the 2 Daily*Time variables always hold the ActiveStartTime/ActiveEndTime time portions but with
						-- the day set to the curr hypo time.
						SET @lv__DayOf_withActiveEndTime = @lv__pred__schedule__DailyEndTime;
					END
					--ELSE		--leave those variables NULL
					
					SET @lv__NextDay_withActiveStartTime = @lv__pred__schedule__hitime;
				END --Every X Days - day portion


				--Every X Months, - day portion
				-- Reminder: 
				--			freq_interval is the days-of-week, i.e. a bitmap where 1=Sun, 2=Mon, 4=Tue, 8=Wed, 16=Thur, etc
				--			freq_subday_type: 1=once a day, 2=every X seconds, 4=every X minutes, 8=every X hours
				--			freq_subday_interval:		the "X" in the above line
				--			freq_recurrence_factor:		the "X" in "every X weeks"
				IF @lv__pred__schedule__freq_type = 8
				BEGIN
					--The base for the Every X Weeks calculation is active_start_date (just like for Every X days above). It *appears*
					-- that SQL Agent just does a datediff(week), because a job I set up w/an ActiveStartDate on a Tuesday and every 3 weeks
					-- still had a next Monday start time as being the first week. 

					--So with that in mind, let's use dateadd(week, ) to add weeks on to the enabled-DOW of that first week (right after ActiveStartTime)
					-- and then find the first day that is = to our hypo current time. In actuality, we find up to 2 days: 
					--	if our hypo current time is actually an enabled DOW, we'll find the first dateadd(week, ) value that is datediff(day) = 0
					-- (i.e. same day) to our hypo current time. If our hypo current time is NOT an enabled DOW, then we'll get a NULL there.
					-- Either way, we'll get a "earliest next day", i.e. the first dateadd(week) value that is DATEDIFF(day) > 0, and will 
					-- serve as our "next day" if hypo current time isn't on an enabled DOW or it was but we already are past the run time/time window.

					; WITH 
					--CTE chain #1
					TheFirstWeek_Base AS (
						SELECT [DayAndTime] = @lv__pred__schedule__ActiveStartTime, [DOW] = LOWER(DATENAME(WEEKDAY, @lv__pred__schedule__ActiveStartTime)) UNION ALL 
						SELECT DATEADD(DAY, 1, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 1, @lv__pred__schedule__ActiveStartTime))) UNION ALL 
						SELECT DATEADD(DAY, 2, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 2, @lv__pred__schedule__ActiveStartTime))) UNION ALL 
						SELECT DATEADD(DAY, 3, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 3, @lv__pred__schedule__ActiveStartTime))) UNION ALL 
						SELECT DATEADD(DAY, 4, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 4, @lv__pred__schedule__ActiveStartTime))) UNION ALL 
						SELECT DATEADD(DAY, 5, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 5, @lv__pred__schedule__ActiveStartTime))) UNION ALL 
						SELECT DATEADD(DAY, 6, @lv__pred__schedule__ActiveStartTime), LOWER(DATENAME(WEEKDAY, DATEADD(DAY, 6, @lv__pred__schedule__ActiveStartTime)))
					),
					DaysEnabled AS (
						--Split out the bitmap into the full 7 days, NULL when not enabled for that day.
						SELECT [DayEnabled] =	CASE WHEN @lv__pred__schedule__freq_interval & 1 >  0 THEN 'sunday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 2 >  0 THEN 'monday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 4 >  0 THEN 'tuesday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 8 >  0 THEN 'wednesday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 16 > 0 THEN 'thursday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 32 > 0 THEN 'friday' ELSE NULL END UNION ALL
						SELECT					CASE WHEN @lv__pred__schedule__freq_interval & 64 > 0 THEN 'saturday' ELSE NULL END
					),
					TheFirstWeek AS (
						--and finally, eliminate the days of the first week when this schedule isn't enabled.
						SELECT 
							w.DayAndTime
						FROM DaysEnabled d
							INNER JOIN TheFirstWeek_Base w		--join removes the DOW=NULL records
								ON d.DayEnabled = w.DOW
					),
							--CTE chain #2
							--now, generate row numbers and dateadd(week, rn ) them to the "TheFirstWeek" CTE
							rownum_base as (
								SELECT 0 as col1 UNION ALL
								SELECT 0 UNION ALL
								SELECT 0 UNION ALL
								SELECT 0
							),
							rownum_crossjoin as (
								--produces 1024 rows, or about 20 years. If a SQL Job's schedule was created > 20 years ago,
								--then this might break. :-/
								SELECT 
									rn = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
								FROM rownum_base t0
									CROSS JOIN rownum_base t1
									CROSS JOIN rownum_base t2
									CROSS JOIN rownum_base t3
									CROSS JOIN rownum_base t4
							),
							EveryXRowNumber as (
								--If the schedule is set to run every X weeks and X is *not* 1,
								-- we need to eliminate rn values so that our DATEADD(week) logic 
								-- below only results in every X weeks logic
								SELECT rn = (rn-1)
								FROM rownum_crossjoin 
								WHERE (rn-1) % @lv__pred__schedule__freq_recurrence_factor = 0
							),
					--CTE chain #3 (merges #1 and #2)
					WeeksForward as (
						SELECT 
							w.DayAndTime,
							DayAndTimePlusRN = DATEADD(WEEK, rn, w.DayAndTime),
							IsSameDay = CASE WHEN DATEDIFF(DAY, @lv__pred__cursor__HypotheticalCurrentTime,DATEADD(WEEK, rn, w.DayAndTime)) = 0 THEN N'Y'
											ELSE N'N'
											END
						FROM TheFirstWeek w
							CROSS JOIN EveryXRowNumber r
						--Limit to days that are equal to the hypo day or after it. We limit to 365 days after; 
						-- the X in Every X Weeks can get quite big (don't know if there's a limit) but practically 
						-- speaking, people aren't going to be creating jobs that run every 60 weeks.
						WHERE DATEDIFF(DAY, @lv__pred__cursor__HypotheticalCurrentTime,DATEADD(WEEK, rn, w.DayAndTime)) BETWEEN 0 AND 365
					)
					SELECT 
						@lv__DayOf_withActiveStartTime = MIN(CASE WHEN IsSameDay = N'Y' THEN df.DayAndTimePlusRN ELSE NULL END),
						@lv__NextDay_withActiveStartTime = MIN(CASE WHEN IsSameDay = N'N' THEN df.DayAndTimePlusRN ELSE NULL END)
					FROM WeeksForward df
					;

					IF @lv__DayOf_withActiveStartTime IS NOT NULL
					BEGIN
						--Set the endtime, too, just in case this is a subday-recurring schedule rather than a one-time-in-a-day sched
						--Remember that the 2 Daily*Time variables always hold the ActiveStartTime/ActiveEndTime time portions but with
						-- the day set to the curr hypo time.
						SET @lv__DayOf_withActiveEndTime = @lv__pred__schedule__DailyEndTime;
					END
				END	--Weekly frequency


/*************************************************************************************************************************
**************************************************************************************************************************
***************************************************** Time Logic *********************************************************
**************************************************************************************************************************
*************************************************************************************************************************/
				--one-time runs can't be recurring, and their logic is completely handled above. So this is a no-op
				--IF @lv__pred__schedule__freq_type = 1
				--BEGIN

				--END

				--Daily-frequency (TODO: aren't most or all of the freq_types the same for the subday logic?
				--					4 and 8 are the ones I've implemented above in the day logic so far)
				IF @lv__pred__schedule__freq_type = 4
					OR @lv__pred__schedule__freq_type = 8
				BEGIN

					IF @lv__DayOf_withActiveStartTime IS NULL
					BEGIN
						--this job isn't to run on this schedule on this day. We simply use the ActiveStartTime of
						-- the Next Day it is to run, as long as the schedule hasn't expired.
						IF @lv__NextDay_withActiveStartTime <= @lv__pred__schedule__ActiveEndtime	--ensure sched isn't expired
						BEGIN
							INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
							SELECT @lv__NextDay_withActiveStartTime;
						END
					END
					ELSE
					BEGIN
						--Hypo is on the same day as the 2 DayOf variables. 
						
						--For both one-time-a-day and recurring-time-window schedules, DailyStartTime represents the 
						-- earliest time a schedule can run, so we already have our answer.
						IF @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__schedule__DailyStartTime
						BEGIN
							INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
							SELECT @lv__pred__schedule__DailyStartTime;
						END
						ELSE	--hypo is > DailyStartTime
						BEGIN
							IF @lv__pred__schedule__freq_subday_type = 1	--specified time of day
							BEGIN
								--since we know Hypo is already > than DailyStartTime, we missed our run for today.
								-- Use the NextRun variable
								IF @lv__NextDay_withActiveStartTime <= @lv__pred__schedule__ActiveEndtime	--ensure sched isn't expired
								BEGIN
									INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
									SELECT @lv__NextDay_withActiveStartTime;
								END
							END
							ELSE 	-- freq_subday_type is > 1; recurring-time-window schedule; remember, we now know that Hypo is > the StartTime
							BEGIN
								IF @lv__pred__cursor__HypotheticalCurrentTime > @lv__pred__schedule__DailyEndtime
								BEGIN	--we're after the time window; again, use the next day run
									IF @lv__NextDay_withActiveStartTime <= @lv__pred__schedule__ActiveEndtime	--ensure sched isn't expired
									BEGIN
										INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
										SELECT @lv__NextDay_withActiveStartTime;
									END
								END
								ELSE IF @lv__pred__cursor__HypotheticalCurrentTime = @lv__pred__schedule__DailyEndtime	--unlikely, but...
								BEGIN
									IF @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__schedule__ActiveEndtime	--ensure sched isn't expired
									BEGIN
										INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
										SELECT @lv__pred__cursor__HypotheticalCurrentTime;
									END
								END
								ELSE
								BEGIN
									--We are on the same day as lotime, the schedule is a recurring-time-window-during-the-day subtype, 
									-- and we are still in that time window (b/c current hypo time is > DailyStartTime and < DailyEndTime)
									--We need to use the same approach that we used above, where we calc a lotime & a hitime to figure out
									-- the next time in our interval.

									--Calculate the # of freq_subday_interval occurrences between starttime and hypo time,
									-- then add that # of intervals and #+1 back to the start time, and see which one most nearly *follows* Hypo
									SET @lv__pred__schedule__intvdiff = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
											THEN DATEDIFF(HOUR, 
																@lv__pred__schedule__DailyStartTime, 
																@lv__pred__cursor__HypotheticalCurrentTime
														) / @lv__pred__schedule__freq_subday_interval

											WHEN @lv__pred__schedule__freq_subday_type = 4
											THEN DATEDIFF(MINUTE, 
																@lv__pred__schedule__DailyStartTime, 
																@lv__pred__cursor__HypotheticalCurrentTime
														) / @lv__pred__schedule__freq_subday_interval

											WHEN @lv__pred__schedule__freq_subday_type = 2
											THEN DATEDIFF(SECOND, 
																@lv__pred__schedule__DailyStartTime, 
																@lv__pred__cursor__HypotheticalCurrentTime
														) / @lv__pred__schedule__freq_subday_interval
										END
										);

									SET @lv__pred__schedule__lotime = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
												THEN DATEADD(HOUR, 
																	@lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
											WHEN @lv__pred__schedule__freq_subday_type = 4
												THEN DATEADD(MINUTE, 
																	@lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
											WHEN @lv__pred__schedule__freq_subday_type = 2 
												THEN DATEADD(SECOND, 
																	@lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
										END );
										
									SET @lv__pred__schedule__hitime = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
												THEN DATEADD(HOUR, 
																	(@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
											WHEN @lv__pred__schedule__freq_subday_type = 4
												THEN DATEADD(MINUTE, 
																	(@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
											WHEN @lv__pred__schedule__freq_subday_type = 2 
												THEN DATEADD(SECOND, 
																	(@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
																	@lv__pred__schedule__DailyStartTime
															)
										END
										);

									IF @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__schedule__lotime		--is it even possible for lo to be < than hypo?
									BEGIN
										--Use "lo" as the next execution time, as long as it is before the time-window-end
										IF @lv__pred__schedule__lotime <= @lv__pred__schedule__DailyEndtime
										BEGIN
											INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
											SELECT @lv__pred__schedule__lotime;
										END
										ELSE
										BEGIN
											--past the end-time for this day, so use our next day variable
											IF @lv__NextDay_withActiveStartTime <= @lv__pred__schedule__ActiveEndtime
											BEGIN
												INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
												SELECT @lv__NextDay_withActiveStartTime;
											END
										END
									END
									ELSE
									BEGIN
										--Use "hi" as the next execution time.
										IF @lv__pred__schedule__hitime <= @lv__pred__schedule__DailyEndtime
										BEGIN
											INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
											SELECT @lv__pred__schedule__hitime;
										END
										ELSE
										BEGIN	--past the end-time for this day, so use our next day variable
											IF @lv__NextDay_withActiveStartTime <= @lv__pred__schedule__ActiveEndtime
											BEGIN
												INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
												SELECT @lv__pred__cursor__tmptime;
											END
										END	-- is @hi interval-based value before today's window end time?
									END --is Hypothetical time <= subday-interval-based @lotime?
								END --is our curr hypo past the EndTime of the range, or within the range?
							END		--is freq_subday_type a specific time of day or an Every X sec/min/hour?
						END  --is the curr hypo less than the DailyStartTime?
					END --is hypo on the same day as the 2 DayOf variables?
				END	--Daily frequency

				--Monthly frequency
				IF @lv__pred__schedule__freq_type = 16
				BEGIN
					/* I've had some difficulty in understanding the logic used by SQL Agent when the schedule is set for
						"Every X months" and X is > 1.		(i.e. the freq_recurrence_factor is > 1)
						Because we limit @PointInTime to 3 days in the future and the @HoursForward value is also limited, in practice
						we can probably just use the Next Scheduled Time that we obtained from msdb.dbo.sysjobschedules.

						If that is NULL somehow, and @lv__pred__schedule__freq_recurrence_factor = 1, then we do
						a "college try" effort on obtaining the next execution.
					*/
					IF @lv__pred__schedule__NextScheduleTimeFromSystem IS NOT NULL 
						AND @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__schedule__NextScheduleTimeFromSystem
					BEGIN
						INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
						SELECT @lv__pred__schedule__NextScheduleTimeFromSystem
					END
					ELSE	--our Next Scheduled Time was invalid or our hypothetical time is already past it. If the schedule is an
					BEGIN	-- "Every 1 month" sort of thing, then it is pretty straightforward to capture the next time
						IF @lv__pred__schedule__freq_recurrence_factor = 1
						BEGIN
							--get a "first day of the month" value from our Hypo time
							SET @lv__pred__cursor__tmptime = CONVERT(DATETIME, 
								CONVERT(CHAR(4),YEAR(@lv__pred__cursor__HypotheticalCurrentTime)) + '-' +
								CONVERT(VARCHAR(2),MONTH(@lv__pred__cursor__HypotheticalCurrentTime)) +  '-01'
								);

							--Is our hypothetical time already past the day-of-the-month? If so, add a month.
							IF DATEPART(DAY, @lv__pred__cursor__HypotheticalCurrentTime) > @lv__pred__schedule__freq_interval
							BEGIN
								SET @lv__pred__cursor__tmptime = DATEADD(MONTH, 1, @lv__pred__cursor__tmptime);
							END

							--If our month doesn't have as many days as specified in @lv__pred__schedule__freq_interval, 
							-- then add a month. Since we never have consecutive months with < 31 days, we are guaranteed 
							-- to get a month with enough days just by adding 1 month.
							IF DATEPART(DAY, DATEADD(DAY, -1, DATEADD(MONTH, 1, @lv__pred__cursor__tmptime))) < @lv__pred__schedule__freq_interval
							BEGIN
								SET @lv__pred__cursor__tmptime = DATEADD(MONTH, 1, @lv__pred__cursor__tmptime);
							END

							--tmptime is set to the first of the month. Add the day/time portion in based on the Active Start Time
							SET @lv__pred__cursor__tmptime2 = DATEADD(MONTH, 
								DATEDIFF(MONTH, @lv__pred__schedule__ActiveStartTime, @lv__pred__cursor__tmptime),
								@lv__pred__schedule__ActiveStartTime);

							--And for the end-time
							SET @lv__pred__cursor__tmptime3 = DATEADD(MONTH, 
								DATEDIFF(MONTH, @lv__pred__schedule__ActiveEndtime, @lv__pred__cursor__tmptime),
								@lv__pred__schedule__ActiveEndtime);

							--If this is a day in the future, then add it in
							IF DATEDIFF(DAY, @lv__pred__cursor__HypotheticalCurrentTime, @lv__pred__cursor__tmptime2) > 0 
							BEGIN
								IF @lv__pred__cursor__tmptime2 <= @lv__pred__schedule__ActiveEndtime
								BEGIN
									INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
									SELECT @lv__pred__cursor__tmptime2;
								END
							END
							ELSE	--because of the above logic, tmptime2 cannot be an EARLIER day than Hypo, so it must be the same day
							BEGIN
								IF @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__cursor__tmptime2
								BEGIN	--before the start time; doesn't matter whether schedule is one-time-a-day or recurring
									INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
									SELECT @lv__pred__cursor__tmptime2;
								END
								ELSE IF @lv__pred__schedule__freq_subday_type = 1
									OR @lv__pred__cursor__HypotheticalCurrentTime > @lv__pred__cursor__tmptime3
								BEGIN
									--if a once-a-day schedule, or if a recurring schedule that we are after the end-time, 
									-- then we are too late. 
									--Because this proc limits "looking forward" to just a few days, the fact that this
									-- schedule won't be applicable for a months means we aren't even going to try to
									-- assemble the correct "next start time".

									--No-op
									SET @lv__pred__cursor__HypotheticalCurrentTime = @lv__pred__cursor__HypotheticalCurrentTime;
								END
								ELSE
								BEGIN
									--we are in the middle of a recurring-time-window schedule's time frame. 
									--Remember, tmptime2 is the beginning of the range, and tmptime3 is the end
									SET @lv__pred__schedule__intvdiff = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
											THEN DATEDIFF(HOUR, @lv__pred__cursor__tmptime2, 
														@lv__pred__cursor__HypotheticalCurrentTime) / @lv__pred__schedule__freq_subday_interval
											WHEN @lv__pred__schedule__freq_subday_type = 4
											THEN DATEDIFF(MINUTE, @lv__pred__cursor__tmptime2, 
														@lv__pred__cursor__HypotheticalCurrentTime) / @lv__pred__schedule__freq_subday_interval
											WHEN @lv__pred__schedule__freq_subday_type = 2
											THEN DATEDIFF(SECOND, @lv__pred__cursor__tmptime2, 
														@lv__pred__cursor__HypotheticalCurrentTime) / @lv__pred__schedule__freq_subday_interval
										END
										);

									SET @lv__pred__schedule__lotime = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
											THEN DATEADD(HOUR, @lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
													@lv__pred__cursor__tmptime2)
											WHEN @lv__pred__schedule__freq_subday_type = 4
											THEN DATEADD(MINUTE, @lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
													@lv__pred__cursor__tmptime2)
											WHEN @lv__pred__schedule__freq_subday_type = 2 
											THEN DATEADD(SECOND, @lv__pred__schedule__intvdiff*@lv__pred__schedule__freq_subday_interval, 
													@lv__pred__cursor__tmptime2)
										END );

									SET @lv__pred__schedule__hitime = (
										CASE 
											WHEN @lv__pred__schedule__freq_subday_type = 8 
											THEN DATEADD(HOUR, (@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
															@lv__pred__cursor__tmptime2)
											WHEN @lv__pred__schedule__freq_subday_type = 4
											THEN DATEADD(MINUTE, (@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
															@lv__pred__cursor__tmptime2)
											WHEN @lv__pred__schedule__freq_subday_type = 2 
											THEN DATEADD(SECOND, (@lv__pred__schedule__intvdiff+1)*@lv__pred__schedule__freq_subday_interval, 
															@lv__pred__cursor__tmptime2)
										END
										);

									IF @lv__pred__cursor__HypotheticalCurrentTime <= @lv__pred__schedule__lotime
									BEGIN
										--Use "lo" as the next execution time, as long as it is before the time-window-end
										IF @lv__pred__schedule__lotime <= @lv__pred__schedule__DailyEndtime
										BEGIN
											INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
											SELECT @lv__pred__schedule__lotime;
										END
										--ELSE  next exec is a month out, so don't worry about it
									END
									ELSE
									BEGIN
										--Use "hi" as the next execution time.
										IF @lv__pred__schedule__hitime <= @lv__pred__schedule__DailyEndtime
										BEGIN
											INSERT INTO #ScheduleNextExecTimes (ExecStartTime)
											SELECT @lv__pred__schedule__hitime;
										END
										--ELSE	next exec is a month out, so don't worry about it
									END
								END
							END
						END
					END
				END	--Monthly frequency


				--TODO: Monthly-relative frequency		--TODO: worry about freq_recurrence_factor > 1 later???
				IF @lv__pred__schedule__freq_type = 32
				BEGIN

					SET @lv__pred__schedule__freq_type = 32
				END	--Monthly-relative frequency

			END	--end of Schedule loop that obtains "Next Exec Time" for each schedule

			CLOSE PredictIterationSchedules;
			DEALLOCATE PredictIterationSchedules;

			SELECT * FROM #ScheduleNextExecTimes

			SET @lv__pred__schedule__MinNextTime = NULL;

			SELECT @lv__pred__schedule__MinNextTime = MIN(t.ExecStartTime)
			FROM #ScheduleNextExecTimes t
			WHERE t.ExecStartTime IS NOT NULL;

			IF @lv__pred__schedule__MinNextTime IS NULL
			BEGIN
				SET @lv__pred__cursor__HypotheticalCurrentTime = DATEADD(SECOND, 1,@OverallWindowEndTime);
			END
			ELSE
			BEGIN
				INSERT INTO #HypotheticalRuns (
					native_job_id, 
					JobStartTime, 
					JobExpectedEndTime
				)
				SELECT 
					@lv__pred__cursor__NativeJobID, 
					@lv__pred__schedule__MinNextTime, 
					DATEADD(SECOND, 
									@lv__pred__cursor__AvgSuccessDur_seconds, 
									@lv__pred__schedule__MinNextTime
							);

				--Set the new hypothetical time to 1 second after the hypothetical run completes
				SET @lv__pred__cursor__HypotheticalCurrentTime = 
					DATEADD(SECOND, @lv__pred__cursor__AvgSuccessDur_seconds+1, @lv__pred__schedule__MinNextTime)
			END
		END 
	END	--end of outer WHILE loop

	CLOSE CreateHypotheticalRuns
	DEALLOCATE CreateHypotheticalRuns

	RETURN 0;
END


