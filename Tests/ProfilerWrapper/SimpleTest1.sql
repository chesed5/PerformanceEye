USE [master]
GO
DECLARE @lmsg NVARCHAR(MAX);
EXEC [PerfEye21].[PerformanceEye].[ProfilerTraceBySPID_Start] @TraceCategories=N'All', 
												@IncludePerfWarnings=N'Y',
												@SPID=NULL,			--defaults to current SPID
												@Duration=0,
												@ReturnMessage=@lmsg OUTPUT
												;
PRINT ISNULL(@lmsg, N'<null>');

select * from sys.traces t;

select count_big(*) 
from master.dbo.spt_values t0 
		cross join master.dbo.spt_values t1
		cross join master.dbo.spt_values t2
		cross join (select top 10 * from master.dbo.spt_values) t3


EXEC [PerfEye21].[PerformanceEye].[ProfilerTraceBySPID_Stop] @SPID=NULL,		--will use the current @@SPID to find the sys.traces ID via a CorePE mapping table
												@ReturnMessage=@lmsg OUTPUT
												;
PRINT ISNULL(@lmsg, N'<null>');

