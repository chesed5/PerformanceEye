SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CorePE].[ProfilerTraceEvents](
	[EventGroup] [nvarchar](40) NOT NULL,
	[trace_event_id] [smallint] NOT NULL,
	[event_name] [nvarchar](128) NOT NULL,
	[category_name] [nvarchar](128) NOT NULL,
	[isEnabled] [nchar](1) NOT NULL,
 CONSTRAINT [PK_ProfilerTraceEvents] PRIMARY KEY CLUSTERED 
(
	[EventGroup] ASC,
	[trace_event_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON
GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_event_name_category_name] ON [CorePE].[ProfilerTraceEvents]
(
	[EventGroup] ASC,
	[event_name] ASC,
	[category_name] ASC
)
INCLUDE ( 	[isEnabled]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [CorePE].[ProfilerTraceEvents]  WITH CHECK ADD  CONSTRAINT [CK_ProfilerTraceEvents_isEnabled] CHECK  (([isEnabled]=N'N' OR [isEnabled]=N'Y'))
GO
ALTER TABLE [CorePE].[ProfilerTraceEvents] CHECK CONSTRAINT [CK_ProfilerTraceEvents_isEnabled]
GO
