SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[Camera_TasksAndWaits](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[task_address] [varbinary](8) NOT NULL,
	[parent_task_address] [varbinary](8) NULL,
	[session_id] [smallint] NOT NULL,
	[request_id] [smallint] NOT NULL,
	[exec_context_id] [smallint] NOT NULL,
	[tstate] [nchar](1) NOT NULL,
	[scheduler_id] [int] NULL,
	[context_switches_count] [bigint] NOT NULL,
	[FKDimWaitType] [smallint] NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[wait_duration_ms] [bigint] NOT NULL,
	[wait_special_category] [tinyint] NOT NULL,
	[wait_order_category] [tinyint] NOT NULL,
	[wait_special_number] [int] NOT NULL,
	[wait_special_tag] [nvarchar](100) NOT NULL,
	[task_priority] [int] NOT NULL,
	[blocking_task_address] [varbinary](8) NULL,
	[blocking_session_id] [smallint] NOT NULL,
	[blocking_exec_context_id] [smallint] NOT NULL,
	[resource_description] [nvarchar](3072) NULL,
	[resource_dbid] [int] NOT NULL,
	[resource_associatedobjid] [bigint] NOT NULL,
	[cxp_wait_direction] [tinyint] NOT NULL,
	[resolution_successful] [bit] NOT NULL,
	[resolved_name] [nvarchar](256) NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
CREATE CLUSTERED INDEX [CL_SPIDCaptureTime] ON [AutoWho].[Camera_TasksAndWaits]
(
	[SPIDCaptureTime] ASC,
	[session_id] ASC,
	[request_id] ASC,
	[task_priority] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
