SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[LightweightTasks](
	[SPIDCaptureTime] [datetime] NULL,
	[task__task_address] [varbinary](8) NOT NULL,
	[task__task_state] [nvarchar](60) NULL,
	[task__context_switches_count] [int] NULL,
	[task__pending_io_count] [int] NULL,
	[task__pending_io_byte_count] [bigint] NULL,
	[task__pending_io_byte_average] [int] NULL,
	[task__scheduler_id] [int] NOT NULL,
	[task__session_id] [smallint] NULL,
	[task__exec_context_id] [int] NULL,
	[task__request_id] [int] NULL,
	[task__worker_address] [varbinary](8) NULL,
	[task__host_address] [varbinary](8) NOT NULL,
	[task__parent_task_address] [varbinary](8) NULL,
	[taskusage__is_remote_task] [bit] NULL,
	[taskusage__user_objects_alloc_page_count] [bigint] NULL,
	[taskusage__user_objects_dealloc_page_count] [bigint] NULL,
	[taskusage__internal_objects_alloc_page_count] [bigint] NULL,
	[taskusage__internal_objects_dealloc_page_count] [bigint] NULL,
	[wait_duration_ms] [bigint] NULL,
	[wait_type] [nvarchar](60) NULL,
	[resource_address] [varbinary](8) NULL,
	[blocking_task_address] [varbinary](8) NULL,
	[blocking_session_id] [smallint] NULL,
	[blocking_exec_context_id] [int] NULL,
	[resource_description] [nvarchar](3072) NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
