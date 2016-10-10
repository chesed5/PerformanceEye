SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[TAWException](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[task_address] [varbinary](8) NOT NULL,
	[parent_task_address] [varbinary](8) NULL,
	[session_id] [smallint] NOT NULL,
	[request_id] [smallint] NOT NULL,
	[exec_context_id] [smallint] NOT NULL,
	[tstate] [nchar](1) NOT NULL,
	[scheduler_id] [int] NULL,
	[context_switches_count] [bigint] NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[wait_latch_subtype] [nvarchar](100) NOT NULL,
	[wait_duration_ms] [bigint] NOT NULL,
	[wait_special_category] [tinyint] NOT NULL,
	[wait_order_category] [tinyint] NOT NULL,
	[wait_special_number] [int] NULL,
	[wait_special_tag] [nvarchar](100) NULL,
	[task_priority] [int] NOT NULL,
	[blocking_task_address] [varbinary](8) NULL,
	[blocking_session_id] [smallint] NULL,
	[blocking_exec_context_id] [smallint] NULL,
	[resource_description] [nvarchar](3072) NULL,
	[resource_dbid] [int] NULL,
	[resource_associatedobjid] [bigint] NULL,
	[RecordReason] [tinyint] NULL
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
