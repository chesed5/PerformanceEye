SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[LockDetails](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[request_session_id] [smallint] NOT NULL,
	[request_request_id] [smallint] NULL,
	[TimeIdentifier] [datetime] NOT NULL,
	[request_exec_context_id] [smallint] NULL,
	[request_owner_type] [tinyint] NULL,
	[request_owner_id] [bigint] NULL,
	[request_owner_guid] [nvarchar](40) NULL,
	[resource_type] [nvarchar](60) NULL,
	[resource_subtype] [nvarchar](60) NULL,
	[resource_database_id] [int] NULL,
	[resource_description] [nvarchar](256) NULL,
	[resource_associated_entity_id] [bigint] NULL,
	[resource_lock_partition] [int] NULL,
	[request_mode] [nvarchar](60) NULL,
	[request_type] [nvarchar](60) NULL,
	[request_status] [nvarchar](60) NULL,
	[RecordCount] [bigint] NULL
) ON [PRIMARY]

GO
CREATE CLUSTERED INDEX [CL_SPIDCaptureTime] ON [AutoWho].[LockDetails]
(
	[SPIDCaptureTime] ASC,
	[request_session_id] ASC,
	[request_request_id] ASC,
	[TimeIdentifier] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
