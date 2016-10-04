SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[TransactionDetails](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[session_id] [smallint] NOT NULL,
	[TimeIdentifier] [datetime] NOT NULL,
	[dtat_transaction_id] [bigint] NOT NULL,
	[dtat_name] [nvarchar](32) NULL,
	[dtat_transaction_begin_time] [datetime] NULL,
	[dtat_transaction_type] [smallint] NULL,
	[dtat_transaction_uow] [uniqueidentifier] NULL,
	[dtat_transaction_state] [smallint] NULL,
	[dtat_dtc_state] [smallint] NULL,
	[dtst_enlist_count] [smallint] NULL,
	[dtst_is_user_transaction] [bit] NULL,
	[dtst_is_local] [bit] NULL,
	[dtst_is_enlisted] [bit] NULL,
	[dtst_is_bound] [bit] NULL,
	[dtdt_database_id] [int] NULL,
	[dtdt_database_transaction_begin_time] [datetime] NULL,
	[dtdt_database_transaction_type] [smallint] NULL,
	[dtdt_database_transaction_state] [smallint] NULL,
	[dtdt_database_transaction_log_record_count] [bigint] NULL,
	[dtdt_database_transaction_log_bytes_used] [bigint] NULL,
	[dtdt_database_transaction_log_bytes_reserved] [bigint] NULL,
	[dtdt_database_transaction_log_bytes_used_system] [int] NULL,
	[dtdt_database_transaction_log_bytes_reserved_system] [int] NULL,
	[dtasdt_tran_exists] [bit] NULL,
	[dtasdt_transaction_sequence_num] [bigint] NULL,
	[dtasdt_commit_sequence_num] [bigint] NULL,
	[dtasdt_is_snapshot] [smallint] NULL,
	[dtasdt_first_snapshot_sequence_num] [bigint] NULL,
	[dtasdt_max_version_chain_traversed] [int] NULL,
	[dtasdt_average_version_chain_traversed] [real] NULL,
	[dtasdt_elapsed_time_seconds] [bigint] NULL
) ON [PRIMARY]

GO
CREATE CLUSTERED INDEX [CL_SpidCaptureTime] ON [AutoWho].[TransactionDetails]
(
	[SPIDCaptureTime] ASC,
	[session_id] ASC,
	[TimeIdentifier] ASC,
	[dtat_transaction_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
