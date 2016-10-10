SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[Options_History](
	[HistoryInsertDate] [datetime] NOT NULL,
	[TriggerAction] [nvarchar](40) NOT NULL,
	[RowID] [int] NOT NULL,
	[AutoWhoEnabled] [nchar](1) NOT NULL,
	[BeginTime] [smallint] NOT NULL,
	[EndTime] [smallint] NOT NULL,
	[IntervalLength] [smallint] NOT NULL,
	[IncludeIdleWithTran] [nchar](1) NOT NULL,
	[IncludeIdleWithoutTran] [nchar](1) NOT NULL,
	[DurationFilter] [int] NOT NULL,
	[IncludeDBs] [nvarchar](4000) NOT NULL,
	[ExcludeDBs] [nvarchar](4000) NOT NULL,
	[HighTempDBThreshold] [int] NOT NULL,
	[CollectSystemSpids] [nchar](1) NOT NULL,
	[HideSelf] [nchar](1) NOT NULL,
	[ObtainBatchText] [nchar](1) NOT NULL,
	[ObtainQueryPlanForStatement] [nchar](1) NOT NULL,
	[ObtainQueryPlanForBatch] [nchar](1) NOT NULL,
	[ObtainLocksForBlockRelevantThreshold] [int] NOT NULL,
	[InputBufferThreshold] [int] NOT NULL,
	[ParallelWaitsThreshold] [int] NOT NULL,
	[QueryPlanThreshold] [int] NOT NULL,
	[QueryPlanThresholdBlockRel] [int] NOT NULL,
	[BlockingChainThreshold] [int] NOT NULL,
	[BlockingChainDepth] [tinyint] NOT NULL,
	[TranDetailsThreshold] [int] NOT NULL,
	[MediumDurationThreshold] [int] NOT NULL,
	[HighDurationThreshold] [int] NOT NULL,
	[BatchDurationThreshold] [int] NOT NULL,
	[LongTransactionThreshold] [int] NOT NULL,
	[Retention_IdleSPIDs_NoTran] [int] NOT NULL,
	[Retention_IdleSPIDs_WithShortTran] [int] NOT NULL,
	[Retention_IdleSPIDs_WithLongTran] [int] NOT NULL,
	[Retention_IdleSPIDs_HighTempDB] [int] NOT NULL,
	[Retention_ActiveLow] [int] NOT NULL,
	[Retention_ActiveMedium] [int] NOT NULL,
	[Retention_ActiveHigh] [int] NOT NULL,
	[Retention_ActiveBatch] [int] NOT NULL,
	[Retention_CaptureTimes] [int] NOT NULL,
	[DebugSpeed] [nchar](1) NOT NULL,
	[ThresholdFilterRefresh] [smallint] NOT NULL,
	[SaveBadDims] [nchar](1) NOT NULL,
	[Enable8666] [nchar](1) NOT NULL,
	[ResolvePageLatches] [nchar](1) NOT NULL,
	[ResolveLockWaits] [nchar](1) NOT NULL,
	[LastModifiedUser] [nvarchar](128) NOT NULL,
 CONSTRAINT [PK_Options_History] PRIMARY KEY CLUSTERED 
(
	[HistoryInsertDate] ASC,
	[TriggerAction] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
