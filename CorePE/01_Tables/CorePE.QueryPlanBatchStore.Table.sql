SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [CorePE].[QueryPlanBatchStore](
	[PKQueryPlanBatchStoreID] [bigint] IDENTITY(1,1) NOT NULL,
	[AWBatchPlanHash] [varbinary](64) NOT NULL,
	[plan_handle] [varbinary](64) NOT NULL,
	[dbid] [smallint] NOT NULL,
	[objectid] [int] NOT NULL,
	[fail_to_obtain] [bit] NOT NULL,
	[query_plan] [nvarchar](max) NOT NULL,
	[Insertedby_SPIDCaptureTime] [datetime] NOT NULL,
	[LastTouchedBy_SPIDCaptureTime] [datetime] NOT NULL,
 CONSTRAINT [PK_QueryPlanBatchStore] PRIMARY KEY CLUSTERED 
(
	[PKQueryPlanBatchStoreID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
SET ANSI_PADDING ON

GO
CREATE NONCLUSTERED INDEX [ncl_AWBatchPlanHash] ON [CorePE].[QueryPlanBatchStore]
(
	[AWBatchPlanHash] ASC
)
INCLUDE ( 	[plan_handle],
	[dbid],
	[objectid]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON

GO
CREATE NONCLUSTERED INDEX [ncl_LastTouched] ON [CorePE].[QueryPlanBatchStore]
(
	[LastTouchedBy_SPIDCaptureTime] ASC
)
INCLUDE ( 	[AWBatchPlanHash],
	[plan_handle],
	[dbid],
	[objectid],
	[PKQueryPlanBatchStoreID]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
