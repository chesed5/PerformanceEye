SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[DimWaitType](
	[DimWaitTypeID] [smallint] IDENTITY(30,1) NOT NULL,
	[wait_type] [nvarchar](60) NOT NULL,
	[wait_type_short] [nvarchar](60) NOT NULL,
	[latch_subtype] [nvarchar](100) NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimWaitType] PRIMARY KEY CLUSTERED 
(
	[DimWaitTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON

GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_allattributes] ON [AutoWho].[DimWaitType]
(
	[wait_type] ASC,
	[wait_type_short] ASC,
	[latch_subtype] ASC
)
INCLUDE ( 	[DimWaitTypeID],
	[TimeAdded]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [AutoWho].[DimWaitType] ADD  CONSTRAINT [DF_AutoWho_DimWaitType_TimeAdded]  DEFAULT (getdate()) FOR [TimeAdded]
GO
