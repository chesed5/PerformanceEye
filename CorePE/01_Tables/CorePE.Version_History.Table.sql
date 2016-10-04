SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CorePE].[Version_History](
	[Version] [nvarchar](20) NOT NULL,
	[EffectiveDate] [datetime] NOT NULL,
	[HistoryInsertDate] [datetime] NOT NULL,
	[TriggerAction] [nvarchar](20) NOT NULL
) ON [PRIMARY]

GO
