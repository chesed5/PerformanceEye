SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[SignalTable](
	[SignalName] [nvarchar](100) NOT NULL,
	[SignalValue] [nvarchar](100) NULL,
	[InsertTime] [datetime] NOT NULL
) ON [PRIMARY]
GO
