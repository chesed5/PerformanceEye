SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [CorePE].[OrdinalCachePosition](
	[Utility] [nvarchar](30) NOT NULL,
	[StartTime] [datetime] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[session_id] [smallint] NOT NULL,
	[CurrentPosition] [int] NOT NULL,
	[LastOptionsHash] [varbinary](64) NOT NULL,
 CONSTRAINT [PK_OrdinalCachePosition] PRIMARY KEY CLUSTERED 
(
	[Utility] ASC,
	[StartTime] ASC,
	[EndTime] ASC,
	[session_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
