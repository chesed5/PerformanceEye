SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CorePE].[DBIDNameMapping](
	[DBID] [int] NOT NULL,
	[DBName] [nvarchar](256) NOT NULL,
	[EffectiveStartTime] [datetime] NOT NULL,
	[EffectiveEndTime] [datetime] NULL,
 CONSTRAINT [PK_DBIDNameMapping_1] PRIMARY KEY CLUSTERED 
(
	[DBName] ASC,
	[EffectiveStartTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON

GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_DBID_EffectiveStartTime] ON [CorePE].[DBIDNameMapping]
(
	[DBID] ASC,
	[EffectiveStartTime] ASC
)
INCLUDE ( 	[DBName],
	[EffectiveEndTime]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
