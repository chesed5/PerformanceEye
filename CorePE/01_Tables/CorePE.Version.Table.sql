SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CorePE].[Version](
	[Version] [nvarchar](20) NOT NULL,
	[EffectiveDate] [datetime] NOT NULL
) ON [PRIMARY]

GO
ALTER TABLE [CorePE].[Version] ADD  CONSTRAINT [DF_Version_InsertedDate]  DEFAULT (getdate()) FOR [EffectiveDate]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
