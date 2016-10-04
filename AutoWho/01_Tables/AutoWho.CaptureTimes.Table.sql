SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[CaptureTimes](
	[SPIDCaptureTime] [datetime] NOT NULL,
	[UTCCaptureTime] [datetime] NOT NULL,
	[RunWasSuccessful] [tinyint] NOT NULL,
	[CaptureSummaryPopulated] [tinyint] NOT NULL,
	[AutoWhoDuration_ms] [int] NOT NULL,
	[SpidsCaptured] [int] NULL,
	[DurationBreakdown] [varchar](1000) NULL,
 CONSTRAINT [PK_AutoWho_CaptureTimes] PRIMARY KEY CLUSTERED 
(
	[SPIDCaptureTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
CREATE UNIQUE NONCLUSTERED INDEX [NCL_SearchFields] ON [AutoWho].[CaptureTimes]
(
	[RunWasSuccessful] ASC,
	[CaptureSummaryPopulated] ASC,
	[SPIDCaptureTime] ASC
)
INCLUDE ( 	[AutoWhoDuration_ms]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
