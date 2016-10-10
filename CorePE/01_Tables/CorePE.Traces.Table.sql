SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [CorePE].[Traces](
	[TraceID] [int] IDENTITY(1,1) NOT NULL,
	[Utility] [nvarchar](20) NOT NULL,
	[Type] [nvarchar](20) NOT NULL CONSTRAINT [DF_AutoWhoTraces_Type]  DEFAULT (N'N''Background'),
	[CreateTime] [datetime] NOT NULL CONSTRAINT [DF_AutoWhoTraces_CreateTime]  DEFAULT (getdate()),
	[IntendedStopTime] [datetime] NOT NULL,
	[StopTime] [datetime] NULL,
	[AbortCode] [nchar](1) NULL,
	[TerminationMessage] [nvarchar](MAX) NULL,
	[Payload_int] [int] NULL,
	[Payload_bigint] [bigint] NULL, 
	[Payload_decimal] [decimal](28,9) NULL,
	[Payload_datetime] [datetime] NULL,
	[Payload_nvarchar] [nvarchar](MAX) NULL
 CONSTRAINT [PK_AutoWhoTraces] PRIMARY KEY CLUSTERED 
(
	[TraceID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
