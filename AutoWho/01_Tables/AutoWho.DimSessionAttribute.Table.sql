SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [AutoWho].[DimSessionAttribute](
	[DimSessionAttributeID] [int] IDENTITY(30,1) NOT NULL,
	[host_name] [nvarchar](128) NOT NULL,
	[program_name] [nvarchar](128) NOT NULL,
	[client_version] [int] NOT NULL,
	[client_interface_name] [nvarchar](32) NOT NULL,
	[endpoint_id] [int] NOT NULL,
	[transaction_isolation_level] [smallint] NOT NULL,
	[deadlock_priority] [smallint] NOT NULL,
	[group_id] [int] NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimSessionAttributes] PRIMARY KEY CLUSTERED 
(
	[DimSessionAttributeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING ON

GO
CREATE UNIQUE NONCLUSTERED INDEX [AK_allattributes] ON [AutoWho].[DimSessionAttribute]
(
	[host_name] ASC,
	[program_name] ASC,
	[client_version] ASC,
	[client_interface_name] ASC,
	[endpoint_id] ASC,
	[transaction_isolation_level] ASC,
	[deadlock_priority] ASC,
	[group_id] ASC
)
INCLUDE ( 	[DimSessionAttributeID],
	[TimeAdded]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [AutoWho].[DimSessionAttribute] ADD  CONSTRAINT [DF_AutoWho_DimSessionAttributes_TimeAdded]  DEFAULT (getdate()) FOR [TimeAdded]
GO
