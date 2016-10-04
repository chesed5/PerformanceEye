SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_PADDING ON
GO
CREATE TABLE [AutoWho].[DimNetAddress](
	[DimNetAddressID] [smallint] IDENTITY(30,1) NOT NULL,
	[client_net_address] [varchar](48) NOT NULL,
	[local_net_address] [varchar](48) NOT NULL,
	[local_tcp_port] [int] NOT NULL,
	[TimeAdded] [datetime] NOT NULL,
 CONSTRAINT [PK_AutoWho_DimNetAddress] PRIMARY KEY CLUSTERED 
(
	[DimNetAddressID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
SET ANSI_PADDING OFF
GO
ALTER TABLE [AutoWho].[DimNetAddress] ADD  CONSTRAINT [DF_AutoWho_DimNetAddress_TimeAdded]  DEFAULT (getdate()) FOR [TimeAdded]
GO
