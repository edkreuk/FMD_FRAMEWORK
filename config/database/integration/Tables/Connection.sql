CREATE TABLE [integration].[Connection] (
    [ConnectionId]        INT              IDENTITY (1, 1) NOT NULL,
    [ConnectionGuid]      UNIQUEIDENTIFIER NOT NULL,
    [Name]                VARCHAR (200)    NOT NULL,
    [Type]                VARCHAR (50)     NOT NULL,
    [GatewayType]         VARCHAR (50)     NULL,
    [DatasourceReference] VARCHAR (MAX)    NULL,
    [IsActive]            BIT              DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_ConnectionId] PRIMARY KEY CLUSTERED ([ConnectionId] ASC),
    CONSTRAINT [UC_integration_Connection] UNIQUE NONCLUSTERED ([ConnectionGuid] ASC)
);


GO

