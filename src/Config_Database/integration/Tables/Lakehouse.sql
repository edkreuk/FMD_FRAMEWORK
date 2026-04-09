CREATE TABLE [integration].[Lakehouse] (
    [LakehouseId]   INT              IDENTITY (1, 1) NOT NULL,
    [LakehouseGuid] UNIQUEIDENTIFIER NOT NULL,
    [WorkspaceGuid] UNIQUEIDENTIFIER NOT NULL,
    [Name]          VARCHAR (100)    NOT NULL,
    [IsActive]      BIT              DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_Lakehouse] PRIMARY KEY CLUSTERED ([LakehouseId] ASC),
    CONSTRAINT [FK_Lakehouse_Workspace] FOREIGN KEY ([WorkspaceGuid]) REFERENCES [integration].[Workspace] ([WorkspaceGuid]),
    CONSTRAINT [UC_integration_Lakehouse] UNIQUE NONCLUSTERED ([LakehouseGuid] ASC)
);


GO

