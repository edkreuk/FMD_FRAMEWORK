CREATE TABLE [integration].[Workspace] (
    [WorkspaceId]   INT              IDENTITY (1, 1) NOT NULL,
    [WorkspaceGuid] UNIQUEIDENTIFIER NOT NULL,
    [Name]          VARCHAR (100)    NOT NULL,
    CONSTRAINT [PK_integration_Workspace] PRIMARY KEY CLUSTERED ([WorkspaceId] ASC),
    CONSTRAINT [UC_integration_Workspace_WorkspaceGuid] UNIQUE NONCLUSTERED ([WorkspaceGuid] ASC)
);


GO

