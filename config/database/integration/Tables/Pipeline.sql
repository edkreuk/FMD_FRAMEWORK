CREATE TABLE [integration].[Pipeline] (
    [PipelineId]    INT              IDENTITY (1, 1) NOT NULL,
    [PipelineGuid]  UNIQUEIDENTIFIER NOT NULL,
    [WorkspaceGuid] UNIQUEIDENTIFIER NOT NULL,
    [Name]          VARCHAR (200)    NOT NULL,
    [IsActive]      BIT              DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_PipelineId] PRIMARY KEY CLUSTERED ([PipelineId] ASC),
    CONSTRAINT [UC_integration_Pipeline] UNIQUE NONCLUSTERED ([PipelineGuid] ASC)
);


GO

