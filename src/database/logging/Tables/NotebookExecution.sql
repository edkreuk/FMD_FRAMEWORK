CREATE TABLE [logging].[NotebookExecution] (
    [WorkspaceGuid]         UNIQUEIDENTIFIER NULL,
    [PipelineRunGuid]       UNIQUEIDENTIFIER NULL,
    [PipelineParentRunGuid] UNIQUEIDENTIFIER NULL,
    [NotebookGuid]          UNIQUEIDENTIFIER NULL,
    [NotebookName]          VARCHAR (100)    NULL,
    [NotebookParameters]    VARCHAR (8000)   NULL,
    [EntityId]              INT              NULL,
    [EntityLayer]           VARCHAR (50)     NULL,
    [TriggerType]           VARCHAR (50)     NULL,
    [TriggerGuid]           UNIQUEIDENTIFIER NULL,
    [TriggerTime]           DATETIME2 (6)    NULL,
    [LogType]               VARCHAR (50)     NULL,
    [LogDateTime]           DATETIME2 (6)    NULL,
    [LogData]               VARCHAR (8000)   NULL
);


GO

