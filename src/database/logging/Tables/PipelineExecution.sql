CREATE TABLE [logging].[PipelineExecution] (
    [WorkspaceGuid]         UNIQUEIDENTIFIER NULL,
    [PipelineRunGuid]       UNIQUEIDENTIFIER NULL,
    [PipelineParentRunGuid] UNIQUEIDENTIFIER NULL,
    [PipelineGuid]          UNIQUEIDENTIFIER NULL,
    [PipelineName]          VARCHAR (100)    NULL,
    [PipelineParameters]    VARCHAR (8000)   NULL,
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

