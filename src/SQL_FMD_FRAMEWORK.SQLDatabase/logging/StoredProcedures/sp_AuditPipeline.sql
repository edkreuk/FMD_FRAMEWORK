
CREATE PROCEDURE [logging].[sp_AuditPipeline]
    @PipelineGuid UNIQUEIDENTIFIER = NULL
    ,@PipelineName VARCHAR(100) = NULL
    ,@PipelineRunGuid UNIQUEIDENTIFIER = NULL
    ,@PipelineParentRunGuid UNIQUEIDENTIFIER = NULL
    ,@PipelineParameters VARCHAR(8000) = NULL
    ,@TriggerType VARCHAR(50) = NULL
    ,@TriggerGuid UNIQUEIDENTIFIER = NULL
    ,@TriggerTime datetime = NULL
    ,@LogData VARCHAR(8000) = NULL
    ,@LogType varchar(50) --Choice between Start/End/Fail, based on this Type the correct execution will be done
    ,@WorkspaceGuid UNIQUEIDENTIFIER = NULL
    ,@EntityId INT = NULL
    ,@EntityLayer varchar(50) = NULL
AS

    INSERT INTO [logging].[PipelineExecution]
           ([PipelineRunGuid]
           ,[PipelineParentRunGuid]
           ,[PipelineGuid]
           ,[PipelineName]
           ,[PipelineParameters]
           ,[TriggerType]
           ,[TriggerGuid]
           ,[TriggerTime]
           ,[LogDateTime]
           ,[LogType]
           ,[LogData]
           ,[WorkspaceGuid]
           ,[EntityId]
           ,[EntityLayer]
            )
     VALUES (
           @PipelineRunGuid,
           @PipelineParentRunGuid,
           @PipelineGuid,
           @PipelineName,
           @PipelineParameters,
           @TriggerType,
           @TriggerGuid,
           @TriggerTime,
           getdate(),
           @LogType,
           @LogData,
           @WorkspaceGuid,
           @EntityId,
           @EntityLayer
           )

GO

