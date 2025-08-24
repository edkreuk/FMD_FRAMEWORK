create view logging.test

AS


SELECT  [WorkspaceGuid]
      ,[PipelineRunGuid]
      ,[PipelineParentRunGuid]
      ,[PipelineGuid]
      ,[PipelineName]
      ,[PipelineParameters]
      ,[EntityId]
      ,[EntityLayer]
      ,[TriggerType]
      ,[TriggerGuid]
      ,[TriggerTime]
      ,[LogType]
      ,[LogDateTime]
      ,[LogData]
  FROM [logging].[PipelineExecution]

GO

