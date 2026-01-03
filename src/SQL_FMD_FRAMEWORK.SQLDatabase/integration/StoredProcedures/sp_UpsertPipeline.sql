


    CREATE     PROCEDURE [integration].[sp_UpsertPipeline](
        @PipelineId UNIQUEIDENTIFIER
        ,@WorkspaceId UNIQUEIDENTIFIER
        ,@Name NVARCHAR(100) 
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    IF NOT EXISTS(SELECT  1
                FROM [integration].[Pipeline]
                WHERE [PipelineGuid] = @PipelineId)
    BEGIN
        INSERT INTO [integration].[Pipeline]
            ([Name]
            ,[PipelineGuid]
            ,[WorkspaceGuid])
        VALUES (@Name
            ,@PipelineId
            ,@WorkspaceId);

    END
    ELSE
    BEGIN

        UPDATE [integration].[Pipeline]
        SET [Name] = @Name,
            [WorkspaceGuid] = @WorkspaceId
        WHERE [PipelineGuid] = @PipelineId
    END

GO

