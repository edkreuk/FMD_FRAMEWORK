


    CREATE    PROCEDURE [integration].[sp_UpsertWorkspace](
        @WorkspaceId UNIQUEIDENTIFIER
        ,@Name NVARCHAR(100) 
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    IF NOT EXISTS(SELECT  1
                FROM [integration].[Workspace]
                WHERE [WorkspaceGuid] = @WorkspaceId)
    BEGIN
        INSERT INTO [integration].[Workspace]
            ([Name]
            ,[WorkspaceGuid])
        VALUES (@Name
            ,@WorkspaceId);

    END
    ELSE
    BEGIN

        UPDATE [integration].[Workspace]
        SET [Name] = @Name
        WHERE [WorkspaceGuid] = @WorkspaceId
    END

GO

