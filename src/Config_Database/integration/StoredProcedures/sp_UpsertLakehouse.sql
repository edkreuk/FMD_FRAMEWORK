


    CREATE     PROCEDURE [integration].[sp_UpsertLakehouse](
        @LakehouseId UNIQUEIDENTIFIER
        ,@WorkspaceId UNIQUEIDENTIFIER
        ,@Name NVARCHAR(100) 
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    IF NOT EXISTS(SELECT  1
                FROM [integration].[Lakehouse]
                WHERE [LakehouseGuid] = @LakehouseId)
    BEGIN
        INSERT INTO [integration].[Lakehouse]
            ([Name]
            ,[LakehouseGuid]
            ,[WorkspaceGuid])
        VALUES (@Name
            ,@LakehouseId
            ,@WorkspaceId);

    END
    ELSE
    BEGIN

        UPDATE [integration].[Lakehouse]
        SET [Name] = @Name,
            [WorkspaceGuid] = @WorkspaceId
        WHERE [LakehouseGuid] = @LakehouseId
    END

GO

