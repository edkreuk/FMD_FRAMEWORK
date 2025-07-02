


    CREATE     PROCEDURE [integration].[sp_GetLakehouse](
         @WorkspaceGuid UNIQUEIDENTIFIER
        ,@Name NVARCHAR(100)
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @LakehouseId int
    BEGIN
        set @LakehouseId= ( select LakehouseId from   [integration].[Lakehouse] where  [Name] = @Name and  [WorkspaceGuid] = @WorkspaceGuid)
    END

            SELECT @LakehouseId AS LakehouseId

GO

