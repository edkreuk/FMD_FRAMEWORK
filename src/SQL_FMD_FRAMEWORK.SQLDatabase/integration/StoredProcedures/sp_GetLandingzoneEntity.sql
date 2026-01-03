




    CREATE     PROCEDURE [integration].[sp_GetLandingzoneEntity](
         @LakehouseId int
        ,@SourceSchema NVARCHAR(100)
        ,@SourceName NVARCHAR(200)
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @LandingzoneEntityId int
    BEGIN
        set @LandingzoneEntityId = ( select isnull(LandingzoneEntityId,0) as LandingzoneEntityId  from   [integration].[LandingzoneEntity] where  [SourceName] = @SourceName and  [SourceSchema] = @SourceSchema and [LakehouseId] = @LakehouseId)
    END

            SELECT @LandingzoneEntityId AS LandingzoneEntityId

GO

