



    CREATE     PROCEDURE [integration].[sp_GetBronzeLayerEntity](
         @LandingzoneEntityId int
       
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @BronzeLayerEntityId int
    BEGIN
        set @BronzeLayerEntityId = ( select isnull(BronzeLayerEntityId,0) as BronzeLayerEntityId from   [integration].[BronzeLayerEntity] where LandingzoneEntityId= @LandingzoneEntityId)
    END

            SELECT @BronzeLayerEntityId AS BronzeLayerEntityId

GO

