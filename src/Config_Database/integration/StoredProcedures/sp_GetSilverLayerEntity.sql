




    CREATE     PROCEDURE [integration].[sp_GetSilverLayerEntity](
         @BronzeLayerEntityId int
       
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @SilverLayerEntityId int
    BEGIN
        set @SilverLayerEntityId = ( select  isnull(SilverLayerEntityId,0) as SilverLayerEntityId from   [integration].[SilverLayerEntity] where BronzeLayerEntityId= @BronzeLayerEntityId)
    END

            SELECT @SilverLayerEntityId AS SilverLayerEntityId

GO

