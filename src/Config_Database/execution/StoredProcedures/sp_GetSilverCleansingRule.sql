
    CREATE PROCEDURE [execution].[sp_GetSilverCleansingRule](
         @SilverLayerEntityId INT
)

WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (CleansingRules NVARCHAR(MAX));

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[SilverLayerEntity]
                  WHERE [SilverLayerEntityId] = @SilverLayerEntityId)

    BEGIN
        THROW 50000, 'SilverLayerEntity not found.', 1;
    END

    ELSE
    BEGIN
       select CleansingRules FROM [integration].[SilverLayerEntity]
              
        WHERE [SilverLayerEntityId] = @SilverLayerEntityId;
    END

    SELECT CleansingRules FROM @OutputTable;
END

GO

