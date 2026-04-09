
    CREATE PROCEDURE [execution].[sp_GetBronzeCleansingRule](
         @BronzeLayerEntityId INT
)

WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (CleansingRules NVARCHAR(MAX));

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[BronzeLayerEntity]
                  WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId)

    BEGIN
        THROW 50000, 'BronzeLayerEntity not found.', 1;
    END

    ELSE
    BEGIN
       select CleansingRules FROM [integration].[BronzeLayerEntity]
              
        WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;
    END

    SELECT CleansingRules FROM @OutputTable;
END

GO

