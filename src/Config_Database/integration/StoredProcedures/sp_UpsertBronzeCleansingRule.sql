
    CREATE PROCEDURE [integration].[sp_UpsertBronzeCleansingRule](
         @BronzeLayerEntityId INT
        ,@CleansingRules NVARCHAR(max) 
)

WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (BronzeLayerEntityId INT, CleansingRules NVARCHAR(MAX));

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[BronzeLayerEntity]
                  WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId)

    BEGIN
        THROW 50000, 'BronzeLayerEntity not found.', 1;
    END

    ELSE
    BEGIN
        UPDATE [integration].[BronzeLayerEntity]
        SET [CleansingRules] = @CleansingRules
                 OUTPUT INSERTED.[BronzeLayerEntityId],INSERTED.[CleansingRules] INTO @OutputTable
        WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;
    END

    SELECT BronzeLayerEntityId, CleansingRules FROM @OutputTable;
END

GO

