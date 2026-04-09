
    CREATE PROCEDURE [integration].[sp_UpsertSilverCleansingRule](
         @SilverLayerEntityId INT
        ,@CleansingRules NVARCHAR(max) 
)

WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OutputTable TABLE (SilverLayerEntityId INT, CleansingRules NVARCHAR(MAX));

    IF NOT EXISTS(SELECT 1
                  FROM [integration].[SilverLayerEntity]
                  WHERE [SilverLayerEntityId] = @SilverLayerEntityId)

    BEGIN
        THROW 50000, 'SilverLayerEntity not found.', 1;
    END

    ELSE
    BEGIN
        UPDATE [integration].[SilverLayerEntity]
        SET [CleansingRules] = @CleansingRules
                 OUTPUT INSERTED.[SilverLayerEntityId],INSERTED.[CleansingRules] INTO @OutputTable
        WHERE [SilverLayerEntityId] = @SilverLayerEntityId;
    END

    SELECT SilverLayerEntityId, CleansingRules FROM @OutputTable;
END

GO

