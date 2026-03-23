/*
=============================================================================
003_sp_GetSilverDQRule.sql
=============================================================================
Creates (or alters) execution.sp_GetSilverDQRule.

Returns the DQRules JSON string for a given SilverLayerEntityId.
Called by NB_FMD_LOAD_BRONZE_SILVER to retrieve metadata-driven
Data Quality rules before processing data from the Bronze layer.

Mirrors the pattern of execution.sp_GetSilverCleansingRule.
=============================================================================
*/

CREATE OR ALTER PROCEDURE [execution].[sp_GetSilverDQRule]
    @SilverLayerEntityId BIGINT
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (
        SELECT 1
        FROM [integration].[SilverLayerEntity]
        WHERE [SilverLayerEntityId] = @SilverLayerEntityId
    )
    BEGIN
        THROW 50000, 'SilverLayerEntity not found.', 1;
    END

    SELECT [DQRules]
    FROM [integration].[SilverLayerEntity]
    WHERE [SilverLayerEntityId] = @SilverLayerEntityId;
END
GO
