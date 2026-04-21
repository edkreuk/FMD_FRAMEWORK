/*
=============================================================================
002_sp_GetBronzeDQRule.sql
=============================================================================
Creates (or alters) execution.sp_GetBronzeDQRule.

Returns the DQRules JSON string for a given BronzeLayerEntityId.
Called by NB_FMD_LOAD_LANDING_BRONZE to retrieve metadata-driven
Data Quality rules before processing data from the Landing Zone.

Mirrors the pattern of execution.sp_GetBronzeCleansingRule.
=============================================================================
*/

CREATE OR ALTER PROCEDURE [execution].[sp_GetBronzeDQRule]
    @BronzeLayerEntityId BIGINT
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (
        SELECT 1
        FROM [integration].[BronzeLayerEntity]
        WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId
    )
    BEGIN
        THROW 50000, 'BronzeLayerEntity not found.', 1;
    END

    SELECT [DQRules]
    FROM [integration].[BronzeLayerEntity]
    WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;
END
GO
