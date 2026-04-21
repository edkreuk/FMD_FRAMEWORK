/*
=============================================================================
005_sp_UpsertSilverDQRule.sql
=============================================================================
Creates (or alters) integration.sp_UpsertSilverDQRule.

Sets the DQRules JSON string for a given SilverLayerEntityId.
Use this stored procedure (or the Setup notebook) to configure
metadata-driven Data Quality rules for a Silver layer entity.

Example usage:
    EXEC [integration].[sp_UpsertSilverDQRule]
        @SilverLayerEntityId = 1,
        @DQRules = N'[{"rule":"not_null","columns":"CustomerId","action":"reject"}]';

Mirrors the pattern of integration.sp_UpsertSilverCleansingRule.
=============================================================================
*/

CREATE OR ALTER PROCEDURE [integration].[sp_UpsertSilverDQRule]
    @SilverLayerEntityId BIGINT,
    @DQRules             NVARCHAR(MAX)
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

    UPDATE [integration].[SilverLayerEntity]
    SET [DQRules] = @DQRules
    WHERE [SilverLayerEntityId] = @SilverLayerEntityId;

    SELECT [SilverLayerEntityId], [DQRules]
    FROM [integration].[SilverLayerEntity]
    WHERE [SilverLayerEntityId] = @SilverLayerEntityId;
END
GO
