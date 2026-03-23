/*
=============================================================================
004_sp_UpsertBronzeDQRule.sql
=============================================================================
Creates (or alters) integration.sp_UpsertBronzeDQRule.

Sets the DQRules JSON string for a given BronzeLayerEntityId.
Use this stored procedure (or the Setup notebook) to configure
metadata-driven Data Quality rules for a Bronze layer entity.

Example usage:
    EXEC [integration].[sp_UpsertBronzeDQRule]
        @BronzeLayerEntityId = 1,
        @DQRules = N'[{"rule":"not_null","columns":"CustomerId","action":"reject"}]';

Mirrors the pattern of integration.sp_UpsertBronzeCleansingRule.
=============================================================================
*/

CREATE OR ALTER PROCEDURE [integration].[sp_UpsertBronzeDQRule]
    @BronzeLayerEntityId BIGINT,
    @DQRules             NVARCHAR(MAX)
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

    UPDATE [integration].[BronzeLayerEntity]
    SET [DQRules] = @DQRules
    WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;

    SELECT [BronzeLayerEntityId], [DQRules]
    FROM [integration].[BronzeLayerEntity]
    WHERE [BronzeLayerEntityId] = @BronzeLayerEntityId;
END
GO
