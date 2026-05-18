-- =============================================================================
-- Table: integration.SilverLayerEntityDQCheck
-- Description: Data quality check configuration per Silver Layer Entity
-- Dependencies: integration.SilverLayerEntity
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'SilverLayerEntityDQCheck'
)
BEGIN
    CREATE TABLE [integration].[SilverLayerEntityDQCheck] (
        [SilverLayerEntityDQCheckId]  BIGINT         NOT NULL IDENTITY(1,1),
        [SilverLayerEntityId]         BIGINT         NOT NULL,
        [Name]                        NVARCHAR(200)  NULL,
        [Description]                 NVARCHAR(MAX)  NULL,
        [IsActive]                    BIT            NOT NULL,

        CONSTRAINT [PK_integration_SilverLayerEntityDQCheck] PRIMARY KEY ([SilverLayerEntityDQCheckId]),

        CONSTRAINT [FK_SilverLayerEntityDQCheck_SilverLayerEntity]
            FOREIGN KEY ([SilverLayerEntityId])
            REFERENCES [integration].[SilverLayerEntity] ([SilverLayerEntityId])
    );
END;
