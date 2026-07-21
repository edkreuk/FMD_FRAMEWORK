-- =============================================================================
-- Table: integration.Rules
-- Description: Individual data quality rules linked to a DQ check configuration
-- Dependencies: integration.SilverLayerEntityDQCheck, integration.RuleType,
--               integration.RuleSeverity
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'Rules'
)
BEGIN
    CREATE TABLE [integration].[Rules] (
        [RuleId]                       BIGINT         NOT NULL IDENTITY(1,1),
        [SilverLayerEntityDQCheckId]   BIGINT         NOT NULL,
        [RuleTypeId]                   INT            NOT NULL,
        [RuleSeverityId]               INT            NOT NULL,
        [Name]                         NVARCHAR(200)  NULL,
        [RuleExpression]               NVARCHAR(MAX)  NULL,
        [ErrorMessage]                 NVARCHAR(MAX)  NULL,
        [IsActive]                     BIT            NOT NULL,

        CONSTRAINT [PK_integration_Rules] PRIMARY KEY ([RuleId]),

        CONSTRAINT [FK_Rules_SilverLayerEntityDQCheck]
            FOREIGN KEY ([SilverLayerEntityDQCheckId])
            REFERENCES [integration].[SilverLayerEntityDQCheck] ([SilverLayerEntityDQCheckId]),

        CONSTRAINT [FK_Rules_RuleType]
            FOREIGN KEY ([RuleTypeId])
            REFERENCES [integration].[RuleType] ([RuleTypeId]),

        CONSTRAINT [FK_Rules_RuleSeverity]
            FOREIGN KEY ([RuleSeverityId])
            REFERENCES [integration].[RuleSeverity] ([RuleSeverityId])
    );
END;
