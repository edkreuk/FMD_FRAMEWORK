-- =============================================================================
-- Table: integration.RuleType
-- Description: Lookup table for data quality rule types
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'RuleType'
)
BEGIN
    CREATE TABLE [integration].[RuleType] (
        [RuleTypeId]   INT            NOT NULL IDENTITY(1,1),
        [Name]         NVARCHAR(100)  NOT NULL,
        [Description]  NVARCHAR(500)  NULL,
        [IsActive]     BIT            NOT NULL,

        CONSTRAINT [PK_integration_RuleType] PRIMARY KEY ([RuleTypeId])
    );
END;
