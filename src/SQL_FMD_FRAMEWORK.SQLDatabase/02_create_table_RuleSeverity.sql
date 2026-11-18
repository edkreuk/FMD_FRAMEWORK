-- =============================================================================
-- Table: integration.RuleSeverity
-- Description: Lookup table for data quality rule severity levels
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'RuleSeverity'
)
BEGIN
    CREATE TABLE [integration].[RuleSeverity] (
        [RuleSeverityId]  INT            NOT NULL IDENTITY(1,1),
        [Name]            NVARCHAR(100)  NOT NULL,
        [Description]     NVARCHAR(500)  NULL,
        [Level]           INT            NULL,
        [IsActive]        BIT            NOT NULL,

        CONSTRAINT [PK_integration_RuleSeverity] PRIMARY KEY ([RuleSeverityId])
    );
END;
