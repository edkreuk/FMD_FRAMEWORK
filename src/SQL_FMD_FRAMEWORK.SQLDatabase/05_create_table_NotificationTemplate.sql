-- =============================================================================
-- Table: integration.NotificationTemplate
-- Description: Lookup table for reusable notification templates
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'NotificationTemplate'
)
BEGIN
    CREATE TABLE [integration].[NotificationTemplate] (
        [NotificationTemplateId]  INT            NOT NULL IDENTITY(1,1),
        [Name]                    NVARCHAR(200)  NOT NULL,
        [Subject]                 NVARCHAR(500)  NULL,
        [Body]                    NVARCHAR(MAX)  NULL,
        [IsActive]                BIT            NOT NULL,

        CONSTRAINT [PK_integration_NotificationTemplate] PRIMARY KEY ([NotificationTemplateId])
    );
END;
