-- =============================================================================
-- Table: integration.Notifications
-- Description: Notification configuration per DQ check
-- Dependencies: integration.SilverLayerEntityDQCheck,
--               integration.NotificationTemplate
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'integration' AND t.name = 'Notifications'
)
BEGIN
    CREATE TABLE [integration].[Notifications] (
        [NotificationId]               BIGINT         NOT NULL IDENTITY(1,1),
        [SilverLayerEntityDQCheckId]   BIGINT         NOT NULL,
        [NotificationTemplateId]       INT            NOT NULL,
        [Recipient]                    NVARCHAR(500)  NULL,
        [IsActive]                     BIT            NOT NULL,

        CONSTRAINT [PK_integration_Notifications] PRIMARY KEY ([NotificationId]),

        CONSTRAINT [FK_Notifications_SilverLayerEntityDQCheck]
            FOREIGN KEY ([SilverLayerEntityDQCheckId])
            REFERENCES [integration].[SilverLayerEntityDQCheck] ([SilverLayerEntityDQCheckId]),

        CONSTRAINT [FK_Notifications_NotificationTemplate]
            FOREIGN KEY ([NotificationTemplateId])
            REFERENCES [integration].[NotificationTemplate] ([NotificationTemplateId])
    );
END;
