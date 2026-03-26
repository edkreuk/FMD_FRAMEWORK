-- =============================================================================
-- Table: logging.dq_logging
-- Description: Execution log for data quality check runs
-- Dependencies: integration.SilverLayerEntityDQCheck, integration.Rules
-- =============================================================================

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'logging' AND t.name = 'dq_logging'
)
BEGIN
    CREATE TABLE [logging].[dq_logging] (
        [DQLogId]                      BIGINT             NOT NULL IDENTITY(1,1),
        [SilverLayerEntityDQCheckId]   BIGINT             NOT NULL,
        [RuleId]                       BIGINT             NULL,
        [LogDateTime]                  DATETIME2(6)       NULL     DEFAULT SYSDATETIME(),
        [Status]                       NVARCHAR(50)       NULL     CHECK ([Status] IN ('Running', 'Success', 'Failed', 'Warning')),
        [RecordsChecked]               INT                NULL,
        [RecordsFailed]                INT                NULL,
        [ErrorMessage]                 NVARCHAR(MAX)      NULL,
        [PipelineRunGuid]              UNIQUEIDENTIFIER   NULL,

        CONSTRAINT [PK_logging_dq_logging] PRIMARY KEY ([DQLogId]),

        CONSTRAINT [FK_dq_logging_SilverLayerEntityDQCheck]
            FOREIGN KEY ([SilverLayerEntityDQCheckId])
            REFERENCES [integration].[SilverLayerEntityDQCheck] ([SilverLayerEntityDQCheckId]),

        CONSTRAINT [FK_dq_logging_Rules]
            FOREIGN KEY ([RuleId])
            REFERENCES [integration].[Rules] ([RuleId])
    );
END;
