CREATE TABLE [execution].[PipelineBronzeLayerEntity] (
    [PipelineBronzeLayerEntityId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [BronzeLayerEntityId]         BIGINT         NOT NULL,
    [TableName]                   NVARCHAR (300) NOT NULL,
    [SchemaName]                  NVARCHAR (MAX) NOT NULL,
    [InsertDateTime]              DATETIME       NULL,
    [IsProcessed]                 BIT            NOT NULL,
    [LoadEndDateTime]             DATETIME       NULL,
    CONSTRAINT [PK_execution_PipelineBronzeLayerEntity] PRIMARY KEY CLUSTERED ([PipelineBronzeLayerEntityId] ASC)
);


GO

