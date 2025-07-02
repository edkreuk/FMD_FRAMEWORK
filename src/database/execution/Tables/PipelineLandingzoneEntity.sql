CREATE TABLE [execution].[PipelineLandingzoneEntity] (
    [PipelineLandingzoneEntityId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [LandingzoneEntityId]         BIGINT         NOT NULL,
    [FilePath]                    NVARCHAR (300) NOT NULL,
    [FileName]                    NVARCHAR (MAX) NOT NULL,
    [InsertDateTime]              DATETIME       NULL,
    [IsProcessed]                 BIT            NOT NULL,
    [LoadEndDateTime]             DATETIME       NULL,
    CONSTRAINT [PK_execution_PipelineLandingzoneEntity] PRIMARY KEY CLUSTERED ([PipelineLandingzoneEntityId] ASC)
);


GO

