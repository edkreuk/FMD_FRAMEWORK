CREATE TABLE [integration].[LandingzoneEntity] (
    [LandingzoneEntityId] BIGINT          IDENTITY (1, 1) NOT NULL,
    [DataSourceId]        INT             NOT NULL,
    [LakehouseId]         INT             NOT NULL,
    [SourceSchema]        NVARCHAR (100)  NULL,
    [SourceName]          NVARCHAR (200)  NOT NULL,
    [FileName]            NVARCHAR (200)  NOT NULL,
    [FileType]            NVARCHAR (20)   NOT NULL,
    [FilePath]            NVARCHAR (500)  NOT NULL,
    [IsIncremental]       BIT             DEFAULT ((0)) NOT NULL,
    [IsIncrementalColumn] NVARCHAR (50)   NULL,
    [IsActive]            BIT             DEFAULT ((1)) NOT NULL,
    [SourceCustomSelect]  NVARCHAR (4000) NULL,
    CONSTRAINT [PK_integration_LandingzoneEntity] PRIMARY KEY CLUSTERED ([LandingzoneEntityId] ASC),
    CONSTRAINT [UC_integration_LandingzoneEntity] UNIQUE NONCLUSTERED ([SourceSchema] ASC, [SourceName] ASC, [DataSourceId] ASC)
);


GO

