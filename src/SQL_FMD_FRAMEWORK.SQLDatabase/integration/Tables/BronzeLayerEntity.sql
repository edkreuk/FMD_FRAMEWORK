CREATE TABLE [integration].[BronzeLayerEntity] (
    [BronzeLayerEntityId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [LandingzoneEntityId] BIGINT         NOT NULL,
    [LakehouseId]         INT            NOT NULL,
    [Schema]              NVARCHAR (100) NOT NULL,
    [Name]                NVARCHAR (200) NOT NULL,
    [PrimaryKeys]         NVARCHAR (200) NOT NULL,
    [FileType]            NVARCHAR (20)  DEFAULT ('Delta') NOT NULL,
    [CleansingRules]      NVARCHAR (MAX) NULL,
    [IsActive]            BIT            DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_BronzeLayerEntity] PRIMARY KEY CLUSTERED ([BronzeLayerEntityId] ASC),
    CONSTRAINT [FK_BronzeLayerEntity_LandingzoneEntity] FOREIGN KEY ([LandingzoneEntityId]) REFERENCES [integration].[LandingzoneEntity] ([LandingzoneEntityId]),
    CONSTRAINT [UC_integration_BronzeLayerEntity] UNIQUE NONCLUSTERED ([LakehouseId] ASC, [Schema] ASC, [Name] ASC)
);


GO

