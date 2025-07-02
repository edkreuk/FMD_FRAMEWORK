CREATE TABLE [integration].[SilverLayerEntity] (
    [SilverLayerEntityId] BIGINT         IDENTITY (1, 1) NOT NULL,
    [BronzeLayerEntityId] BIGINT         NOT NULL,
    [LakehouseId]         INT            NOT NULL,
    [Schema]              NVARCHAR (100) NULL,
    [Name]                NVARCHAR (200) NULL,
    [FileType]            NVARCHAR (20)  DEFAULT ('Delta') NOT NULL,
    [CleansingRules]      NVARCHAR (MAX) NULL,
    [IsActive]            BIT            DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_SilverLayerEntity] PRIMARY KEY CLUSTERED ([SilverLayerEntityId] ASC),
    CONSTRAINT [FK_SilverLayerEntity_BronzeLayerEntityId] FOREIGN KEY ([BronzeLayerEntityId]) REFERENCES [integration].[BronzeLayerEntity] ([BronzeLayerEntityId]),
    CONSTRAINT [UC_integration_BSilverLayerEntity] UNIQUE NONCLUSTERED ([LakehouseId] ASC, [Schema] ASC, [Name] ASC)
);


GO

