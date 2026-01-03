CREATE TABLE [integration].[DataSource] (
    [DataSourceId] INT            IDENTITY (1, 1) NOT NULL,
    [ConnectionId] INT            NOT NULL,
    [Name]         VARCHAR (100)  NOT NULL,
    [Namespace]    VARCHAR (100)  NOT NULL,
    [Type]         VARCHAR (30)   NULL,
    [Description]  NVARCHAR (200) NULL,
    [IsActive]     BIT            DEFAULT ((1)) NOT NULL,
    CONSTRAINT [PK_integration_DataSource] PRIMARY KEY CLUSTERED ([DataSourceId] ASC),
    CONSTRAINT [UC_integration_DataSource] UNIQUE NONCLUSTERED ([ConnectionId] ASC, [Name] ASC, [Type] ASC)
);


GO

