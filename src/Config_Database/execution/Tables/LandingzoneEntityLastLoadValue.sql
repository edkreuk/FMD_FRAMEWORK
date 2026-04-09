CREATE TABLE [execution].[LandingzoneEntityLastLoadValue] (
    [LandingzoneEntityValueId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [LandingzoneEntityId]      BIGINT        NULL,
    [LoadValue]                VARCHAR (50)  NULL,
    [LastLoadDatetime]         DATETIME2 (7) NULL,
    CONSTRAINT [PK_execution_Source_LastLoadValue] PRIMARY KEY CLUSTERED ([LandingzoneEntityValueId] ASC),
    CONSTRAINT [UC_execution_LandingzoneEntityLastLoadValue_Guid] UNIQUE NONCLUSTERED ([LandingzoneEntityId] ASC)
);


GO

