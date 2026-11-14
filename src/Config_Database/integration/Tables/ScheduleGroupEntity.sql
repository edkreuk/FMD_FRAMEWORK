CREATE TABLE [integration].[ScheduleGroupEntity] (
    [ScheduleGroupId]     INT    NOT NULL,
    [LandingzoneEntityId] BIGINT NOT NULL,
    CONSTRAINT [PK_integration_ScheduleGroupEntity] PRIMARY KEY CLUSTERED ([ScheduleGroupId] ASC, [LandingzoneEntityId] ASC),
    CONSTRAINT [FK_ScheduleGroupEntity_ScheduleGroup] FOREIGN KEY ([ScheduleGroupId]) REFERENCES [integration].[ScheduleGroup] ([ScheduleGroupId]),
    CONSTRAINT [FK_ScheduleGroupEntity_LandingzoneEntity] FOREIGN KEY ([LandingzoneEntityId]) REFERENCES [integration].[LandingzoneEntity] ([LandingzoneEntityId])
);

GO

CREATE NONCLUSTERED INDEX [IX_ScheduleGroupEntity_LandingzoneEntityId]
    ON [integration].[ScheduleGroupEntity] ([LandingzoneEntityId] ASC);

GO
