CREATE TABLE [integration].[ScheduleGroup] (
    [ScheduleGroupId]   INT            IDENTITY (1, 1) NOT NULL,
    [ScheduleGroupName] NVARCHAR (100) NOT NULL,
    CONSTRAINT [PK_integration_ScheduleGroup] PRIMARY KEY CLUSTERED ([ScheduleGroupId] ASC),
    CONSTRAINT [UC_integration_ScheduleGroup] UNIQUE NONCLUSTERED ([ScheduleGroupName] ASC)
);

GO
