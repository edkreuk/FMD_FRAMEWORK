CREATE TRIGGER [integration].[tr_LandingzoneEntity_DefaultScheduleGroup]
ON [integration].[LandingzoneEntity]
WITH EXECUTE AS CALLER
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM inserted)
        RETURN;

    DECLARE @ScheduleGroupId INT;

    SELECT @ScheduleGroupId = [ScheduleGroupId]
    FROM [integration].[ScheduleGroup] WITH (UPDLOCK, HOLDLOCK)
    WHERE [ScheduleGroupName] = N'Default';

    IF @ScheduleGroupId IS NULL
    BEGIN
        INSERT INTO [integration].[ScheduleGroup] ([ScheduleGroupName])
        VALUES (N'Default');

        SET @ScheduleGroupId = CONVERT(INT, SCOPE_IDENTITY());
    END;

    INSERT INTO [integration].[ScheduleGroupEntity] ([ScheduleGroupId], [LandingzoneEntityId])
    SELECT @ScheduleGroupId, LZE.[LandingzoneEntityId]
    FROM inserted LZE
    WHERE NOT EXISTS (
        SELECT 1
        FROM [integration].[ScheduleGroupEntity] SGE WITH (UPDLOCK, HOLDLOCK)
        WHERE SGE.[ScheduleGroupId] = @ScheduleGroupId
            AND SGE.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
    );
END;

GO
