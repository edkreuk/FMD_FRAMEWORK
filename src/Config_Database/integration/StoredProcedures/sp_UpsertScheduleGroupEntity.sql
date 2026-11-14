CREATE PROCEDURE [integration].[sp_UpsertScheduleGroupEntity]
    @LandingzoneEntityId BIGINT,
    @ScheduleGroupName   NVARCHAR (100)
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ScheduleGroupId INT;

    IF NOT EXISTS (
        SELECT 1
        FROM [integration].[LandingzoneEntity]
        WHERE [LandingzoneEntityId] = @LandingzoneEntityId
    )
        THROW 50001, 'The landing-zone entity does not exist.', 1;

    SELECT @ScheduleGroupId = [ScheduleGroupId]
    FROM [integration].[ScheduleGroup]
    WHERE [ScheduleGroupName] = @ScheduleGroupName;

    IF @ScheduleGroupId IS NULL
        THROW 50003, 'The schedule group does not exist. Create it with integration.sp_UpsertScheduleGroup first.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF NOT EXISTS (
            SELECT 1
            FROM [integration].[ScheduleGroupEntity] WITH (UPDLOCK, HOLDLOCK)
            WHERE [ScheduleGroupId] = @ScheduleGroupId
                AND [LandingzoneEntityId] = @LandingzoneEntityId
        )
            INSERT INTO [integration].[ScheduleGroupEntity] ([ScheduleGroupId], [LandingzoneEntityId])
            VALUES (@ScheduleGroupId, @LandingzoneEntityId);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;

    SELECT [ScheduleGroupId], [LandingzoneEntityId]
    FROM [integration].[ScheduleGroupEntity]
    WHERE [ScheduleGroupId] = @ScheduleGroupId
        AND [LandingzoneEntityId] = @LandingzoneEntityId;
END;

GO
