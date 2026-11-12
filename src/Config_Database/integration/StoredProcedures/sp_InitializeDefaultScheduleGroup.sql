CREATE PROCEDURE [integration].[sp_InitializeDefaultScheduleGroup]
    @LandingzoneEntityId BIGINT = NULL
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF @LandingzoneEntityId IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM [integration].[LandingzoneEntity]
                WHERE [LandingzoneEntityId] = @LandingzoneEntityId
            )
            THROW 50001, 'The landing-zone entity does not exist.', 1;

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
        FROM [integration].[LandingzoneEntity] LZE
        WHERE (@LandingzoneEntityId IS NULL OR LZE.[LandingzoneEntityId] = @LandingzoneEntityId)
            AND NOT EXISTS (
                SELECT 1
                FROM [integration].[ScheduleGroupEntity] SGE WITH (UPDLOCK, HOLDLOCK)
                WHERE SGE.[ScheduleGroupId] = @ScheduleGroupId
                    AND SGE.[LandingzoneEntityId] = LZE.[LandingzoneEntityId]
            );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;

GO
