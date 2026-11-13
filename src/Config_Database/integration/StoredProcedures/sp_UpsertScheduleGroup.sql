CREATE PROCEDURE [integration].[sp_UpsertScheduleGroup]
    @ScheduleGroupName NVARCHAR (100),
    @ScheduleGroupId   INT = NULL
WITH EXECUTE AS CALLER
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(LTRIM(RTRIM(@ScheduleGroupName)), N'') IS NULL
        THROW 50002, 'ScheduleGroupName is required.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        IF @ScheduleGroupId IS NULL
        BEGIN
            SELECT @ScheduleGroupId = [ScheduleGroupId]
            FROM [integration].[ScheduleGroup] WITH (UPDLOCK, HOLDLOCK)
            WHERE [ScheduleGroupName] = @ScheduleGroupName;

            IF @ScheduleGroupId IS NULL
            BEGIN
                INSERT INTO [integration].[ScheduleGroup] ([ScheduleGroupName])
                VALUES (@ScheduleGroupName);

                SET @ScheduleGroupId = CONVERT(INT, SCOPE_IDENTITY());
            END;
        END
        ELSE
        BEGIN
            DECLARE @CurrentName NVARCHAR (100);

            SELECT @CurrentName = [ScheduleGroupName]
            FROM [integration].[ScheduleGroup] WITH (UPDLOCK, HOLDLOCK)
            WHERE [ScheduleGroupId] = @ScheduleGroupId;

            IF @CurrentName IS NULL
                THROW 50003, 'The schedule group does not exist.', 1;

            -- New landing-zone entities are always assigned to the group named Default.
            IF @CurrentName = N'Default' AND @ScheduleGroupName <> N'Default'
                THROW 50004, 'The Default schedule group cannot be renamed.', 1;

            IF EXISTS (
                SELECT 1
                FROM [integration].[ScheduleGroup] WITH (UPDLOCK, HOLDLOCK)
                WHERE [ScheduleGroupName] = @ScheduleGroupName
                    AND [ScheduleGroupId] <> @ScheduleGroupId
            )
                THROW 50005, 'Another schedule group already uses this name.', 1;

            UPDATE [integration].[ScheduleGroup]
            SET [ScheduleGroupName] = @ScheduleGroupName
            WHERE [ScheduleGroupId] = @ScheduleGroupId;
        END;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;

    SELECT [ScheduleGroupId], [ScheduleGroupName]
    FROM [integration].[ScheduleGroup]
    WHERE [ScheduleGroupId] = @ScheduleGroupId;
END;

GO
