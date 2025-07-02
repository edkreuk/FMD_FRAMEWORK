    CREATE    PROCEDURE [execution].[sp_UpsertLandingZoneEntityLastLoadValue] (
        @LandingzoneEntityId BIGINT,
        @LastLoadValue VARCHAR(50)
        )
        WITH EXECUTE AS CALLER
    AS
    BEGIN
        SET NOCOUNT ON
                                                                            
        IF EXISTS (
                SELECT 1
                FROM [execution].[LandingzoneEntityLastLoadValue]
                WHERE [LandingzoneEntityId] = @LandingzoneEntityId
                )
        BEGIN
            UPDATE x
            SET [LoadValue] = @LastLoadValue,
                LastLoadDatetime = CONVERT(DATETIME2(7), GETDATE())
            FROM [execution].[LandingzoneEntityLastLoadValue] x
            WHERE [LandingzoneEntityId] = @LandingzoneEntityId
        END                                     
        ELSE
        BEGIN
            INSERT INTO [execution].[LandingzoneEntityLastLoadValue] (
                [LandingzoneEntityId],
                [LoadValue],
                LastLoadDatetime
                )
            VALUES (                             
                @LandingzoneEntityId,
                @LastLoadValue,
                getdate()
                )
        END
    --Output for Fabric Pipeline
    SELECT @LandingzoneEntityId AS LandingzoneEntityId, 
            @LastLoadValue as LastLoadValue;
        SET NOCOUNT OFF;
    END

GO

