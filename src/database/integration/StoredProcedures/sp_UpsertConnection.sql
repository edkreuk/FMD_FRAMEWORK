


    CREATE    PROCEDURE [integration].[sp_UpsertConnection](
        @ConnectionGuid UNIQUEIDENTIFIER
        ,@Name nvarchar(200) 
        ,@Type nvarchar(50) 
        ,@IsActive bit 

    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    IF NOT EXISTS(SELECT  1
                FROM [integration].[Connection]
                WHERE [ConnectionGuid] = @ConnectionGuid)
    BEGIN
        INSERT INTO [integration].[Connection]
            ([ConnectionGuid]
            ,[Name]
            ,[Type]
            ,[IsActive]
    )
        VALUES (
            @ConnectionGuid

            ,@Name
            ,@Type

            ,@IsActive
    );
    END
    ELSE
    BEGIN
        UPDATE [integration].[Connection]
        SET [Name] = @Name
            ,[Type] = @Type
        
            ,[IsActive] = @IsActive
    
        WHERE [ConnectionGuid] = @ConnectionGuid  
    END

GO

