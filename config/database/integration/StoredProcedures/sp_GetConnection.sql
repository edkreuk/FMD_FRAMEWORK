





    CREATE     PROCEDURE [integration].[sp_GetConnection](
       @ConnectionGuid uniqueidentifier
    
    )
    WITH EXECUTE AS CALLER
    AS 

    SET NOCOUNT ON; 

    declare @ConnectionId int
    BEGIN
        set @ConnectionId= ( select isnull(ConnectionId,0) as ConnectionId  from   [integration].[Connection] where [ConnectionGuid] = @ConnectionGuid)
    END

            SELECT @ConnectionId AS ConnectionId

GO

