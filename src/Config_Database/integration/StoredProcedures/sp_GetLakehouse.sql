
CREATE   PROCEDURE [integration].[sp_GetLakehouse](
    @WorkspaceGuid UNIQUEIDENTIFIER,
    @Name NVARCHAR(100),
    @LakehouseId INT OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;
    SELECT @LakehouseId = LakehouseId
    FROM [integration].[Lakehouse]
    WHERE [Name] = @Name AND [WorkspaceGuid] = @WorkspaceGuid
END

GO

